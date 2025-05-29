from __future__ import annotations

import asyncio
import inspect
import json
import logging
import uuid
import warnings
from asyncio import Task
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from types import ModuleType
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional, ParamSpec, Set, Type, TypeVar, cast

from opentelemetry.trace import TracerProvider
from timeplus_messaging.consumer import SingleTopicConsumer
from timeplus_messaging.producer import TimeplusLogProducer

from ._agent import Agent
from ._agent_id import AgentId
from ._agent_instantiation import AgentInstantiationContext
from ._agent_metadata import AgentMetadata
from ._agent_runtime import AgentRuntime
from ._agent_type import AgentType
from ._cancellation_token import CancellationToken
from ._intervention import InterventionHandler
from ._message_context import MessageContext
from ._message_handler_context import MessageHandlerContext
from ._runtime_impl_helpers import SubscriptionManager, get_impl
from ._serialization import JSON_DATA_CONTENT_TYPE, MessageSerializer, SerializationRegistry
from ._subscription import Subscription
from ._telemetry import EnvelopeMetadata, MessageRuntimeTracingConfig, TraceHelper, get_telemetry_envelope_metadata
from ._topic import TopicId
from .logging import (
    AgentConstructionExceptionEvent,
    DeliveryStage,
    MessageEvent,
    MessageHandlerExceptionEvent,
    MessageKind,
)

logger = logging.getLogger("autogen_core")
event_logger = logging.getLogger("autogen_core.events")

# We use a type parameter in some functions which shadows the built-in `type` function.
# This is a workaround to avoid shadowing the built-in `type` function.
type_func_alias = type


@dataclass
class PendingRequest:
    """Tracks pending RPC requests waiting for responses"""

    future: asyncio.Future[Any]
    original_sender: AgentId | None
    timeout_task: asyncio.Task | None = None


@dataclass(kw_only=True)
class TimeplusMessageEnvelope:
    message: Any
    sender: Optional[AgentId]
    topic_id: TopicId
    recipient: AgentId | None
    metadata: Optional[EnvelopeMetadata] = None
    message_id: str
    is_response: bool = False  # Mark response messages
    is_error: bool = False  # Mark error responses

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)

        if self.sender:
            data["sender"] = str(self.sender)

        if self.topic_id:
            data["topic_id"] = str(self.topic_id)

        if self.recipient:
            data["recipient"] = str(self.recipient)

        # CRITICAL FIX: Improve message serialization
        if hasattr(self.message, "__dict__"):
            data["message"] = {
                "_type": type(self.message).__name__,
                "_module": type(self.message).__module__,
                "_content": self.message.__dict__,
            }
        elif isinstance(self.message, (str, int, float, bool, list, dict, type(None))):
            data["message"] = self.message
        else:
            data["message"] = str(self.message)
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TimeplusMessageEnvelope:
        """Create an envelope from a dictionary."""
        if "sender" in data and data["sender"]:
            data["sender"] = AgentId.from_str(cast(str, data["sender"]))

        if "topic_id" in data and data["topic_id"]:
            data["topic_id"] = TopicId.from_str(cast(str, data["topic_id"]))

        if "recipient" in data and data["recipient"]:
            data["recipient"] = AgentId.from_str(cast(str, data["recipient"]))

        message: dict | None = data.get("message")

        if isinstance(message, dict) and "_type" in message and "_content" in message:
            try:
                module_name = cast(str, message.get("_module", "__main__"))
                class_name = cast(str, message["_type"])
                content = cast(Dict[str, Any], message["_content"])

                module = cast(ModuleType, __import__(module_name, fromlist=["*"]))
                cls_type = cast(Type[Any], getattr(module, class_name))

                # CRITICAL FIX: Use proper object construction
                try:
                    # Try to create object with the content as kwargs
                    obj = cls_type(**content)
                except TypeError:
                    # Fallback: create empty object and set attributes
                    obj = cls_type.__new__(cls_type)
                    # Call __init__ if it exists and can be called without args
                    if hasattr(obj, "__init__"):
                        try:
                            obj.__init__()
                        except TypeError:
                            pass  # __init__ requires arguments we don't have

                    # Set the attributes
                    for key, value in content.items():
                        setattr(obj, key, value)

                data["message"] = obj

            except (ImportError, AttributeError, TypeError, ValueError):
                data["message"] = message["_content"]
        else:
            logger.debug(f"🔍 Message is not structured, keeping as-is: {message}")

        # Handle missing fields for backward compatibility
        data.setdefault("is_response", False)
        data.setdefault("is_error", False)

        result = cls(**data)
        return result


P = ParamSpec("P")
T = TypeVar("T", bound=Agent)


class RunContext:
    def __init__(self, runtime: TimeplusAgentRuntime) -> None:
        self._runtime = runtime
        self._run_task = asyncio.create_task(self._run())
        self._stopped = asyncio.Event()
        self._ready = asyncio.Event()

    async def _run(self) -> None:
        self._ready.set()
        while True:
            if self._stopped.is_set():
                return

            await self._runtime._process_next()  # type: ignore

    async def stop(self) -> None:
        self._stopped.set()
        await self._run_task

    async def stop_when_idle(self) -> None:
        # self._stopped.set()
        # TODO: implement idle check? maybe just long running forever utile stopped by user
        await self._run_task

    async def stop_when(self, condition: Callable[[], bool], check_period: float = 1.0) -> None:
        async def check_condition() -> None:
            while not condition():
                await asyncio.sleep(check_period)
            await self.stop()

        await asyncio.create_task(check_condition())

    async def wait_until_ready(self):
        await self._ready.wait()


class TimeplusAgentRuntime(AgentRuntime):
    """ """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8463,
        user: str = "default",
        password: str = "",
        database: str = "default",
        *,
        intervention_handlers: List[InterventionHandler] | None = None,
        tracer_provider: TracerProvider | None = None,
        ignore_unhandled_exceptions: bool = True,
    ) -> None:
        self._tracer_helper = TraceHelper(tracer_provider, MessageRuntimeTracingConfig("SingleThreadedAgentRuntime"))

        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database

        # generate a unique topic name if not provided
        unique_topic_name = f"autogen_runtime_{uuid.uuid4()}".replace("-", "_")
        self._runtime_topic = unique_topic_name

        self._topic_producer = TimeplusLogProducer(
            host=self._host, port=self._port, user=self._user, password=self._password, database=self._database
        )

        self._topic_producer._ensure_stream_exists(self._runtime_topic)

        self._consumer = SingleTopicConsumer(
            self._runtime_topic,
            host=self._host,
            port=self._port,
            group_id="test_group",
            user=self._user,
            password=self._password,
            database=self._database,
            auto_offset_reset="earliest",
        )

        # (namespace, type) -> List[AgentId]
        self._agent_factories: Dict[
            str, Callable[[], Agent | Awaitable[Agent]] | Callable[[AgentRuntime, AgentId], Agent | Awaitable[Agent]]
        ] = {}
        self._instantiated_agents: Dict[AgentId, Agent] = {}
        self._intervention_handlers = intervention_handlers
        self._background_tasks: Set[Task[Any]] = set()
        self._run_context: RunContext | None = None
        self._subscription_manager = SubscriptionManager()
        self._serialization_registry = SerializationRegistry()
        self._ignore_unhandled_handler_exceptions = ignore_unhandled_exceptions
        self._background_exception: BaseException | None = None
        self._agent_instance_types: Dict[str, Type[Agent]] = {}

        self._pending_requests: Dict[str, PendingRequest] = {}
        self._request_timeout = 30.0  # seconds

    @property
    def unprocessed_messages_count(
        self,
    ) -> int:
        return 0  # TODO: implement this

    @property
    def _known_agent_names(self) -> Set[str]:
        return set(self._agent_factories.keys())

    async def _create_otel_attributes(
        self,
        sender_agent_id: AgentId | None = None,
        recipient_agent_id: AgentId | None = None,
        message_context: MessageContext | None = None,
        message: Any = None,
    ) -> Mapping[str, str]:
        """Create OpenTelemetry attributes for the given agent and message.

        Args:
            sender_agent (Agent, optional): The sender agent instance.
            recipient_agent (Agent, optional): The recipient agent instance.
            message (Any): The message instance.

        Returns:
            Attributes: A dictionary of OpenTelemetry attributes.
        """
        if not sender_agent_id and not recipient_agent_id and not message:
            return {}
        attributes: Dict[str, str] = {}
        if sender_agent_id:
            sender_agent = await self._get_agent(sender_agent_id)
            attributes["sender_agent_type"] = sender_agent.id.type
            attributes["sender_agent_class"] = sender_agent.__class__.__name__
        if recipient_agent_id:
            recipient_agent = await self._get_agent(recipient_agent_id)
            attributes["recipient_agent_type"] = recipient_agent.id.type
            attributes["recipient_agent_class"] = recipient_agent.__class__.__name__

        if message_context:
            serialized_message_context = {
                "sender": str(message_context.sender),
                "topic_id": str(message_context.topic_id),
                "is_rpc": message_context.is_rpc,
                "message_id": message_context.message_id,
            }
            attributes["message_context"] = json.dumps(serialized_message_context)

        if message:
            try:
                serialized_message = self._try_serialize(message)
            except Exception as e:
                serialized_message = str(e)
        else:
            serialized_message = "No Message"
        attributes["message"] = serialized_message

        return attributes

    # Returns the response of the message
    async def send_message(
        self,
        message: Any,
        recipient: AgentId,
        *,
        sender: AgentId | None = None,
        cancellation_token: CancellationToken | None = None,
        message_id: str | None = None,
    ) -> Any:
        logger.debug(f"send_message from  recipient {recipient}: {message}")
        if message_id is None:
            message_id = str(uuid.uuid4())

        # Create future for the response
        future = asyncio.get_event_loop().create_future()

        # Set up timeout
        timeout_task = asyncio.create_task(self._timeout_request(message_id, self._request_timeout))

        # Store pending request
        self._pending_requests[message_id] = PendingRequest(
            future=future, original_sender=sender, timeout_task=timeout_task
        )

        logger.debug(f"all pending request {self._pending_requests}")

        event_logger.info(
            MessageEvent(
                payload=self._try_serialize(message),
                sender=sender,
                receiver=recipient,
                kind=MessageKind.DIRECT,
                delivery_stage=DeliveryStage.SEND,
            )
        )

        envelope = TimeplusMessageEnvelope(
            message=message,
            sender=sender,
            topic_id=None,
            recipient=recipient,
            metadata=get_telemetry_envelope_metadata(),
            message_id=message_id,
        )

        # Publish to Timeplus
        producer = TimeplusLogProducer(
            host=self._host, port=self._port, user=self._user, password=self._password, database=self._database
        )
        producer.send(
            topic=self._runtime_topic,
            value=json.dumps(envelope.to_dict()),
            key=message_id,
        )
        producer.flush()
        producer.close()

        # Link cancellation token if provided
        if cancellation_token:
            cancellation_token.link_future(future)

        logger.debug(f"wait response from  recipient {recipient}")

        response = await future

        logger.debug(f"got response from  recipient {recipient}")
        # Wait for response
        return response

    async def _timeout_request(self, message_id: str, timeout: float) -> None:
        """Handle request timeout"""
        await asyncio.sleep(timeout)

        if message_id in self._pending_requests:
            pending = self._pending_requests.pop(message_id)
            if not pending.future.done():
                pending.future.set_exception(TimeoutError(f"Request {message_id} timed out after {timeout} seconds"))

    async def publish_message(
        self,
        message: Any,
        topic_id: TopicId,
        *,
        sender: AgentId | None = None,
        cancellation_token: CancellationToken | None = None,
        message_id: str | None = None,
    ) -> None:
        logger.debug(f"publish message to {topic_id} from {sender}")

        with self._tracer_helper.trace_block(
            "create",
            topic_id,
            parent=None,
            extraAttributes={"message_type": type(message).__name__},
        ):
            if cancellation_token is None:
                cancellation_token = CancellationToken()
            content = message.__dict__ if hasattr(message, "__dict__") else message
            logger.info(f"Publishing message of type {type(message).__name__} to all subscribers: {content}")

            if message_id is None:
                message_id = str(uuid.uuid4())

            event_logger.info(
                MessageEvent(
                    payload=self._try_serialize(message),
                    sender=sender,
                    receiver=topic_id,
                    kind=MessageKind.PUBLISH,
                    delivery_stage=DeliveryStage.SEND,
                )
            )

            envelope = TimeplusMessageEnvelope(
                message=message,
                sender=sender,
                topic_id=topic_id,
                recipient=None,
                metadata=get_telemetry_envelope_metadata(),
                message_id=message_id,
            )

            # TODO: publish message to timeplus topic
            producer = TimeplusLogProducer(
                host=self._host, port=self._port, user=self._user, password=self._password, database=self._database
            )
            producer.send(
                topic=self._runtime_topic,
                value=json.dumps(envelope.to_dict()),
                key=message_id,
            )

            producer.flush()
            producer.close()

    async def save_state(self) -> Mapping[str, Any]:
        """Save the state of all instantiated agents.

        This method calls the :meth:`~autogen_core.BaseAgent.save_state` method on each agent and returns a dictionary
        mapping agent IDs to their state.

        .. note::
            This method does not currently save the subscription state. We will add this in the future.

        Returns:
            A dictionary mapping agent IDs to their state.

        """
        state: Dict[str, Dict[str, Any]] = {}
        for agent_id in self._instantiated_agents:
            state[str(agent_id)] = dict(await (await self._get_agent(agent_id)).save_state())
        return state

    async def load_state(self, state: Mapping[str, Any]) -> None:
        """Load the state of all instantiated agents.

        This method calls the :meth:`~autogen_core.BaseAgent.load_state` method on each agent with the state
        provided in the dictionary. The keys of the dictionary are the agent IDs, and the values are the state
        dictionaries returned by the :meth:`~autogen_core.BaseAgent.save_state` method.

        .. note::

            This method does not currently load the subscription state. We will add this in the future.

        """
        for agent_id_str in state:
            agent_id = AgentId.from_str(agent_id_str)
            if agent_id.type in self._known_agent_names:
                await (await self._get_agent(agent_id)).load_state(state[str(agent_id)])

    async def _process_send(self, envelope: TimeplusMessageEnvelope) -> None:
        logger.debug(f"process send envelope: {envelope}")
        print(f"process send envelope: {envelope}")
        recipient = envelope.recipient

        if recipient is None:
            logger.error(f"Recipient is None in _process_send for envelope: {envelope}")
            if envelope.sender:  # If there's an original sender, notify them of the error
                err = ValueError("Recipient was None for a direct message, cannot process.")
                await self._send_error_response(original_envelope=envelope, error=err)
            return

        try:
            sender_id = str(envelope.sender) if envelope.sender is not None else "Unknown"
            logger.info(
                f"Calling message handler for {recipient} with message type {type(envelope.message).__name__} sent by {sender_id}"
            )
            event_logger.info(
                MessageEvent(
                    payload=self._try_serialize(envelope.message),
                    sender=envelope.sender,
                    receiver=recipient,
                    kind=MessageKind.DIRECT,
                    delivery_stage=DeliveryStage.DELIVER,
                )
            )

            # Ensure agent type exists before trying to get/create agent
            if recipient.type not in self._known_agent_names:  # _known_agent_names check
                raise LookupError(f"Agent type '{recipient.type}' does not exist.")

            recipient_agent = await self._get_agent(recipient)  # Can raise LookupError if agent_id not found by factory
            
            print(f"get recipient agent: {recipient_agent}")

            message_context = MessageContext(
                sender=envelope.sender,
                topic_id=None,  # Direct message, not via a topic subscription
                is_rpc=True,  # Direct send implies a request-response pattern
                message_id=envelope.message_id,
                cancellation_token=None,  # TODO: Consider how to propagate CancellationToken if needed
            )

            actual_response_data = None  # Initialize
            with self._tracer_helper.trace_block(
                "process",
                recipient_agent.id,
                parent=envelope.metadata,
                attributes=await self._create_otel_attributes(
                    sender_agent_id=envelope.sender,
                    recipient_agent_id=recipient,
                    message_context=message_context,
                    message=envelope.message,
                ),
            ):
                with MessageHandlerContext.populate_context(recipient_agent.id):  #
                    logger.debug(
                        f"recipient {recipient} ({recipient_agent}) for message: {envelope.message}, type: {type(envelope.message)}"
                    )
                    print(f"wait agent response: {recipient_agent} for message: {envelope.message}, type: {type(envelope.message)}")    
                    actual_response_data = await recipient_agent.on_message(
                        envelope.message,
                        ctx=message_context,
                    )
                    print(f"agent  response: {actual_response_data}")
                    logger.debug(f"get response from agent {recipient} ({recipient_agent}): {actual_response_data}")

            
            # If recipient_agent.on_message completed successfully:
            event_logger.info(
                MessageEvent(
                    payload=self._try_serialize(actual_response_data),
                    sender=envelope.recipient,  # The agent that handled the message is now the sender of the response
                    receiver=envelope.sender,  # The original sender is the recipient of the response
                    kind=MessageKind.RESPOND,
                    delivery_stage=DeliveryStage.SEND,
                )
            )
            logger.debug(
                f"################## sending successful response: {actual_response_data} for original envelope: {envelope}"
            )
            print(f"################## sending response: {actual_response_data} for original envelope: {envelope}") 
            await self._send_response(original_envelope=envelope, response=actual_response_data)

        except BaseException as e:
            # This catches exceptions from _get_agent, recipient_agent.on_message, or LookupError for unknown type
            logger.error(
                f"Exception in _process_send for recipient {recipient} processing envelope {envelope.message_id}: {e}",
                exc_info=True,
            )
            event_logger.info(
                MessageHandlerExceptionEvent(
                    payload=self._try_serialize(envelope.message),  # Original message payload
                    handling_agent=recipient,  # The agent that was supposed to handle
                    exception=e,
                )
            )
            # Send an error response back to the original sender
            logger.debug(
                f"################## sending error response for exception: {e} for original envelope: {envelope}"
            )
            await self._send_error_response(original_envelope=envelope, error=e)

    async def _send_response(self, original_envelope: TimeplusMessageEnvelope, response: Any) -> None:
        """Send a successful response back to the original sender"""
        if original_envelope.sender is None:
            return  # No one to respond to

        response_envelope = TimeplusMessageEnvelope(
            message=response,
            sender=original_envelope.recipient,
            topic_id=None,
            recipient=original_envelope.sender,
            metadata=get_telemetry_envelope_metadata(),
            message_id=original_envelope.message_id,  # Use same message_id for correlation
            is_response=True,  # Add this field to identify responses
        )

        logger.debug(f"###################### send response envelope: {response_envelope}")

        await self._publish_envelope(response_envelope)

    async def _send_error_response(self, original_envelope: TimeplusMessageEnvelope, error: BaseException) -> None:
        """Send an error response back to the original sender"""
        if original_envelope.sender is None:
            return

        # Create a serializable error representation
        error_response = {"error_type": type(error).__name__, "error_message": str(error), "is_error": True}

        response_envelope = TimeplusMessageEnvelope(
            message=error_response,
            sender=original_envelope.recipient,
            topic_id=None,
            recipient=original_envelope.sender,
            metadata=get_telemetry_envelope_metadata(),
            message_id=original_envelope.message_id,
            is_response=True,
            is_error=True,  # Add this field
        )

        await self._publish_envelope(response_envelope)

    async def _process_response(self, envelope: TimeplusMessageEnvelope) -> None:
        """Process a response message by resolving the corresponding future"""
        message_id = envelope.message_id

        logger.debug(f"Processing response for message_id: {message_id}")

        if message_id not in self._pending_requests:
            logger.warning(f"Received response for unknown request: {message_id}")
            return

        pending = self._pending_requests.pop(message_id)

        # Cancel timeout task
        if pending.timeout_task and not pending.timeout_task.done():
            pending.timeout_task.cancel()

        if pending.future.done():
            logger.warning(f"Received response for already completed request: {message_id}")
            return

        # Handle error responses (check the actual value, not attribute existence)
        if envelope.is_error:
            logger.debug(f"Processing error response for {message_id}")
            error_data = envelope.message
            if isinstance(error_data, dict) and error_data.get("is_error"):
                error_type = error_data.get("error_type", "RemoteError")
                error_message = error_data.get("error_message", "Unknown error")

                # Create appropriate exception
                if error_type == "LookupError":
                    exception = LookupError(error_message)
                elif error_type == "TimeoutError":
                    exception = TimeoutError(error_message)
                else:
                    exception = RuntimeError(f"{error_type}: {error_message}")

                pending.future.set_exception(exception)
            else:
                pending.future.set_exception(RuntimeError("Unknown error format"))
        else:
            # Successful response
            logger.debug(f"Processing successful response for {message_id}")
            pending.future.set_result(envelope.message)

        event_logger.info(
            MessageEvent(
                payload=self._try_serialize(envelope.message),
                sender=envelope.sender,
                receiver=envelope.recipient,
                kind=MessageKind.RESPOND,
                delivery_stage=DeliveryStage.DELIVER,
            )
        )

    async def _publish_envelope(self, envelope: TimeplusMessageEnvelope) -> None:
        """Helper method to publish an envelope to Timeplus"""
        producer = TimeplusLogProducer(
            host=self._host, port=self._port, user=self._user, password=self._password, database=self._database
        )
        producer.send(
            topic=self._runtime_topic,
            value=json.dumps(envelope.to_dict()),
            key=envelope.message_id,
        )
        producer.flush()
        producer.close()

    async def _process_publish(self, message_envelope: TimeplusMessageEnvelope) -> None:
        with self._tracer_helper.trace_block("publish", message_envelope.topic_id, parent=message_envelope.metadata):
            try:
                responses: List[Awaitable[Any]] = []
                recipients = await self._subscription_manager.get_subscribed_recipients(message_envelope.topic_id)
                for agent_id in recipients:
                    # Avoid sending the message back to the sender
                    if message_envelope.sender is not None and agent_id == message_envelope.sender:
                        continue

                    sender_agent = (
                        await self._get_agent(message_envelope.sender) if message_envelope.sender is not None else None
                    )
                    sender_name = str(sender_agent.id) if sender_agent is not None else "Unknown"
                    logger.info(
                        f"Calling message handler for {agent_id.type} with message type {type(message_envelope.message).__name__} published by {sender_name}"
                    )

                    event_logger.info(
                        MessageEvent(
                            payload=self._try_serialize(message_envelope.message),
                            sender=message_envelope.sender,
                            receiver=None,
                            kind=MessageKind.PUBLISH,
                            delivery_stage=DeliveryStage.DELIVER,
                        )
                    )
                    message_context = MessageContext(
                        sender=message_envelope.sender,
                        topic_id=message_envelope.topic_id,
                        is_rpc=False,
                        cancellation_token=None,
                        message_id=message_envelope.message_id,
                    )
                    agent = await self._get_agent(agent_id)

                    async def _on_message(agent: Agent, message_context: MessageContext) -> Any:
                        with self._tracer_helper.trace_block(
                            "process",
                            agent.id,
                            parent=message_envelope.metadata,
                            attributes=await self._create_otel_attributes(
                                sender_agent_id=message_envelope.sender,
                                recipient_agent_id=agent.id,
                                message_context=message_context,
                                message=message_envelope.message,
                            ),
                        ):
                            with MessageHandlerContext.populate_context(agent.id):
                                try:
                                    return await agent.on_message(
                                        message_envelope.message,
                                        ctx=message_context,
                                    )
                                except BaseException as e:
                                    logger.error(f"Error processing publish message for {agent.id}", exc_info=True)
                                    event_logger.info(
                                        MessageHandlerExceptionEvent(
                                            payload=self._try_serialize(message_envelope.message),
                                            handling_agent=agent.id,
                                            exception=e,
                                        )
                                    )
                                    raise e

                    response = await _on_message(agent, message_context)
                    responses.append(response)
            except BaseException as e:
                if not self._ignore_unhandled_handler_exceptions:
                    self._background_exception = e

    async def _process_next(self) -> None:
        """Enhanced process_next to handle responses"""

        if self._background_exception is not None:
            e = self._background_exception
            self._background_exception = None
            raise e

        try:
            records = self._consumer.poll(timeout_ms=10)

            if not records:
                return

            print(f"###################### get records: {records}")

            for _, messages in records.items():
                for message in messages:
                    envelope: TimeplusMessageEnvelope = TimeplusMessageEnvelope.from_dict(message.value)
                    logger.info(f"Processing envelope: {envelope}")

                    # Check if this is a response message (check the actual value, not just attribute existence)
                    task: Optional[asyncio.Task] = None
                    if envelope.is_response:
                        logger.info(f"Processing response message: {envelope.message_id}")
                        #await self._process_response(envelope)
                        task = asyncio.create_task(self._process_response(envelope))
                        
                    elif envelope.recipient is not None:
                        # Direct message (send)
                        logger.info(f"Processing direct message to {envelope.recipient}")
                        #await self._process_send(envelope)
                        task = asyncio.create_task(self._process_send(envelope))
                    else:
                        # Publish message (no specific recipient, has topic_id)
                        logger.info(f"Processing publish message to topic {envelope.topic_id}")
                        print(f"Processing publish message to topic {envelope.topic_id}")
                        task = asyncio.create_task(self._process_publish(envelope))
                        
                    if task is not None:
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)

        except Exception as e:
            logger.error(f"Error processing Timeplus message: {e}", exc_info=True)
            if not self._ignore_unhandled_handler_exceptions:
                self._background_exception = e

        await asyncio.sleep(0)

    def start(self) -> None:
        """Start the runtime message processing loop. This runs in a background task.

        Example:

        .. code-block:: python

            import asyncio
            from autogen_core import SingleThreadedAgentRuntime


            async def main() -> None:
                runtime = SingleThreadedAgentRuntime()
                runtime.start()

                # ... do other things ...

                await runtime.stop()


            asyncio.run(main())

        """
        if self._run_context is not None:
            raise RuntimeError("Runtime is already started")
        self._run_context = RunContext(self)

    async def close(self) -> None:
        """Enhanced close method to cleanup pending requests"""
        # Cancel all pending requests
        for _, pending in self._pending_requests.items():
            if pending.timeout_task and not pending.timeout_task.done():
                pending.timeout_task.cancel()
            if not pending.future.done():
                pending.future.set_exception(RuntimeError("Runtime is shutting down"))

        self._pending_requests.clear()

        # Call parent close
        await super().close()

    async def stop(self) -> None:
        """Immediately stop the runtime message processing loop. The currently processing message will be completed, but all others following it will be discarded."""
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")

        try:
            await self._run_context.stop()
        finally:
            self._run_context = None

    async def stop_when_idle(self) -> None:
        """Stop the runtime message processing loop when there is
        no outstanding message being processed or queued. This is the most common way to stop the runtime."""
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")

        try:
            await self._run_context.stop_when_idle()
        finally:
            self._run_context = None

    async def wait_until_ready(self) -> None:
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")

        await self._run_context.wait_until_ready()

    async def stop_when(self, condition: Callable[[], bool]) -> None:
        """Stop the runtime message processing loop when the condition is met.

        .. caution::

            This method is not recommended to be used, and is here for legacy
            reasons. It will spawn a busy loop to continually check the
            condition. It is much more efficient to call `stop_when_idle` or
            `stop` instead. If you need to stop the runtime based on a
            condition, consider using a background task and asyncio.Event to
            signal when the condition is met and the background task should call
            stop.

        """
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")
        await self._run_context.stop_when(condition)

        self._run_context = None

    async def agent_metadata(self, agent: AgentId) -> AgentMetadata:
        return (await self._get_agent(agent)).metadata

    async def agent_save_state(self, agent: AgentId) -> Mapping[str, Any]:
        return await (await self._get_agent(agent)).save_state()

    async def agent_load_state(self, agent: AgentId, state: Mapping[str, Any]) -> None:
        await (await self._get_agent(agent)).load_state(state)

    async def register_factory(
        self,
        type: str | AgentType,
        agent_factory: Callable[[], T | Awaitable[T]],
        *,
        expected_class: type[T] | None = None,
    ) -> AgentType:
        if isinstance(type, str):
            type = AgentType(type)

        if type.type in self._agent_factories:
            raise ValueError(f"Agent with type {type} already exists.")

        async def factory_wrapper() -> T:
            maybe_agent_instance = agent_factory()
            if inspect.isawaitable(maybe_agent_instance):
                agent_instance = await maybe_agent_instance
            else:
                agent_instance = maybe_agent_instance

            if expected_class is not None and type_func_alias(agent_instance) != expected_class:
                raise ValueError("Factory registered using the wrong type.")

            return agent_instance

        self._agent_factories[type.type] = factory_wrapper

        return type

    async def register_agent_instance(
        self,
        agent_instance: Agent,
        agent_id: AgentId,
    ) -> AgentId:
        def agent_factory() -> Agent:
            raise RuntimeError(
                "Agent factory was invoked for an agent instance that was not registered. This is likely due to the agent type being incorrectly subscribed to a topic. If this exception occurs when publishing a message to the DefaultTopicId, then it is likely that `skip_class_subscriptions` needs to be turned off when registering the agent."
            )

        if agent_id in self._instantiated_agents:
            raise ValueError(f"Agent with id {agent_id} already exists.")

        if agent_id.type not in self._agent_factories:
            self._agent_factories[agent_id.type] = agent_factory
            self._agent_instance_types[agent_id.type] = type_func_alias(agent_instance)
        else:
            if self._agent_factories[agent_id.type].__code__ != agent_factory.__code__:
                raise ValueError("Agent factories and agent instances cannot be registered to the same type.")
            if self._agent_instance_types[agent_id.type] != type_func_alias(agent_instance):
                raise ValueError("Agent instances must be the same object type.")

        await agent_instance.bind_id_and_runtime(id=agent_id, runtime=self)
        self._instantiated_agents[agent_id] = agent_instance
        return agent_id

    async def _invoke_agent_factory(
        self,
        agent_factory: Callable[[], T | Awaitable[T]] | Callable[[AgentRuntime, AgentId], T | Awaitable[T]],
        agent_id: AgentId,
    ) -> T:
        with AgentInstantiationContext.populate_context((self, agent_id)):
            try:
                if len(inspect.signature(agent_factory).parameters) == 0:
                    factory_one = cast(Callable[[], T], agent_factory)
                    agent = factory_one()
                elif len(inspect.signature(agent_factory).parameters) == 2:
                    warnings.warn(
                        "Agent factories that take two arguments are deprecated. Use AgentInstantiationContext instead. Two arg factories will be removed in a future version.",
                        stacklevel=2,
                    )
                    factory_two = cast(Callable[[AgentRuntime, AgentId], T], agent_factory)
                    agent = factory_two(self, agent_id)
                else:
                    raise ValueError("Agent factory must take 0 or 2 arguments.")

                if inspect.isawaitable(agent):
                    agent = cast(T, await agent)
                return agent

            except BaseException as e:
                event_logger.info(
                    AgentConstructionExceptionEvent(
                        agent_id=agent_id,
                        exception=e,
                    )
                )
                logger.error(f"Error constructing agent {agent_id}", exc_info=True)
                raise

    async def _get_agent(self, agent_id: AgentId) -> Agent:
        if agent_id in self._instantiated_agents:
            return self._instantiated_agents[agent_id]

        if agent_id.type not in self._agent_factories:
            raise LookupError(f"Agent with name {agent_id.type} not found.")

        agent_factory = self._agent_factories[agent_id.type]
        agent = await self._invoke_agent_factory(agent_factory, agent_id)
        self._instantiated_agents[agent_id] = agent
        return agent

    # TODO: uncomment out the following type ignore when this is fixed in mypy: https://github.com/python/mypy/issues/3737
    async def try_get_underlying_agent_instance(self, id: AgentId, type: Type[T] = Agent) -> T:  # type: ignore[assignment]
        if id.type not in self._agent_factories:
            raise LookupError(f"Agent with name {id.type} not found.")

        # TODO: check if remote
        agent_instance = await self._get_agent(id)

        if not isinstance(agent_instance, type):
            raise TypeError(
                f"Agent with name {id.type} is not of type {type.__name__}. It is of type {type_func_alias(agent_instance).__name__}"
            )

        return agent_instance

    async def add_subscription(self, subscription: Subscription) -> None:
        await self._subscription_manager.add_subscription(subscription)

    async def remove_subscription(self, id: str) -> None:
        await self._subscription_manager.remove_subscription(id)

    async def get(
        self, id_or_type: AgentId | AgentType | str, /, key: str = "default", *, lazy: bool = True
    ) -> AgentId:
        return await get_impl(
            id_or_type=id_or_type,
            key=key,
            lazy=lazy,
            instance_getter=self._get_agent,
        )

    def add_message_serializer(self, serializer: MessageSerializer[Any] | Sequence[MessageSerializer[Any]]) -> None:
        self._serialization_registry.add_serializer(serializer)

    def _try_serialize(self, message: Any) -> str:
        try:
            type_name = self._serialization_registry.type_name(message)
            return self._serialization_registry.serialize(
                message, type_name=type_name, data_content_type=JSON_DATA_CONTENT_TYPE
            ).decode("utf-8")
        except ValueError:
            return "Message could not be serialized"
