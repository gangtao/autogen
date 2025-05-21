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

from timeplus_messaging.producer import TimeplusLogProducer
from timeplus_messaging.consumer import SingleTopicConsumer
from opentelemetry.trace import TracerProvider

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


@dataclass(kw_only=True)
class TimeplusMessageEnvelope:
    message: Any
    sender: Optional[AgentId]
    topic_id: TopicId
    recipient: AgentId | None
    metadata: Optional[EnvelopeMetadata] = None
    message_id: str

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)

        if self.sender:
            data["sender"] = str(self.sender)

        if self.topic_id:
            data["topic_id"] = str(self.topic_id)

        if self.recipient:
            data["recipient"] = str(self.recipient)

        if hasattr(self.message, "__dict__"):
            data["message"] = {
                "_type": type(self.message).__name__,
                "_module": type(self.message).__module__,
                "_content": self.message.__dict__,
            }
        elif not isinstance(self.message, (str, int, float, bool, list, dict, type(None))):
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

                obj = cls_type.__new__(cls_type)

                for key, value in content.items():
                    setattr(obj, key, value)

                data["message"] = obj

            except (ImportError, AttributeError, TypeError, ValueError):
                data["message"] = message["_content"]

        return cls(**data)


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
            host=self._host, 
            port=self._port, 
            user=self._user, 
            password=self._password,
            database=self._database)
        
        self._topic_producer._ensure_stream_exists(self._runtime_topic)
        
        self._consumer = SingleTopicConsumer(self._runtime_topic,
            host=self._host, 
            port=self._port, 
            group_id="test_group", 
            user=self._user, 
            password=self._password,
            database=self._database,
            auto_offset_reset="earliest")

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
        # print(f"Sending message to {recipient} from {sender}")
        if message_id is None:
            message_id = str(uuid.uuid4())

        event_logger.info(
            MessageEvent(
                payload=self._try_serialize(message),
                sender=sender,
                receiver=recipient,
                kind=MessageKind.DIRECT,
                delivery_stage=DeliveryStage.SEND,
            )
        )

        with self._tracer_helper.trace_block(
            "create",
            recipient,
            parent=None,
            extraAttributes={"message_type": type(message).__name__},
        ):
            if cancellation_token is None:
                cancellation_token = CancellationToken()

            envelope = TimeplusMessageEnvelope(
                message=message,
                sender=sender,
                topic_id=None,
                recipient=recipient,
                metadata=get_telemetry_envelope_metadata(),
                message_id=message_id,
            )

            # publish message to timeplus topic
            producer = TimeplusLogProducer(host=self._host, port=self._port, user=self._user, password=self._password, database=self._database)
            producer.send(
                topic=self._runtime_topic,
                value=json.dumps(envelope.to_dict()),
                key=message_id,
            )

            producer.flush()
            producer.close()

    async def publish_message(
        self,
        message: Any,
        topic_id: TopicId,
        *,
        sender: AgentId | None = None,
        cancellation_token: CancellationToken | None = None,
        message_id: str | None = None,
    ) -> None:
        # print(f"publish message to {topic_id} from {sender}")

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
            producer = TimeplusLogProducer(host=self._host, port=self._port, user=self._user, password=self._password, database=self._database)
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
        recipient = envelope.recipient

        if recipient is None:
            return

        if recipient.type not in self._known_agent_names:
            raise LookupError(f"Agent type '{recipient.type}' does not exist.")

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
            recipient_agent = await self._get_agent(recipient)

            message_context = MessageContext(
                sender=envelope.sender,
                topic_id=None,
                is_rpc=True,
                message_id=envelope.message_id,
                cancellation_token=None,
            )

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
                with MessageHandlerContext.populate_context(recipient_agent.id):
                    response = await recipient_agent.on_message(
                        envelope.message,
                        ctx=message_context,
                    )
                    # TODO : handle response here, send to timeplus topic as well
                    # print(f"recipient {recipient} for response: {response}")
        except BaseException as e:
            event_logger.info(
                MessageHandlerExceptionEvent(
                    payload=self._try_serialize(envelope.message),
                    handling_agent=recipient,
                    exception=e,
                )
            )

        event_logger.info(
            MessageEvent(
                payload=self._try_serialize(response),
                sender=envelope.recipient,
                receiver=envelope.sender,
                kind=MessageKind.RESPOND,
                delivery_stage=DeliveryStage.SEND,
            )
        )
        message_id = str(uuid.uuid4())

        response_envelope = TimeplusMessageEnvelope(
            message=response,
            sender=envelope.recipient,
            topic_id=None,
            recipient=envelope.sender,
            metadata=get_telemetry_envelope_metadata(),
            message_id=message_id,
        )

        producer = TimeplusLogProducer(host=self._host, port=self._port, user=self._user, password=self._password, database=self._database)  # type: ignore
        producer.send(
            topic=self._runtime_topic,
            value=json.dumps(response_envelope.to_dict()),
            key=message_id,
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

                    future = _on_message(agent, message_context)
                    responses.append(future)

                await asyncio.gather(*responses)
            except BaseException as e:
                if not self._ignore_unhandled_handler_exceptions:
                    self._background_exception = e

    async def _process_next(self) -> None:  # type: ignore
        """Process the next message in the queue."""

        try:
            # Poll for messages (e.g. wait up to 10 ms = 0.01 second)
            records = self._consumer.poll(
                timeout_ms=10
            )
            
            if not records:
                return
            
            if records is not None:
                logger.debug(f"get records {records}, type: {type(records)}")

            for _, messages in records.items():
                for message in messages:
                    envelope: TimeplusMessageEnvelope = TimeplusMessageEnvelope.from_dict(message.value)
                    logger.debug(f"get the envelope: {envelope}")
                    recipient: AgentId | None = envelope.recipient
                    if recipient is not None:
                        await self._process_send(envelope)
                    else:
                        await self._process_publish(envelope)

        except Exception as e:
            logger.error(f"Error processing Timeplus message: {e}")

        await asyncio.sleep(0)  # Yield control to the event loop

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
        """Calls :meth:`stop` if applicable and the :meth:`Agent.close` method on all instantiated agents"""
        # stop the runtime if it hasn't been stopped yet
        if self._run_context is not None:
            await self.stop()
        # close all the agents that have been instantiated
        for agent_id in self._instantiated_agents:
            agent = await self._get_agent(agent_id)
            await agent.close()

        self._consumer.close()  # type: ignore

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
