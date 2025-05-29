from __future__ import annotations

import asyncio
import inspect
import json
import logging
import pickle
import uuid
import warnings
import time
from asyncio import CancelledError, Future, Task
from collections.abc import Sequence
from dataclasses import dataclass, asdict, is_dataclass
from typing import Any, Awaitable, Callable, Dict, List, Mapping, ParamSpec, Set, Type, TypeVar, cast
import base64

from opentelemetry.trace import TracerProvider

from .logging import (
    AgentConstructionExceptionEvent,
    DeliveryStage,
    MessageDroppedEvent,
    MessageEvent,
    MessageHandlerExceptionEvent,
    MessageKind,
)

from ._agent import Agent
from ._agent_id import AgentId
from ._agent_instantiation import AgentInstantiationContext
from ._agent_metadata import AgentMetadata
from ._agent_runtime import AgentRuntime
from ._agent_type import AgentType
from ._cancellation_token import CancellationToken
from ._intervention import DropMessage, InterventionHandler
from ._message_context import MessageContext
from ._message_handler_context import MessageHandlerContext
from ._runtime_impl_helpers import SubscriptionManager, get_impl
from ._serialization import JSON_DATA_CONTENT_TYPE, MessageSerializer, SerializationRegistry
from ._subscription import Subscription
from ._telemetry import EnvelopeMetadata, MessageRuntimeTracingConfig, TraceHelper, get_telemetry_envelope_metadata
from ._topic import TopicId
from .exceptions import MessageDroppedException

# Import Timeplus messaging components
from timeplus_messaging.consumer import SingleTopicConsumer
from timeplus_messaging.producer import TimeplusLogProducer

logger = logging.getLogger("autogen_core")
event_logger = logging.getLogger("autogen_core.events")

# We use a type parameter in some functions which shadows the built-in `type` function.
type_func_alias = type


def _warn_if_none(value: Any, handler_name: str) -> None:
    """
    Utility function to check if the intervention handler returned None and issue a warning.
    """
    if value is None:
        warnings.warn(
            f"Intervention handler {handler_name} returned None. This might be unintentional. "
            "Consider returning the original message or DropMessage explicitly.",
            RuntimeWarning,
            stacklevel=2,
        )


class EnhancedTimeplusMessageSerializer:
    """Enhanced serialization handling for Timeplus transport with better control object support"""
    
    def __init__(self, serialization_registry: SerializationRegistry):
        self._registry = serialization_registry
    
    def serialize(self, obj: Any) -> str:
        """Serialize an object to JSON string with enhanced control object handling"""
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[SERIALIZER] Attempting to serialize object of type: {type(obj).__name__}")
            
            # Handle special control objects that need custom serialization
            if self._is_cancellation_token(obj):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[SERIALIZER] Serializing CancellationToken")
                return self._serialize_cancellation_token(obj)
            
            if self._is_message_context(obj):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[SERIALIZER] Serializing MessageContext")
                return self._serialize_message_context(obj)
            
            # Handle specific autogen types that commonly cause issues
            if hasattr(obj, '__class__'):
                class_name = obj.__class__.__name__
                
                # Handle FunctionExecutionResult and similar types
                if class_name in ['FunctionExecutionResult', 'FunctionCall']:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[SERIALIZER] Serializing {class_name} as object with __dict__")
                    
                    if hasattr(obj, '__dict__'):
                        clean_dict = self._clean_object_dict(obj.__dict__)
                        return json.dumps({
                            "_autogen_type": "object",
                            "_class": class_name,
                            "_module": obj.__class__.__module__,
                            "_data": clean_dict
                        })
            
            # Force dataclass serialization for Message objects to avoid registry issues
            if hasattr(obj, '__dataclass_fields__') and obj.__class__.__name__ == 'Message':
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[SERIALIZER] Serializing dataclass Message")
                return json.dumps({
                    "_autogen_type": "dataclass",
                    "_class": obj.__class__.__name__,
                    "_module": obj.__class__.__module__,
                    "_data": asdict(obj)
                })
            
            # Try the registry first (for other objects)
            try:
                type_name = self._registry.type_name(obj)
                # Use the same method signature as in SingleThreadedAgentRuntime
                serialized_bytes = self._registry.serialize(
                    obj, type_name=type_name, data_content_type=JSON_DATA_CONTENT_TYPE
                )
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[SERIALIZER] Successfully serialized using registry with type_name: {type_name}")
                return json.dumps({
                    "_autogen_type": "registry",
                    "_type_name": type_name,
                    "_data": serialized_bytes.decode("utf-8") if isinstance(serialized_bytes, bytes) else str(serialized_bytes)
                })
            except (ValueError, AttributeError, TypeError) as e:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[SERIALIZER] Registry serialization failed: {e}")
                pass
            
            # Handle dataclasses (general case)
            if is_dataclass(obj):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[SERIALIZER] Serializing general dataclass")
                return json.dumps({
                    "_autogen_type": "dataclass",
                    "_class": obj.__class__.__name__,
                    "_module": obj.__class__.__module__,
                    "_data": asdict(obj)
                })
            
            # Handle objects with __dict__ (exclude problematic attributes)
            if hasattr(obj, "__dict__"):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[SERIALIZER] Serializing object with __dict__")
                clean_dict = self._clean_object_dict(obj.__dict__)
                return json.dumps({
                    "_autogen_type": "object",
                    "_class": obj.__class__.__name__,  
                    "_module": obj.__class__.__module__,
                    "_data": clean_dict
                })
            
            # Handle basic types
            if isinstance(obj, (str, int, float, bool, list, dict, type(None))):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[SERIALIZER] Serializing primitive type")
                return json.dumps({
                    "_autogen_type": "primitive",
                    "_data": obj
                })
            
            # Fallback to pickle for complex objects
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("[SERIALIZER] Falling back to pickle serialization")
            pickled = base64.b64encode(pickle.dumps(obj)).decode('utf-8')
            return json.dumps({
                "_autogen_type": "pickle",
                "_class": obj.__class__.__name__,
                "_module": obj.__class__.__module__,
                "_data": pickled
            })
            
        except Exception as e:
            logger.error(f"[SERIALIZER] Failed to serialize object {type(obj)}: {e}")
            return json.dumps({
                "_autogen_type": "error",
                "_error": str(e),
                "_repr": repr(obj)
            })
    
    def deserialize(self, data_str: str) -> Any:
        """Deserialize object from JSON string with enhanced control object handling"""
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[DESERIALIZER] Attempting to deserialize data: {data_str[:100]}...")
            data = json.loads(data_str)
            
            if not isinstance(data, dict) or "_autogen_type" not in data:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[DESERIALIZER] Data is not autogen serialized format, returning as-is")
                return data
            
            obj_type = data["_autogen_type"]
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"[DESERIALIZER] Deserializing object of type: {obj_type}")
            
            if obj_type == "cancellation_token":
                return self._deserialize_cancellation_token(data)
            
            if obj_type == "message_context":
                return self._deserialize_message_context(data)
            
            if obj_type == "registry":
                type_name = data["_type_name"]
                serialized_data = data["_data"]
                
                # Handle both string and bytes data
                if isinstance(serialized_data, str):
                    serialized_bytes = serialized_data.encode("utf-8")
                else:
                    serialized_bytes = serialized_data
                
                # Use the same method signature as in SingleThreadedAgentRuntime._try_serialize
                try:
                    result = self._registry.deserialize(
                        serialized_bytes, type_name=type_name, data_content_type=JSON_DATA_CONTENT_TYPE
                    )
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[DESERIALIZER] Successfully deserialized using registry")
                    return result
                except Exception as final_error:
                    logger.warning(f"[DESERIALIZER] Registry deserialization failed: {final_error}")
                    # Fall through to other methods
            
            elif obj_type == "dataclass":
                module = __import__(data["_module"], fromlist=[data["_class"]])
                cls = getattr(module, data["_class"])
                result = cls(**data["_data"])
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[DESERIALIZER] Successfully deserialized dataclass: {cls.__name__}")
                return result
            
            elif obj_type == "object":
                module = __import__(data["_module"], fromlist=[data["_class"]])
                cls = getattr(module, data["_class"])
                try:
                    result = cls(**data["_data"])
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[DESERIALIZER] Successfully deserialized object: {cls.__name__}")
                    return result
                except TypeError:
                    # Fallback: create and set attributes
                    obj = cls.__new__(cls)
                    if hasattr(obj, "__init__"):
                        try:
                            obj.__init__()
                        except TypeError:
                            pass
                    for key, value in data["_data"].items():
                        setattr(obj, key, value)
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"[DESERIALIZER] Deserialized object using fallback method: {cls.__name__}")
                    return obj
            
            elif obj_type == "primitive":
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("[DESERIALIZER] Returning primitive data")
                return data["_data"]
            
            elif obj_type == "pickle":
                pickled_data = base64.b64decode(data["_data"].encode('utf-8'))
                result = pickle.loads(pickled_data)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[DESERIALIZER] Successfully unpickled object: {type(result).__name__}")
                return result
            
            elif obj_type == "error":
                logger.warning(f"[DESERIALIZER] Received error-serialized object: {data['_error']}")
                return f"<Serialization Error: {data['_error']}>"
            
            else:
                logger.warning(f"[DESERIALIZER] Unknown serialization type: {obj_type}")
            
            return data
                
        except Exception as e:
            logger.error(f"[DESERIALIZER] Failed to deserialize data: {e}")
            # Log the actual data for debugging (first 200 chars)
            logger.error(f"[DESERIALIZER] Problematic data: {data_str[:200]}...")
            import traceback
            logger.error(f"[DESERIALIZER] Full traceback: {traceback.format_exc()}")
            return f"<Deserialization Error: {e}>"
    
    def _is_cancellation_token(self, obj) -> bool:
        """Check if object is a CancellationToken"""
        return hasattr(obj, '__class__') and obj.__class__.__name__ == 'CancellationToken'
    
    def _is_message_context(self, obj) -> bool:
        """Check if object is a MessageContext"""
        return hasattr(obj, '__class__') and obj.__class__.__name__ == 'MessageContext'
    
    def _serialize_cancellation_token(self, token) -> str:
        """Serialize CancellationToken to a transferable format"""
        return json.dumps({
            "_autogen_type": "cancellation_token",
            "_data": {
                "is_cancelled": getattr(token, '_cancelled', False),
                "cancel_reason": getattr(token, '_cancel_reason', None),
                "token_id": str(uuid.uuid4())  # Generate new ID for tracking
            }
        })
    
    def _deserialize_cancellation_token(self, data) -> CancellationToken:
        """Deserialize CancellationToken - create a new local token"""
        token = CancellationToken()
        token_data = data["_data"]
        
        # If the original was cancelled, cancel this one too
        if token_data.get("is_cancelled", False):
            # Note: This is a limitation - we can't perfectly recreate the cancellation state
            # but we can at least mark it as cancelled
            if hasattr(token, 'cancel'):
                try:
                    token.cancel()
                except Exception:
                    pass  # Some cancellation tokens might not support this
        
        return token
    
    def _serialize_message_context(self, ctx) -> str:
        """Serialize MessageContext to a transferable format"""
        return json.dumps({
            "_autogen_type": "message_context",
            "_data": {
                "sender": str(ctx.sender) if ctx.sender else None,
                "topic_id": str(ctx.topic_id) if ctx.topic_id else None,
                "is_rpc": ctx.is_rpc,
                "message_id": ctx.message_id,
                # Skip cancellation_token - will be recreated on the other side
            }
        })
    
    def _deserialize_message_context(self, data) -> MessageContext:
        """Deserialize MessageContext - recreate with new CancellationToken"""
        ctx_data = data["_data"]
        
        return MessageContext(
            sender=AgentId.from_str(ctx_data["sender"]) if ctx_data["sender"] else None,
            topic_id=TopicId.from_str(ctx_data["topic_id"]) if ctx_data["topic_id"] else None,
            is_rpc=ctx_data["is_rpc"],
            message_id=ctx_data["message_id"],
            cancellation_token=CancellationToken()  # Always create new token
        )
    
    def _clean_object_dict(self, obj_dict: dict) -> dict:
        """Remove non-serializable attributes from object dictionary"""
        clean_dict = {}
        
        for key, value in obj_dict.items():
            # Skip private attributes and known problematic types
            if key.startswith('_'):
                continue
            
            # Skip functions, methods, and other non-serializable types
            if callable(value):
                continue
            
            # Skip known problematic types
            if any(type_name in str(type(value)) for type_name in ['Future', 'Task', 'Event', 'Lock']):
                continue
            
            try:
                # Test if the value is JSON serializable
                json.dumps(value, default=str)
                clean_dict[key] = value
            except (TypeError, ValueError):
                # If not serializable, convert to string representation
                clean_dict[key] = str(value)
        
        return clean_dict


@dataclass(kw_only=True)
class TimeplusEnvelope:
    """Base envelope for all message types in Timeplus transport"""
    message_type: str  # "send", "publish", "response"
    message_id: str
    sender: AgentId | None
    message_payload: str  # Serialized message
    metadata: str | None = None  # Serialized metadata
    
    # For send messages
    recipient: AgentId | None = None
    
    # For publish messages  
    topic_id: TopicId | None = None
    
    # For response messages
    is_error: bool = False
    
    def to_json(self) -> str:
        """Convert envelope to JSON string for Timeplus"""
        data = {
            "message_type": self.message_type,
            "message_id": self.message_id,
            "sender": str(self.sender) if self.sender else None,
            "message_payload": self.message_payload,
            "metadata": self.metadata,
            "recipient": str(self.recipient) if self.recipient else None,
            "topic_id": str(self.topic_id) if self.topic_id else None,
            "is_error": self.is_error
        }
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, data) -> TimeplusEnvelope:
        """Create envelope from JSON string or dict"""
        # Handle both string and dict inputs from Timeplus consumer
        if isinstance(data, str):
            parsed_data = json.loads(data)
        elif isinstance(data, dict):
            parsed_data = data
        else:
            raise ValueError(f"Expected str or dict, got {type(data)}")
        
        return cls(
            message_type=parsed_data["message_type"],
            message_id=parsed_data["message_id"],
            sender=AgentId.from_str(parsed_data["sender"]) if parsed_data["sender"] else None,
            message_payload=parsed_data["message_payload"],
            metadata=parsed_data.get("metadata"),
            recipient=AgentId.from_str(parsed_data["recipient"]) if parsed_data.get("recipient") else None,
            topic_id=TopicId.from_str(parsed_data["topic_id"]) if parsed_data.get("topic_id") else None,
            is_error=parsed_data.get("is_error", False)
        )


class PendingRequest:
    """Tracks a pending send_message request waiting for response"""
    
    def __init__(self, future: Future[Any], cancellation_token: CancellationToken):
        self.future = future
        self.cancellation_token = cancellation_token
        self.timeout_task: Task[None] | None = None


class SimplifiedTimeplusProducer:
    """Producer that uses the monkey patch approach for best performance"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self._host = host
        self._port = port  
        self._user = user
        self._password = password
        self._database = database
        self._producer = None
        self._stream_created = set()
        
    def _get_producer(self):
        """Get or create producer instance (reuse for better performance)"""
        
        self._producer = TimeplusLogProducer(
            host=self._host,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database
        )
        return self._producer
    
    def ensure_stream_exists(self, stream_name: str):
        """Ensure stream exists using existing producer connection"""
        if stream_name not in self._stream_created:
            try:
                producer = self._get_producer()
                producer._ensure_stream_exists(stream_name)
                self._stream_created.add(stream_name)
                logger.info(f"[SIMPLE_PRODUCER] Stream '{stream_name}' created/verified")
            except Exception as e:
                logger.warning(f"[SIMPLE_PRODUCER] Stream creation warning for '{stream_name}': {e}")
                # Mark as created anyway to avoid repeated attempts
                self._stream_created.add(stream_name)
    
    def send(self, topic: str, value: str, key: str = None):
        """Send message using existing producer connection"""
        try:
            producer = self._get_producer()
            producer.send(topic=topic, value=value, key=key)
            producer.flush()
            logger.debug(f"[SIMPLE_PRODUCER] Message sent successfully to {topic}")
        except Exception as e:
            logger.error(f"[SIMPLE_PRODUCER] Failed to send message: {e}")
            raise
    
    def close(self):
        """Close the producer"""
        if self._producer:
            try:
                self._producer.close()
            except Exception as e:
                logger.warning(f"[SIMPLE_PRODUCER] Error closing producer: {e}")
            self._producer = None


class SimplifiedTimeplusConsumer:
    """Consumer that reuses connection but recreates on errors"""
    
    def __init__(self, topic: str, host: str, port: int, group_id: str, user: str, password: str, database: str, auto_offset_reset: str = "latest"):
        self._topic = topic
        self._host = host
        self._port = port
        self._group_id = group_id
        self._user = user
        self._password = password
        self._database = database
        self._auto_offset_reset = auto_offset_reset
        self._consumer = None
        logger.info(f"[SIMPLE_CONSUMER] Initialized consumer wrapper for topic: {topic}")
    
    def _get_consumer(self):
        """Get or create consumer instance"""
        if self._consumer is None:
            logger.debug(f"[SIMPLE_CONSUMER] Creating consumer connection")
            self._consumer = SingleTopicConsumer(
                topic=self._topic,
                host=self._host,
                port=self._port,
                group_id=self._group_id,
                user=self._user,
                password=self._password,
                database=self._database,
                auto_offset_reset=self._auto_offset_reset
            )
        return self._consumer
    
    def poll(self, timeout_ms: int = 100):
        """Poll messages using existing consumer connection"""
        try:
            consumer = self._get_consumer()
            records = consumer.poll(timeout_ms=timeout_ms)
            
            if records:
                logger.debug(f"[SIMPLE_CONSUMER] Polled {sum(len(msgs) for msgs in records.values())} messages")
            
            return records
            
        except Exception as e:
            logger.error(f"[SIMPLE_CONSUMER] Poll failed: {e}")
            # Force recreation on error
            if self._consumer:
                try:
                    self._consumer.close()
                except:
                    pass
                self._consumer = None
            raise
    
    def close(self):
        """Close the consumer connection"""
        if self._consumer:
            try:
                self._consumer.close()
                logger.info(f"[SIMPLE_CONSUMER] Consumer closed")
            except Exception as e:
                logger.debug(f"[SIMPLE_CONSUMER] Error closing consumer: {e}")
            self._consumer = None


class SafeTimeplusConsumer:
    """Consumer that creates fresh connections for polling to avoid conflicts"""
    
    def __init__(self, topic: str, host: str, port: int, group_id: str, user: str, password: str, database: str, auto_offset_reset: str = "latest"):
        self._topic = topic
        self._host = host
        self._port = port
        self._group_id = group_id
        self._user = user
        self._password = password
        self._database = database
        self._auto_offset_reset = auto_offset_reset
        logger.info(f"[SAFE_CONSUMER] Initialized consumer wrapper for topic: {topic}")
    
    def _create_consumer(self):
        """Create a fresh consumer instance"""
        logger.debug(f"[SAFE_CONSUMER] Creating fresh consumer connection")
        return SingleTopicConsumer(
            topic=self._topic,
            host=self._host,
            port=self._port,
            group_id=self._group_id,
            user=self._user,
            password=self._password,
            database=self._database,
            auto_offset_reset=self._auto_offset_reset
        )
    
    def poll(self, timeout_ms: int = 100):
        """Poll messages using a fresh consumer connection"""
        consumer = None
        try:
            consumer = self._create_consumer()
            records = consumer.poll(timeout_ms=timeout_ms)
            logger.debug(f"[SAFE_CONSUMER] Polled {sum(len(msgs) for msgs in records.values()) if records else 0} messages")
            return records
        except Exception as e:
            logger.error(f"[SAFE_CONSUMER] Poll failed: {e}")
            raise
        finally:
            if consumer:
                try:
                    consumer.close()
                    logger.debug(f"[SAFE_CONSUMER] Closed consumer connection")
                except Exception as e:
                    logger.debug(f"[SAFE_CONSUMER] Error closing consumer: {e}")
    
    def close(self):
        """Nothing to close since we create fresh connections each time"""
        logger.info(f"[SAFE_CONSUMER] Consumer wrapper closed")
        pass


P = ParamSpec("P")
T = TypeVar("T", bound=Agent)


class RunContext:
    """Manages the runtime execution context"""
    
    def __init__(self, runtime: TimeplusAgentRuntime) -> None:
        self._runtime = runtime
        self._run_task = asyncio.create_task(self._run())
        self._stopped = asyncio.Event()

    async def _run(self) -> None:
        """Main processing loop"""
        logger.info("[RUNTIME] Starting main processing loop")
        while True:
            if self._stopped.is_set():
                logger.info("[RUNTIME] Processing loop stopped")
                return

            try:
                await self._runtime._process_next()
            except Exception as e:
                logger.error(f"[RUNTIME] Error in runtime processing loop: {e}", exc_info=True)
                if not self._runtime._ignore_unhandled_handler_exceptions:
                    self._runtime._background_exception = e
                    return
                # Small delay to prevent tight error loops
                await asyncio.sleep(0.1)

    async def stop(self) -> None:
        """Stop the runtime immediately"""
        logger.info("[RUNTIME] Stopping runtime immediately")
        self._stopped.set()
        # Cancel all pending requests
        for pending in self._runtime._pending_requests.values():
            if pending.timeout_task:
                pending.timeout_task.cancel()
            if not pending.future.done():
                pending.future.set_exception(RuntimeError("Runtime stopped"))
        self._runtime._pending_requests.clear()
        await self._run_task

    async def stop_when_idle(self) -> None:
        """Stop when no pending requests"""
        logger.info("[RUNTIME] Waiting for idle state before stopping")
        # Wait for pending requests to complete
        while self._runtime._pending_requests:
            logger.debug(f"[RUNTIME] Waiting for {len(self._runtime._pending_requests)} pending requests")
            await asyncio.sleep(0.1)
        
        logger.info("[RUNTIME] Runtime is idle, stopping")
        self._stopped.set()
        await self._run_task

    async def stop_when(self, condition: Callable[[], bool], check_period: float = 1.0) -> None:
        """Stop when condition is met"""
        async def check_condition() -> None:
            while not condition():
                await asyncio.sleep(check_period)
            await self.stop()

        await asyncio.create_task(check_condition())


class SafeTimeplusProducer:
    """Wrapper around TimeplusLogProducer that avoids connection conflicts"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self._host = host
        self._port = port  
        self._user = user
        self._password = password
        self._database = database
        self._producer = None
        self._stream_created = set()  # Track which streams we've already created
        
    def _get_producer(self):
        """Get or create producer instance"""
        if self._producer is None:
            self._producer = TimeplusLogProducer(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._database
            )
        return self._producer
    
    def ensure_stream_exists(self, stream_name: str):
        """Ensure stream exists, but only check once per stream"""
        if stream_name not in self._stream_created:
            try:
                producer = self._get_producer()
                producer._ensure_stream_exists(stream_name)
                self._stream_created.add(stream_name)
                logger.info(f"[SAFE_PRODUCER] Stream '{stream_name}' created/verified")
            except Exception as e:
                logger.warning(f"[SAFE_PRODUCER] Stream creation warning for '{stream_name}': {e}")
                # Mark as created anyway to avoid repeated attempts
                self._stream_created.add(stream_name)
    
    def send(self, topic: str, value: str, key: str = None):
        """Send message without checking stream existence (assumes already created)"""
        producer = self._get_producer()
        # Use the raw send method that doesn't check stream existence
        if hasattr(producer, '_send_raw'):
            producer._send_raw(topic=topic, value=value, key=key)
        else:
            # Fallback to regular send, but stream should already exist
            producer.send(topic=topic, value=value, key=key)
    
    def flush(self):
        """Flush pending messages"""
        if self._producer:
            self._producer.flush()
    
    def close(self):
        """Close the producer"""
        if self._producer:
            try:
                self._producer.close()
            except Exception as e:
                logger.warning(f"[SAFE_PRODUCER] Error closing producer: {e}")
            self._producer = None


class TimeplusAgentRuntime(AgentRuntime):
    """
    A Timeplus-based agent runtime that processes messages through Timeplus streams.
    
    This runtime provides distributed agent communication by serializing messages
    through Timeplus streams, maintaining the same API as SingleThreadedAgentRuntime
    while enabling multi-node deployment.
    
    Args:
        host: Timeplus host address
        port: Timeplus port
        user: Timeplus username  
        password: Timeplus password
        database: Timeplus database name
        stream_name: Custom stream name (auto-generated if None)
        intervention_handlers: Message intervention handlers
        tracer_provider: OpenTelemetry tracer provider
        ignore_unhandled_exceptions: Whether to ignore background exceptions
        request_timeout: Timeout for send_message requests in seconds
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 8463,
        user: str = "default", 
        password: str = "",
        database: str = "default",
        stream_name: str | None = None,
        intervention_handlers: List[InterventionHandler] | None = None,
        tracer_provider: TracerProvider | None = None,
        ignore_unhandled_exceptions: bool = True,
        request_timeout: float = 30.0,
    ) -> None:
        
        # Timeplus connection settings
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._request_timeout = request_timeout
        
        # Generate unique stream name for this runtime
        self._stream_name = stream_name or f"autogen_runtime_{uuid.uuid4()}".replace("-", "_")
        logger.info(f"[RUNTIME] Initializing TimeplusAgentRuntime with stream: {self._stream_name}")
        
        # Initialize serialization with common autogen types
        self._serialization_registry = SerializationRegistry()
        self._register_common_serializers()
        self._message_serializer = EnhancedTimeplusMessageSerializer(self._serialization_registry)
        
        # Initialize Timeplus components - use simplified wrappers with connection reuse
        self._producer: SimplifiedTimeplusProducer | None = None
        self._consumer: SimplifiedTimeplusConsumer | None = None
        self._consumer_error_count = 0
        
        # Runtime state (mirrored from SingleThreadedAgentRuntime)
        self._tracer_helper = TraceHelper(tracer_provider, MessageRuntimeTracingConfig("TimeplusAgentRuntime"))
        self._agent_factories: Dict[
            str, Callable[[], Agent | Awaitable[Agent]] | Callable[[AgentRuntime, AgentId], Agent | Awaitable[Agent]]
        ] = {}
        self._instantiated_agents: Dict[AgentId, Agent] = {}
        self._intervention_handlers = intervention_handlers
        self._background_tasks: Set[Task[Any]] = set()
        self._subscription_manager = SubscriptionManager()
        self._run_context: RunContext | None = None
        self._ignore_unhandled_handler_exceptions = ignore_unhandled_exceptions
        self._background_exception: BaseException | None = None
        self._agent_instance_types: Dict[str, Type[Agent]] = {}
        
        # Timeplus-specific state
        self._pending_requests: Dict[str, PendingRequest] = {}

    def _register_common_serializers(self) -> None:
        """Register serializers for common autogen types to prevent serialization failures"""
        try:
            # Import common autogen types that might need serialization
            from autogen_core.models._types import FunctionExecutionResult
            from autogen_core.models import FunctionCall
            from pydantic import BaseModel
            
            # Create a generic serializer for pydantic models and dataclasses
            def serialize_pydantic_model(obj: BaseModel) -> bytes:
                return obj.model_dump_json().encode('utf-8')
            
            def deserialize_pydantic_model(data: bytes, type_name: str) -> Any:
                # This is a simplified deserializer - in practice you'd need to 
                # map type_name back to the actual class
                import json
                return json.loads(data.decode('utf-8'))
            
            # Register serializers for common types
            logger.debug("[REGISTRY] Registering common autogen serializers")
            
            # Note: The actual registration depends on your SerializationRegistry implementation
            # This is a placeholder showing the concept
            
        except ImportError as e:
            logger.debug(f"[REGISTRY] Could not import some autogen types for serialization: {e}")
        except Exception as e:
            logger.warning(f"[REGISTRY] Failed to register common serializers: {e}")

    def _initialize_timeplus_components(self) -> None:
        """Initialize Timeplus producer and consumer with simplified connection handling"""
        logger.info("[TIMEPLUS] Initializing Timeplus components")
        
        # Initialize simplified producer wrapper
        if self._producer is None:
            logger.info("[TIMEPLUS] Creating simplified producer wrapper")
            try:
                self._producer = SimplifiedTimeplusProducer(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._password,
                    database=self._database
                )
                
                # Ensure stream exists ONCE during initialization (synchronous)
                logger.info(f"[TIMEPLUS] Ensuring stream '{self._stream_name}' exists")
                self._producer.ensure_stream_exists(self._stream_name)
                
            except Exception as e:
                logger.error(f"[TIMEPLUS] Failed to create producer: {e}")
                raise
        
        # Initialize simplified consumer wrapper
        if self._consumer is None:
            logger.info("[TIMEPLUS] Creating simplified consumer wrapper")
            try:
                # Create unique consumer group
                consumer_group = f"autogen_consumer_{uuid.uuid4()}"
                logger.info(f"[TIMEPLUS] Using consumer group: {consumer_group}")
                
                # Create consumer wrapper with connection reuse
                self._consumer = SimplifiedTimeplusConsumer(
                    topic=self._stream_name,
                    host=self._host,
                    port=self._port,
                    group_id=consumer_group,
                    user=self._user,
                    password=self._password,
                    database=self._database,
                    auto_offset_reset="latest"
                )
                logger.info("[TIMEPLUS] Consumer wrapper initialized successfully")
                
            except Exception as e:
                logger.error(f"[TIMEPLUS] Failed to create consumer: {e}")
                raise

    def _serialize_metadata(self, metadata: EnvelopeMetadata | None) -> str | None:
        """Safely serialize EnvelopeMetadata to JSON string"""
        if not metadata:
            return None
        
        try:
            # Try dataclass serialization first
            if is_dataclass(metadata):
                return json.dumps(asdict(metadata))
            
            # Try direct dict conversion
            if hasattr(metadata, '__dict__'):
                return json.dumps(metadata.__dict__)
            
            # Fallback to extracting common attributes
            return json.dumps({
                "trace_id": getattr(metadata, "trace_id", None),
                "span_id": getattr(metadata, "span_id", None),
                "timestamp": getattr(metadata, "timestamp", None),
            })
        except Exception as e:
            logger.warning(f"[TIMEPLUS] Failed to serialize metadata: {e}")
            return None

    @property
    def unprocessed_messages_count(self) -> int:
        """Return number of unprocessed messages (pending requests for Timeplus)"""
        return len(self._pending_requests)

    @property
    def _known_agent_names(self) -> Set[str]:
        """Return set of known agent type names"""
        return set(self._agent_factories.keys())

    async def _create_otel_attributes(
        self,
        sender_agent_id: AgentId | None = None,
        recipient_agent_id: AgentId | None = None,
        message_context: MessageContext | None = None,
        message: Any = None,
    ) -> Mapping[str, str]:
        """Create OpenTelemetry attributes - mirrored from SingleThreadedAgentRuntime"""
        if not sender_agent_id and not recipient_agent_id and not message:
            return {}
        
        attributes: Dict[str, str] = {}
        
        if sender_agent_id:
            try:
                sender_agent = await self._get_agent(sender_agent_id)
                attributes["sender_agent_type"] = sender_agent.id.type
                attributes["sender_agent_class"] = sender_agent.__class__.__name__
            except LookupError:
                attributes["sender_agent_type"] = sender_agent_id.type
                
        if recipient_agent_id:
            try:
                recipient_agent = await self._get_agent(recipient_agent_id)
                attributes["recipient_agent_type"] = recipient_agent.id.type
                attributes["recipient_agent_class"] = recipient_agent.__class__.__name__
            except LookupError:
                attributes["recipient_agent_type"] = recipient_agent_id.type

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

    async def send_message(
        self,
        message: Any,
        recipient: AgentId,
        *,
        sender: AgentId | None = None,
        cancellation_token: CancellationToken | None = None,
        message_id: str | None = None,
    ) -> Any:
        """Send message to specific agent - adapted from SingleThreadedAgentRuntime"""
        
        if cancellation_token is None:
            cancellation_token = CancellationToken()

        if message_id is None:
            message_id = str(uuid.uuid4())

        logger.info(f"[MESSAGE] Sending message {message_id} to {recipient.type}")

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
            future = asyncio.get_event_loop().create_future()
            
            if recipient.type not in self._known_agent_names:
                logger.error(f"[MESSAGE] Recipient {recipient.type} not found in known agents: {list(self._known_agent_names)}")
                future.set_exception(Exception("Recipient not found"))
                return await future

            content = message.__dict__ if hasattr(message, "__dict__") else message
            logger.info(f"[MESSAGE] Sending message of type {type(message).__name__} to {recipient.type}: {content}")

            # Create pending request tracking
            pending_request = PendingRequest(future, cancellation_token)
            
            # Set up timeout
            async def timeout_handler():
                await asyncio.sleep(self._request_timeout)
                if message_id in self._pending_requests:
                    logger.warning(f"[MESSAGE] Request {message_id} timed out")
                    self._pending_requests.pop(message_id)
                    if not future.done():
                        future.set_exception(TimeoutError(f"Request {message_id} timed out"))
            
            pending_request.timeout_task = asyncio.create_task(timeout_handler())
            self._pending_requests[message_id] = pending_request
            logger.debug(f"[MESSAGE] Added pending request {message_id}, total pending: {len(self._pending_requests)}")

            # Serialize and send envelope
            serialized_message = self._message_serializer.serialize(message)
            metadata_str = self._serialize_metadata(get_telemetry_envelope_metadata())
            
            envelope = TimeplusEnvelope(
                message_type="send",
                message_id=message_id,
                sender=sender,
                message_payload=serialized_message,
                metadata=metadata_str,
                recipient=recipient
            )
            
            await self._publish_envelope(envelope)
            cancellation_token.link_future(future)

            return await future

    async def publish_message(
        self,
        message: Any,
        topic_id: TopicId,
        *,
        sender: AgentId | None = None,
        cancellation_token: CancellationToken | None = None,
        message_id: str | None = None,
    ) -> None:
        """Publish message to topic - adapted from SingleThreadedAgentRuntime"""
        
        with self._tracer_helper.trace_block(
            "create",
            topic_id,
            parent=None,
            extraAttributes={"message_type": type(message).__name__},
        ):
            if cancellation_token is None:
                cancellation_token = CancellationToken()
                
            content = message.__dict__ if hasattr(message, "__dict__") else message
            logger.info(f"[MESSAGE] Publishing message of type {type(message).__name__} to all subscribers: {content}")

            if message_id is None:
                message_id = str(uuid.uuid4())

            logger.info(f"[MESSAGE] Publishing message {message_id} to topic {topic_id}")

            event_logger.info(
                MessageEvent(
                    payload=self._try_serialize(message),
                    sender=sender,
                    receiver=topic_id,
                    kind=MessageKind.PUBLISH,
                    delivery_stage=DeliveryStage.SEND,
                )
            )

            # Serialize and send envelope
            serialized_message = self._message_serializer.serialize(message)
            metadata_str = self._serialize_metadata(get_telemetry_envelope_metadata())
            
            envelope = TimeplusEnvelope(
                message_type="publish", 
                message_id=message_id,
                sender=sender,
                message_payload=serialized_message,
                metadata=metadata_str,
                topic_id=topic_id
            )
            
            await self._publish_envelope(envelope)

    async def _publish_envelope(self, envelope: TimeplusEnvelope) -> None:
        """Publish envelope to Timeplus stream using simplified producer"""
        if self._producer is None:
            raise RuntimeError("Runtime not started - call start() first")
        
        try:
            envelope_json = envelope.to_json()
            logger.debug(f"[TIMEPLUS] Publishing envelope: {envelope.message_type} {envelope.message_id}")
            
            # Use executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            def _send_message():
                """Send message using simplified producer"""
                try:
                    logger.debug(f"[TIMEPLUS] Sending to stream {self._stream_name}")
                    # create producer to avoid connection conflicts
                    self._producer = SimplifiedTimeplusProducer(
                        host=self._host,
                        port=self._port,
                        user=self._user,
                        password=self._password,
                        database=self._database
                    )
                    
                    self._producer.send(
                        topic=self._stream_name,
                        value=envelope_json,
                        key=envelope.message_id
                    )
                    logger.debug(f"[TIMEPLUS] Successfully sent envelope {envelope.message_id}")
                except Exception as e:
                    logger.error(f"[TIMEPLUS] Failed to send envelope {envelope.message_id}: {e}")
                    raise
            
            await loop.run_in_executor(None, _send_message)
            
        except Exception as e:
            logger.error(f"[TIMEPLUS] Failed to publish envelope: {e}")
            raise

    async def _process_send(self, envelope: TimeplusEnvelope) -> None:
        """Process send message with proper MessageContext handling"""
        
        logger.info(f"[PROCESS] Processing send message {envelope.message_id} to {envelope.recipient}")
        
        with self._tracer_helper.trace_block("send", envelope.recipient, parent=None):
            recipient = envelope.recipient

            if recipient is None or recipient.type not in self._known_agent_names:
                error_msg = f"Agent type '{recipient.type if recipient else 'None'}' does not exist."
                logger.error(f"[PROCESS] {error_msg}")
                error = LookupError(error_msg)
                await self._send_error_response(envelope, error)
                return

            try:
                # Deserialize message
                message = self._message_serializer.deserialize(envelope.message_payload)
                logger.debug(f"[PROCESS] Deserialized message of type: {type(message).__name__}")
                
                # Apply interventions BEFORE processing
                if self._intervention_handlers:
                    logger.debug(f"[PROCESS] Applying {len(self._intervention_handlers)} intervention handlers")
                    for handler in self._intervention_handlers:
                        try:
                            # Create temporary MessageContext for intervention
                            temp_context = MessageContext(
                                sender=envelope.sender,
                                topic_id=None,
                                is_rpc=True,
                                cancellation_token=CancellationToken(),
                                message_id=envelope.message_id,
                            )
                            
                            temp_message = await handler.on_send(
                                message, message_context=temp_context, recipient=envelope.recipient
                            )
                            _warn_if_none(temp_message, "on_send")
                            
                            if temp_message is DropMessage or isinstance(temp_message, DropMessage):
                                logger.info(f"[PROCESS] Message {envelope.message_id} dropped by intervention handler")
                                event_logger.info(
                                    MessageDroppedEvent(
                                        payload=self._try_serialize(message),
                                        sender=envelope.sender,
                                        receiver=envelope.recipient,
                                        kind=MessageKind.DIRECT,
                                    )
                                )
                                await self._send_error_response(envelope, MessageDroppedException())
                                return
                            
                            message = temp_message
                            
                        except BaseException as e:
                            logger.error(f"[PROCESS] Intervention handler failed: {e}")
                            await self._send_error_response(envelope, e)
                            return
                
                sender_id = str(envelope.sender) if envelope.sender is not None else "Unknown"
                logger.info(f"[PROCESS] Calling message handler for {recipient} from {sender_id}")
                
                event_logger.info(
                    MessageEvent(
                        payload=self._try_serialize(message),
                        sender=envelope.sender,
                        receiver=recipient,
                        kind=MessageKind.DIRECT,
                        delivery_stage=DeliveryStage.DELIVER,
                    )
                )
                
                recipient_agent = await self._get_agent(recipient)

                # Create fresh MessageContext for the actual processing
                # This ensures the cancellation token is local to this runtime instance
                message_context = MessageContext(
                    sender=envelope.sender,
                    topic_id=None,
                    is_rpc=True,
                    cancellation_token=CancellationToken(),  # Fresh token for local processing
                    message_id=envelope.message_id,
                )
                
                with self._tracer_helper.trace_block(
                    "process",
                    recipient_agent.id,
                    parent=None,
                    attributes=await self._create_otel_attributes(
                        sender_agent_id=envelope.sender,
                        recipient_agent_id=recipient,
                        message_context=message_context,
                        message=message,
                    ),
                ):
                    with MessageHandlerContext.populate_context(recipient_agent.id):
                        logger.debug(f"[PROCESS] Invoking agent.on_message for {recipient_agent.id}")
                        response = await recipient_agent.on_message(
                            message,
                            ctx=message_context,
                        )
                        logger.debug(f"[PROCESS] Agent {recipient_agent.id} responded with: {type(response).__name__}")
                        
            except CancelledError as e:
                logger.info(f"[PROCESS] Message {envelope.message_id} was cancelled")
                event_logger.info(
                    MessageHandlerExceptionEvent(
                        payload=envelope.message_payload,
                        handling_agent=recipient,
                        exception=e,
                    )
                )
                await self._send_error_response(envelope, e)
                return
                
            except BaseException as e:
                logger.error(f"[PROCESS] Error processing message {envelope.message_id}: {e}", exc_info=True)
                event_logger.info(
                    MessageHandlerExceptionEvent(
                        payload=envelope.message_payload,
                        handling_agent=recipient,
                        exception=e,
                    )
                )
                await self._send_error_response(envelope, e)
                return

            event_logger.info(
                MessageEvent(
                    payload=self._try_serialize(response),
                    sender=envelope.recipient,
                    receiver=envelope.sender,
                    kind=MessageKind.RESPOND,
                    delivery_stage=DeliveryStage.SEND,
                )
            )

            # Send successful response
            await self._send_response(envelope, response)

    async def _send_response(self, original_envelope: TimeplusEnvelope, response: Any) -> None:
        """Send response back to original sender"""
        if original_envelope.sender is None:
            logger.debug(f"[RESPONSE] No sender for message {original_envelope.message_id}, skipping response")
            return
        
        logger.debug(f"[RESPONSE] Sending response for message {original_envelope.message_id}")
        serialized_response = self._message_serializer.serialize(response)
        
        response_envelope = TimeplusEnvelope(
            message_type="response",
            message_id=original_envelope.message_id,  # Same ID for correlation
            sender=original_envelope.recipient,  # Agent responding
            message_payload=serialized_response,
            recipient=original_envelope.sender,  # Original sender gets response
            is_error=False
        )
        
        await self._publish_envelope(response_envelope)

    async def _send_error_response(self, original_envelope: TimeplusEnvelope, error: BaseException) -> None:
        """Send error response back to original sender"""
        if original_envelope.sender is None:
            logger.debug(f"[ERROR] No sender for message {original_envelope.message_id}, skipping error response")
            return
        
        logger.info(f"[ERROR] Sending error response for message {original_envelope.message_id}: {type(error).__name__}")
        error_dict = {
            "error_type": type(error).__name__,
            "error_message": str(error)
        }
        serialized_error = self._message_serializer.serialize(error_dict)
        
        error_envelope = TimeplusEnvelope(
            message_type="response",
            message_id=original_envelope.message_id,
            sender=original_envelope.recipient,
            message_payload=serialized_error,
            recipient=original_envelope.sender,
            is_error=True
        )
        
        await self._publish_envelope(error_envelope)

    async def _process_publish(self, envelope: TimeplusEnvelope) -> None:
        """Process publish message with proper MessageContext handling"""
        
        logger.info(f"[PROCESS] Processing publish message {envelope.message_id} to topic {envelope.topic_id}")
        
        with self._tracer_helper.trace_block("publish", envelope.topic_id, parent=None):
            try:
                message = self._message_serializer.deserialize(envelope.message_payload)
                logger.debug(f"[PROCESS] Deserialized published message of type: {type(message).__name__}")
                
                # Apply interventions BEFORE processing
                if self._intervention_handlers:
                    logger.debug(f"[PROCESS] Applying {len(self._intervention_handlers)} intervention handlers for publish")
                    for handler in self._intervention_handlers:
                        try:
                            temp_context = MessageContext(
                                sender=envelope.sender,
                                topic_id=envelope.topic_id,
                                is_rpc=False,
                                cancellation_token=CancellationToken(),
                                message_id=envelope.message_id,
                            )
                            
                            temp_message = await handler.on_publish(message, message_context=temp_context)
                            _warn_if_none(temp_message, "on_publish")
                            
                            if temp_message is DropMessage or isinstance(temp_message, DropMessage):
                                logger.info(f"[PROCESS] Published message {envelope.message_id} dropped by intervention handler")
                                event_logger.info(
                                    MessageDroppedEvent(
                                        payload=self._try_serialize(message),
                                        sender=envelope.sender,
                                        receiver=envelope.topic_id,
                                        kind=MessageKind.PUBLISH,
                                    )
                                )
                                return
                            
                            message = temp_message
                            
                        except BaseException as e:
                            logger.error(f"[PROCESS] Intervention handler failed for publish: {e}", exc_info=True)
                            return
                
                responses: List[Awaitable[Any]] = []
                recipients = await self._subscription_manager.get_subscribed_recipients(envelope.topic_id)
                logger.info(f"[PROCESS] Found {len(recipients)} subscribers for topic {envelope.topic_id}")
                
                for agent_id in recipients:
                    # Avoid sending the message back to the sender
                    if envelope.sender is not None and agent_id == envelope.sender:
                        logger.debug(f"[PROCESS] Skipping sender {agent_id} in publish")
                        continue

                    sender_agent = (
                        await self._get_agent(envelope.sender) if envelope.sender is not None else None
                    )
                    sender_name = str(sender_agent.id) if sender_agent is not None else "Unknown"
                    logger.info(
                        f"[PROCESS] Calling message handler for {agent_id.type} published by {sender_name}"
                    )
                    
                    event_logger.info(
                        MessageEvent(
                            payload=self._try_serialize(message),
                            sender=envelope.sender,
                            receiver=None,
                            kind=MessageKind.PUBLISH,
                            delivery_stage=DeliveryStage.DELIVER,
                        )
                    )
                    
                    # Create fresh MessageContext for each recipient
                    message_context = MessageContext(
                        sender=envelope.sender,
                        topic_id=envelope.topic_id,
                        is_rpc=False,
                        cancellation_token=CancellationToken(),  # Fresh token for each recipient
                        message_id=envelope.message_id,
                    )
                    agent = await self._get_agent(agent_id)

                    async def _on_message(agent: Agent, message_context: MessageContext) -> Any:
                        with self._tracer_helper.trace_block(
                            "process",
                            agent.id,
                            parent=None,
                            attributes=await self._create_otel_attributes(
                                sender_agent_id=envelope.sender,
                                recipient_agent_id=agent.id,
                                message_context=message_context,
                                message=message,
                            ),
                        ):
                            with MessageHandlerContext.populate_context(agent.id):
                                try:
                                    logger.debug(f"[PROCESS] Invoking agent.on_message for subscriber {agent.id}")
                                    return await agent.on_message(
                                        message,
                                        ctx=message_context,
                                    )
                                except BaseException as e:
                                    logger.error(f"[PROCESS] Error processing publish message for {agent.id}: {e}", exc_info=True)
                                    event_logger.info(
                                        MessageHandlerExceptionEvent(
                                            payload=self._try_serialize(message),
                                            handling_agent=agent.id,
                                            exception=e,
                                        )
                                    )
                                    raise e

                    future = _on_message(agent, message_context)
                    responses.append(future)

                if responses:
                    logger.debug(f"[PROCESS] Waiting for {len(responses)} publish responses")
                    await asyncio.gather(*responses)
                    logger.debug(f"[PROCESS] All publish responses completed")
                
            except BaseException as e:
                logger.error(f"[PROCESS] Error in publish processing: {e}", exc_info=True)
                if not self._ignore_unhandled_handler_exceptions:
                    self._background_exception = e

    async def _process_response(self, envelope: TimeplusEnvelope) -> None:
        """Process response message by resolving pending future"""
        
        message_id = envelope.message_id
        logger.debug(f"[RESPONSE] Processing response for message {message_id}")
        
        if message_id not in self._pending_requests:
            logger.warning(f"[RESPONSE] Received response for unknown request: {message_id}")
            return
        
        pending = self._pending_requests.pop(message_id)
        logger.debug(f"[RESPONSE] Removed pending request {message_id}, remaining: {len(self._pending_requests)}")
        
        # Cancel timeout
        if pending.timeout_task:
            pending.timeout_task.cancel()
        
        if pending.future.done():
            logger.debug(f"[RESPONSE] Future for {message_id} already completed")
            return
        
        if envelope.is_error:
            # Handle error response
            logger.info(f"[RESPONSE] Processing error response for {message_id}")
            error_data = self._message_serializer.deserialize(envelope.message_payload)
            if isinstance(error_data, dict) and "error_type" in error_data:
                error_type = error_data["error_type"]
                error_message = error_data["error_message"]
                
                if error_type == "LookupError":
                    exception = LookupError(error_message)
                elif error_type == "TimeoutError":
                    exception = TimeoutError(error_message)
                elif error_type == "CancelledError":
                    exception = CancelledError(error_message)
                elif error_type == "MessageDroppedException":
                    exception = MessageDroppedException()
                else:
                    exception = RuntimeError(f"{error_type}: {error_message}")
                
                pending.future.set_exception(exception)
            else:
                pending.future.set_exception(RuntimeError("Unknown error response format"))
        else:
            # Handle successful response
            logger.debug(f"[RESPONSE] Processing successful response for {message_id}")
            response = self._message_serializer.deserialize(envelope.message_payload)
            pending.future.set_result(response)
        
        event_logger.info(
            MessageEvent(
                payload=envelope.message_payload,
                sender=envelope.sender,
                receiver=envelope.recipient,
                kind=MessageKind.RESPOND,
                delivery_stage=DeliveryStage.DELIVER,
            )
        )

    async def _process_next(self) -> None:
        """Process next message using simplified consumer with connection reuse"""
        
        if self._background_exception is not None:
            e = self._background_exception
            self._background_exception = None
            raise e

        if self._consumer is None:
            raise RuntimeError("Runtime not started - call start() first")

        try:
            # Use executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            def _poll_messages():
                """Poll messages using simplified consumer"""
                try:
                    return self._consumer.poll(timeout_ms=100)
                except Exception as e:
                    logger.error(f"[CONSUMER] Poll failed: {e}")
                    raise
            
            # Poll in executor to prevent blocking
            records = await loop.run_in_executor(None, _poll_messages)
            
            # Reset error count on successful poll
            self._consumer_error_count = 0
            
            if not records:
                await asyncio.sleep(0.01)
                return

            logger.debug(f"[CONSUMER] Received {sum(len(messages) for messages in records.values())} messages")

            for topic_partition, messages in records.items():
                for message in messages:
                    try:
                        envelope = TimeplusEnvelope.from_json(message.value)
                        logger.debug(f"[CONSUMER] Processing envelope: {envelope.message_type} {envelope.message_id}")
                        
                        if envelope.message_type == "send":
                            task = asyncio.create_task(self._process_send(envelope))
                            self._background_tasks.add(task)
                            task.add_done_callback(self._background_tasks.discard)
                            
                        elif envelope.message_type == "publish":
                            task = asyncio.create_task(self._process_publish(envelope))
                            self._background_tasks.add(task)
                            task.add_done_callback(self._background_tasks.discard)
                            
                        elif envelope.message_type == "response":
                            await self._process_response(envelope)
                        
                        else:
                            logger.warning(f"[CONSUMER] Unknown message type: {envelope.message_type}")
                            
                    except Exception as e:
                        logger.error(f"[CONSUMER] Error processing message: {e}", exc_info=True)
                        if not self._ignore_unhandled_handler_exceptions:
                            self._background_exception = e

        except Exception as e:
            self._consumer_error_count += 1
            logger.error(f"[CONSUMER] Consumer poll error (count: {self._consumer_error_count}): {e}", exc_info=True)
            
            # Exponential backoff for consumer errors
            backoff_delay = min(self._consumer_error_count * 0.5, 5.0)
            logger.info(f"[CONSUMER] Backing off for {backoff_delay}s")
            await asyncio.sleep(backoff_delay)
            
            if not self._ignore_unhandled_handler_exceptions:
                self._background_exception = e

        await asyncio.sleep(0)

    # State management methods - mirrored from SingleThreadedAgentRuntime
    async def save_state(self) -> Mapping[str, Any]:
        """Save the state of all instantiated agents"""
        state: Dict[str, Dict[str, Any]] = {}
        for agent_id in self._instantiated_agents:
            state[str(agent_id)] = dict(await (await self._get_agent(agent_id)).save_state())
        return state

    async def load_state(self, state: Mapping[str, Any]) -> None:
        """Load the state of all instantiated agents"""
        for agent_id_str in state:
            agent_id = AgentId.from_str(agent_id_str)
            if agent_id.type in self._known_agent_names:
                await (await self._get_agent(agent_id)).load_state(state[str(agent_id)])

    async def process_next(self) -> None:
        """Process the next message in the queue - for compatibility"""
        await self._process_next()

    def start(self) -> None:
        """Start the runtime message processing loop"""
        if self._run_context is not None:
            raise RuntimeError("Runtime is already started")
        
        logger.info("[RUNTIME] Starting TimeplusAgentRuntime")
        
        # Initialize Timeplus components
        self._initialize_timeplus_components()
        
        # Start processing loop
        self._run_context = RunContext(self)
        logger.info("[RUNTIME] TimeplusAgentRuntime started successfully")

    async def close(self) -> None:
        """Close the runtime and cleanup all resources"""
        logger.info("[RUNTIME] Closing TimeplusAgentRuntime")
        
        # Stop the runtime if it's running
        if self._run_context is not None:
            await self.stop()
        
        # Cancel all pending requests
        for pending in self._pending_requests.values():
            if pending.timeout_task:
                pending.timeout_task.cancel()
            if not pending.future.done():
                pending.future.set_exception(RuntimeError("Runtime is shutting down"))
        self._pending_requests.clear()
        
        # Close Timeplus components (wrappers don't hold persistent connections)
        if self._producer:
            try:
                logger.info("[TIMEPLUS] Closing producer wrapper")
                self._producer.close()
            except Exception as e:
                logger.warning(f"[TIMEPLUS] Error closing producer wrapper: {e}")
            self._producer = None
            
        if self._consumer:
            try:
                logger.info("[TIMEPLUS] Closing consumer wrapper")
                self._consumer.close()
            except Exception as e:
                logger.warning(f"[TIMEPLUS] Error closing consumer wrapper: {e}")
            self._consumer = None
        
        # Close all instantiated agents
        for agent_id in self._instantiated_agents:
            agent = await self._get_agent(agent_id)
            await agent.close()
        
        logger.info("[RUNTIME] TimeplusAgentRuntime closed")

    async def stop(self) -> None:
        """Immediately stop the runtime message processing loop"""
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")

        logger.info("[RUNTIME] Stopping TimeplusAgentRuntime")
        try:
            await self._run_context.stop()
        finally:
            self._run_context = None

    async def stop_when_idle(self) -> None:
        """Stop the runtime when there are no outstanding messages being processed"""
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")

        logger.info("[RUNTIME] Stopping TimeplusAgentRuntime when idle")
        try:
            await self._run_context.stop_when_idle()
        finally:
            self._run_context = None

    async def stop_when(self, condition: Callable[[], bool]) -> None:
        """Stop the runtime when the condition is met"""
        if self._run_context is None:
            raise RuntimeError("Runtime is not started")
        
        logger.info("[RUNTIME] Stopping TimeplusAgentRuntime when condition is met")
        await self._run_context.stop_when(condition)
        self._run_context = None

    # Agent management methods - mirrored from SingleThreadedAgentRuntime
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
        """Register an agent factory - mirrored from SingleThreadedAgentRuntime"""
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
        logger.info(f"[REGISTRY] Registered agent factory for type: {type.type}")
        return type

    async def register_agent_instance(
        self,
        agent_instance: Agent,
        agent_id: AgentId,
    ) -> AgentId:
        """Register a specific agent instance - mirrored from SingleThreadedAgentRuntime"""
        def agent_factory() -> Agent:
            raise RuntimeError(
                "Agent factory was invoked for an agent instance that was not registered. "
                "This is likely due to the agent type being incorrectly subscribed to a topic."
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
        logger.info(f"[REGISTRY] Registered agent instance: {agent_id}")
        return agent_id

    async def _invoke_agent_factory(
        self,
        agent_factory: Callable[[], T | Awaitable[T]] | Callable[[AgentRuntime, AgentId], T | Awaitable[T]],
        agent_id: AgentId,
    ) -> T:
        """Invoke agent factory - mirrored from SingleThreadedAgentRuntime"""
        with AgentInstantiationContext.populate_context((self, agent_id)):
            try:
                if len(inspect.signature(agent_factory).parameters) == 0:
                    factory_one = cast(Callable[[], T], agent_factory)
                    agent = factory_one()
                elif len(inspect.signature(agent_factory).parameters) == 2:
                    warnings.warn(
                        "Agent factories that take two arguments are deprecated. Use AgentInstantiationContext instead.",
                        DeprecationWarning,
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
        """Get or create agent instance - mirrored from SingleThreadedAgentRuntime"""
        if agent_id in self._instantiated_agents:
            return self._instantiated_agents[agent_id]

        if agent_id.type not in self._agent_factories:
            raise LookupError(f"Agent with name {agent_id.type} not found.")

        agent_factory = self._agent_factories[agent_id.type]
        agent = await self._invoke_agent_factory(agent_factory, agent_id)
        self._instantiated_agents[agent_id] = agent
        logger.debug(f"[REGISTRY] Created agent instance: {agent_id}")
        return agent

    async def try_get_underlying_agent_instance(self, id: AgentId, type: Type[T] = Agent) -> T:
        """Get underlying agent instance with type checking - mirrored from SingleThreadedAgentRuntime"""
        if id.type not in self._agent_factories:
            raise LookupError(f"Agent with name {id.type} not found.")

        agent_instance = await self._get_agent(id)

        if not isinstance(agent_instance, type):
            raise TypeError(
                f"Agent with name {id.type} is not of type {type.__name__}. "
                f"It is of type {type_func_alias(agent_instance).__name__}"
            )

        return agent_instance

    # Subscription management - mirrored from SingleThreadedAgentRuntime
    async def add_subscription(self, subscription: Subscription) -> None:
        await self._subscription_manager.add_subscription(subscription)
        logger.debug(f"[SUBSCRIPTION] Added subscription: {subscription}")

    async def remove_subscription(self, id: str) -> None:
        await self._subscription_manager.remove_subscription(id)
        logger.debug(f"[SUBSCRIPTION] Removed subscription: {id}")

    async def get(
        self, id_or_type: AgentId | AgentType | str, /, key: str = "default", *, lazy: bool = True
    ) -> AgentId:
        return await get_impl(
            id_or_type=id_or_type,
            key=key,
            lazy=lazy,
            instance_getter=self._get_agent,
        )

    # Serialization management - mirrored from SingleThreadedAgentRuntime
    def add_message_serializer(self, serializer: MessageSerializer[Any] | Sequence[MessageSerializer[Any]]) -> None:
        self._serialization_registry.add_serializer(serializer)

    def _try_serialize(self, message: Any) -> str:
        """Try to serialize message for logging - improved version with better fallback"""
        try:
            # First try the registry serialization
            type_name = self._serialization_registry.type_name(message)
            serialized_bytes = self._serialization_registry.serialize(
                message, type_name=type_name, data_content_type=JSON_DATA_CONTENT_TYPE
            )
            
            if isinstance(serialized_bytes, bytes):
                return serialized_bytes.decode("utf-8")
            else:
                return str(serialized_bytes)
                
        except (ValueError, AttributeError, TypeError) as registry_error:
            # Registry failed, try our enhanced serializer as fallback
            logger.debug(f"[SERIALIZE] Registry failed ({registry_error}), using enhanced serializer")
            try:
                return self._message_serializer.serialize(message)
            except Exception as enhanced_error:
                logger.debug(f"[SERIALIZE] Enhanced serializer failed ({enhanced_error}), using basic fallback")
                
                # Final fallback - create a basic representation
                try:
                    if hasattr(message, '__dict__'):
                        # Try to serialize object attributes
                        obj_dict = {}
                        for key, value in message.__dict__.items():
                            try:
                                # Test if the value is JSON serializable
                                json.dumps(value)
                                obj_dict[key] = value
                            except:
                                obj_dict[key] = str(value)
                        
                        return json.dumps({
                            "type": type(message).__name__,
                            "module": type(message).__module__,
                            "data": obj_dict
                        })
                    else:
                        # For primitives or other types
                        return json.dumps({
                            "type": type(message).__name__,
                            "value": str(message)
                        })
                except Exception:
                    # Last resort
                    return f"{{\"type\": \"{type(message).__name__}\", \"repr\": \"{repr(message)}\"}}"