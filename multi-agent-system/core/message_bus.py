"""Async message bus for inter-agent communication.

Supports three patterns:
- pub/sub: agents subscribe to topics
- request/reply: direct request with response
- broadcast: send to all agents
"""

import asyncio
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class MessageType(Enum):
    COMMAND = "command"
    EVENT = "event"
    QUERY = "query"
    RESPONSE = "response"
    ERROR = "error"
    HEARTBEAT = "heartbeat"


@dataclass
class Message:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    type: MessageType = MessageType.EVENT
    topic: str = ""
    sender: str = ""
    recipient: str = ""  # empty = broadcast to subscribers
    payload: Dict[str, Any] = field(default_factory=dict)
    correlation_id: str = ""  # for request/reply
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": self.type.value,
            "topic": self.topic,
            "sender": self.sender,
            "recipient": self.recipient,
            "payload": self.payload,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Message":
        return cls(
            id=data.get("id", str(uuid.uuid4())[:8]),
            type=MessageType(data.get("type", "event")),
            topic=data.get("topic", ""),
            sender=data.get("sender", ""),
            recipient=data.get("recipient", ""),
            payload=data.get("payload", {}),
            correlation_id=data.get("correlation_id", ""),
            timestamp=data.get("timestamp", datetime.now().isoformat()),
        )


class MessageBus:
    """Async publish/subscribe message bus."""

    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._message_log: List[Message] = []
        self._max_log_size = 1000
        self._agent_queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    def register_agent(self, agent_name: str) -> asyncio.Queue:
        """Register an agent and return its message queue."""
        queue = asyncio.Queue()
        self._agent_queues[agent_name] = queue
        return queue

    def unregister_agent(self, agent_name: str):
        """Remove an agent from the bus."""
        self._agent_queues.pop(agent_name, None)
        for topic in list(self._subscribers.keys()):
            self._subscribers[topic] = [
                cb for cb in self._subscribers[topic] if getattr(cb, "__agent__", "") != agent_name
            ]

    def subscribe(self, topic: str, callback: Callable, agent_name: str = ""):
        """Subscribe to a topic with a callback."""
        callback.__agent__ = agent_name
        self._subscribers[topic].append(callback)

    def unsubscribe(self, topic: str, callback: Callable):
        """Unsubscribe from a topic."""
        if topic in self._subscribers:
            self._subscribers[topic] = [cb for cb in self._subscribers[topic] if cb != callback]

    async def publish(self, message: Message):
        """Publish a message to a topic. All subscribers receive it."""
        self._log_message(message)

        # Deliver to direct recipient queue
        if message.recipient and message.recipient in self._agent_queues:
            await self._agent_queues[message.recipient].put(message)

        # Deliver to topic subscribers
        if message.topic in self._subscribers:
            for callback in self._subscribers[message.topic]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(message)
                    else:
                        callback(message)
                except Exception as e:
                    print(f"[MessageBus] Error in subscriber callback: {e}")

        # Deliver to all topic subscribers via agent queues (fallback)
        topic = f"topic:{message.topic}"
        if topic in self._subscribers:
            for callback in self._subscribers[topic]:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(message)
                    else:
                        callback(message)
                except Exception as e:
                    print(f"[MessageBus] Error in topic subscriber: {e}")

    async def request(self, message: Message, timeout: float = 30.0) -> Message:
        """Send a request and wait for a response."""
        correlation_id = message.correlation_id or str(uuid.uuid4())[:8]
        message.correlation_id = correlation_id
        message.type = MessageType.QUERY

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[correlation_id] = future

        await self.publish(message)

        try:
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        except asyncio.TimeoutError:
            self._pending_requests.pop(correlation_id, None)
            return Message(
                type=MessageType.ERROR,
                topic=message.topic,
                sender="message_bus",
                recipient=message.sender,
                payload={"error": "Request timed out", "timeout": timeout},
                correlation_id=correlation_id,
            )

    async def reply(self, original: Message, response_payload: dict):
        """Send a reply to a request."""
        response = Message(
            type=MessageType.RESPONSE,
            topic=original.topic,
            sender=original.recipient,
            recipient=original.sender,
            payload=response_payload,
            correlation_id=original.correlation_id,
        )

        # Resolve pending future
        if original.correlation_id in self._pending_requests:
            self._pending_requests[original.correlation_id].set_result(response)
            del self._pending_requests[original.correlation_id]

        # Also send to agent queue
        if response.recipient in self._agent_queues:
            await self._agent_queues[response.recipient].put(response)

    async def broadcast(self, message: Message):
        """Send a message to all registered agents."""
        self._log_message(message)
        for agent_name, queue in self._agent_queues.items():
            if agent_name != message.sender:
                await queue.put(message)

    def _log_message(self, message: Message):
        self._message_log.append(message)
        if len(self._message_log) > self._max_log_size:
            self._message_log = self._message_log[-self._max_log_size:]

    def get_recent_messages(self, limit: int = 50) -> List[dict]:
        return [m.to_dict() for m in self._message_log[-limit:]]

    def get_stats(self) -> dict:
        return {
            "agent_count": len(self._agent_queues),
            "subscriber_topics": list(self._subscribers.keys()),
            "pending_requests": len(self._pending_requests),
            "total_messages": len(self._message_log),
        }
