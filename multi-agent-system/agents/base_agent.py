"""Abstract base agent — all specialized agents inherit from this."""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.message_bus import Message, MessageBus, MessageType
from core.task_queue import Task, TaskQueue, TaskStatus
from core.knowledge_base import KnowledgeBase


class BaseAgent(ABC):
    """Abstract base for all agents.

    Provides:
    - Message loop with inbox queue
    - Heartbeat mechanism
    - Capability registration
    - Tool invocation
    - Status reporting
    """

    def __init__(
        self,
        name: str,
        message_bus: MessageBus,
        task_queue: TaskQueue,
        knowledge_base: KnowledgeBase,
        capabilities: List[str] = None,
    ):
        self.name = name
        self._bus = message_bus
        self._task_queue = task_queue
        self._kb = knowledge_base
        self._capabilities = capabilities or []
        self._inbox: asyncio.Queue = None
        self._running = False
        self._heartbeat_interval = 10.0
        self._status: Dict[str, Any] = {
            "state": "initialized",
            "last_heartbeat": "",
            "tasks_processed": 0,
            "errors": 0,
        }

    async def start(self):
        """Start the agent's message processing loop."""
        self._inbox = self._bus.register_agent(self.name)
        self._running = True

        # Register capabilities
        for cap in self._capabilities:
            await self._kb.register_capability(self.name, cap)

        await self._kb.record_event("agent_started", {
            "agent": self.name,
            "capabilities": self._capabilities,
        })

        await self.on_start()

        # Run main loop and heartbeat concurrently
        await asyncio.gather(
            self._message_loop(),
            self._heartbeat_loop(),
            self._task_loop(),
        )

    async def stop(self):
        """Stop the agent."""
        self._running = False
        self._bus.unregister_agent(self.name)
        await self._kb.remove_agent(self.name)
        await self._kb.record_event("agent_stopped", {"agent": self.name})
        await self.on_stop()

    async def _message_loop(self):
        """Main message processing loop."""
        while self._running:
            try:
                message = await asyncio.wait_for(self._inbox.get(), timeout=1.0)
                await self._handle_message(message)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._status["errors"] += 1
                await self._kb.record_event("agent_error", {
                    "agent": self.name,
                    "error": str(e),
                })

    async def _heartbeat_loop(self):
        """Send periodic heartbeats."""
        while self._running:
            await asyncio.sleep(self._heartbeat_interval)
            if not self._running:
                break
            self._status["last_heartbeat"] = datetime.now().isoformat()
            await self._bus.publish(Message(
                type=MessageType.HEARTBEAT,
                topic="system.heartbeat",
                sender=self.name,
                payload={
                    "agent": self.name,
                    "status": self._status,
                    "capabilities": self._capabilities,
                },
            ))
            await self._kb.record_metric(f"agent.{self.name}.heartbeat", time.time())

    async def _task_loop(self):
        """Process tasks assigned to this agent."""
        while self._running:
            try:
                task = await asyncio.get_event_loop().run_in_executor(
                    None, self._task_queue.get_next_task, self.name
                )
                if task:
                    await self._process_task(task)
                else:
                    await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._status["errors"] += 1
                await self._kb.record_event("task_loop_error", {
                    "agent": self.name,
                    "error": str(e),
                })

    async def _handle_message(self, message: Message):
        """Route incoming messages to appropriate handlers."""
        if message.type == MessageType.QUERY:
            response = await self.on_query(message)
            if response:
                await self._bus.reply(message, response)
        elif message.type == MessageType.COMMAND:
            await self.on_command(message)
        elif message.type == MessageType.EVENT:
            await self.on_event(message)
        elif message.type == MessageType.HEARTBEAT:
            await self.on_heartbeat(message)

    async def _process_task(self, task: Task):
        """Process an assigned task."""
        task.status = TaskStatus.RUNNING
        self._status["tasks_processed"] += 1

        await self._kb.record_event("task_started", {
            "agent": self.name,
            "task_id": task.id,
            "task_name": task.name,
        })

        try:
            result = await self.execute_task(task)
            self._task_queue.mark_completed(task.id, result)
            await self._kb.record_event("task_completed", {
                "agent": self.name,
                "task_id": task.id,
                "result": result,
            })
            # Notify completion
            await self._bus.publish(Message(
                type=MessageType.EVENT,
                topic="task.completed",
                sender=self.name,
                payload={"task_id": task.id, "result": result},
            ))
        except Exception as e:
            self._task_queue.mark_failed(task.id, str(e))
            await self._kb.record_event("task_failed", {
                "agent": self.name,
                "task_id": task.id,
                "error": str(e),
            })

    @abstractmethod
    async def execute_task(self, task: Task) -> dict:
        """Execute a task. Must be implemented by subclasses."""
        ...

    # ---- Hook methods ----

    async def on_start(self):
        """Called when agent starts."""
        pass

    async def on_stop(self):
        """Called when agent stops."""
        pass

    async def on_query(self, message: Message) -> Optional[dict]:
        """Handle a query (request/reply). Override to respond."""
        return None

    async def on_command(self, message: Message):
        """Handle a command. Override to take action."""
        pass

    async def on_event(self, message: Message):
        """Handle an event notification. Override to react."""
        pass

    async def on_heartbeat(self, message: Message):
        """Handle a heartbeat from another agent."""
        pass

    def get_status(self) -> dict:
        return dict(self._status)
