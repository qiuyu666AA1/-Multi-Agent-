"""Coordinator Agent — task decomposition, routing, and workflow management.

This is the "front desk" agent that:
1. Receives user requests via the message bus
2. Decomposes complex requests into subtasks
3. Routes tasks to the appropriate specialized agents
4. Tracks overall progress and aggregates results
5. Handles priority and conflict resolution
"""

import asyncio
import uuid
from typing import Any, Dict, List, Optional

from core.message_bus import Message, MessageBus, MessageType
from core.task_queue import Task, TaskPriority, TaskQueue, TaskStatus
from core.knowledge_base import KnowledgeBase
from agents.base_agent import BaseAgent


class CoordinatorAgent(BaseAgent):
    """Central coordinator that orchestrates multi-agent workflows."""

    def __init__(self, message_bus: MessageBus, task_queue: TaskQueue, knowledge_base: KnowledgeBase):
        super().__init__(
            name="coordinator",
            message_bus=message_bus,
            task_queue=task_queue,
            knowledge_base=knowledge_base,
            capabilities=[
                "task_decomposition",
                "agent_routing",
                "workflow_management",
                "priority_resolution",
                "result_aggregation",
            ],
        )
        self._active_sessions: Dict[str, dict] = {}
        self._decomposition_rules = self._build_decomposition_rules()

    def _build_decomposition_rules(self) -> dict:
        """Build rules for decomposing user requests into agent tasks."""
        return {
            "health_check": {
                "agent": "executor",
                "tasks": [
                    {"name": "Check system resources", "agent": "executor", "action": "check_resources"},
                    {"name": "Check service status", "agent": "monitor", "action": "check_services"},
                    {"name": "Generate health report", "agent": "analyst", "action": "health_report"},
                ],
            },
            "data_analysis": {
                "agent": "analyst",
                "tasks": [
                    {"name": "Collect data", "agent": "executor", "action": "collect_data"},
                    {"name": "Analyze data", "agent": "analyst", "action": "analyze_data"},
                    {"name": "Generate report", "agent": "analyst", "action": "generate_report"},
                ],
            },
            "deploy": {
                "agent": "executor",
                "tasks": [
                    {"name": "Pre-deploy checks", "agent": "monitor", "action": "pre_deploy_check"},
                    {"name": "Execute deploy", "agent": "executor", "action": "deploy"},
                    {"name": "Post-deploy verify", "agent": "monitor", "action": "post_deploy_verify"},
                ],
            },
            "incident_response": {
                "agent": "executor",
                "tasks": [
                    {"name": "Assess incident", "agent": "analyst", "action": "assess_incident"},
                    {"name": "Contain incident", "agent": "executor", "action": "contain"},
                    {"name": "Remediate", "agent": "executor", "action": "remediate"},
                    {"name": "Verify resolution", "agent": "monitor", "action": "verify"},
                ],
            },
            "report": {
                "agent": "analyst",
                "tasks": [
                    {"name": "Gather data", "agent": "executor", "action": "gather_data"},
                    {"name": "Analyze trends", "agent": "analyst", "action": "analyze_trends"},
                    {"name": "Build report", "agent": "analyst", "action": "build_report"},
                ],
            },
            "maintenance": {
                "agent": "executor",
                "tasks": [
                    {"name": "Backup data", "agent": "executor", "action": "backup"},
                    {"name": "Cleanup logs", "agent": "executor", "action": "cleanup_logs"},
                    {"name": "Verify backup", "agent": "monitor", "action": "verify_backup"},
                    {"name": "Maintenance report", "agent": "analyst", "action": "maintenance_report"},
                ],
            },
        }

    async def on_command(self, message: Message):
        """Handle incoming commands — these are typically user requests."""
        command = message.payload.get("command", "")
        params = message.payload.get("params", {})

        if command == "process_request":
            await self._process_user_request(message, params)
        elif command == "get_status":
            await self._send_status(message.sender)
        elif command == "cancel_request":
            await self._cancel_request(params.get("session_id", ""), message.sender)

    async def on_query(self, message: Message) -> Optional[dict]:
        """Handle queries about system state."""
        query_type = message.payload.get("type", "")
        if query_type == "agent_status":
            agents = await self._kb.get_all_facts()
            return {
                "coordinator": self.get_status(),
                "knowledge_base_stats": await self._kb.get_stats(),
            }
        elif query_type == "task_status":
            task_id = message.payload.get("task_id", "")
            task = self._task_queue.get_task(task_id)
            return task.to_dict() if task else {"error": "Task not found"}
        return None

    async def on_event(self, message: Message):
        """React to events from other agents."""
        if message.topic == "task.completed":
            task_id = message.payload.get("task_id", "")
            # Update session progress
            for session_id, session in self._active_sessions.items():
                if task_id in session.get("task_ids", []):
                    session["completed"] += 1
                    await self._check_session_complete(session_id)

    async def execute_task(self, task: Task) -> dict:
        """Execute a coordinator-specific task."""
        action = task.payload.get("action", "")
        if action == "decompose":
            return await self._decompose_request(task.payload)
        elif action == "aggregate":
            return await self._aggregate_results(task.payload)
        elif action == "route":
            return await self._route_task(task.payload)
        return {"status": "ok", "action": action}

    async def _process_user_request(self, message: Message, params: dict):
        """Process a user request — decompose and distribute."""
        request_type = params.get("type", "health_check")
        priority = params.get("priority", "normal")
        session_id = str(uuid.uuid4())[:8]

        # Map priority
        priority_map = {
            "critical": TaskPriority.CRITICAL,
            "high": TaskPriority.HIGH,
            "normal": TaskPriority.NORMAL,
            "low": TaskPriority.LOW,
        }
        task_priority = priority_map.get(priority, TaskPriority.NORMAL)

        # Get decomposition rules
        rules = self._decomposition_rules.get(request_type, self._decomposition_rules["health_check"])

        # Create session
        self._active_sessions[session_id] = {
            "request_type": request_type,
            "total_tasks": len(rules["tasks"]),
            "completed": 0,
            "task_ids": [],
            "results": {},
            "requester": message.sender,
        }

        # Create and assign tasks
        prev_task_id = None
        for i, task_def in enumerate(rules["tasks"]):
            task = self._task_queue.create_task(
                name=task_def["name"],
                description=f"Auto-generated from {request_type} request",
                priority=task_priority,
                payload={
                    "session_id": session_id,
                    "request_type": request_type,
                    "action": task_def["action"],
                    "params": params,
                    "step": i + 1,
                },
                assigned_agent=task_def["agent"],
                created_by=self.name,
            )

            self._active_sessions[session_id]["task_ids"].append(task.id)

            # Notify the target agent
            await self._bus.publish(Message(
                type=MessageType.COMMAND,
                topic=f"agent.{task_def['agent']}",
                sender=self.name,
                recipient=task_def["agent"],
                payload={
                    "command": "execute_step",
                    "task_id": task.id,
                    "session_id": session_id,
                    "step": {
                        "name": task_def["name"],
                        "action": task_def["action"],
                        "payload": params,
                    },
                },
            ))

        # Notify the requester
        await self._bus.publish(Message(
            type=MessageType.RESPONSE,
            topic="request.processed",
            sender=self.name,
            recipient=message.sender,
            payload={
                "session_id": session_id,
                "request_type": request_type,
                "task_count": len(rules["tasks"]),
                "status": "processing",
            },
        ))

        await self._kb.record_event("request_processed", {
            "session_id": session_id,
            "type": request_type,
            "task_count": len(rules["tasks"]),
        })

    async def _check_session_complete(self, session_id: str):
        """Check if all tasks in a session are done and aggregate."""
        session = self._active_sessions.get(session_id)
        if not session:
            return

        if session["completed"] >= session["total_tasks"]:
            # Collect all task results
            results = {}
            all_success = True
            for task_id in session["task_ids"]:
                task = self._task_queue.get_task(task_id)
                if task:
                    results[task.name] = {
                        "status": task.status.value,
                        "result": task.result,
                        "error": task.error_message,
                    }
                    if task.status != TaskStatus.COMPLETED:
                        all_success = False

            # Send completion notification
            await self._bus.publish(Message(
                type=MessageType.EVENT,
                topic="session.completed",
                sender=self.name,
                recipient=session["requester"],
                payload={
                    "session_id": session_id,
                    "request_type": session["request_type"],
                    "success": all_success,
                    "results": results,
                },
            ))

            await self._kb.record_event("session_completed", {
                "session_id": session_id,
                "success": all_success,
                "results": results,
            })

            # Clean up
            del self._active_sessions[session_id]

    async def _decompose_request(self, payload: dict) -> dict:
        """Decompose a request into subtasks."""
        request_type = payload.get("type", "")
        rules = self._decomposition_rules.get(request_type, {})
        return {
            "request_type": request_type,
            "subtasks": rules.get("tasks", []),
            "estimated_duration": len(rules.get("tasks", [])) * 5,
        }

    async def _aggregate_results(self, payload: dict) -> dict:
        """Aggregate results from multiple tasks."""
        task_ids = payload.get("task_ids", [])
        results = {}
        for task_id in task_ids:
            task = self._task_queue.get_task(task_id)
            if task:
                results[task.name] = task.result
        return {"aggregated": results}

    async def _route_task(self, payload: dict) -> dict:
        """Route a task to the appropriate agent."""
        action = payload.get("action", "")
        # Simple routing logic — can be extended with ML-based routing
        routing_map = {
            "check": "monitor",
            "analyze": "analyst",
            "execute": "executor",
            "deploy": "executor",
            "backup": "executor",
            "report": "analyst",
        }
        for key, agent in routing_map.items():
            if action.startswith(key):
                return {"agent": agent, "action": action}
        return {"agent": "executor", "action": action}

    async def _send_status(self, requester: str):
        """Send current coordinator status."""
        queue_stats = self._task_queue.get_stats()
        kb_stats = await self._kb.get_stats()

        await self._bus.publish(Message(
            type=MessageType.RESPONSE,
            topic="status",
            sender=self.name,
            recipient=requester,
            payload={
                "coordinator_status": self.get_status(),
                "active_sessions": len(self._active_sessions),
                "task_queue": queue_stats,
                "knowledge_base": kb_stats,
            },
        ))

    async def _cancel_request(self, session_id: str, requester: str):
        """Cancel all tasks in a session."""
        session = self._active_sessions.pop(session_id, None)
        if session:
            for task_id in session["task_ids"]:
                self._task_queue.mark_cancelled(task_id)
            await self._kb.record_event("session_cancelled", {"session_id": session_id})
