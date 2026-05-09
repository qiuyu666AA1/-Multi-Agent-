"""Workflow orchestrator — coordinates multi-agent task execution.

The Orchestrator manages the lifecycle of complex workflows that span
multiple agents. It handles task decomposition, dependency resolution,
parallel execution, and result aggregation.
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from .message_bus import Message, MessageBus, MessageType
from .task_queue import Task, TaskPriority, TaskQueue, TaskStatus
from .knowledge_base import KnowledgeBase


class WorkflowStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowStep:
    name: str
    description: str
    agent_type: str  # which type of agent should handle this
    action: str  # what action to perform
    payload: dict = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)  # step names
    timeout: float = 60.0
    retry: int = 1


@dataclass
class Workflow:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    description: str = ""
    status: WorkflowStatus = WorkflowStatus.CREATED
    steps: List[WorkflowStep] = field(default_factory=list)
    task_ids: Dict[str, str] = field(default_factory=dict)  # step_name -> task_id
    step_results: Dict[str, dict] = field(default_factory=dict)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: str = ""
    error: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status.value,
            "steps": [
                {
                    "name": s.name,
                    "description": s.description,
                    "agent_type": s.agent_type,
                    "action": s.action,
                    "dependencies": s.dependencies,
                }
                for s in self.steps
            ],
            "step_results": self.step_results,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
        }


class Orchestrator:
    """Central workflow orchestrator."""

    def __init__(self, message_bus: MessageBus, task_queue: TaskQueue, knowledge_base: KnowledgeBase):
        self._bus = message_bus
        self._task_queue = task_queue
        self._kb = knowledge_base
        self._workflows: Dict[str, Workflow] = {}
        self._active_workflows: Dict[str, asyncio.Task] = {}
        self._running = False

    # ---- Predefined Workflows ----

    def create_system_health_check_workflow(self) -> Workflow:
        """Create a system health check workflow."""
        return Workflow(
            name="System Health Check",
            description="Comprehensive system health assessment",
            steps=[
                WorkflowStep(
                    name="check_disk",
                    description="Check disk usage",
                    agent_type="executor",
                    action="check_disk",
                    payload={"threshold": 90},
                ),
                WorkflowStep(
                    name="check_memory",
                    description="Check memory usage",
                    agent_type="executor",
                    action="check_memory",
                    payload={"threshold": 90},
                ),
                WorkflowStep(
                    name="check_processes",
                    description="Check critical processes",
                    agent_type="executor",
                    action="check_processes",
                    payload={"processes": ["python", "nginx", "postgres"]},
                ),
                WorkflowStep(
                    name="analyze_health",
                    description="Analyze health metrics",
                    agent_type="analyst",
                    action="analyze_health",
                    dependencies=["check_disk", "check_memory", "check_processes"],
                ),
                WorkflowStep(
                    name="notify",
                    description="Send notification if issues found",
                    agent_type="executor",
                    action="notify",
                    dependencies=["analyze_health"],
                ),
            ],
        )

    def create_data_analysis_workflow(self, data_source: str) -> Workflow:
        """Create a data analysis workflow."""
        return Workflow(
            name=f"Data Analysis: {data_source}",
            description=f"Analyze data from {data_source}",
            steps=[
                WorkflowStep(
                    name="collect_data",
                    description=f"Collect data from {data_source}",
                    agent_type="executor",
                    action="collect_data",
                    payload={"source": data_source},
                ),
                WorkflowStep(
                    name="clean_data",
                    description="Clean and normalize data",
                    agent_type="analyst",
                    action="clean_data",
                    dependencies=["collect_data"],
                ),
                WorkflowStep(
                    name="analyze",
                    description="Run statistical analysis",
                    agent_type="analyst",
                    action="run_analysis",
                    dependencies=["clean_data"],
                ),
                WorkflowStep(
                    name="generate_report",
                    description="Generate analysis report",
                    agent_type="analyst",
                    action="generate_report",
                    dependencies=["analyze"],
                ),
            ],
        )

    def create_incident_response_workflow(self, incident_type: str, severity: str) -> Workflow:
        """Create an incident response workflow."""
        return Workflow(
            name=f"Incident Response: {incident_type}",
            description=f"Automated response to {incident_type} incident",
            steps=[
                WorkflowStep(
                    name="assess",
                    description="Assess incident scope and impact",
                    agent_type="analyst",
                    action="assess_incident",
                    payload={"type": incident_type, "severity": severity},
                ),
                WorkflowStep(
                    name="contain",
                    description="Contain the incident",
                    agent_type="executor",
                    action="contain_incident",
                    dependencies=["assess"],
                    payload={"type": incident_type},
                ),
                WorkflowStep(
                    name="remediate",
                    description="Apply remediation steps",
                    agent_type="executor",
                    action="remediate",
                    dependencies=["contain"],
                ),
                WorkflowStep(
                    name="verify",
                    description="Verify system stability",
                    agent_type="monitor",
                    action="verify_stability",
                    dependencies=["remediate"],
                ),
                WorkflowStep(
                    name="postmortem",
                    description="Generate postmortem report",
                    agent_type="analyst",
                    action="generate_postmortem",
                    dependencies=["verify"],
                ),
            ],
        )

    # ---- Workflow Management ----

    async def submit_workflow(self, workflow: Workflow) -> str:
        """Submit a workflow for execution."""
        self._workflows[workflow.id] = workflow
        workflow.status = WorkflowStatus.RUNNING

        await self._kb.record_event("workflow_submitted", {
            "workflow_id": workflow.id,
            "workflow_name": workflow.name,
            "step_count": len(workflow.steps),
        })

        # Start workflow execution in background
        task = asyncio.create_task(self._execute_workflow(workflow))
        self._active_workflows[workflow.id] = task
        return workflow.id

    async def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return False

        workflow.status = WorkflowStatus.CANCELLED
        workflow.completed_at = datetime.now().isoformat()

        # Cancel the background task
        if workflow_id in self._active_workflows:
            self._active_workflows[workflow_id].cancel()
            del self._active_workflows[workflow_id]

        # Cancel all associated tasks
        for task_id in workflow.task_ids.values():
            self._task_queue.mark_cancelled(task_id)

        await self._kb.record_event("workflow_cancelled", {"workflow_id": workflow_id})
        return True

    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        return self._workflows.get(workflow_id)

    def get_all_workflows(self) -> List[Workflow]:
        return list(self._workflows.values())

    def get_workflow_progress(self, workflow_id: str) -> dict:
        """Get detailed progress for a workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return {}

        steps_progress = []
        for step in workflow.steps:
            task_id = workflow.task_ids.get(step.name)
            task = self._task_queue.get_task(task_id) if task_id else None
            steps_progress.append({
                "name": step.name,
                "agent_type": step.agent_type,
                "status": task.status.value if task else "pending",
                "result": task.result if task else {},
            })

        return {
            "workflow_id": workflow.id,
            "name": workflow.name,
            "status": workflow.status.value,
            "total_steps": len(workflow.steps),
            "completed_steps": sum(
                1 for s in steps_progress if s["status"] in ("completed", "failed")
            ),
            "steps": steps_progress,
        }

    # ---- Internal Execution ----

    async def _execute_workflow(self, workflow: Workflow):
        """Execute all steps of a workflow with dependency ordering."""
        try:
            # Create tasks for all steps
            step_tasks: Dict[str, Task] = {}
            for step in workflow.steps:
                task = Task(
                    name=f"[{workflow.name}] {step.name}",
                    description=step.description,
                    priority=TaskPriority.HIGH,
                    payload={
                        "workflow_id": workflow.id,
                        "step_name": step.name,
                        "agent_type": step.agent_type,
                        "action": step.action,
                        **step.payload,
                    },
                    assigned_agent=step.agent_type,
                )
                self._task_queue.add_task(task)
                workflow.task_ids[step.name] = task.id
                step_tasks[step.name] = task

                # Publish task assignment message
                await self._bus.publish(Message(
                    type=MessageType.COMMAND,
                    topic=f"agent.{step.agent_type}",
                    sender="orchestrator",
                    recipient=step.agent_type,
                    payload={
                        "command": "execute_step",
                        "task_id": task.id,
                        "workflow_id": workflow.id,
                        "step": {
                            "name": step.name,
                            "action": step.action,
                            "payload": step.payload,
                        },
                    },
                ))

            # Wait for all tasks to complete
            completed = set()
            failed = set()
            total = len(workflow.steps)

            while len(completed) + len(failed) < total:
                for step in workflow.steps:
                    name = step.name
                    if name in completed or name in failed:
                        continue

                    task_id = workflow.task_ids.get(name)
                    task = self._task_queue.get_task(task_id)

                    if task and task.status == TaskStatus.COMPLETED:
                        workflow.step_results[name] = task.result
                        completed.add(name)
                        await self._kb.record_event("step_completed", {
                            "workflow_id": workflow.id,
                            "step": name,
                            "result": task.result,
                        })
                    elif task and task.status == TaskStatus.FAILED:
                        # Check if we should retry
                        if step.retry > 0:
                            task.status = TaskStatus.QUEUED
                            step.retry -= 1
                        else:
                            failed.add(name)
                            await self._kb.record_event("step_failed", {
                                "workflow_id": workflow.id,
                                "step": name,
                                "error": task.error_message,
                            })

                await asyncio.sleep(0.5)

            # Finalize workflow
            if failed:
                workflow.status = WorkflowStatus.FAILED
                workflow.error = f"Failed steps: {', '.join(failed)}"
            else:
                workflow.status = WorkflowStatus.COMPLETED
                # Aggregate results
                await self._aggregate_results(workflow)

            workflow.completed_at = datetime.now().isoformat()

            await self._kb.record_event("workflow_completed", {
                "workflow_id": workflow.id,
                "status": workflow.status.value,
                "failed_steps": list(failed),
            })

            # Notify completion
            await self._bus.broadcast(Message(
                type=MessageType.EVENT,
                topic="workflow.completed",
                sender="orchestrator",
                payload={
                    "workflow_id": workflow.id,
                    "workflow_name": workflow.name,
                    "status": workflow.status.value,
                    "completed": len(completed),
                    "failed": len(failed),
                },
            ))

        except asyncio.CancelledError:
            workflow.status = WorkflowStatus.CANCELLED
            workflow.completed_at = datetime.now().isoformat()
            raise

    async def _aggregate_results(self, workflow: Workflow):
        """Aggregate step results into a workflow summary."""
        summary = {
            "workflow_name": workflow.name,
            "total_steps": len(workflow.steps),
            "results": workflow.step_results,
        }
        await self._kb.set_fact(f"workflow:{workflow.id}:summary", summary)

        # Store in collection for reporting
        await self._kb.add_to_collection("completed_workflows", {
            "id": workflow.id,
            "name": workflow.name,
            "completed_at": workflow.completed_at,
            "step_count": len(workflow.steps),
        })
