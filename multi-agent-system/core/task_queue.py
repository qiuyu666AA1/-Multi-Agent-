"""Priority-based task queue with dependency tracking."""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class TaskStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


class TaskPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Task:
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = ""
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING
    priority: TaskPriority = TaskPriority.NORMAL
    assigned_agent: str = ""
    created_by: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    result: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)  # task IDs that must complete first
    subtasks: List[str] = field(default_factory=list)
    parent_task: str = ""
    tags: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    started_at: str = ""
    completed_at: str = ""
    error_message: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "status": self.status.value,
            "priority": self.priority.name,
            "assigned_agent": self.assigned_agent,
            "created_by": self.created_by,
            "payload": self.payload,
            "result": self.result,
            "dependencies": self.dependencies,
            "subtasks": self.subtasks,
            "parent_task": self.parent_task,
            "tags": self.tags,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error_message": self.error_message,
        }

    @property
    def is_ready(self) -> bool:
        return self.status == TaskStatus.PENDING and not self.dependencies

    @property
    def duration_ms(self) -> float:
        if self.started_at and self.completed_at:
            start = datetime.fromisoformat(self.started_at)
            end = datetime.fromisoformat(self.completed_at)
            return (end - start).total_seconds() * 1000
        return 0


class TaskQueue:
    """Priority queue for tasks with dependency resolution."""

    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self._queues: Dict[TaskPriority, List[str]] = {p: [] for p in TaskPriority}
        self._lock = asyncio.Lock()
        self._completion_events: Dict[str, asyncio.Event] = {}

    def add_task(self, task: Task) -> str:
        """Add a task to the queue. Returns task ID."""
        self._tasks[task.id] = task
        task.status = TaskStatus.QUEUED
        self._queues[task.priority].append(task.id)
        self._completion_events[task.id] = asyncio.Event()
        return task.id

    def create_task(
        self,
        name: str,
        description: str = "",
        priority: TaskPriority = TaskPriority.NORMAL,
        payload: dict = None,
        dependencies: List[str] = None,
        tags: List[str] = None,
        created_by: str = "",
    ) -> Task:
        """Create and enqueue a new task."""
        task = Task(
            name=name,
            description=description,
            priority=priority,
            payload=payload or {},
            dependencies=dependencies or [],
            tags=tags or [],
            created_by=created_by,
        )
        self.add_task(task)
        return task

    def get_next_task(self, agent_name: str = "") -> Optional[Task]:
        """Get the highest priority ready task, optionally for a specific agent."""
        for priority in sorted(TaskPriority, key=lambda p: p.value):
            queue = self._queues[priority]
            for task_id in list(queue):
                task = self._tasks.get(task_id)
                if not task:
                    continue
                if task.status != TaskStatus.QUEUED:
                    continue
                # Check dependencies
                if task.dependencies:
                    deps_met = all(
                        self._tasks.get(dep_id, Task()).status == TaskStatus.COMPLETED
                        for dep_id in task.dependencies
                    )
                    if not deps_met:
                        task.status = TaskStatus.BLOCKED
                        queue.remove(task_id)
                        continue
                # Check agent assignment
                if agent_name and task.assigned_agent and task.assigned_agent != agent_name:
                    continue
                queue.remove(task_id)
                task.status = TaskStatus.ASSIGNED
                task.started_at = datetime.now().isoformat()
                return task
        return None

    def mark_completed(self, task_id: str, result: dict = None) -> bool:
        """Mark a task as completed."""
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.status = TaskStatus.COMPLETED
        task.result = result or {}
        task.completed_at = datetime.now().isoformat()
        if task_id in self._completion_events:
            self._completion_events[task_id].set()

        # Unblock dependent tasks
        self._unblock_dependents(task_id)
        return True

    def mark_failed(self, task_id: str, error: str) -> bool:
        """Mark a task as failed, optionally retry."""
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.retry_count += 1
        if task.retry_count <= task.max_retries:
            task.status = TaskStatus.QUEUED
            task.error_message = error
            self._queues[task.priority].append(task.id)
        else:
            task.status = TaskStatus.FAILED
            task.error_message = error
            task.completed_at = datetime.now().isoformat()
            if task_id in self._completion_events:
                self._completion_events[task_id].set()
        return True

    def mark_cancelled(self, task_id: str) -> bool:
        """Cancel a task."""
        task = self._tasks.get(task_id)
        if not task:
            return False
        task.status = TaskStatus.CANCELLED
        task.completed_at = datetime.now().isoformat()
        if task_id in self._completion_events:
            self._completion_events[task_id].set()
        return True

    def get_task(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)

    def get_all_tasks(self) -> List[Task]:
        return list(self._tasks.values())

    def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        return [t for t in self._tasks.values() if t.status == status]

    def get_tasks_by_agent(self, agent_name: str) -> List[Task]:
        return [t for t in self._tasks.values() if t.assigned_agent == agent_name]

    async def wait_for_task(self, task_id: str, timeout: float = None) -> bool:
        """Wait for a task to complete or fail."""
        event = self._completion_events.get(task_id)
        if not event:
            return False
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def _unblock_dependents(self, completed_task_id: str):
        """Re-queue tasks whose dependencies are met."""
        for task in self._tasks.values():
            if task.status == TaskStatus.BLOCKED and completed_task_id in task.dependencies:
                deps_met = all(
                    self._tasks.get(dep_id, Task()).status == TaskStatus.COMPLETED
                    for dep_id in task.dependencies
                )
                if deps_met:
                    task.status = TaskStatus.QUEUED
                    self._queues[task.priority].append(task.id)

    def get_stats(self) -> dict:
        tasks = list(self._tasks.values())
        status_counts = {}
        for s in TaskStatus:
            status_counts[s.value] = sum(1 for t in tasks if t.status == s)
        return {
            "total": len(tasks),
            "by_status": status_counts,
            "by_priority": {
                p.name: len(self._queues[p]) for p in TaskPriority
            },
            "avg_duration_ms": (
                sum(t.duration_ms for t in tasks if t.duration_ms > 0)
                / max(1, sum(1 for t in tasks if t.duration_ms > 0))
            ),
        }
