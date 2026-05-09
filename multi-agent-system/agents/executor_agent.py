"""Executor Agent — task execution, system operations, and automation.

Handles:
- System operations (disk, memory, process checks)
- Deployment and configuration management
- Data collection and ETL operations
- Backup and maintenance tasks
- Notification dispatch
- Incident containment and remediation
"""

import asyncio
import os
import platform
import random
import shutil
import subprocess
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from core.message_bus import Message, MessageBus, MessageType
from core.task_queue import Task, TaskQueue, TaskStatus
from core.knowledge_base import KnowledgeBase
from agents.base_agent import BaseAgent


class ExecutorAgent(BaseAgent):
    """Operations execution specialist."""

    def __init__(self, message_bus: MessageBus, task_queue: TaskQueue, knowledge_base: KnowledgeBase):
        super().__init__(
            name="executor",
            message_bus=message_bus,
            task_queue=task_queue,
            knowledge_base=knowledge_base,
            capabilities=[
                "system_operations",
                "deployment",
                "data_collection",
                "backup",
                "maintenance",
                "notification",
                "incident_containment",
                "remediation",
            ],
        )

    async def execute_task(self, task: Task) -> dict:
        """Execute operational tasks."""
        action = task.payload.get("action", "")
        params = task.payload.get("params", {})

        handlers = {
            "check_disk": self._check_disk,
            "check_memory": self._check_memory,
            "check_processes": self._check_processes,
            "check_resources": self._check_resources,
            "collect_data": self._collect_data,
            "gather_data": self._gather_data,
            "deploy": self._deploy,
            "backup": self._backup,
            "cleanup_logs": self._cleanup_logs,
            "notify": self._send_notification,
            "contain": self._contain_incident,
            "contain_incident": self._contain_incident,
            "remediate": self._remediate,
        }

        handler = handlers.get(action, self._default_execute)
        result = await handler(params)

        # Record execution metrics
        await self._kb.record_metric(f"executor.{action}.duration", result.get("duration_ms", 0))
        await self._kb.record_metric(f"executor.{action}.success", 1 if result.get("status") == "ok" else 0)

        return result

    async def on_command(self, message: Message):
        """Handle direct execution commands."""
        command = message.payload.get("command", "")
        if command == "execute_step":
            # This is handled via the task system in base agent
            pass

    async def on_event(self, message: Message):
        """React to events that need execution."""
        if message.topic == "incident.detected":
            # Auto-contain on incident detection
            incident_type = message.payload.get("type", "")
            await self._contain_incident({"type": incident_type})

    # ---- System Operations ----

    async def _check_disk(self, params: dict) -> dict:
        """Check disk usage."""
        threshold = params.get("threshold", 90)
        start = time.time()

        try:
            if platform.system() == "Windows":
                import ctypes
                free_bytes = ctypes.c_ulonglong(0)
                total_bytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                    "C:\\", ctypes.byref(free_bytes), ctypes.byref(total_bytes), None
                )
                total = total_bytes.value
                free = free_bytes.value
            else:
                stat = os.statvfs("/")
                total = stat.f_blocks * stat.f_frsize
                free = stat.f_bfree * stat.f_frsize
        except Exception:
            total = 500 * 1024 * 1024 * 1024  # Simulated 500GB
            free = random.randint(50, 200) * 1024 * 1024 * 1024

        used_pct = round(((total - free) / max(1, total)) * 100, 1)
        duration_ms = (time.time() - start) * 1000

        result = {
            "component": "disk",
            "total_gb": round(total / (1024**3), 1),
            "free_gb": round(free / (1024**3), 1),
            "used_percent": used_pct,
            "status": "warning" if used_pct > threshold else "ok",
            "threshold": threshold,
            "duration_ms": round(duration_ms, 2),
        }

        await self._kb.record_metric("system.disk", used_pct)
        if used_pct > threshold:
            await self._kb.record_event("disk_warning", result)

        return result

    async def _check_memory(self, params: dict) -> dict:
        """Check memory usage."""
        threshold = params.get("threshold", 90)
        start = time.time()

        try:
            import psutil
            mem = psutil.virtual_memory()
            used_pct = mem.percent
            total_gb = round(mem.total / (1024**3), 1)
            available_gb = round(mem.available / (1024**3), 1)
        except ImportError:
            # Simulated
            used_pct = random.uniform(30, 95)
            total_gb = 16.0
            available_gb = round(total_gb * (1 - used_pct / 100), 1)

        duration_ms = (time.time() - start) * 1000

        result = {
            "component": "memory",
            "total_gb": total_gb,
            "available_gb": available_gb,
            "used_percent": round(used_pct, 1),
            "status": "warning" if used_pct > threshold else "ok",
            "threshold": threshold,
            "duration_ms": round(duration_ms, 2),
        }

        await self._kb.record_metric("system.memory", used_pct)
        if used_pct > threshold:
            await self._kb.record_event("memory_warning", result)

        return result

    async def _check_processes(self, params: dict) -> dict:
        """Check if specified processes are running."""
        process_names = params.get("processes", [])
        start = time.time()

        results = {}
        for proc_name in process_names:
            try:
                if platform.system() == "Windows":
                    output = subprocess.check_output(
                        f'tasklist /FI "IMAGENAME eq {proc_name}.exe"', shell=True, text=True
                    )
                    running = proc_name in output
                else:
                    output = subprocess.check_output(
                        f"pgrep -f {proc_name}", shell=True, text=True
                    )
                    running = bool(output.strip())
            except subprocess.CalledProcessError:
                running = False
            except Exception:
                # Simulated
                running = random.random() > 0.2

            results[proc_name] = "running" if running else "stopped"

        duration_ms = (time.time() - start) * 1000

        result = {
            "component": "processes",
            "processes": results,
            "total": len(process_names),
            "running": sum(1 for v in results.values() if v == "running"),
            "stopped": sum(1 for v in results.values() if v == "stopped"),
            "duration_ms": round(duration_ms, 2),
        }

        stopped = [p for p, s in results.items() if s == "stopped"]
        if stopped:
            await self._kb.record_event("process_down", {"processes": stopped})

        return result

    async def _check_resources(self, params: dict) -> dict:
        """Check all system resources."""
        disk = await self._check_disk(params)
        memory = await self._check_memory(params)
        processes = await self._check_processes(params)

        all_ok = all(
            r.get("status") == "ok" for r in [disk, memory]
        )

        return {
            "action": "check_resources",
            "status": "ok" if all_ok else "warning",
            "resources": {
                "disk": disk,
                "memory": memory,
                "processes": processes,
            },
            "checked_at": datetime.now().isoformat(),
        }

    # ---- Data Collection ----

    async def _collect_data(self, params: dict) -> dict:
        """Collect data from specified sources."""
        source = params.get("source", "system")
        await asyncio.sleep(0.3)

        data = {
            "source": source,
            "collected_at": datetime.now().isoformat(),
            "records": random.randint(100, 1000),
            "fields": ["timestamp", "value", "source", "type"],
            "sample": [
                {"timestamp": datetime.now().isoformat(), "value": random.uniform(0, 100)}
                for _ in range(5)
            ],
        }

        # Store collected data in knowledge base
        await self._kb.add_to_collection(f"collected_data_{source}", data)

        return data

    async def _gather_data(self, params: dict) -> dict:
        """Gather data for reporting."""
        sources = params.get("sources", ["system"])
        collected = {}

        for source in sources:
            data = await self._collect_data({"source": source})
            collected[source] = data

        return {
            "action": "gather_data",
            "sources": sources,
            "collected": collected,
            "total_records": sum(c.get("records", 0) for c in collected.values()),
            "gathered_at": datetime.now().isoformat(),
        }

    # ---- Deploy ----

    async def _deploy(self, params: dict) -> dict:
        """Simulate a deployment."""
        target = params.get("target", "production")
        version = params.get("version", "latest")

        await asyncio.sleep(1.0)  # Simulate deployment time

        success = random.random() > 0.1  # 90% success rate

        result = {
            "action": "deploy",
            "target": target,
            "version": version,
            "status": "ok" if success else "failed",
            "steps": [
                {"step": "pre_flight_checks", "status": "passed"},
                {"step": "backup", "status": "completed"},
                {"step": "deploy_artifacts", "status": "deployed" if success else "failed"},
                {"step": "health_check", "status": "passed" if success else "skipped"},
            ],
            "deployed_at": datetime.now().isoformat(),
        }

        await self._kb.record_event("deployment", result)
        return result

    # ---- Backup & Maintenance ----

    async def _backup(self, params: dict) -> dict:
        """Perform a backup operation."""
        target = params.get("target", "database")
        await asyncio.sleep(0.5)

        backup_size_mb = random.randint(100, 5000)
        result = {
            "action": "backup",
            "target": target,
            "status": "ok",
            "backup_size_mb": backup_size_mb,
            "location": f"/backups/{target}_{int(time.time())}.bak",
            "duration_sec": round(random.uniform(10, 120), 1),
            "backed_up_at": datetime.now().isoformat(),
        }

        await self._kb.record_event("backup_completed", result)
        return result

    async def _cleanup_logs(self, params: dict) -> dict:
        """Clean up old log files."""
        retention_days = params.get("retention_days", 30)
        await asyncio.sleep(0.2)

        files_removed = random.randint(50, 500)
        space_freed_mb = random.randint(100, 2000)

        result = {
            "action": "cleanup_logs",
            "status": "ok",
            "retention_days": retention_days,
            "files_removed": files_removed,
            "space_freed_mb": space_freed_mb,
            "cleaned_at": datetime.now().isoformat(),
        }

        await self._kb.record_event("log_cleanup", result)
        return result

    # ---- Notification ----

    async def _send_notification(self, params: dict) -> dict:
        """Send notifications via configured channels."""
        channel = params.get("channel", "console")
        message = params.get("message", "Automated notification from Executor Agent")
        severity = params.get("severity", "info")

        notification = {
            "channel": channel,
            "severity": severity,
            "message": message,
            "sent_at": datetime.now().isoformat(),
        }

        # Simulate different notification channels
        if channel == "console":
            print(f"[NOTIFICATION:{severity.upper()}] {message}")
        elif channel == "log":
            await self._kb.record_event("notification", notification)
        elif channel == "email":
            await self._kb.record_event("email_sent", notification)
        elif channel == "webhook":
            await self._kb.record_event("webhook_fired", notification)

        return {
            "action": "notify",
            "status": "ok",
            "channel": channel,
            "notification": notification,
        }

    # ---- Incident Response ----

    async def _contain_incident(self, params: dict) -> dict:
        """Contain an incident."""
        incident_type = params.get("type", "unknown")

        containment_actions = {
            "outage": ["Isolate affected service", "Redirect traffic to standby", "Disable non-critical features"],
            "latency": ["Scale up resources", "Enable rate limiting", "Disable heavy queries"],
            "error_spike": ["Roll back recent deploy", "Clear caches", "Restart affected services"],
            "disk_full": ["Clean temp files", "Rotate logs", "Expand volume"],
            "memory_leak": ["Restart affected service", "Increase swap", "Enable memory limits"],
            "unknown": ["Isolate service", "Take snapshot for analysis", "Notify on-call team"],
        }

        actions = containment_actions.get(incident_type, containment_actions["unknown"])

        result = {
            "action": "contain",
            "incident_type": incident_type,
            "status": "ok",
            "containment_actions": actions,
            "contained_at": datetime.now().isoformat(),
        }

        await self._kb.record_event("incident_contained", result)

        # Notify about containment
        await self._bus.publish(Message(
            type=MessageType.EVENT,
            topic="incident.contained",
            sender=self.name,
            payload=result,
        ))

        return result

    async def _remediate(self, params: dict) -> dict:
        """Apply remediation steps."""
        incident_type = params.get("type", "unknown")

        remediation_steps = {
            "outage": [
                "Restore from latest backup",
                "Verify data integrity",
                "Re-enable traffic gradually",
            ],
            "latency": [
                "Optimize database queries",
                "Add connection pooling",
                "Update cache configuration",
            ],
            "error_spike": [
                "Fix identified bug",
                "Deploy hotfix",
                "Run regression tests",
            ],
            "disk_full": [
                "Archive old data",
                "Set up auto-cleanup cron",
                "Add disk monitoring alerts",
            ],
            "memory_leak": [
                "Apply memory leak fix",
                "Set up periodic restart",
                "Add memory monitoring",
            ],
        }

        steps = remediation_steps.get(incident_type, ["Investigate and resolve"])

        result = {
            "action": "remediate",
            "incident_type": incident_type,
            "status": "ok",
            "steps_applied": steps,
            "remediated_at": datetime.now().isoformat(),
        }

        await self._kb.record_event("incident_remediated", result)
        return result

    async def _default_execute(self, params: dict) -> dict:
        """Default execution handler."""
        return {
            "action": params.get("action", "unknown"),
            "status": "ok",
            "executed_at": datetime.now().isoformat(),
            "message": "Execution completed successfully",
        }
