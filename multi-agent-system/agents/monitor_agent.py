"""Monitor Agent — system health monitoring, alerting, and metric collection.

Handles:
- Continuous health checking of system components
- Metric collection and aggregation
- Threshold-based alerting
- Service status tracking
- Anomaly detection triggers
- Heartbeat monitoring of other agents
"""

import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from core.message_bus import Message, MessageBus, MessageType
from core.task_queue import Task, TaskQueue, TaskStatus
from core.knowledge_base import KnowledgeBase
from agents.base_agent import BaseAgent


class MonitorAgent(BaseAgent):
    """System monitoring and alerting specialist."""

    def __init__(self, message_bus: MessageBus, task_queue: TaskQueue, knowledge_base: KnowledgeBase):
        super().__init__(
            name="monitor",
            message_bus=message_bus,
            task_queue=task_queue,
            knowledge_base=knowledge_base,
            capabilities=[
                "health_checking",
                "metric_collection",
                "alerting",
                "service_monitoring",
                "anomaly_detection",
                "heartbeat_tracking",
            ],
        )
        self._heartbeat_interval = 5.0  # Monitor checks more frequently
        self._agent_heartbeats: Dict[str, float] = {}  # agent -> last heartbeat time
        self._alert_thresholds = {
            "disk_usage": 90,
            "memory_usage": 85,
            "cpu_usage": 90,
            "agent_timeout": 30,  # seconds without heartbeat
            "error_rate": 0.05,  # 5% error rate
        }
        self._active_alerts: Set[str] = set()

    async def on_start(self):
        """Start continuous monitoring tasks."""
        # These run in background alongside the main loops
        asyncio.create_task(self._continuous_resource_monitoring())

    async def on_heartbeat(self, message: Message):
        """Track heartbeats from other agents."""
        agent = message.payload.get("agent", message.sender)
        self._agent_heartbeats[agent] = time.time()
        await self._kb.record_metric(f"agent.{agent}.last_heartbeat", time.time())

    async def on_event(self, message: Message):
        """React to system events."""
        if message.topic == "task.failed":
            task_id = message.payload.get("task_id", "")
            await self._check_error_threshold()
        elif message.topic == "incident.contained":
            # Start intensified monitoring after containment
            await self._intensify_monitoring(message.payload)

    async def on_query(self, message: Message) -> Optional[dict]:
        """Handle monitoring queries."""
        query_type = message.payload.get("type", "")
        if query_type == "system_status":
            return await self._get_system_status()
        elif query_type == "agent_status":
            return await self._get_agent_statuses()
        elif query_type == "active_alerts":
            return {"alerts": list(self._active_alerts)}
        return None

    async def execute_task(self, task: Task) -> dict:
        """Execute monitoring tasks."""
        action = task.payload.get("action", "")

        handlers = {
            "check_services": self._check_services,
            "pre_deploy_check": self._pre_deploy_check,
            "post_deploy_verify": self._post_deploy_verify,
            "verify": self._verify_stability,
            "verify_stability": self._verify_stability,
            "verify_backup": self._verify_backup,
            "health_check": self._run_health_check,
        }

        handler = handlers.get(action, self._default_monitor)
        return await handler(task.payload.get("params", {}))

    # ---- Continuous Monitoring ----

    async def _continuous_resource_monitoring(self):
        """Background task that continuously checks system resources."""
        while self._running:
            try:
                # Simulate resource metric collection
                cpu = random.uniform(10, 95)
                mem = random.uniform(20, 90)
                disk = random.uniform(30, 95)

                await self._kb.record_metric("system.cpu", cpu)
                await self._kb.record_metric("system.memory", mem)
                await self._kb.record_metric("system.disk", disk)

                # Check thresholds and alert
                if cpu > self._alert_thresholds["cpu_usage"]:
                    await self._raise_alert("cpu_high", f"CPU usage at {cpu:.1f}%", "warning")
                if mem > self._alert_thresholds["memory_usage"]:
                    await self._raise_alert("memory_high", f"Memory usage at {mem:.1f}%", "critical")
                if disk > self._alert_thresholds["disk_usage"]:
                    await self._raise_alert("disk_high", f"Disk usage at {disk:.1f}%", "critical")

                # Check for stale agents
                now = time.time()
                for agent_name, last_hb in list(self._agent_heartbeats.items()):
                    if now - last_hb > self._alert_thresholds["agent_timeout"]:
                        await self._raise_alert(
                            "agent_unresponsive",
                            f"Agent {agent_name} has not sent heartbeat in {now - last_hb:.0f}s",
                            "critical",
                        )

                await asyncio.sleep(10)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._kb.record_event("monitor_error", {"error": str(e)})
                await asyncio.sleep(5)

    # ---- Health Checks ----

    async def _check_services(self, params: dict) -> dict:
        """Check status of all monitored services."""
        services = params.get("services", ["api", "database", "worker", "frontend"])
        results = {}

        for svc in services:
            # Simulate service check
            healthy = random.random() > 0.1
            response_time = random.uniform(5, 200)
            results[svc] = {
                "status": "healthy" if healthy else "unhealthy",
                "response_time_ms": round(response_time, 2),
                "checked_at": datetime.now().isoformat(),
            }

            if not healthy:
                await self._raise_alert(
                    f"service_down:{svc}",
                    f"Service {svc} is unhealthy",
                    "critical",
                )

        return {
            "action": "check_services",
            "services": results,
            "total": len(services),
            "healthy": sum(1 for r in results.values() if r["status"] == "healthy"),
            "unhealthy": sum(1 for r in results.values() if r["status"] != "healthy"),
        }

    async def _run_health_check(self, params: dict) -> dict:
        """Run a comprehensive health check."""
        services = await self._check_services({})

        # Check agent statuses
        agent_status = {}
        all_agents = await self._kb.find_agents_with_capability("health_checking")
        for agent in ["coordinator", "analyst", "executor"] + all_agents:
            last_hb = self._agent_heartbeats.get(agent, 0)
            agent_status[agent] = {
                "alive": (time.time() - last_hb) < 30,
                "last_heartbeat_sec_ago": round(time.time() - last_hb, 1) if last_hb else "never",
            }

        return {
            "action": "health_check",
            "overall_status": "healthy" if services["unhealthy"] == 0 else "degraded",
            "services": services,
            "agents": agent_status,
            "checked_at": datetime.now().isoformat(),
        }

    # ---- Deploy Checks ----

    async def _pre_deploy_check(self, params: dict) -> dict:
        """Run pre-deployment checks."""
        checks = {
            "disk_space": random.random() > 0.05,
            "memory_available": random.random() > 0.05,
            "service_health": random.random() > 0.1,
            "backup_current": True,
            "no_active_incidents": len(self._active_alerts) == 0,
        }

        all_pass = all(checks.values())

        return {
            "action": "pre_deploy_check",
            "status": "ok" if all_pass else "blocked",
            "checks": checks,
            "all_passed": all_pass,
            "checked_at": datetime.now().isoformat(),
        }

    async def _post_deploy_verify(self, params: dict) -> dict:
        """Verify system stability after deployment."""
        await asyncio.sleep(0.5)

        # Simulate verification checks
        checks = {
            "health_endpoint": random.random() > 0.05,
            "error_rate": random.random() > 0.1,
            "response_time": random.random() > 0.1,
            "database_connectivity": True,
        }

        all_pass = all(checks.values())

        result = {
            "action": "post_deploy_verify",
            "status": "ok" if all_pass else "rollback_recommended",
            "checks": checks,
            "all_passed": all_pass,
            "verified_at": datetime.now().isoformat(),
        }

        if not all_pass:
            await self._raise_alert(
                "post_deploy_failure",
                "Post-deployment verification failed",
                "critical",
            )

        return result

    async def _verify_stability(self, params: dict) -> dict:
        """Verify overall system stability."""
        # Check recent metrics
        cpu_metrics = await self._kb.get_metrics("system.cpu", limit=20)
        mem_metrics = await self._kb.get_metrics("system.memory", limit=20)

        cpu_stable = True
        mem_stable = True

        if cpu_metrics and len(cpu_metrics) > 5:
            cpu_values = [v for _, v in cpu_metrics[-5:]]
            cpu_stable = max(cpu_values) - min(cpu_values) < 20

        if mem_metrics and len(mem_metrics) > 5:
            mem_values = [v for _, v in mem_metrics[-5:]]
            mem_stable = max(mem_values) - min(mem_values) < 15

        stable = cpu_stable and mem_stable

        return {
            "action": "verify_stability",
            "status": "stable" if stable else "unstable",
            "details": {
                "cpu_stable": cpu_stable,
                "memory_stable": mem_stable,
            },
            "verified_at": datetime.now().isoformat(),
        }

    async def _verify_backup(self, params: dict) -> dict:
        """Verify backup integrity."""
        await asyncio.sleep(0.3)

        checks = {
            "file_exists": True,
            "checksum_valid": random.random() > 0.05,
            "size_expected": random.random() > 0.1,
            "restore_test": random.random() > 0.1,
        }

        all_pass = all(checks.values())

        return {
            "action": "verify_backup",
            "status": "ok" if all_pass else "failed",
            "checks": checks,
            "verified_at": datetime.now().isoformat(),
        }

    # ---- Alerting ----

    async def _raise_alert(self, alert_id: str, message: str, severity: str):
        """Raise an alert if not already active."""
        if alert_id in self._active_alerts:
            return

        self._active_alerts.add(alert_id)

        alert = {
            "id": alert_id,
            "message": message,
            "severity": severity,
            "raised_at": datetime.now().isoformat(),
            "raised_by": self.name,
        }

        await self._kb.record_event("alert_raised", alert)
        await self._kb.set_fact(f"alert:{alert_id}", alert)

        # Broadcast alert
        await self._bus.broadcast(Message(
            type=MessageType.EVENT,
            topic="alert.raised",
            sender=self.name,
            payload=alert,
        ))

        # Print alert to console
        prefix = "🔴" if severity == "critical" else "🟡" if severity == "warning" else "🔵"
        print(f"{prefix} [ALERT:{severity.upper()}] {message}")

    async def _clear_alert(self, alert_id: str):
        """Clear a resolved alert."""
        if alert_id not in self._active_alerts:
            return

        self._active_alerts.discard(alert_id)

        await self._kb.record_event("alert_cleared", {
            "id": alert_id,
            "cleared_at": datetime.now().isoformat(),
        })
        await self._kb.delete_fact(f"alert:{alert_id}")

        await self._bus.broadcast(Message(
            type=MessageType.EVENT,
            topic="alert.cleared",
            sender=self.name,
            payload={"id": alert_id},
        ))

    async def _check_error_threshold(self):
        """Check if error rate exceeds threshold."""
        recent_events = await self._kb.get_recent_events("task_failed", limit=50)
        recent_completed = await self._kb.get_recent_events("task_completed", limit=50)

        total = len(recent_events) + len(recent_completed)
        if total > 0:
            error_rate = len(recent_events) / total
            if error_rate > self._alert_thresholds["error_rate"]:
                await self._raise_alert(
                    "high_error_rate",
                    f"Task error rate at {error_rate:.1%} (threshold: {self._alert_thresholds['error_rate']:.1%})",
                    "warning",
                )

    async def _intensify_monitoring(self, incident_data: dict):
        """Intensify monitoring after an incident."""
        await self._kb.record_event("monitoring_intensified", {
            "reason": "incident_contained",
            "incident": incident_data,
        })
        # Temporarily reduce check intervals
        original_interval = self._heartbeat_interval
        self._heartbeat_interval = 2.0  # Check more frequently
        await asyncio.sleep(60)  # Intensify for 60 seconds
        self._heartbeat_interval = original_interval

    # ---- Status Queries ----

    async def _get_system_status(self) -> dict:
        """Get overall system status."""
        cpu = await self._kb.get_latest_metric("system.cpu")
        mem = await self._kb.get_latest_metric("system.memory")
        disk = await self._kb.get_latest_metric("system.disk")

        agent_count = len(self._agent_heartbeats)
        alert_count = len(self._active_alerts)

        if alert_count > 0:
            overall = "degraded"
        elif agent_count < 3:
            overall = "degraded"
        else:
            overall = "healthy"

        return {
            "overall": overall,
            "metrics": {
                "cpu_percent": round(cpu, 1) if cpu else None,
                "memory_percent": round(mem, 1) if mem else None,
                "disk_percent": round(disk, 1) if disk else None,
            },
            "agents_tracked": agent_count,
            "active_alerts": alert_count,
            "timestamp": datetime.now().isoformat(),
        }

    async def _get_agent_statuses(self) -> dict:
        """Get status of all agents."""
        statuses = {}
        now = time.time()
        for agent_name, last_hb in self._agent_heartbeats.items():
            statuses[agent_name] = {
                "last_heartbeat_sec_ago": round(now - last_hb, 1),
                "alive": (now - last_hb) < self._alert_thresholds["agent_timeout"],
            }
        return {"agents": statuses, "timestamp": datetime.now().isoformat()}

    async def _default_monitor(self, params: dict) -> dict:
        return {
            "action": params.get("action", "monitor"),
            "status": "ok",
            "monitored_at": datetime.now().isoformat(),
        }
