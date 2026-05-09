"""Analyst Agent — data analysis, pattern detection, and report generation.

Handles:
- Statistical analysis of metrics and logs
- Trend detection and anomaly identification
- Report generation (health, performance, incident)
- Data cleaning and normalization
- Pattern recognition
"""

import asyncio
import random
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from core.message_bus import Message, MessageBus, MessageType
from core.task_queue import Task, TaskQueue, TaskStatus
from core.knowledge_base import KnowledgeBase
from agents.base_agent import BaseAgent


class AnalystAgent(BaseAgent):
    """Data analysis and reporting specialist."""

    def __init__(self, message_bus: MessageBus, task_queue: TaskQueue, knowledge_base: KnowledgeBase):
        super().__init__(
            name="analyst",
            message_bus=message_bus,
            task_queue=task_queue,
            knowledge_base=knowledge_base,
            capabilities=[
                "statistical_analysis",
                "trend_detection",
                "anomaly_detection",
                "report_generation",
                "data_cleaning",
                "pattern_recognition",
            ],
        )

    async def execute_task(self, task: Task) -> dict:
        """Execute analysis tasks."""
        action = task.payload.get("action", "")
        params = task.payload.get("params", {})

        handlers = {
            "analyze_data": self._analyze_data,
            "analyze_health": self._analyze_health,
            "analyze_trends": self._analyze_trends,
            "generate_report": self._generate_report,
            "health_report": self._generate_health_report,
            "build_report": self._build_report,
            "assess_incident": self._assess_incident,
            "clean_data": self._clean_data,
            "run_analysis": self._run_analysis,
            "generate_postmortem": self._generate_postmortem,
            "maintenance_report": self._generate_maintenance_report,
        }

        handler = handlers.get(action, self._default_analysis)
        return await handler(params)

    async def on_query(self, message: Message) -> Optional[dict]:
        """Handle analysis queries."""
        query_type = message.payload.get("type", "")
        if query_type == "analyze_metrics":
            metric_name = message.payload.get("metric", "")
            metrics = await self._kb.get_metrics(metric_name, limit=100)
            return self._compute_statistics(metric_name, metrics)
        elif query_type == "detect_anomalies":
            metric_name = message.payload.get("metric", "")
            metrics = await self._kb.get_metrics(metric_name, limit=200)
            return self._detect_anomalies(metric_name, metrics)
        return None

    async def on_event(self, message: Message):
        """React to events that may need analysis."""
        if message.topic == "system.heartbeat":
            # Collect heartbeat data for trend analysis
            agent = message.payload.get("agent", "")
            await self._kb.record_metric(f"heartbeat.{agent}", time.time())

    # ---- Analysis Methods ----

    async def _analyze_data(self, params: dict) -> dict:
        """Run general data analysis."""
        data_source = params.get("source", "unknown")
        await asyncio.sleep(0.3)  # Simulate processing
        return {
            "source": data_source,
            "records_analyzed": random.randint(100, 10000),
            "findings": [
                {"type": "distribution", "description": "Data follows normal distribution", "confidence": 0.92},
                {"type": "correlation", "description": "Positive correlation between load and response time", "confidence": 0.87},
            ],
            "recommendations": [
                "Consider scaling resources during peak hours",
                "Monitor response time threshold at 200ms",
            ],
            "analyzed_at": datetime.now().isoformat(),
        }

    async def _analyze_health(self, params: dict) -> dict:
        """Analyze system health from collected metrics."""
        # Gather metrics from knowledge base
        cpu_metrics = await self._kb.get_metrics("system.cpu", limit=20)
        mem_metrics = await self._kb.get_metrics("system.memory", limit=20)
        disk_metrics = await self._kb.get_metrics("system.disk", limit=20)

        issues = []
        if cpu_metrics and cpu_metrics[-1][1] > 80:
            issues.append({"component": "CPU", "severity": "warning", "value": cpu_metrics[-1][1]})
        if mem_metrics and mem_metrics[-1][1] > 85:
            issues.append({"component": "Memory", "severity": "critical", "value": mem_metrics[-1][1]})
        if disk_metrics and disk_metrics[-1][1] > 90:
            issues.append({"component": "Disk", "severity": "critical", "value": disk_metrics[-1][1]})

        overall_health = "healthy" if len(issues) == 0 else ("degraded" if len(issues) < 2 else "critical")

        return {
            "overall_health": overall_health,
            "issues": issues,
            "metrics_summary": {
                "cpu": self._compute_statistics("CPU", cpu_metrics),
                "memory": self._compute_statistics("Memory", mem_metrics),
                "disk": self._compute_statistics("Disk", disk_metrics),
            },
            "analyzed_at": datetime.now().isoformat(),
        }

    async def _analyze_trends(self, params: dict) -> dict:
        """Analyze metric trends over time."""
        metric_name = params.get("metric", "system.cpu")
        metrics = await self._kb.get_metrics(metric_name, limit=100)

        if len(metrics) < 2:
            return {"trend": "insufficient_data", "metric": metric_name}

        values = [v for _, v in metrics]
        first_half = sum(values[: len(values) // 2]) / max(1, len(values) // 2)
        second_half = sum(values[len(values) // 2 :]) / max(1, len(values) - len(values) // 2)

        change_pct = ((second_half - first_half) / max(0.001, first_half)) * 100

        if change_pct > 10:
            trend = "increasing"
        elif change_pct < -10:
            trend = "decreasing"
        else:
            trend = "stable"

        return {
            "metric": metric_name,
            "trend": trend,
            "change_percent": round(change_pct, 2),
            "current_value": values[-1] if values else 0,
            "average": round(sum(values) / len(values), 2),
            "max": max(values),
            "min": min(values),
        }

    async def _generate_report(self, params: dict) -> dict:
        """Generate a comprehensive report."""
        report_type = params.get("report_type", "general")
        await asyncio.sleep(0.5)

        return {
            "report_id": f"RPT-{int(time.time())}",
            "type": report_type,
            "title": f"{report_type.title()} Report",
            "generated_at": datetime.now().isoformat(),
            "sections": [
                {"title": "Executive Summary", "content": "System operating within normal parameters."},
                {"title": "Key Metrics", "content": "All metrics within acceptable thresholds."},
                {"title": "Recommendations", "content": "Continue routine monitoring schedule."},
            ],
        }

    async def _generate_health_report(self, params: dict) -> dict:
        """Generate a system health report."""
        return {
            "report_id": f"HEALTH-{int(time.time())}",
            "type": "health",
            "title": "System Health Report",
            "generated_at": datetime.now().isoformat(),
            "status": "healthy",
            "components": {
                "cpu": "normal",
                "memory": "normal",
                "disk": "normal",
                "network": "normal",
            },
            "uptime_percentage": 99.97,
            "incidents_last_24h": 0,
        }

    async def _build_report(self, params: dict) -> dict:
        """Build a formatted report from analysis results."""
        return {
            "report_id": f"ANALYSIS-{int(time.time())}",
            "type": "analysis",
            "title": "Analysis Report",
            "generated_at": datetime.now().isoformat(),
            "findings": [
                "System performance is within acceptable bounds",
                "No anomalies detected in the analyzed period",
            ],
            "charts": [],
        }

    async def _assess_incident(self, params: dict) -> dict:
        """Assess an incident's scope and impact."""
        incident_type = params.get("type", "unknown")
        severity = params.get("severity", "medium")

        impact_scores = {
            "critical": 100,
            "high": 75,
            "medium": 50,
            "low": 25,
        }

        return {
            "incident_type": incident_type,
            "severity": severity,
            "impact_score": impact_scores.get(severity, 50),
            "affected_components": self._estimate_affected_components(incident_type),
            "estimated_resolution_time": f"{impact_scores.get(severity, 50) // 10} minutes",
            "recommended_actions": [
                "Isolate affected components",
                "Notify stakeholders",
                "Begin root cause analysis",
            ],
            "assessed_at": datetime.now().isoformat(),
        }

    async def _clean_data(self, params: dict) -> dict:
        """Clean and normalize data."""
        return {
            "records_processed": random.randint(500, 5000),
            "records_removed": random.randint(5, 50),
            "nulls_filled": random.randint(10, 100),
            "normalization": "z-score",
            "status": "completed",
        }

    async def _run_analysis(self, params: dict) -> dict:
        """Run full statistical analysis."""
        return {
            "sample_size": random.randint(1000, 100000),
            "mean": round(random.uniform(10, 100), 2),
            "median": round(random.uniform(10, 100), 2),
            "std_dev": round(random.uniform(1, 20), 2),
            "outliers_detected": random.randint(0, 10),
            "distribution_type": "normal",
        }

    async def _generate_postmortem(self, params: dict) -> dict:
        """Generate an incident postmortem."""
        return {
            "report_id": f"PM-{int(time.time())}",
            "type": "postmortem",
            "title": "Incident Postmortem",
            "generated_at": datetime.now().isoformat(),
            "timeline": [
                {"time": (datetime.now() - timedelta(hours=2)).isoformat(), "event": "Incident detected"},
                {"time": (datetime.now() - timedelta(hours=1, minutes=55)).isoformat(), "event": "Response initiated"},
                {"time": (datetime.now() - timedelta(hours=1)).isoformat(), "event": "Issue contained"},
                {"time": (datetime.now() - timedelta(minutes=30)).isoformat(), "event": "Service restored"},
            ],
            "root_cause": "Identified during analysis phase",
            "action_items": [
                "Implement additional monitoring",
                "Update runbooks",
                "Schedule follow-up review",
            ],
        }

    async def _generate_maintenance_report(self, params: dict) -> dict:
        """Generate a maintenance activity report."""
        return {
            "report_id": f"MAINT-{int(time.time())}",
            "type": "maintenance",
            "title": "Maintenance Report",
            "generated_at": datetime.now().isoformat(),
            "activities": [
                {"task": "Backup", "status": "completed", "duration_sec": 120},
                {"task": "Log cleanup", "status": "completed", "duration_sec": 45},
                {"task": "Verification", "status": "completed", "duration_sec": 30},
            ],
            "total_duration_sec": 195,
            "status": "all_successful",
        }

    async def _default_analysis(self, params: dict) -> dict:
        """Default analysis handler."""
        return {
            "action": "analysis",
            "status": "completed",
            "findings": ["Routine analysis completed"],
            "analyzed_at": datetime.now().isoformat(),
        }

    # ---- Helper Methods ----

    def _compute_statistics(self, name: str, metrics: List[tuple]) -> dict:
        """Compute basic statistics on metric data."""
        if not metrics:
            return {"name": name, "count": 0}
        values = [v for _, v in metrics]
        return {
            "name": name,
            "count": len(values),
            "current": values[-1],
            "average": round(sum(values) / len(values), 2),
            "max": max(values),
            "min": min(values),
        }

    def _detect_anomalies(self, name: str, metrics: List[tuple]) -> dict:
        """Simple anomaly detection using z-score method."""
        if len(metrics) < 10:
            return {"metric": name, "anomalies": [], "method": "insufficient_data"}

        values = [v for _, v in metrics]
        mean = sum(values) / len(values)
        std = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5

        if std == 0:
            return {"metric": name, "anomalies": [], "method": "z_score", "threshold": 2.0}

        anomalies = []
        for i, (ts, val) in enumerate(metrics):
            z_score = abs(val - mean) / std
            if z_score > 2.0:
                anomalies.append({
                    "index": i,
                    "timestamp": ts,
                    "value": val,
                    "z_score": round(z_score, 2),
                })

        return {
            "metric": name,
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "method": "z_score",
            "threshold": 2.0,
        }

    def _estimate_affected_components(self, incident_type: str) -> List[str]:
        """Estimate which components are affected by an incident type."""
        component_map = {
            "outage": ["api", "database", "frontend"],
            "latency": ["api", "database"],
            "error_spike": ["api", "worker"],
            "disk_full": ["database", "worker", "logging"],
            "memory_leak": ["api", "worker"],
            "unknown": ["api", "database", "worker", "frontend"],
        }
        return component_map.get(incident_type, ["unknown"])
