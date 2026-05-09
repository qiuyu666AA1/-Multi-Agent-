"""Shared knowledge base for agent coordination.

Stores system state, agent capabilities, learned patterns, and operational data
that all agents can read and contribute to.
"""

import asyncio
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set


class KnowledgeBase:
    """Thread-safe shared knowledge store."""

    def __init__(self):
        self._lock = asyncio.Lock()
        self._facts: Dict[str, Any] = {}  # key-value facts
        self._collections: Dict[str, List[Any]] = defaultdict(list)  # named collections
        self._agent_capabilities: Dict[str, Set[str]] = defaultdict(set)  # agent -> capabilities
        self._metrics: Dict[str, List[tuple]] = defaultdict(list)  # time-series metrics
        self._events: List[dict] = []  # event log
        self._max_events = 500
        self._max_metrics_per_key = 200

    # ---- Facts ----

    async def set_fact(self, key: str, value: Any):
        async with self._lock:
            self._facts[key] = value

    async def get_fact(self, key: str, default: Any = None) -> Any:
        async with self._lock:
            return self._facts.get(key, default)

    async def delete_fact(self, key: str) -> bool:
        async with self._lock:
            return self._facts.pop(key, None) is not None

    async def get_all_facts(self) -> dict:
        async with self._lock:
            return dict(self._facts)

    # ---- Collections ----

    async def add_to_collection(self, collection: str, item: Any):
        async with self._lock:
            self._collections[collection].append(item)

    async def get_collection(self, collection: str) -> List[Any]:
        async with self._lock:
            return list(self._collections.get(collection, []))

    async def clear_collection(self, collection: str):
        async with self._lock:
            self._collections[collection].clear()

    # ---- Agent Capabilities ----

    async def register_capability(self, agent_name: str, capability: str):
        async with self._lock:
            self._agent_capabilities[agent_name].add(capability)

    async def get_agent_capabilities(self, agent_name: str) -> Set[str]:
        async with self._lock:
            return set(self._agent_capabilities.get(agent_name, set()))

    async def find_agents_with_capability(self, capability: str) -> List[str]:
        async with self._lock:
            return [
                name
                for name, caps in self._agent_capabilities.items()
                if capability in caps
            ]

    async def remove_agent(self, agent_name: str):
        async with self._lock:
            self._agent_capabilities.pop(agent_name, None)

    # ---- Metrics ----

    async def record_metric(self, name: str, value: float):
        async with self._lock:
            self._metrics[name].append((time.time(), value))
            if len(self._metrics[name]) > self._max_metrics_per_key:
                self._metrics[name] = self._metrics[name][-self._max_metrics_per_key:]

    async def get_metrics(self, name: str, limit: int = 50) -> List[tuple]:
        async with self._lock:
            return list(self._metrics.get(name, []))[-limit:]

    async def get_latest_metric(self, name: str) -> Optional[float]:
        async with self._lock:
            values = self._metrics.get(name, [])
            return values[-1][1] if values else None

    # ---- Events ----

    async def record_event(self, event_type: str, data: dict):
        async with self._lock:
            event = {
                "type": event_type,
                "data": data,
                "timestamp": datetime.now().isoformat(),
            }
            self._events.append(event)
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events:]

    async def get_recent_events(self, event_type: str = None, limit: int = 50) -> List[dict]:
        async with self._lock:
            events = self._events
            if event_type:
                events = [e for e in events if e["type"] == event_type]
            return events[-limit:]

    # ---- Stats ----

    async def get_stats(self) -> dict:
        async with self._lock:
            return {
                "fact_count": len(self._facts),
                "collection_count": len(self._collections),
                "agent_count": len(self._agent_capabilities),
                "metric_keys": list(self._metrics.keys()),
                "event_count": len(self._events),
            }
