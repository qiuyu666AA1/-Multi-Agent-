#!/usr/bin/env python3
"""Multi-Agent Collaborative Operations Automation System — Main Entry Point.

Launches all agents, the web dashboard, and orchestrates the system lifecycle.

Usage:
    python run.py                  # Full system with web dashboard
    python run.py --no-web         # CLI-only mode
    python run.py --demo           # Run with demo scenario
"""

import argparse
import asyncio
import logging
import os
import signal
import sys
import threading
from datetime import datetime

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.message_bus import MessageBus
from core.task_queue import TaskQueue
from core.knowledge_base import KnowledgeBase
from core.orchestrator import Orchestrator
from agents.coordinator_agent import CoordinatorAgent
from agents.analyst_agent import AnalystAgent
from agents.executor_agent import ExecutorAgent
from agents.monitor_agent import MonitorAgent
from tools.tool_registry import ToolRegistry
from tools.system_tools import SystemTools
from tools.data_tools import DataTools
from tools.notification_tools import NotificationTools

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("multi-agent-system")


class MultiAgentSystem:
    """Main system orchestrator — manages the full agent lifecycle."""

    def __init__(self, enable_web: bool = True, web_host: str = "127.0.0.1", web_port: int = 5000):
        self.enable_web = enable_web
        self.web_host = web_host
        self.web_port = web_port

        # Core components
        self.message_bus = MessageBus()
        self.task_queue = TaskQueue()
        self.knowledge_base = KnowledgeBase()
        self.orchestrator = Orchestrator(self.message_bus, self.task_queue, self.knowledge_base)
        self.tool_registry = ToolRegistry()

        # Agents
        self.agents = {}

        # Runtime state
        self._agent_tasks = []
        self._web_thread = None
        self._running = False

    def _register_tools(self):
        """Register all built-in tools."""
        logger.info("Registering tools...")
        SystemTools.register_all(self.tool_registry)
        DataTools.register_all(self.tool_registry)
        NotificationTools.register_all(self.tool_registry)
        logger.info(f"Registered {len(self.tool_registry.list_tools())} tools in {len(self.tool_registry.list_categories())} categories")

    def _create_agents(self):
        """Create all agent instances."""
        logger.info("Creating agents...")
        self.agents = {
            "coordinator": CoordinatorAgent(self.message_bus, self.task_queue, self.knowledge_base),
            "analyst": AnalystAgent(self.message_bus, self.task_queue, self.knowledge_base),
            "executor": ExecutorAgent(self.message_bus, self.task_queue, self.knowledge_base),
            "monitor": MonitorAgent(self.message_bus, self.task_queue, self.knowledge_base),
        }
        logger.info(f"Created {len(self.agents)} agents: {', '.join(self.agents.keys())}")

    async def start(self):
        """Start the multi-agent system."""
        self._running = True
        logger.info("=" * 60)
        logger.info("  Multi-Agent Collaborative Operations System")
        logger.info("=" * 60)

        # Register tools
        self._register_tools()

        # Create agents
        self._create_agents()

        # Start the web dashboard
        if self.enable_web:
            self._start_web_dashboard()

        # Start all agents concurrently
        logger.info("Starting all agents...")
        self._agent_tasks = [
            asyncio.create_task(agent.start(), name=name)
            for name, agent in self.agents.items()
        ]

        logger.info(f"All {len(self.agents)} agents started")
        await self.knowledge_base.record_event("system_started", {
            "agent_count": len(self.agents),
            "tool_count": len(self.tool_registry.list_tools()),
            "started_at": datetime.now().isoformat(),
        })

        # Notify agents that system is ready
        await self.message_bus.broadcast(
            __import__('core.message_bus', fromlist=['Message']).Message(
                type=__import__('core.message_bus', fromlist=['MessageType']).MessageType.EVENT,
                topic="system.ready",
                sender="system",
                payload={"message": "System initialization complete"},
            )
        )

        logger.info("System ready. Agents are running.")
        print("\n" + "=" * 60)
        print("  SYSTEM READY")
        if self.enable_web:
            print(f"  Dashboard: http://{self.web_host}:{self.web_port}")
        print("  Press Ctrl+C to stop")
        print("=" * 60 + "\n")

    async def stop(self):
        """Gracefully stop the entire system."""
        logger.info("Shutting down...")
        self._running = False

        # Stop all agents
        for name, agent in self.agents.items():
            logger.info(f"Stopping agent: {name}")
            agent._running = False

        # Cancel agent tasks
        for task in self._agent_tasks:
            task.cancel()

        # Wait for tasks to finish
        await asyncio.gather(*self._agent_tasks, return_exceptions=True)

        await self.knowledge_base.record_event("system_stopped", {
            "stopped_at": datetime.now().isoformat(),
        })

        logger.info("All agents stopped. System shutdown complete.")

    def _start_web_dashboard(self):
        """Start the Flask web dashboard in a separate thread."""
        from web.app import create_app

        app = create_app(
            message_bus=self.message_bus,
            task_queue=self.task_queue,
            knowledge_base=self.knowledge_base,
            orchestrator=self.orchestrator,
            agents=self.agents,
        )

        def run_web():
            app.run(host=self.web_host, port=self.web_port, debug=False, use_reloader=False)

        self._web_thread = threading.Thread(target=run_web, daemon=True)
        self._web_thread.start()
        logger.info(f"Web dashboard started on http://{self.web_host}:{self.web_port}")


async def run_demo(system: MultiAgentSystem):
    """Run a demonstration scenario to show the system in action."""
    logger.info("Running demonstration scenario...")

    # Wait for agents to initialize
    await asyncio.sleep(2)

    print("\n" + "─" * 60)
    print("  DEMO: System Health Check Workflow")
    print("─" * 60)

    # Submit a health check workflow
    workflow = system.orchestrator.create_system_health_check_workflow()
    workflow_id = await system.orchestrator.submit_workflow(workflow)

    print(f"  Submitted: {workflow.name} (ID: {workflow_id})")

    # Wait for it to complete
    await asyncio.sleep(5)

    progress = system.orchestrator.get_workflow_progress(workflow_id)
    print(f"\n  Workflow Progress:")
    for step in progress.get("steps", []):
        icon = "✓" if step["status"] == "completed" else "◌" if step["status"] == "pending" else "✗"
        print(f"    {icon} {step['name']} ({step['agent_type']}): {step['status']}")

    print("\n" + "─" * 60)
    print("  DEMO: Manual Task Creation")
    print("─" * 60)

    # Create some tasks
    tasks_created = []
    for i, (name, agent, priority) in enumerate([
        ("Analyze System Logs", "analyst", "normal"),
        ("Check Database Health", "monitor", "high"),
        ("Rotate Backup Files", "executor", "normal"),
        ("Generate Weekly Report", "analyst", "low"),
    ]):
        from core.task_queue import TaskPriority
        priority_map = {
            "critical": TaskPriority.CRITICAL,
            "high": TaskPriority.HIGH,
            "normal": TaskPriority.NORMAL,
            "low": TaskPriority.LOW,
        }
        task = system.task_queue.create_task(
            name=name,
            description=f"Demo task {i+1}",
            priority=priority_map[priority],
            assigned_agent=agent,
            created_by="demo",
            tags=["demo"],
        )
        tasks_created.append(task)
        print(f"  Created: {task.name} (ID: {task.id}, Agent: {agent}, Priority: {priority})")

    # Wait for tasks to process
    await asyncio.sleep(8)

    print("\n  Task Results:")
    for task in tasks_created:
        updated = system.task_queue.get_task(task.id)
        status = updated.status.value if updated else "unknown"
        icon = "✓" if status == "completed" else "◌"
        print(f"    {icon} {task.name}: {status}")

    # Show system stats
    queue_stats = system.task_queue.get_stats()
    kb_stats = await system.knowledge_base.get_stats()

    print("\n" + "─" * 60)
    print("  System Statistics")
    print("─" * 60)
    print(f"  Tasks: Total={queue_stats['total']}, By Status={queue_stats['by_status']}")
    print(f"  Knowledge Base: {kb_stats}")
    msgs = system.message_bus.get_stats()
    print(f"  Messages: {msgs['total_messages']}, Agents Connected: {msgs['agent_count']}")

    print("\n  Demo complete! The system continues running.")
    print("  Open http://127.0.0.1:5000 to view the dashboard.\n")


async def main():
    parser = argparse.ArgumentParser(description="Multi-Agent Collaborative Operations System")
    parser.add_argument("--no-web", action="store_true", help="Run without web dashboard")
    parser.add_argument("--demo", action="store_true", help="Run demo scenario on startup")
    parser.add_argument("--host", default="127.0.0.1", help="Web dashboard host")
    parser.add_argument("--port", type=int, default=5000, help="Web dashboard port")
    args = parser.parse_args()

    system = MultiAgentSystem(
        enable_web=not args.no_web,
        web_host=args.host,
        web_port=args.port,
    )

    # Handle graceful shutdown
    loop = asyncio.get_event_loop()

    def shutdown():
        logger.info("Received shutdown signal")
        asyncio.create_task(system.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            signal.signal(sig, lambda s, f: shutdown())

    try:
        await system.start()

        if args.demo:
            await run_demo(system)

        # Keep running until stopped
        while system._running:
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        await system.stop()


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║   Multi-Agent Collaborative Operations Automation       ║
    ║   多智能体协同运营自动化系统                              ║
    ║                                                          ║
    ║   Agents: Coordinator | Analyst | Executor | Monitor     ║
    ║   Version: 1.0.0                                        ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSystem terminated by user.")
