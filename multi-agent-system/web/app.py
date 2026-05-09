"""Flask web dashboard for multi-agent system monitoring and control."""

import json
import time
from datetime import datetime
from typing import Optional

from flask import Flask, render_template, request, jsonify, Response

from core.message_bus import MessageBus, Message, MessageType
from core.task_queue import TaskQueue, TaskPriority
from core.knowledge_base import KnowledgeBase
from core.orchestrator import Orchestrator


class SystemState:
    """Shared system state holder, set after system initialization."""
    message_bus: Optional[MessageBus] = None
    task_queue: Optional[TaskQueue] = None
    knowledge_base: Optional[KnowledgeBase] = None
    orchestrator: Optional[Orchestrator] = None
    agents: dict = {}


state = SystemState()


def create_app(
    message_bus: MessageBus = None,
    task_queue: TaskQueue = None,
    knowledge_base: KnowledgeBase = None,
    orchestrator: Orchestrator = None,
    agents: dict = None,
) -> Flask:
    """Create and configure the Flask web application."""
    state.message_bus = message_bus
    state.task_queue = task_queue
    state.knowledge_base = knowledge_base
    state.orchestrator = orchestrator
    state.agents = agents or {}

    app = Flask(__name__)

    # ---- Routes ----

    @app.route("/")
    def index():
        """Main dashboard page."""
        return render_template("index.html")

    @app.route("/dashboard")
    def dashboard():
        """Full dashboard."""
        return render_template("dashboard.html")

    # ---- API: Overview ----

    @app.route("/api/status")
    def api_status():
        """Get overall system status."""
        if not state.task_queue or not state.knowledge_base:
            return jsonify({"error": "System not initialized"}), 503

        queue_stats = state.task_queue.get_stats()

        agent_statuses = {}
        for name, agent in state.agents.items():
            agent_statuses[name] = agent.get_status()

        return jsonify({
            "system": {
                "uptime": "running",
                "agent_count": len(state.agents),
                "timestamp": datetime.now().isoformat(),
            },
            "agents": agent_statuses,
            "tasks": queue_stats,
            "messages": {
                "total": len(state.message_bus._message_log) if state.message_bus else 0,
            },
        })

    @app.route("/api/agents")
    def api_agents():
        """Get agent details."""
        agents_data = {}
        for name, agent in state.agents.items():
            agents_data[name] = {
                "name": name,
                "status": agent.get_status(),
                "capabilities": agent._capabilities if hasattr(agent, '_capabilities') else [],
            }
        return jsonify({"agents": agents_data, "count": len(agents_data)})

    # ---- API: Tasks ----

    @app.route("/api/tasks")
    def api_tasks():
        """Get all tasks with optional filtering."""
        if not state.task_queue:
            return jsonify({"error": "Not initialized"}), 503

        status_filter = request.args.get("status", "")
        agent_filter = request.args.get("agent", "")
        limit = int(request.args.get("limit", 50))

        if status_filter:
            from core.task_queue import TaskStatus
            try:
                status_enum = TaskStatus(status_filter)
                tasks = state.task_queue.get_tasks_by_status(status_enum)
            except ValueError:
                tasks = state.task_queue.get_all_tasks()
        elif agent_filter:
            tasks = state.task_queue.get_tasks_by_agent(agent_filter)
        else:
            tasks = state.task_queue.get_all_tasks()

        tasks = tasks[-limit:]
        return jsonify({
            "tasks": [t.to_dict() for t in tasks],
            "count": len(tasks),
        })

    @app.route("/api/tasks", methods=["POST"])
    def api_create_task():
        """Create a new task."""
        if not state.task_queue:
            return jsonify({"error": "Not initialized"}), 503

        data = request.get_json() or {}
        priority_map = {
            "critical": TaskPriority.CRITICAL,
            "high": TaskPriority.HIGH,
            "normal": TaskPriority.NORMAL,
            "low": TaskPriority.LOW,
        }

        task = state.task_queue.create_task(
            name=data.get("name", "Untitled Task"),
            description=data.get("description", ""),
            priority=priority_map.get(data.get("priority", "normal"), TaskPriority.NORMAL),
            payload=data.get("payload", {}),
            assigned_agent=data.get("assigned_agent", ""),
            tags=data.get("tags", []),
            created_by="web_ui",
        )

        # If coordinator is running, notify it
        if state.message_bus and "coordinator" in state.agents:
            import asyncio
            async def notify():
                await state.message_bus.publish(Message(
                    type=MessageType.COMMAND,
                    topic="agent.coordinator",
                    sender="web_ui",
                    recipient="coordinator",
                    payload={
                        "command": "process_request",
                        "params": {
                            "type": data.get("request_type", "health_check"),
                            "priority": data.get("priority", "normal"),
                            **data.get("payload", {}),
                        },
                    },
                ))
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(notify())
            except RuntimeError:
                pass

        return jsonify({"task": task.to_dict(), "status": "created"}), 201

    @app.route("/api/tasks/<task_id>")
    def api_task_detail(task_id: str):
        """Get task details."""
        if not state.task_queue:
            return jsonify({"error": "Not initialized"}), 503

        task = state.task_queue.get_task(task_id)
        if not task:
            return jsonify({"error": "Task not found"}), 404

        return jsonify(task.to_dict())

    @app.route("/api/tasks/<task_id>/cancel", methods=["POST"])
    def api_task_cancel(task_id: str):
        """Cancel a task."""
        if not state.task_queue:
            return jsonify({"error": "Not initialized"}), 503

        success = state.task_queue.mark_cancelled(task_id)
        return jsonify({"cancelled": success})

    # ---- API: Workflows ----

    @app.route("/api/workflows")
    def api_workflows():
        """Get all workflows."""
        if not state.orchestrator:
            return jsonify({"error": "Orchestrator not initialized"}), 503

        workflows = state.orchestrator.get_all_workflows()
        return jsonify({
            "workflows": [w.to_dict() for w in workflows],
            "count": len(workflows),
        })

    @app.route("/api/workflows", methods=["POST"])
    def api_create_workflow():
        """Create and submit a new workflow."""
        if not state.orchestrator:
            return jsonify({"error": "Orchestrator not initialized"}), 503

        data = request.get_json() or {}
        workflow_type = data.get("type", "health_check")

        if workflow_type == "health_check":
            workflow = state.orchestrator.create_system_health_check_workflow()
        elif workflow_type == "data_analysis":
            workflow = state.orchestrator.create_data_analysis_workflow(
                data.get("data_source", "system")
            )
        elif workflow_type == "incident_response":
            workflow = state.orchestrator.create_incident_response_workflow(
                data.get("incident_type", "unknown"),
                data.get("severity", "medium"),
            )
        else:
            return jsonify({"error": f"Unknown workflow type: {workflow_type}"}), 400

        import asyncio
        async def submit():
            return await state.orchestrator.submit_workflow(workflow)

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(submit(), loop)
                workflow_id = future.result(timeout=5)
            else:
                workflow_id = asyncio.run(submit())
        except RuntimeError:
            workflow_id = asyncio.run(submit())

        return jsonify({"workflow_id": workflow_id, "status": "submitted"}), 201

    @app.route("/api/workflows/<workflow_id>")
    def api_workflow_detail(workflow_id: str):
        """Get workflow progress."""
        if not state.orchestrator:
            return jsonify({"error": "Not initialized"}), 503

        progress = state.orchestrator.get_workflow_progress(workflow_id)
        if not progress:
            return jsonify({"error": "Workflow not found"}), 404

        return jsonify(progress)

    # ---- API: Events / Messages ----

    @app.route("/api/events")
    def api_events():
        """Get recent events/messages."""
        event_type = request.args.get("type", "")
        limit = int(request.args.get("limit", 50))

        if state.message_bus:
            messages = state.message_bus.get_recent_messages(limit)
        else:
            messages = []

        return jsonify({"events": messages, "count": len(messages)})

    @app.route("/api/events/stream")
    def api_events_stream():
        """SSE stream of real-time events."""
        def event_stream():
            last_count = len(state.message_bus._message_log) if state.message_bus else 0
            while True:
                time.sleep(1)
                if state.message_bus:
                    current_count = len(state.message_bus._message_log)
                    if current_count > last_count:
                        new_messages = state.message_bus._message_log[last_count:current_count]
                        last_count = current_count
                        for msg in new_messages:
                            yield f"data: {json.dumps(msg.to_dict())}\n\n"
        return Response(event_stream(), mimetype="text/event-stream")

    # ---- API: Knowledge Base ----

    @app.route("/api/kb/facts")
    def api_kb_facts():
        """Get all knowledge base facts."""
        if not state.knowledge_base:
            return jsonify({"error": "Not initialized"}), 503
        import asyncio
        async def get_facts():
            return await state.knowledge_base.get_all_facts()
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(get_facts(), loop)
                facts = future.result(timeout=5)
            else:
                facts = asyncio.run(get_facts())
        except RuntimeError:
            facts = asyncio.run(get_facts())
        return jsonify({"facts": facts})

    @app.route("/api/kb/stats")
    def api_kb_stats():
        """Get knowledge base statistics."""
        if not state.knowledge_base:
            return jsonify({"error": "Not initialized"}), 503
        import asyncio
        async def get_stats():
            return await state.knowledge_base.get_stats()
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(get_stats(), loop)
                stats = future.result(timeout=5)
            else:
                stats = asyncio.run(get_stats())
        except RuntimeError:
            stats = asyncio.run(get_stats())
        return jsonify({"stats": stats})

    # ---- API: Control ----

    @app.route("/api/control/agent/<agent_name>/<action>", methods=["POST"])
    def api_agent_control(agent_name: str, action: str):
        """Control agents (restart, stop, status)."""
        agent = state.agents.get(agent_name)
        if not agent:
            return jsonify({"error": f"Agent '{agent_name}' not found"}), 404

        if action == "status":
            return jsonify({"agent": agent_name, "status": agent.get_status()})
        else:
            return jsonify({"error": f"Unknown action: {action}"}), 400

    return app
