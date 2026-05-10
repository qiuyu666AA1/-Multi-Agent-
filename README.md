# Multi-Agent Collaborative Operations Automation System

多智能体协同运营自动化系统 — 基于 Python 异步架构的智能运维多智能体协作系统。

## Architecture

```
┌──────────────────────────────────────────────────┐
│                   Web Dashboard                    │
│            (Flask + SSE real-time)                │
└──────┬───────────────────────────────────────────┘
       │
┌──────▼───────────────────────────────────────────┐
│                 Orchestrator                       │
│     (Workflow engine, dependency resolution)       │
└──────┬───────────────────────────────────────────┘
       │
┌──────▼──────┐ ┌────────────┐ ┌──────────┐ ┌──────┐
│ Coordinator │ │  Analyst   │ │ Executor │ │Monitor│
│  (路由调度)  │ │ (数据分析)  │ │ (任务执行)│ │(监控) │
└──────┬──────┘ └─────┬──────┘ └────┬─────┘ └──┬───┘
       └───────────────┼─────────────┼──────────┘
                       │             │
              ┌────────▼──────┐  ┌──▼───────────┐
              │  Message Bus  │  │  Task Queue   │
              │  (pub/sub)    │  │  (priority)   │
              └────────┬──────┘  └──┬───────────┘
                       │             │
              ┌────────▼─────────────▼───────────┐
              │         Knowledge Base            │
              │  (facts, metrics, events, KB)     │
              └──────────────────────────────────┘
```

## Agents

| Agent | Role | Capabilities |
|-------|------|-------------|
| **Coordinator** | Task decomposition & routing | Request parsing, subtask generation, agent routing, session tracking |
| **Analyst** | Data analysis & reporting | Statistical analysis, trend detection, anomaly detection, report generation |
| **Executor** | System operations | Resource checks, deployments, backups, log cleanup, incident containment |
| **Monitor** | Health monitoring & alerting | Continuous resource monitoring, heartbeat tracking, threshold alerts, service health |

## Workflows

Pre-built workflow templates in `config.yaml`:

- **Health Check** — Disk → Memory → Processes → Analyze → Notify
- **Data Analysis** — Collect → Clean → Analyze → Report
- **Incident Response** — Assess → Contain → Remediate → Verify → Postmortem
- **Maintenance** — Backup → Cleanup Logs → Verify → Report

## Quick Start

### Prerequisites

- Python 3.8+
- pip

### Install

```bash
git clone <repo-url>
cd multi-agent-system
pip install -r requirements.txt
```

### Run

```bash
# Full system with web dashboard
python run.py

# CLI-only mode (no web UI)
python run.py --no-web

# Run with demo scenario
python run.py --demo

# Custom host/port
python run.py --host 0.0.0.0 --port 8080
```

The web dashboard will be available at `http://127.0.0.1:5000`.

## Project Structure

```
multi-agent-system/
├── agents/                   # Specialized agents
│   ├── base_agent.py         # Abstract base agent (message loop, heartbeat, task processing)
│   ├── coordinator_agent.py  # Task decomposition, routing, workflow orchestration
│   ├── analyst_agent.py      # Statistical analysis, anomaly detection, reporting
│   ├── executor_agent.py     # System operations, deployments, incident response
│   └── monitor_agent.py      # Resource monitoring, alerting, heartbeat tracking
├── core/                     # Core infrastructure
│   ├── message_bus.py        # Async publish/subscribe message bus
│   ├── task_queue.py         # Priority-based distributed task queue
│   ├── knowledge_base.py     # Facts, metrics, events, and agent capability store
│   └── orchestrator.py       # Workflow engine with dependency resolution
├── tools/                    # Tool registry and built-in tools
│   ├── tool_registry.py      # Central tool registration and discovery
│   ├── system_tools.py       # System-level utilities
│   ├── data_tools.py         # Data processing utilities
│   └── notification_tools.py # Console, email, webhook notifications
├── web/                      # Web dashboard
│   ├── app.py                # Flask application with REST API + SSE
│   ├── templates/
│   │   ├── base.html         # Base template
│   │   ├── index.html        # Dashboard home
│   │   └── dashboard.html    # Full system dashboard
│   └── static/
│       ├── app.js            # Frontend JavaScript (real-time updates)
│       └── style.css         # Dashboard styles
├── config.yaml               # System configuration
├── requirements.txt          # Python dependencies
└── run.py                    # Main entry point
```

## REST API

The dashboard exposes the following endpoints:

### System
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Overall system status, agent states, task stats |
| GET | `/api/agents` | Agent details and capabilities |

### Tasks
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/tasks` | List tasks (filter: `?status=`, `?agent=`, `?limit=`) |
| POST | `/api/tasks` | Create a new task |
| GET | `/api/tasks/<id>` | Task detail |
| POST | `/api/tasks/<id>/cancel` | Cancel a task |

### Workflows
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/workflows` | List all workflows |
| POST | `/api/workflows` | Create and submit a workflow |
| GET | `/api/workflows/<id>` | Workflow progress with step status |

### Events
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/events` | Recent system events |
| GET | `/api/events/stream` | SSE real-time event stream |

## Configuration

Edit `config.yaml` to customize:

- **Agent settings** — heartbeat intervals, max concurrent tasks
- **Alert thresholds** — CPU, memory, disk, agent timeout, error rate
- **Notification channels** — console, email (SMTP), webhook
- **Task queue** — retry limits, timeouts, cleanup policy
- **Workflow templates** — custom step definitions

## Dependencies

- **Flask** — Web dashboard
- **PyYAML** — Configuration parsing
- **psutil** (optional) — Real system metrics (falls back to simulated data)

All other dependencies use Python standard library (`asyncio`, `json`, `uuid`, `dataclasses`, `subprocess`, etc.).

## License

MIT
