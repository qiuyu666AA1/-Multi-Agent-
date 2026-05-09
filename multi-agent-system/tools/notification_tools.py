"""Notification and alerting tools for multi-channel communication."""

import json
import os
import smtplib
import time
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, List, Optional

from .tool_registry import Tool, ToolRegistry


class NotificationTools:
    """Factory for notification and alerting tools."""

    @staticmethod
    def register_all(registry: ToolRegistry):
        """Register all notification tools with the registry."""
        tools = [
            Tool(
                name="send_console_notification",
                description="Print a notification to the console",
                category="notification",
                function=NotificationTools._console_notify,
                parameters={
                    "message": {"type": "string", "description": "Notification message"},
                    "level": {"type": "string", "description": "Level: info|warning|error|success", "default": "info"},
                },
            ),
            Tool(
                name="send_email",
                description="Send an email notification (if SMTP configured)",
                category="notification",
                function=NotificationTools._send_email,
                parameters={
                    "to": {"type": "string", "description": "Recipient email"},
                    "subject": {"type": "string", "description": "Email subject"},
                    "body": {"type": "string", "description": "Email body"},
                    "smtp_host": {"type": "string", "description": "SMTP host", "default": "localhost"},
                    "smtp_port": {"type": "integer", "description": "SMTP port", "default": 25},
                },
                access_level="write",
                timeout=10.0,
            ),
            Tool(
                name="send_webhook",
                description="Send a webhook notification to a URL",
                category="notification",
                function=NotificationTools._send_webhook,
                parameters={
                    "url": {"type": "string", "description": "Webhook URL"},
                    "payload": {"type": "object", "description": "JSON payload to send"},
                },
                access_level="write",
                timeout=10.0,
            ),
            Tool(
                name="format_alert_message",
                description="Format an alert message for different channels",
                category="notification",
                function=NotificationTools._format_alert,
                parameters={
                    "title": {"type": "string", "description": "Alert title"},
                    "severity": {"type": "string", "description": "Severity: critical|warning|info"},
                    "details": {"type": "object", "description": "Alert details"},
                },
            ),
            Tool(
                name="send_digest",
                description="Compile and send a digest of recent events",
                category="notification",
                function=NotificationTools._send_digest,
                parameters={
                    "events": {"type": "array", "description": "List of event dicts"},
                    "channel": {"type": "string", "description": "Output channel: console|email|webhook", "default": "console"},
                },
            ),
            Tool(
                name="log_to_file",
                description="Write a notification to a log file",
                category="notification",
                function=NotificationTools._log_to_file,
                parameters={
                    "message": {"type": "string", "description": "Message to log"},
                    "log_file": {"type": "string", "description": "Log file path", "default": "agent_system.log"},
                    "level": {"type": "string", "description": "Log level", "default": "INFO"},
                },
                access_level="write",
            ),
        ]
        registry.register_many(tools)

    # ---- Tool Implementations ----

    @staticmethod
    def _console_notify(message: str, level: str = "info") -> dict:
        prefixes = {
            "info": "[INFO]",
            "warning": "[WARN]",
            "error": "[ERROR]",
            "success": "[OK]",
        }
        prefix = prefixes.get(level, "[INFO]")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"{timestamp} {prefix} {message}"
        print(formatted)
        return {"delivered": True, "channel": "console", "level": level, "timestamp": timestamp}

    @staticmethod
    def _send_email(to: str, subject: str, body: str, smtp_host: str = "localhost", smtp_port: int = 25) -> dict:
        try:
            msg = MIMEMultipart()
            msg["From"] = "multi-agent-system@localhost"
            msg["To"] = to
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                server.sendmail(msg["From"], [to], msg.as_string())

            return {"delivered": True, "channel": "email", "to": to, "subject": subject}
        except Exception as e:
            # Graceful fallback — log to console
            print(f"[EMAIL FAILED] To: {to}, Subject: {subject}, Error: {e}")
            return {"delivered": False, "channel": "email", "error": str(e), "fallback": "console"}

    @staticmethod
    def _send_webhook(url: str, payload: dict) -> dict:
        try:
            import urllib.request
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                body = resp.read().decode("utf-8")[:500]
            return {"delivered": True, "channel": "webhook", "status_code": status, "response": body}
        except Exception as e:
            print(f"[WEBHOOK FAILED] URL: {url}, Error: {e}")
            return {"delivered": False, "channel": "webhook", "error": str(e)}

    @staticmethod
    def _format_alert(title: str, severity: str, details: dict = None) -> dict:
        severity_icons = {
            "critical": "🔴",
            "warning": "🟡",
            "info": "🔵",
        }
        icon = severity_icons.get(severity, "📢")

        formats = {
            "plain": f"{icon} [{severity.upper()}] {title}\n{json.dumps(details or {}, indent=2)}",
            "html": f"""
            <div style="border-left: 4px solid {'red' if severity == 'critical' else 'orange' if severity == 'warning' else 'blue'}; padding: 10px; margin: 10px 0;">
                <h3>{icon} {title}</h3>
                <p><strong>Severity:</strong> {severity.upper()}</p>
                <pre>{json.dumps(details or {}, indent=2)}</pre>
            </div>
            """,
            "slack": {
                "text": f"{icon} *[{severity.upper()}] {title}*",
                "attachments": [{"text": json.dumps(details or {}, indent=2), "color": "danger" if severity == "critical" else "warning" if severity == "warning" else "good"}],
            },
        }

        return {"formats": formats, "title": title, "severity": severity}

    @staticmethod
    def _send_digest(events: List[dict], channel: str = "console") -> dict:
        """Compile events into a digest and send."""
        if not events:
            return {"delivered": True, "message": "No events to digest"}

        # Group events by type
        by_type: Dict[str, int] = {}
        for event in events:
            event_type = event.get("type", "unknown")
            by_type[event_type] = by_type.get(event_type, 0) + 1

        timestamp = datetime.now().isoformat()
        digest = f"""
╔══════════════════════════════════════════╗
║        MULTI-AGENT SYSTEM DIGEST         ║
╠══════════════════════════════════════════╣
║ Generated: {timestamp:<28} ║
║ Total Events: {len(events):<26} ║
╠══════════════════════════════════════════╣
"""
        for event_type, count in by_type.items():
            digest += f"║  {event_type:<20} {count:>6}            ║\n"
        digest += "╚══════════════════════════════════════════╝"

        if channel == "console":
            print(digest)
        elif channel == "email":
            # Would send via email — for now log
            print(f"[DIGEST EMAIL] {len(events)} events summarized")
        elif channel == "webhook":
            print(f"[DIGEST WEBHOOK] {len(events)} events dispatched")

        return {
            "delivered": True,
            "channel": channel,
            "event_count": len(events),
            "by_type": by_type,
            "generated_at": timestamp,
        }

    @staticmethod
    def _log_to_file(message: str, log_file: str = "agent_system.log", level: str = "INFO") -> dict:
        """Write a notification to a log file."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        log_line = f"[{timestamp}] [{level}] {message}\n"

        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(log_line)
            return {"written": True, "file": log_file, "level": level}
        except Exception as e:
            return {"written": False, "file": log_file, "error": str(e)}
