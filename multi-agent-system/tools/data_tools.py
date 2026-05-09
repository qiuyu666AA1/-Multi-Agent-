"""Data processing and analysis tools for ETL operations."""

import csv
import json
import os
import random
import time
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Optional

from .tool_registry import Tool, ToolRegistry


class DataTools:
    """Factory for data processing tools."""

    @staticmethod
    def register_all(registry: ToolRegistry):
        """Register all data tools with the registry."""
        tools = [
            Tool(
                name="parse_json",
                description="Parse a JSON string into a Python object",
                category="data",
                function=DataTools._parse_json,
                parameters={
                    "json_string": {"type": "string", "description": "JSON string to parse"},
                },
            ),
            Tool(
                name="parse_csv",
                description="Parse CSV content into a list of dictionaries",
                category="data",
                function=DataTools._parse_csv,
                parameters={
                    "csv_content": {"type": "string", "description": "CSV content to parse"},
                    "delimiter": {"type": "string", "description": "CSV delimiter", "default": ","},
                },
            ),
            Tool(
                name="aggregate_metrics",
                description="Aggregate time-series metrics (avg, min, max, p95, count)",
                category="data",
                function=DataTools._aggregate_metrics,
                parameters={
                    "values": {"type": "array", "description": "List of numeric values"},
                },
            ),
            Tool(
                name="detect_anomalies",
                description="Detect anomalies using IQR method",
                category="data",
                function=DataTools._detect_anomalies,
                parameters={
                    "values": {"type": "array", "description": "List of numeric values"},
                    "threshold": {"type": "number", "description": "IQR multiplier threshold", "default": 1.5},
                },
            ),
            Tool(
                name="format_table",
                description="Format data as a text table",
                category="data",
                function=DataTools._format_table,
                parameters={
                    "data": {"type": "array", "description": "List of dicts to format"},
                    "columns": {"type": "array", "description": "Column names to include"},
                },
            ),
            Tool(
                name="calculate_statistics",
                description="Calculate detailed statistics for a dataset",
                category="data",
                function=DataTools._calculate_statistics,
                parameters={
                    "values": {"type": "array", "description": "List of numeric values"},
                },
            ),
            Tool(
                name="generate_sample_data",
                description="Generate sample test data",
                category="data",
                function=DataTools._generate_sample_data,
                parameters={
                    "record_count": {"type": "integer", "description": "Number of records", "default": 100},
                    "schema": {"type": "string", "description": "Schema type: metrics|logs|events|users", "default": "metrics"},
                },
            ),
            Tool(
                name="filter_data",
                description="Filter a list of dicts by conditions",
                category="data",
                function=DataTools._filter_data,
                parameters={
                    "data": {"type": "array", "description": "List of dicts to filter"},
                    "field": {"type": "string", "description": "Field to filter on"},
                    "operator": {"type": "string", "description": "Comparison: eq, ne, gt, lt, gte, lte, contains"},
                    "value": {"type": "string", "description": "Value to compare against"},
                },
            ),
        ]
        registry.register_many(tools)

    # ---- Tool Implementations ----

    @staticmethod
    def _parse_json(json_string: str) -> dict:
        try:
            data = json.loads(json_string)
            return {"parsed": True, "data": data, "type": type(data).__name__}
        except json.JSONDecodeError as e:
            return {"parsed": False, "error": str(e)}

    @staticmethod
    def _parse_csv(csv_content: str, delimiter: str = ",") -> dict:
        try:
            reader = csv.DictReader(StringIO(csv_content), delimiter=delimiter)
            rows = list(reader)
            return {
                "parsed": True,
                "rows": rows,
                "count": len(rows),
                "columns": reader.fieldnames if hasattr(reader, 'fieldnames') else (list(rows[0].keys()) if rows else []),
            }
        except Exception as e:
            return {"parsed": False, "error": str(e)}

    @staticmethod
    def _aggregate_metrics(values: List[float]) -> dict:
        if not values:
            return {"error": "No values provided"}

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        p95_idx = int(n * 0.95)

        return {
            "count": n,
            "sum": round(sum(values), 2),
            "avg": round(sum(values) / n, 2),
            "min": min(values),
            "max": max(values),
            "p50": sorted_vals[n // 2],
            "p95": sorted_vals[min(p95_idx, n - 1)],
            "p99": sorted_vals[min(int(n * 0.99), n - 1)],
        }

    @staticmethod
    def _detect_anomalies(values: List[float], threshold: float = 1.5) -> dict:
        if len(values) < 4:
            return {"error": "Need at least 4 values", "anomalies": []}

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        iqr = q3 - q1

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        anomalies = []
        for i, v in enumerate(values):
            if v < lower_bound or v > upper_bound:
                anomalies.append({"index": i, "value": v, "bound": "lower" if v < lower_bound else "upper"})

        return {
            "q1": q1,
            "q3": q3,
            "iqr": iqr,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "total_count": len(values),
        }

    @staticmethod
    def _format_table(data: List[dict], columns: List[str] = None) -> dict:
        if not data:
            return {"table": "", "rows": 0}

        if columns is None:
            columns = list(data[0].keys())

        # Build table
        col_widths = {c: len(c) for c in columns}
        for row in data:
            for c in columns:
                val = str(row.get(c, ""))
                col_widths[c] = max(col_widths[c], len(val))

        header = " | ".join(c.ljust(col_widths[c]) for c in columns)
        separator = "-+-".join("-" * col_widths[c] for c in columns)
        rows = [" | ".join(str(row.get(c, "")).ljust(col_widths[c]) for c in columns) for row in data]

        table = "\n".join([header, separator] + rows)
        return {"table": table, "rows": len(data), "columns": columns}

    @staticmethod
    def _calculate_statistics(values: List[float]) -> dict:
        if not values:
            return {"error": "No values provided"}

        n = len(values)
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / n
        std_dev = variance ** 0.5
        sorted_vals = sorted(values)

        return {
            "count": n,
            "mean": round(mean, 4),
            "median": sorted_vals[n // 2] if n % 2 == 1 else (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2,
            "mode": max(set(values), key=values.count) if values else None,
            "std_dev": round(std_dev, 4),
            "variance": round(variance, 4),
            "min": min(values),
            "max": max(values),
            "range": max(values) - min(values),
            "skewness": round(sum(((x - mean) / std_dev) ** 3 for x in values) / n, 4) if std_dev > 0 else 0,
        }

    @staticmethod
    def _generate_sample_data(record_count: int = 100, schema: str = "metrics") -> dict:
        records = []
        base_time = int(time.time())

        if schema == "metrics":
            for i in range(record_count):
                records.append({
                    "timestamp": datetime.fromtimestamp(base_time - (record_count - i) * 60).isoformat(),
                    "cpu_percent": round(random.uniform(10, 90), 1),
                    "memory_percent": round(random.uniform(20, 85), 1),
                    "disk_percent": round(random.uniform(30, 70), 1),
                    "requests_per_sec": random.randint(100, 5000),
                })
        elif schema == "logs":
            levels = ["INFO", "INFO", "INFO", "WARN", "ERROR"]
            services = ["api", "worker", "database", "frontend"]
            for i in range(record_count):
                level = random.choice(levels)
                records.append({
                    "timestamp": datetime.fromtimestamp(base_time - (record_count - i) * 30).isoformat(),
                    "level": level,
                    "service": random.choice(services),
                    "message": f"Sample log message {i}",
                })
        elif schema == "events":
            event_types = ["click", "view", "submit", "error", "login", "logout"]
            for i in range(record_count):
                records.append({
                    "timestamp": datetime.fromtimestamp(base_time - (record_count - i) * 10).isoformat(),
                    "event_type": random.choice(event_types),
                    "user_id": f"user_{random.randint(1, 100)}",
                    "session_id": f"sess_{random.randint(1, 20)}",
                })
        elif schema == "users":
            domains = ["example.com", "test.org", "demo.io"]
            for i in range(record_count):
                records.append({
                    "id": i + 1,
                    "username": f"user_{i+1}",
                    "email": f"user_{i+1}@{random.choice(domains)}",
                    "role": random.choice(["admin", "editor", "viewer"]),
                    "active": random.random() > 0.1,
                })

        return {"schema": schema, "records": records, "count": len(records)}

    @staticmethod
    def _filter_data(data: List[dict], field: str, operator: str, value: str) -> dict:
        operators = {
            "eq": lambda a, b: str(a) == str(b),
            "ne": lambda a, b: str(a) != str(b),
            "gt": lambda a, b: float(a) > float(b),
            "lt": lambda a, b: float(a) < float(b),
            "gte": lambda a, b: float(a) >= float(b),
            "lte": lambda a, b: float(a) <= float(b),
            "contains": lambda a, b: str(b).lower() in str(a).lower(),
        }

        op_func = operators.get(operator)
        if not op_func:
            return {"error": f"Unknown operator: {operator}", "valid_operators": list(operators.keys())}

        try:
            filtered = [row for row in data if field in row and op_func(row[field], value)]
            return {"filtered": filtered, "count": len(filtered), "original_count": len(data)}
        except Exception as e:
            return {"error": str(e)}
