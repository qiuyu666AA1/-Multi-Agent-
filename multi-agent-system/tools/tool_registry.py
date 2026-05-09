"""Pluggable tool registry — agents discover and invoke tools through this."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Tool:
    name: str
    description: str
    category: str
    function: Callable
    parameters: Dict[str, Any] = field(default_factory=dict)  # param_name -> schema
    is_async: bool = False
    access_level: str = "read"  # read, write, admin
    timeout: float = 30.0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "parameters": self.parameters,
            "is_async": self.is_async,
            "access_level": self.access_level,
        }

    async def execute(self, **kwargs) -> dict:
        """Execute the tool with given parameters."""
        start = datetime.now()
        try:
            if self.is_async:
                result = await asyncio.wait_for(
                    self.function(**kwargs), timeout=self.timeout
                )
            else:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self.function(**kwargs)
                )
            return {
                "success": True,
                "tool": self.name,
                "result": result,
                "duration_ms": (datetime.now() - start).total_seconds() * 1000,
            }
        except asyncio.TimeoutError:
            return {
                "success": False,
                "tool": self.name,
                "error": f"Timeout after {self.timeout}s",
                "duration_ms": self.timeout * 1000,
            }
        except Exception as e:
            return {
                "success": False,
                "tool": self.name,
                "error": str(e),
                "duration_ms": (datetime.now() - start).total_seconds() * 1000,
            }


class ToolRegistry:
    """Central registry for all tools."""

    def __init__(self):
        self._tools: Dict[str, Tool] = {}
        self._tools_by_category: Dict[str, List[str]] = {}
        self._usage_stats: Dict[str, int] = {}  # tool_name -> call count

    def register(self, tool: Tool):
        """Register a tool."""
        self._tools[tool.name] = tool
        if tool.category not in self._tools_by_category:
            self._tools_by_category[tool.category] = []
        self._tools_by_category[tool.category].append(tool.name)
        self._usage_stats[tool.name] = 0

    def register_many(self, tools: List[Tool]):
        """Register multiple tools at once."""
        for tool in tools:
            self.register(tool)

    def unregister(self, tool_name: str):
        """Remove a tool."""
        tool = self._tools.pop(tool_name, None)
        if tool and tool.category in self._tools_by_category:
            self._tools_by_category[tool.category].remove(tool_name)

    def get_tool(self, name: str) -> Optional[Tool]:
        """Get a tool by name."""
        return self._tools.get(name)

    def get_tools_by_category(self, category: str) -> List[Tool]:
        """Get all tools in a category."""
        names = self._tools_by_category.get(category, [])
        return [self._tools[n] for n in names if n in self._tools]

    def list_categories(self) -> List[str]:
        """List all tool categories."""
        return list(self._tools_by_category.keys())

    def list_tools(self) -> List[dict]:
        """List all registered tools."""
        return [t.to_dict() for t in self._tools.values()]

    def search_tools(self, query: str) -> List[dict]:
        """Search tools by name or description."""
        query_lower = query.lower()
        results = []
        for tool in self._tools.values():
            if query_lower in tool.name.lower() or query_lower in tool.description.lower():
                results.append(tool.to_dict())
        return results

    async def invoke(self, tool_name: str, **kwargs) -> dict:
        """Invoke a tool by name."""
        tool = self._tools.get(tool_name)
        if not tool:
            return {"success": False, "error": f"Tool '{tool_name}' not found"}
        self._usage_stats[tool_name] += 1
        return await tool.execute(**kwargs)

    def get_stats(self) -> dict:
        return {
            "total_tools": len(self._tools),
            "categories": self.list_categories(),
            "usage": dict(self._usage_stats),
        }
