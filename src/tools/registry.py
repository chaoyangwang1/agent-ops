from dataclasses import dataclass
from typing import Callable, Any


@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: dict  # JSON Schema properties


class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, ToolDefinition] = {}
        self._handlers: dict[str, Callable] = {}

    def register(self, tool: ToolDefinition, handler: Callable = None):
        self._tools[tool.name] = tool
        if handler:
            self._handlers[tool.name] = handler

    def get_openai_schema(self) -> list[dict]:
        return [{
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": {
                    "type": "object",
                    "properties": t.parameters,
                    "required": list(t.parameters.keys()),
                },
            }
        } for t in self._tools.values()]

    def execute(self, name: str, args: dict) -> Any:
        handler = self._handlers.get(name)
        if not handler:
            raise ValueError(f"未知工具: {name}")
        return handler(**args)

    def list_tools(self) -> list[str]:
        return list(self._tools.keys())
