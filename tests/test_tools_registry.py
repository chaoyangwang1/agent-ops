from src.tools.registry import ToolRegistry, ToolDefinition


def test_register_and_list_tools():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="get_topology",
        description="查询服务拓扑",
        parameters={"service": {"type": "string"}},
    ))
    tools = registry.get_openai_schema()
    assert len(tools) == 1
    assert tools[0]["function"]["name"] == "get_topology"


def test_tool_execution():
    registry = ToolRegistry()
    called = False

    def handler(service: str):
        nonlocal called
        called = True
        return {"dependencies": ["redis", "mysql"]}

    registry.register(ToolDefinition(
        name="get_topology",
        description="查询服务拓扑",
        parameters={"service": {"type": "string"}},
    ), handler=handler)

    result = registry.execute("get_topology", {"service": "payment"})
    assert called
    assert "redis" in result["dependencies"]
