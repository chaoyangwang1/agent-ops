from src.agent.tools import create_agent_tools


def test_create_tools_without_clients():
    """无外部依赖时也应创建 mock 工具"""
    registry = create_agent_tools()
    tools = registry.list_tools()
    assert "search_logs" in tools
    assert "get_topology" in tools
    assert "analyze_blast_radius" in tools


def test_mock_tools_are_callable():
    """Mock 工具应能正常调用不报错"""
    registry = create_agent_tools()
    result = registry.execute("search_logs", {"service": "test", "keywords": "error", "time_range": "5m"})
    assert isinstance(result, dict)


def test_tools_have_openai_schema():
    """工具应能生成 OpenAI Function Calling Schema"""
    registry = create_agent_tools()
    schema = registry.get_openai_schema()
    assert len(schema) == 4
    names = [s["function"]["name"] for s in schema]
    assert "search_logs" in names
    assert "get_topology" in names
    assert "analyze_blast_radius" in names
    assert "search_similar_incidents" in names


def test_tools_count_with_incident_store():
    """包含知识库时应有 4 个工具"""
    registry = create_agent_tools()
    tools = registry.list_tools()
    assert len(tools) == 4
    assert "search_similar_incidents" in tools
    # 验证 mock 工具可调用
    result = registry.execute("search_similar_incidents", {"description": "test", "top_k": 3})
    assert isinstance(result, dict)
