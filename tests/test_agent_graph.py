import pytest
from unittest.mock import Mock, patch
from src.agent.state import AgentState
from src.agent.graph import build_graph
from src.tools.registry import ToolRegistry, ToolDefinition


@pytest.fixture
def mock_tools():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="query_prometheus",
        description="查询 Prometheus 指标",
        parameters={"query": {"type": "string"}},
    ), handler=lambda query: {"result": "CPU 92%"})
    return registry


def test_agent_graph_compiles(mock_tools):
    graph = build_graph(mock_tools)
    assert graph is not None


def test_agent_runs_diagnosis_loop(mock_tools):
    """验证 Agent 能完成完整的诊断循环"""
    graph = build_graph(mock_tools)

    initial_state = AgentState(
        alert={
            "alert_id": "test-001",
            "severity": "critical",
            "labels": {"service": "payment"},
            "annotations": {"summary": "CPU > 90%"},
            "source": "prometheus",
        },
        messages=[],
        hypothesis="",
        hypothesis_history=[],
        evidence={},
        diagnosis="",
        action_plan={},
        active_intent="diagnose",
        pending_confirmations=[],
        protected_data=[],
        compressed_memory=[],
        mode="full",
        step_count=0,
        start_time=0,
    )

    with patch("langchain_openai.ChatOpenAI") as mock_llm_cls:
        mock_llm = Mock()
        mock_llm.invoke.return_value = Mock(content="诊断完成", tool_calls=None)
        mock_llm_cls.return_value = mock_llm
        mock_llm.bind_tools.return_value = mock_llm

        result = graph.invoke(initial_state, config={"recursion_limit": 5})
        assert "messages" in result
        assert result["step_count"] >= 0
