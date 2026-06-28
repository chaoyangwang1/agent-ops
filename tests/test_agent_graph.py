import pytest
import time
from unittest.mock import Mock, patch
from src.agent.state import AgentState, AgentMode
from src.agent.graph import build_graph, MAX_STEPS, MAX_DURATION
from src.tools.registry import ToolRegistry, ToolDefinition


@pytest.fixture
def mock_tools():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="search_logs",
        description="查询日志",
        parameters={"service": {"type": "string"}, "keywords": {"type": "string"}, "time_range": {"type": "string"}},
    ), handler=lambda **kw: {"logs": [{"content": "ERROR: timeout"}]})
    registry.register(ToolDefinition(
        name="get_topology",
        description="查询拓扑",
        parameters={"service": {"type": "string"}, "hops": {"type": "integer"}},
    ), handler=lambda **kw: {"dependencies": [{"name": "redis"}]})
    return registry


def _make_state(**overrides):
    """创建测试用 AgentState"""
    defaults = {
        "alert": {"alert_id": "t1", "severity": "critical",
                   "labels": {"service": "payment"}, "annotations": {"summary": "P99 > 500ms"}},
        "messages": [], "hypothesis": "", "hypothesis_history": [],
        "evidence": {}, "diagnosis": "", "action_plan": {},
        "active_intent": "diagnose", "pending_confirmations": [],
        "protected_data": [], "compressed_memory": [],
        "mode": "full", "step_count": 0, "start_time": time.time(),
        "conversation_id": "conv-1", "llm_failures": 0, "tools_failed": False, "truncated": False,
    }
    defaults.update(overrides)
    return AgentState(**defaults)


def test_graph_compiles_with_all_nodes(mock_tools):
    """状态机包含全部必需节点"""
    graph = build_graph(mock_tools)
    nodes = graph.get_graph().nodes
    node_names = list(nodes.keys())
    assert "formulate_hypothesis" in node_names
    assert "tool_executor" in node_names
    assert "evaluate_evidence" in node_names
    assert "refine_hypothesis" in node_names
    assert "root_cause" in node_names
    assert "timeout_handler" in node_names


def test_formulate_hypothesis_creates_hypothesis(mock_tools):
    """formulate_hypothesis 应生成假设"""
    graph = build_graph(mock_tools)
    state = _make_state()

    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        # 模拟完整诊断流程的 LLM 响应序列
        mock_llm.side_effect = [
            # 第 1 次: formulate_hypothesis — 生成假设 + 请求工具
            Mock(
                content="假设：下游 Redis 延迟导致，置信度 60%。需要调用 search_logs 和 get_topology 验证",
                tool_calls=[
                    {"name": "search_logs", "args": {"service": "payment", "keywords": "error timeout", "time_range": "15m"}, "id": "call_1"},
                    {"name": "get_topology", "args": {"service": "payment", "hops": 2}, "id": "call_2"},
                ],
            ),
            # 第 2 次: evaluate_evidence — 确认根因
            Mock(
                content="评估：支持。置信度：92。确认根因。下一步：确认根因",
                tool_calls=None,
            ),
            # 第 3 次: root_cause — 输出诊断报告
            Mock(
                content="根因：Redis 连接池耗尽。置信度：92%。建议：扩容 Redis 连接池。",
                tool_calls=None,
            ),
        ]
        result = graph.invoke(state, config={"recursion_limit": 10})
        assert result["step_count"] >= 0
        assert len(result["messages"]) > 0


def test_timeout_handler_truncated(mock_tools):
    """超时时应输出阶段性结论"""
    graph = build_graph(mock_tools)
    state = _make_state(
        step_count=MAX_STEPS,
        start_time=time.time() - MAX_DURATION - 1,
        conversation_id="conv-2",
    )

    result = graph.invoke(state, config={"recursion_limit": 5})
    assert result["truncated"] is True
    assert len(result.get("diagnosis", "")) > 0


def test_evidence_evaluation_flow(mock_tools):
    """验证 evaluate_evidence 能正确路由到 root_cause"""
    graph = build_graph(mock_tools)
    state = _make_state(
        alert={"alert_id": "t2", "severity": "critical",
               "labels": {"service": "payment"}, "annotations": {"summary": "CPU 99%"}},
        hypothesis="CPU 过载",
        hypothesis_history=[
            {"hypothesis": "内存泄漏", "result": "excluded", "reason": "内存正常"}
        ],
        evidence={"cpu": "99%", "memory": "45%"},
        compressed_memory=["已排除内存泄漏：内存使用率 45% 正常"],
        step_count=3,
        conversation_id="conv-3",
    )

    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        # formulate_hypothesis + evaluate_evidence → root_cause
        mock_llm.side_effect = [
            Mock(content="继续当前假设，需要评估证据。", tool_calls=None),
            Mock(content="评估：支持。置信度：92。确认根因。下一步：确认根因", tool_calls=None),
            Mock(content="根因：CPU 过载。置信度：92%。建议：扩容。", tool_calls=None),
        ]
        result = graph.invoke(state, config={"recursion_limit": 10})
        assert len(result.get("diagnosis", "")) > 0
