import pytest
from unittest.mock import Mock, patch
from src.agent.diagnosis import run_diagnosis, DiagnosisResult
from src.agent.tools import create_agent_tools


@pytest.fixture
def sample_alert():
    return {
        "alert_id": "e2e-001",
        "severity": "critical",
        "status": "firing",
        "source": "prometheus",
        "labels": {"service": "payment", "alertname": "HighLatency"},
        "annotations": {"summary": "支付服务 P99 延迟 > 500ms"},
        "timestamp": "2026-06-28T10:00:00Z",
    }


@pytest.mark.integration
def test_diagnosis_with_mock_llm(sample_alert):
    """端到端诊断流程（mock LLM + mock 工具）"""
    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.side_effect = [
            # 第 1 次: formulate_hypothesis — 生成假设 + 请求工具
            Mock(
                content="假设：下游 Redis 延迟导致。需要查询日志和拓扑验证。",
                tool_calls=[
                    {"name": "search_logs", "args": {"service": "payment", "keywords": "error timeout"}, "id": "c1"},
                    {"name": "get_topology", "args": {"service": "payment", "hops": 2}, "id": "c2"},
                ],
            ),
            # 第 2 次: evaluate_evidence — 证据支持假设
            Mock(
                content="评估：支持。置信度：92。确认根因。下一步：确认根因",
                tool_calls=None,
            ),
            # 第 3 次: root_cause — 输出诊断报告
            Mock(
                content="根因：Redis 连接池耗尽导致支付服务延迟升高。"
                        "证据：日志显示 Redis 连接超时错误，拓扑确认 payment 依赖 Redis。"
                        "置信度：92%。建议：扩容 Redis 连接池。",
                tool_calls=None,
            ),
        ]

        result = run_diagnosis(sample_alert)

        assert isinstance(result, DiagnosisResult)
        assert result.step_count >= 1
        assert len(result.diagnosis) > 0
        assert "Redis" in result.diagnosis


@pytest.mark.integration
def test_diagnosis_handles_llm_failure(sample_alert):
    """LLM 失败时不应崩溃，应返回 truncated 结果"""
    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.side_effect = TimeoutError("LLM 超时")

        result = run_diagnosis(sample_alert)

        assert isinstance(result, DiagnosisResult)
        assert result.truncated is True


@pytest.mark.integration
def test_diagnosis_uses_multiple_tools(sample_alert):
    """验证 Agent 能够调用多个不同工具"""
    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.side_effect = [
            Mock(
                content="需要查询日志、拓扑和爆炸半径",
                tool_calls=[
                    {"name": "search_logs", "args": {"service": "payment", "keywords": "error"}, "id": "c1"},
                    {"name": "get_topology", "args": {"service": "payment", "hops": 2}, "id": "c2"},
                    {"name": "analyze_blast_radius", "args": {"service": "payment", "hops": 3}, "id": "c3"},
                ],
            ),
            Mock(content="评估：支持。置信度：88。确认根因。下一步：确认根因", tool_calls=None),
            Mock(content="根因已确认。", tool_calls=None),
        ]

        result = run_diagnosis(sample_alert)
        assert result.step_count >= 1
