import pytest
from unittest.mock import Mock, patch
from src.agent.diagnosis import run_diagnosis
from src.knowledge.incident_store import IncidentStore


@pytest.mark.integration
def test_diagnosis_with_similar_incidents():
    """端到端：诊断 + 相似故障检索（mock Milvus + LLM）"""
    mock_milvus = Mock()
    mock_milvus.search.return_value = [
        {"id": "inc-001", "distance": 0.1, "summary": "Redis 延迟问题",
         "root_cause": "Redis 连接池耗尽", "severity": "critical"}
    ]
    mock_milvus.insert.return_value = ["id-new"]
    mock_milvus.count.return_value = 1
    store = IncidentStore(milvus=mock_milvus)

    alert = {
        "alert_id": "e2e-001", "severity": "critical",
        "labels": {"service": "payment"},
        "annotations": {"summary": "P99 > 500ms"},
    }

    with patch("src.agent.graph.call_llm_with_retry") as mock_llm, \
         patch("src.knowledge.incident_store.embed_text", return_value=[0.1] * 1536):
        mock_llm.side_effect = [
            Mock(content="需要查历史故障和日志", tool_calls=[
                {"name": "search_similar_incidents",
                 "args": {"description": "payment 延迟", "top_k": 3}, "id": "c1"},
                {"name": "search_logs",
                 "args": {"service": "payment", "keywords": "error"}, "id": "c2"},
            ]),
            Mock(content="评估：支持。置信度：95。确认根因。", tool_calls=None),
            Mock(content="根因：Redis 连接池耗尽（与历史故障 inc-001 相似）。置信度：95%。建议：扩容。",
                 tool_calls=None),
        ]
        result = run_diagnosis(alert)
        assert result.step_count >= 1
        assert "Redis" in result.diagnosis


@pytest.mark.integration
def test_incident_store_auto_add():
    """诊断完成后自动入库"""
    mock_milvus = Mock()
    mock_milvus.search.return_value = []
    mock_milvus.insert.return_value = ["id-auto"]
    store = IncidentStore(milvus=mock_milvus)

    with patch("src.knowledge.incident_store.embed_text", return_value=[0.1] * 1536):
        success = store.add_incident(
            summary="CPU 过高", root_cause="请求量激增",
            service="payment", severity="critical"
        )
    assert success is True
    mock_milvus.insert.assert_called_once()
