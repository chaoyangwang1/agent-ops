import pytest
from unittest.mock import Mock, patch
from src.knowledge.incident_store import IncidentStore


@pytest.fixture
def store():
    mock_milvus = Mock()
    mock_milvus.search.return_value = []
    mock_milvus.insert.return_value = ["id-1"]
    mock_milvus.count.return_value = 1
    return IncidentStore(milvus=mock_milvus)


@patch("src.knowledge.incident_store.embed_text", return_value=[0.1] * 1536)
def test_add_incident(mock_embed, store):
    result = store.add_incident(
        incident_id="inc-001", summary="CPU 过高",
        root_cause="请求量激增", service="payment", severity="critical",
    )
    assert result is True
    assert store.milvus.insert.called


@patch("src.knowledge.incident_store.embed_text", return_value=[0.1] * 1536)
def test_search_similar(mock_embed, store):
    store.milvus.search.return_value = [
        {"id": "inc-001", "distance": 0.1, "summary": "CPU 过高", "root_cause": "请求量激增"}
    ]
    results = store.search_similar("支付服务 CPU 过高", top_k=3)
    assert len(results) > 0
    assert "summary" in results[0]


def test_get_incident_count(store):
    assert store.count() == 1
