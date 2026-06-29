import pytest
from src.knowledge.milvus_client import MilvusManager


@pytest.fixture
def milvus():
    mgr = MilvusManager(host="localhost", port="19530")
    mgr.init_collection()
    yield mgr
    mgr.close()


def test_collection_exists(milvus):
    assert milvus.collection_name == "incidents"
    assert milvus.collection is not None


def test_insert_and_search(milvus):
    milvus.flush()
    vectors = [[0.1] * 1536, [0.2] * 1536]
    data = {
        "incident_id": ["inc-001", "inc-002"],
        "embedding": vectors,
        "summary": ["CPU 过高导致延迟", "内存溢出导致 OOM"],
        "root_cause": ["请求量激增", "内存泄漏"],
        "service": ["payment", "order"],
        "severity": ["critical", "warning"],
        "created_at": [1000000, 2000000],
    }
    ids = milvus.insert(data)
    assert len(ids) == 2
    milvus.flush()
    results = milvus.search([0.1] * 1536, top_k=2)
    assert len(results) > 0
