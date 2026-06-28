import pytest
from src.infra.es_client import ESLogClient


@pytest.fixture
def es():
    return ESLogClient("http://localhost:9200", index_pattern="logs-*")


@pytest.mark.asyncio
async def test_search_logs_basic(es):
    result = await es.search_logs(
        service="test-svc",
        keywords="error",
        time_range="5m",
        max_results=50,
    )
    assert isinstance(result, list)
    # 无数据时返回空列表
    assert result == [] or all("content" in r for r in result)
