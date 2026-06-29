import pytest
import uuid
from src.conversation.models import ConversationRecord
from src.conversation.repository import ConversationRepository


@pytest.fixture
async def repo():
    from src.infra.database import Database
    db = Database("postgresql://agent:agent@localhost:5432/agentops")
    await db.init()
    r = ConversationRepository(db)
    await r.init_table()
    yield r


@pytest.mark.asyncio
async def test_archive_and_get(repo):
    conv_id = str(uuid.uuid4())
    record = ConversationRecord(
        conversation_id=conv_id,
        alert_id="alert-1",
        service="payment",
        severity="critical",
        status="completed",
        diagnosis="根因：Redis 连接池耗尽",
        hypothesis_history=[{"hypothesis": "Redis 问题", "result": "confirmed"}],
        messages=[{"role": "user", "content": "告警信息"}],
        step_count=3,
        truncated=False,
        mode="full",
        duration_seconds=12.5,
    )
    await repo.archive(record)
    result = await repo.get_by_id(conv_id)
    assert result is not None
    assert result.service == "payment"
    assert "Redis" in result.diagnosis


@pytest.mark.asyncio
async def test_list_by_service(repo):
    results = await repo.list_by_service("payment", limit=10)
    assert isinstance(results, list)


@pytest.mark.asyncio
async def test_list_recent(repo):
    results = await repo.list_recent(limit=5)
    assert isinstance(results, list)
