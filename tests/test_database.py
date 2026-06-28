import pytest
import uuid
from src.infra.database import Database
from src.schemas.alert import AlertRecord


@pytest.fixture
async def db():
    d = Database("postgresql://agent:agent@localhost:5432/agentops")
    await d.init()
    yield d


@pytest.mark.asyncio
async def test_create_and_query_alert(db):
    test_id = str(uuid.uuid4())
    alert = AlertRecord(
        alert_id=test_id,
        source="prometheus",
        severity="critical",
        status="firing",
        labels={"service": "payment", "cluster": "prod"},
        annotations={"summary": "CPU > 90%"},
    )
    await db.insert_alert(alert)
    result = await db.get_alert(test_id)
    assert result is not None
    assert result.severity == "critical"


@pytest.mark.asyncio
async def test_list_active_alerts(db):
    alerts = await db.list_active_alerts(limit=10)
    assert isinstance(alerts, list)
