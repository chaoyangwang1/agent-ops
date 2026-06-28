import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app


@pytest.fixture
async def client():
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_ingest_alert(client):
    payload = {
        "source": "prometheus",
        "raw": {
            "status": "firing",
            "alerts": [{
                "labels": {"alertname": "TestAlert", "service": "test-svc"},
                "annotations": {"summary": "test"},
                "startsAt": "2026-06-28T10:00:00Z",
            }]
        }
    }
    resp = await client.post("/api/v1/alerts/ingest", json=payload)
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 1
    assert "alert_ids" in data
