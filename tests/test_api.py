import pytest
from unittest.mock import Mock
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app
from src.api.dependencies import get_token_service
from src.api.routes import get_kafka
from src.auth.token_service import TokenService, TokenPayload


@pytest.fixture
def token_svc():
    return TokenService(secret="test-secret", expire_hours=1)


@pytest.fixture
async def client(token_svc):
    app = create_app()
    app.dependency_overrides[get_token_service] = lambda: token_svc
    app.dependency_overrides[get_kafka] = lambda: Mock()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_health_check(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_ingest_alert(client, token_svc):
    token = token_svc.create_token(TokenPayload(
        user_id="test-user", role="admin", scopes=["write"],
    ))
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
    resp = await client.post(
        "/api/v1/alerts/ingest",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 1
    assert "alert_ids" in data
