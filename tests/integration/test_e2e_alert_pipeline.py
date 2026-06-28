import pytest
from unittest.mock import Mock
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app
from src.api.dependencies import get_token_service
from src.api.routes import get_kafka
from src.auth.token_service import TokenService, TokenPayload


@pytest.fixture
async def client():
    app = create_app()
    token_svc = TokenService(secret="e2e-test-secret", expire_hours=1)
    app.dependency_overrides[get_token_service] = lambda: token_svc
    app.dependency_overrides[get_kafka] = lambda: Mock()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c, token_svc


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_alert_ingest_pipeline(client):
    """端到端：告警接入 → 标准化 → Kafka"""
    c, token_svc = client
    token = token_svc.create_token(TokenPayload(user_id="u1", role="admin", scopes=["write"]))

    payload = {
        "source": "prometheus",
        "raw": {
            "status": "firing",
            "alerts": [
                {
                    "labels": {
                        "alertname": "TestHighCPU",
                        "service": "test-svc",
                        "severity": "critical",
                    },
                    "annotations": {"summary": "CPU > 95%"},
                    "startsAt": "2026-06-28T10:00:00Z",
                }
            ],
        },
    }
    resp = await c.post(
        "/api/v1/alerts/ingest",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["accepted"] == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_health_endpoint(client):
    c, _ = client
    resp = await c.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
