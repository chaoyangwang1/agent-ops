import pytest
from httpx import AsyncClient, ASGITransport
from src.api.main import create_app
from src.api.dependencies import get_token_service
from src.auth.token_service import TokenService, TokenPayload


@pytest.fixture
def token_svc():
    return TokenService(secret="test-secret", expire_hours=1)


@pytest.fixture
async def client(token_svc):
    app = create_app()
    app.dependency_overrides[get_token_service] = lambda: token_svc
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_protected_endpoint_without_token(client):
    resp = await client.post("/api/v1/alerts/ingest", json={"source": "test", "raw": {}})
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_protected_endpoint_with_valid_token(client, token_svc):
    token = token_svc.create_token(TokenPayload(
        user_id="u1", role="admin", scopes=["write"],
    ))
    resp = await client.post(
        "/api/v1/alerts/ingest",
        json={"source": "test", "raw": {}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202  # 不是 401
