import pytest
from src.auth.token_service import TokenService, TokenPayload


@pytest.fixture
def token_svc():
    return TokenService(secret="test-secret", expire_hours=1)


def test_create_and_verify_token(token_svc):
    payload = TokenPayload(user_id="user-1", role="admin", scopes=["read", "write"])
    token = token_svc.create_token(payload)
    assert len(token) > 20

    decoded = token_svc.verify_token(token)
    assert decoded.user_id == "user-1"
    assert decoded.role == "admin"
    assert decoded.scopes == ["read", "write"]


def test_expired_token(token_svc):
    svc = TokenService(secret="test", expire_hours=-1)  # 立即过期
    token = svc.create_token(TokenPayload(user_id="u1", role="viewer", scopes=["read"]))
    with pytest.raises(ValueError, match="Token 已过期"):
        svc.verify_token(token)


def test_invalid_token(token_svc):
    with pytest.raises(ValueError):
        token_svc.verify_token("invalid-token")
