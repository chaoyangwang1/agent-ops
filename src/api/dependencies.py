from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.auth.token_service import TokenService
from src.config import settings

security = HTTPBearer(auto_error=False)


def get_token_service() -> TokenService:
    return TokenService(settings.token_secret, settings.token_expire_hours)


async def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
    svc: TokenService = Depends(get_token_service),
) -> dict:
    """验证 Bearer Token"""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="缺少认证 Token",
        )
    try:
        payload = svc.verify_token(credentials.credentials)
        return {"user_id": payload.user_id, "role": payload.role, "scopes": payload.scopes}
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
        )
