import hashlib
import hmac
import json
import time
from base64 import urlsafe_b64encode, urlsafe_b64decode
from dataclasses import dataclass


@dataclass
class TokenPayload:
    user_id: str
    role: str
    scopes: list[str]
    exp: int = 0

    def is_expired(self) -> bool:
        return self.exp > 0 and self.exp < time.time()


class TokenService:
    def __init__(self, secret: str, expire_hours: int = 24):
        self.secret = secret.encode()
        self.expire_hours = expire_hours

    def create_token(self, payload: TokenPayload) -> str:
        payload.exp = int(time.time()) + self.expire_hours * 3600
        data = json.dumps(payload.__dict__).encode()
        signature = hmac.new(self.secret, data, hashlib.sha256).hexdigest()
        token_body = urlsafe_b64encode(data).decode()
        return f"{token_body}.{signature}"

    def verify_token(self, token: str) -> TokenPayload:
        parts = token.split(".")
        if len(parts) != 2:
            raise ValueError("Token 格式无效")
        token_body, signature = parts
        try:
            data = urlsafe_b64decode(token_body)
        except Exception:
            raise ValueError("Token 无效")

        expected_sig = hmac.new(self.secret, data, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected_sig):
            raise ValueError("Token 签名无效")

        payload = TokenPayload(**json.loads(data))
        if payload.is_expired():
            raise ValueError("Token 已过期")
        return payload
