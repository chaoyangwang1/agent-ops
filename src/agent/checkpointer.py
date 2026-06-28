import json
import time
import logging
import redis
from src.config import settings

logger = logging.getLogger(__name__)

SESSION_TTL = 7200  # 2 小时


class SessionManager:
    """Agent 会话管理器（Redis 缓存 + PostgreSQL 持久化）"""

    def __init__(self, pg_dsn: str = None, redis_url: str = None):
        self.pg_dsn = pg_dsn or settings.pg_dsn
        self.redis_url = redis_url or settings.redis_url
        self._redis = None

    @property
    def redis_client(self):
        if self._redis is None:
            try:
                self._redis = redis.from_url(self.redis_url, decode_responses=True)
            except Exception as e:
                logger.warning(f"Redis 连接失败: {e}")
                self._redis = None
        return self._redis

    def create_session(self, conversation_id: str, metadata: dict):
        """创建会话记录"""
        if not self.redis_client:
            return
        key = f"agent:session:{conversation_id}"
        metadata["created_at"] = time.time()
        metadata["last_active_at"] = time.time()
        self.redis_client.setex(key, SESSION_TTL, json.dumps(metadata))

    def get_session(self, conversation_id: str) -> dict | None:
        """获取会话"""
        if not self.redis_client:
            return None
        key = f"agent:session:{conversation_id}"
        data = self.redis_client.get(key)
        if data:
            return json.loads(data)
        return None

    def update_session(self, conversation_id: str, metadata: dict):
        """更新会话元数据"""
        if not self.redis_client:
            return
        key = f"agent:session:{conversation_id}"
        existing = self.get_session(conversation_id)
        if existing:
            existing.update(metadata)
            existing["last_active_at"] = time.time()
            self.redis_client.setex(key, SESSION_TTL, json.dumps(existing))

    def list_active_sessions(self) -> list[dict]:
        """列出所有活跃会话"""
        if not self.redis_client:
            return []
        sessions = []
        for key in self.redis_client.scan_iter("agent:session:*"):
            data = self.redis_client.get(key)
            if data:
                sessions.append(json.loads(data))
        return sessions

    def delete_session(self, conversation_id: str):
        """删除会话"""
        if not self.redis_client:
            return
        self.redis_client.delete(f"agent:session:{conversation_id}")
