import json
from src.conversation.models import ConversationRecord


class ConversationRepository:
    def __init__(self, db):
        self.db = db

    async def init_table(self):
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id VARCHAR PRIMARY KEY,
                    alert_id VARCHAR,
                    service VARCHAR,
                    severity VARCHAR,
                    status VARCHAR DEFAULT 'active',
                    diagnosis TEXT DEFAULT '',
                    hypothesis_history JSONB DEFAULT '[]',
                    messages JSONB DEFAULT '[]',
                    checkpoint_id VARCHAR DEFAULT '',
                    step_count INT DEFAULT 0,
                    truncated BOOL DEFAULT FALSE,
                    mode VARCHAR DEFAULT 'full',
                    duration_seconds FLOAT DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    completed_at TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_conv_service ON conversations(service);
                CREATE INDEX IF NOT EXISTS idx_conv_severity ON conversations(severity);
                CREATE INDEX IF NOT EXISTS idx_conv_status ON conversations(status);
                CREATE INDEX IF NOT EXISTS idx_conv_created ON conversations(created_at);
            """)

    async def archive(self, record: ConversationRecord):
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO conversations
                (conversation_id, alert_id, service, severity, status, diagnosis,
                 hypothesis_history, messages, checkpoint_id, step_count,
                 truncated, mode, duration_seconds, created_at, completed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (conversation_id) DO UPDATE SET
                    status=$5, diagnosis=$6, hypothesis_history=$7::jsonb,
                    messages=$8::jsonb, step_count=$10, truncated=$11,
                    duration_seconds=$13, completed_at=$15
            """, record.conversation_id, record.alert_id, record.service, record.severity,
               record.status, record.diagnosis,
               json.dumps(record.hypothesis_history), json.dumps(record.messages),
               record.checkpoint_id, record.step_count,
               record.truncated, record.mode, record.duration_seconds,
               record.created_at, record.completed_at)

    async def get_by_id(self, conv_id: str) -> ConversationRecord | None:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM conversations WHERE conversation_id=$1", conv_id)
            if row is None:
                return None
            return self._row_to_record(row)

    async def list_by_service(self, service: str, limit: int = 20) -> list[ConversationRecord]:
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM conversations WHERE service=$1 ORDER BY created_at DESC LIMIT $2",
                service, limit
            )
            return [self._row_to_record(r) for r in rows]

    async def list_recent(self, limit: int = 20) -> list[ConversationRecord]:
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM conversations ORDER BY created_at DESC LIMIT $1", limit
            )
            return [self._row_to_record(r) for r in rows]

    @staticmethod
    def _row_to_record(row) -> ConversationRecord:
        d = dict(row)
        for field in ["hypothesis_history", "messages"]:
            if isinstance(d.get(field), str):
                d[field] = json.loads(d[field])
        return ConversationRecord(**d)
