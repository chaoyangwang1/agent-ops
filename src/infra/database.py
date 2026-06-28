import json
import asyncpg
from src.schemas.alert import AlertRecord


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def init(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id VARCHAR PRIMARY KEY,
                    source VARCHAR NOT NULL,
                    fingerprint VARCHAR,
                    severity VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    labels JSONB DEFAULT '{}',
                    annotations JSONB DEFAULT '{}',
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    value DOUBLE PRECISION,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
                CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
                CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp);
            """)

    async def insert_alert(self, alert: AlertRecord):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO alerts (alert_id, source, fingerprint, severity, status, labels, annotations, timestamp, value)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)
            """, alert.alert_id, alert.source, alert.fingerprint, alert.severity,
               alert.status, json.dumps(alert.labels), json.dumps(alert.annotations),
               alert.timestamp, alert.value)

    async def get_alert(self, alert_id: str) -> AlertRecord | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM alerts WHERE alert_id=$1", alert_id)
            if row is None:
                return None
            data = dict(row)
            data["labels"] = json.loads(data["labels"])
            data["annotations"] = json.loads(data["annotations"])
            return AlertRecord(**data)

    async def list_active_alerts(self, limit: int = 100) -> list[AlertRecord]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM alerts WHERE status='firing' ORDER BY timestamp DESC LIMIT $1", limit
            )
            results = []
            for r in rows:
                data = dict(r)
                data["labels"] = json.loads(data["labels"])
                data["annotations"] = json.loads(data["annotations"])
                results.append(AlertRecord(**data))
            return results
