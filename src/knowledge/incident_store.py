import time
import uuid
import logging
from src.knowledge.embedding import embed_text

logger = logging.getLogger(__name__)


class IncidentStore:
    def __init__(self, milvus):
        self.milvus = milvus

    def add_incident(self, incident_id: str = None, summary: str = "",
                     root_cause: str = "", service: str = "",
                     severity: str = "warning") -> bool:
        try:
            text = f"{summary} {root_cause}"
            embedding = embed_text(text)
            incident_id = incident_id or str(uuid.uuid4())
            data = {
                "incident_id": [incident_id],
                "embedding": [embedding],
                "summary": [summary[:512]],
                "root_cause": [root_cause[:1024]],
                "service": [service[:128]],
                "severity": [severity[:16]],
                "created_at": [int(time.time())],
            }
            self.milvus.insert(data)
            self.milvus.flush()
            logger.info(f"故障案例已入库: {incident_id}")
            return True
        except Exception as e:
            logger.error(f"故障案例入库失败: {e}")
            return False

    def search_similar(self, query: str, top_k: int = 5) -> list[dict]:
        try:
            embedding = embed_text(query)
            return self.milvus.search(embedding, top_k=top_k)
        except Exception as e:
            logger.error(f"相似故障检索失败: {e}")
            return []

    def count(self) -> int:
        return self.milvus.count()
