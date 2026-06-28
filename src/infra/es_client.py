from datetime import datetime, timedelta
from elasticsearch import AsyncElasticsearch
from src.tools.log_search import LogSampler


class ESLogClient:
    def __init__(self, hosts: str, index_pattern: str = "logs-*"):
        self.client = AsyncElasticsearch(hosts)
        self.index_pattern = index_pattern
        self.sampler = LogSampler()

    async def search_logs(
        self,
        service: str,
        keywords: str = "",
        time_range: str = "15m",
        max_results: int = 50,
    ) -> list[dict]:
        time_from = self._parse_time_range(time_range)
        query = {
            "bool": {
                "must": [
                    {"term": {"labels.service": service}},
                ],
                "filter": [{"range": {"@timestamp": {"gte": time_from.isoformat()}}}],
            }
        }
        if keywords:
            query["bool"]["must"].append({"query_string": {"query": keywords}})

        resp = await self.client.search(
            index=self.index_pattern,
            query=query,
            size=min(max_results * 10, 2000),  # 多取一些给采样器
            sort=[{"@timestamp": "desc"}],
        )

        raw_logs = [hit["_source"].get("log", hit["_source"].get("message", ""))
                    for hit in resp["hits"]["hits"]]
        return self.sampler.sample(raw_logs, max_return=max_results)

    @staticmethod
    def _parse_time_range(tr: str) -> datetime:
        num = int("".join(c for c in tr if c.isdigit()) or "15")
        if "h" in tr:
            return datetime.utcnow() - timedelta(hours=num)
        elif "d" in tr:
            return datetime.utcnow() - timedelta(days=num)
        return datetime.utcnow() - timedelta(minutes=num)

    async def close(self):
        await self.client.close()
