import re
from typing import List


class Drain:
    """轻量级日志模式聚类，基于 Drain3 算法简化"""

    def __init__(self, depth: int = 4, similarity_threshold: float = 0.5):
        self.depth = depth
        self.similarity_threshold = similarity_threshold
        self.clusters: dict[str, list] = {}

    @staticmethod
    def _tokenize(log: str) -> list[str]:
        return log.strip().split()

    @staticmethod
    def _get_template(tokens: list[str]) -> str:
        """将数字、IP、路径等替换为通配符"""
        result = []
        for t in tokens:
            if re.match(r'^[\d.]+$', t):       # 数字/IP
                result.append("<*>")
            elif re.match(r'^/[\w/]+$', t):     # 路径
                result.append("<path>")
            elif re.match(r'^[0-9a-f-]{36}$', t):  # UUID
                result.append("<uuid>")
            else:
                result.append(t)
        return " ".join(result)

    def match(self, log: str) -> str:
        tokens = self._tokenize(log)
        template = self._get_template(tokens)
        key = " ".join(tokens[:self.depth]) + "|" + template
        if key not in self.clusters:
            self.clusters[key] = []
        self.clusters[key].append(log)
        return key

    def get_cluster_sizes(self) -> dict[str, int]:
        return {k: len(v) for k, v in self.clusters.items()}


class LogSampler:
    def __init__(self):
        self.drain = Drain()

    def sample(self, logs: List[str], max_return: int = 30) -> List[dict]:
        if not logs:
            return []

        # 聚类
        for log in logs:
            self.drain.match(log)

        total = len(logs)
        sampled = []

        for cluster_key, entries in self.drain.clusters.items():
            ratio = len(entries) / total
            if ratio > 0.5:
                n = min(3, len(entries))
            elif ratio < 0.05:
                n = len(entries)
            else:
                n = min(2, len(entries))
            sampled.extend(entries[:n])

        # 优先保留含 ERROR/WARN 的（保留所有副本）
        priority = [l for l in logs if any(kw in l.upper() for kw in ["ERROR", "WARN", "FATAL", "CRITICAL"])]
        priority_set = set(priority)
        # 去重非异常日志，保留所有异常日志
        sampled_dedup = list(dict.fromkeys(s for s in sampled if s not in priority_set))
        result = priority + sampled_dedup
        result = result[:max_return]

        return [{
            "content": s,
            "critical": any(kw in s.upper() for kw in ["ERROR", "FATAL", "CRITICAL"]),
        } for s in result]
