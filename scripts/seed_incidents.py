#!/usr/bin/env python3
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.knowledge.milvus_client import MilvusManager
from src.knowledge.incident_store import IncidentStore

SEED_DATA = [
    {"summary": "支付服务 P99 延迟超过 500ms", "root_cause": "Redis 连接池耗尽，慢查询 KEYS * 阻塞", "service": "payment", "severity": "critical"},
    {"summary": "订单服务 OOM 崩溃", "root_cause": "内存泄漏，未关闭的数据库连接累积", "service": "order", "severity": "critical"},
    {"summary": "API 网关 502 错误率飙升", "root_cause": "后端服务滚动更新期间健康检查失败", "service": "gateway", "severity": "critical"},
    {"summary": "MySQL 主从延迟超过 30s", "root_cause": "大批量写入操作导致 binlog 积压", "service": "mysql", "severity": "warning"},
    {"summary": "K8s Node NotReady", "root_cause": "节点磁盘空间耗尽，kubelet 无法写入", "service": "infra", "severity": "critical"},
]


def main():
    milvus = MilvusManager()
    milvus.init_collection()
    store = IncidentStore(milvus=milvus)
    for item in SEED_DATA:
        store.add_incident(**item)
    print(f"种子数据导入完成，共 {store.count()} 条")
    milvus.close()


if __name__ == "__main__":
    main()
