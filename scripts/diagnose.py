#!/usr/bin/env python3
"""OPS AI Agent — CLI 诊断工具

用法:
    python scripts/diagnose.py --alert '<JSON>'
    python scripts/diagnose.py --file <alert.json>

示例:
    python scripts/diagnose.py --alert '{"alert_id":"t1","severity":"critical","labels":{"service":"payment"},"annotations":{"summary":"P99 > 500ms"}}'
"""

import sys
import json
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.agent.diagnosis import run_diagnosis
from src.infra.es_client import ESLogClient
from src.infra.neo4j_client import Neo4jClient
from src.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def format_report(result):
    """格式化诊断报告"""
    status = "⚠ 诊断超时（阶段性结论）" if result.truncated else "✓ 诊断完成"
    lines = [
        "=" * 60,
        f"  OPS AI Agent 诊断报告",
        "=" * 60,
        f"会话 ID: {result.conversation_id}",
        f"状态: {status}",
        f"耗时: {result.duration_seconds:.1f}s",
        f"步数: {result.step_count}",
        f"模式: {result.mode}",
        "-" * 60,
        f"当前假设: {result.hypothesis[:200] if result.hypothesis else '无'}",
        "-" * 60,
        f"诊断结论:",
        f"{result.diagnosis}",
        "-" * 60,
    ]
    if result.hypothesis_history:
        lines.append("排除的假设:")
        for h in result.hypothesis_history:
            lines.append(f"  - {h.get('hypothesis', '')[:100]} ({h.get('result', '')})")
    lines.append("=" * 60)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="OPS AI Agent CLI 诊断工具")
    parser.add_argument("--alert", type=str, help="告警 JSON 字符串")
    parser.add_argument("--file", type=str, help="告警 JSON 文件路径")
    parser.add_argument("--no-es", action="store_true", help="不使用 ES（使用 mock）")
    parser.add_argument("--no-neo4j", action="store_true", help="不使用 Neo4j（使用 mock）")
    args = parser.parse_args()

    # 解析告警
    if args.alert:
        alert = json.loads(args.alert)
    elif args.file:
        alert = json.loads(Path(args.file).read_text())
    else:
        alert = {
            "alert_id": "demo-001",
            "severity": "critical",
            "status": "firing",
            "source": "prometheus",
            "labels": {"service": "payment", "alertname": "HighLatency"},
            "annotations": {"summary": "支付服务 P99 延迟 > 500ms"},
            "timestamp": "2026-06-28T10:00:00Z",
        }
        logger.info("使用默认演示告警")

    # 初始化客户端
    es = None if args.no_es else ESLogClient(settings.es_hosts)
    neo4j = None if args.no_neo4j else Neo4jClient(
        settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password
    )

    # 执行诊断
    result = run_diagnosis(alert, es_client=es, neo4j_client=neo4j)

    # 输出报告
    print(format_report(result))

    # 清理
    if es:
        import asyncio
        asyncio.run(es.close())
    if neo4j:
        neo4j.close()


if __name__ == "__main__":
    main()
