from src.tools.registry import ToolRegistry, ToolDefinition
from src.infra.es_client import ESLogClient
from src.infra.neo4j_client import Neo4jClient
from src.config import settings


def create_agent_tools(es_client=None, neo4j_client=None) -> ToolRegistry:
    """创建 Agent 可用的工具集（Phase 2 范围：ES 日志 + Neo4j 拓扑）"""
    registry = ToolRegistry()

    # search_logs 工具
    if es_client:
        def _search_logs(service: str, keywords: str = "", time_range: str = "15m") -> dict:
            import asyncio
            results = asyncio.run(es_client.search_logs(
                service=service, keywords=keywords, time_range=time_range, max_results=20
            ))
            return {"total": len(results), "logs": results}

        registry.register(ToolDefinition(
            name="search_logs",
            description="全文检索服务日志，支持关键词和时间范围过滤",
            parameters={
                "service": {"type": "string", "description": "服务名称"},
                "keywords": {"type": "string", "description": "搜索关键词"},
                "time_range": {"type": "string", "description": "时间范围，如 15m/1h/1d"},
            },
        ), handler=_search_logs)
    else:
        registry.register(ToolDefinition(
            name="search_logs",
            description="全文检索服务日志",
            parameters={
                "service": {"type": "string"},
                "keywords": {"type": "string"},
                "time_range": {"type": "string"},
            },
        ), handler=lambda **kw: {"total": 0, "logs": [], "note": "ES 不可用"})

    # get_topology 工具
    if neo4j_client:
        def _get_topology(service: str, hops: int = 2) -> dict:
            deps = neo4j_client.get_upstream_services(service)
            return {"service": service, "dependencies": deps, "total": len(deps)}

        registry.register(ToolDefinition(
            name="get_topology",
            description="查询服务的拓扑依赖关系（上游服务列表）",
            parameters={
                "service": {"type": "string", "description": "服务名称"},
                "hops": {"type": "integer", "description": "查询跳数，默认 2"},
            },
        ), handler=_get_topology)
    else:
        registry.register(ToolDefinition(
            name="get_topology",
            description="查询服务拓扑依赖",
            parameters={"service": {"type": "string"}, "hops": {"type": "integer"}},
        ), handler=lambda **kw: {"dependencies": [], "note": "Neo4j 不可用"})

    # analyze_blast_radius 工具
    if neo4j_client:
        def _analyze_blast_radius(service: str, hops: int = 3) -> dict:
            affected = neo4j_client.analyze_blast_radius(service, hops=hops)
            return {"affected_services": affected, "total": len(affected)}

        registry.register(ToolDefinition(
            name="analyze_blast_radius",
            description="评估故障爆炸半径，找到下游受影响的服务",
            parameters={
                "service": {"type": "string", "description": "故障源服务名称"},
                "hops": {"type": "integer", "description": "遍历跳数，默认 3"},
            },
        ), handler=_analyze_blast_radius)
    else:
        registry.register(ToolDefinition(
            name="analyze_blast_radius",
            description="评估故障爆炸半径",
            parameters={"service": {"type": "string"}, "hops": {"type": "integer"}},
        ), handler=lambda **kw: {"affected_services": [], "note": "Neo4j 不可用"})

    return registry
