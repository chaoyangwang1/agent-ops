import pytest
from src.infra.neo4j_client import Neo4jClient


@pytest.fixture
def neo4j():
    client = Neo4jClient("bolt://localhost:17687", "neo4j", "password")
    client.init_schema()
    yield client
    client.close()


def test_init_schema(neo4j):
    constraints = neo4j.query("SHOW CONSTRAINTS")
    assert len(constraints) > 0


def test_merge_service_and_dependency(neo4j):
    neo4j.merge_service("payment", {"team": "platform", "namespace": "prod"})
    neo4j.merge_service("redis", {"type": "middleware"})
    neo4j.merge_dependency("payment", "redis", "USES")

    # 查询拓扑
    topo = neo4j.get_upstream_services("payment")
    assert len(topo) > 0
    assert any(d["name"] == "redis" for d in topo)


def test_blast_radius(neo4j):
    neo4j.merge_service("a", {})
    neo4j.merge_service("b", {})
    neo4j.merge_service("c", {})
    neo4j.merge_dependency("a", "b", "DEPENDS_ON")
    neo4j.merge_dependency("b", "c", "DEPENDS_ON")

    affected = neo4j.analyze_blast_radius("a", hops=2)
    assert len(affected) == 2  # b, c
