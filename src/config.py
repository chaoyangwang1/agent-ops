from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_raw_alerts: str = "raw.alerts"
    kafka_topic_aggregated_alerts: str = "aggregated.alerts"

    # Elasticsearch
    es_hosts: str = "http://localhost:9200"
    es_log_index_pattern: str = "logs-*"
    es_alert_index: str = "alerts"

    # PostgreSQL
    pg_dsn: str = "postgresql://agent:agent@localhost:5432/agentops"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:17687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    # LLM
    llm_api_base: str = "https://api.openai.com/v1"
    llm_api_key: str = ""
    llm_model: str = "gpt-4o"

    # Auth
    token_secret: str = "change-me-in-production"
    token_expire_hours: int = 24

    # Agent
    agent_max_steps: int = 15
    agent_max_duration_seconds: int = 300
    agent_auto_trigger: bool = False

    # Milvus
    milvus_host: str = "localhost"
    milvus_port: str = "19530"
    milvus_enabled: bool = True

    # ChatOps
    feishu_webhook_url: str = ""
    dingtalk_webhook_url: str = ""

    model_config = {"env_file": ".env", "env_prefix": "AGENTOPS_"}


settings = Settings()
