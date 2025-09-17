"""Configuration settings for the ETL system."""

import os
from typing import Dict, Any
from pydantic import BaseSettings, Field


class DatabaseConfig(BaseSettings):
    """Database configuration."""
    
    host: str = Field(default="localhost", env="POSTGRES_HOST")
    port: int = Field(default=5432, env="POSTGRES_PORT")
    database: str = Field(default="etl_db", env="POSTGRES_DB")
    username: str = Field(default="etl_user", env="POSTGRES_USER")
    password: str = Field(default="etl_password", env="POSTGRES_PASSWORD")
    schema: str = Field(default="etl_demo", env="POSTGRES_SCHEMA")
    
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class KafkaConfig(BaseSettings):
    """Kafka configuration."""
    
    bootstrap_servers: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    cdc_topic: str = Field(default="etl_cdc_events", env="KAFKA_CDC_TOPIC")
    stream_topic: str = Field(default="etl_stream_events", env="KAFKA_STREAM_TOPIC")
    consumer_group: str = Field(default="etl_consumers", env="KAFKA_CONSUMER_GROUP")


class RedisConfig(BaseSettings):
    """Redis configuration."""
    
    host: str = Field(default="localhost", env="REDIS_HOST")
    port: int = Field(default=6379, env="REDIS_PORT")
    password: str = Field(default="", env="REDIS_PASSWORD")
    db: int = Field(default=0, env="REDIS_DB")
    
    @property
    def connection_string(self) -> str:
        """Get Redis connection string."""
        if self.password:
            return f"redis://:{self.password}@{self.host}:{self.port}/{self.db}"
        return f"redis://{self.host}:{self.port}/{self.db}"


class DebeziumConfig(BaseSettings):
    """Debezium configuration."""
    
    connect_url: str = Field(default="http://localhost:8083", env="DEBEZIUM_CONNECT_URL")
    connector_name: str = Field(default="etl-postgres-connector", env="DEBEZIUM_CONNECTOR_NAME")


class ProcessingConfig(BaseSettings):
    """Processing configuration."""
    
    batch_interval_seconds: int = Field(default=60, env="BATCH_INTERVAL_SECONDS")
    stream_window_seconds: int = Field(default=30, env="STREAM_WINDOW_SECONDS")
    anomaly_threshold: float = Field(default=2.0, env="ANOMALY_THRESHOLD")
    data_generator_interval: int = Field(default=5, env="DATA_GENERATOR_INTERVAL")
    max_records_per_batch: int = Field(default=1000, env="MAX_RECORDS_PER_BATCH")


class MonitoringConfig(BaseSettings):
    """Monitoring configuration."""
    
    metrics_port: int = Field(default=8000, env="METRICS_PORT")
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    enable_prometheus: bool = Field(default=True, env="ENABLE_PROMETHEUS")


class ETLConfig(BaseSettings):
    """Main ETL configuration."""
    
    database: DatabaseConfig = DatabaseConfig()
    kafka: KafkaConfig = KafkaConfig()
    redis: RedisConfig = RedisConfig()
    debezium: DebeziumConfig = DebeziumConfig()
    processing: ProcessingConfig = ProcessingConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global configuration instance
config = ETLConfig()


def get_airflow_connections() -> Dict[str, Any]:
    """Get Airflow connection configurations."""
    return {
        "postgres_etl": {
            "conn_type": "postgres",
            "host": config.database.host,
            "port": config.database.port,
            "schema": config.database.database,
            "login": config.database.username,
            "password": config.database.password,
        },
        "redis_cache": {
            "conn_type": "redis",
            "host": config.redis.host,
            "port": config.redis.port,
            "password": config.redis.password,
            "extra": {"db": config.redis.db},
        },
        "kafka_cluster": {
            "conn_type": "kafka",
            "host": config.kafka.bootstrap_servers,
            "extra": {
                "bootstrap_servers": config.kafka.bootstrap_servers,
                "consumer_group": config.kafka.consumer_group,
            },
        },
    }