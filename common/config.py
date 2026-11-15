"""Environment-driven settings shared by producers, consumers and tooling."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    schema_registry_url: str
    raw_topic: str
    dlq_topic: str
    raw_partitions: int
    dlq_partitions: int
    replication_factor: int
    min_isr: int


def load_settings() -> Settings:
    """Host defaults target the EXTERNAL listeners; containers override via env."""
    return Settings(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:19093,localhost:19094"),
        schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        raw_topic=os.getenv("RAW_TOPIC", "trips.raw.v1"),
        dlq_topic=os.getenv("DLQ_TOPIC", "trips.dlq.v1"),
        raw_partitions=int(os.getenv("RAW_PARTITIONS", "6")),
        dlq_partitions=int(os.getenv("DLQ_PARTITIONS", "3")),
        replication_factor=int(os.getenv("KAFKA_REPLICATION_FACTOR", "3")),
        min_isr=int(os.getenv("KAFKA_MIN_ISR", "2")),
    )
