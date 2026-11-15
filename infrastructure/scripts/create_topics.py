"""Idempotently create platform topics. Usage: python -m infrastructure.scripts.create_topics"""
from __future__ import annotations

import sys

from confluent_kafka.admin import AdminClient, NewTopic

from common.config import load_settings
from common.logging_setup import get_logger

log = get_logger("create-topics")


def main() -> int:
    s = load_settings()
    admin = AdminClient({"bootstrap.servers": s.bootstrap_servers})
    common_cfg = {"min.insync.replicas": str(s.min_isr), "retention.ms": str(24 * 3600 * 1000)}
    topics = [
        NewTopic(s.raw_topic, s.raw_partitions, s.replication_factor, config=common_cfg),
        NewTopic(s.dlq_topic, s.dlq_partitions, s.replication_factor,
                 config={**common_cfg, "retention.ms": str(7 * 24 * 3600 * 1000)}),
    ]
    rc = 0
    for name, fut in admin.create_topics(topics).items():
        try:
            fut.result()
            log.info("topic created", extra={"topic": name})
        except Exception as e:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(e):
                log.info("topic exists", extra={"topic": name})
            else:
                log.error("topic create failed", extra={"topic": name, "error": str(e)})
                rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
