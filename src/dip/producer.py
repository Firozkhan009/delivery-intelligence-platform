from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from dip.config import KafkaConfig

log = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, cfg: KafkaConfig) -> None:
        self._cfg = cfg
        self._producer = Producer({
            **cfg.client_config(),
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
            "compression.type": "lz4",
        })
        self.sent = 0
        self.failed = 0

    def ensure_topic(self) -> None:
        if self._cfg.security_protocol != "PLAINTEXT":
            log.info("managed cluster: create topic %s in the provider console", self._cfg.topic)
            return
        admin = AdminClient(self._cfg.client_config())
        topic = NewTopic(
            self._cfg.topic,
            num_partitions=self._cfg.partitions,
            replication_factor=self._cfg.replication_factor,
        )
        for name, future in admin.create_topics([topic]).items():
            try:
                future.result()
                log.info("created topic %s", name)
            except KafkaException as exc:
                if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    log.info("topic %s already exists", name)
                else:
                    raise

    def _on_delivery(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.sent += 1

    def send(self, event: dict[str, Any]) -> None:
        payload = json.dumps(event, separators=(",", ":")).encode()
        key = event["order_id"].encode()
        while True:
            try:
                self._producer.produce(self._cfg.topic, key=key, value=payload,
                                       on_delivery=self._on_delivery)
                return
            except BufferError:
                self._producer.poll(0.5)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout: float = 15.0) -> int:
        return self._producer.flush(timeout)