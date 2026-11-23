from __future__ import annotations

import os

from dip.config import KafkaConfig


def spark_kafka_options(cfg: KafkaConfig, runtime: str | None = None) -> dict[str, str]:
    """Spark Kafka source options. Databricks shades the Kafka client classes."""
    runtime = runtime or os.getenv("DIP_RUNTIME", "local")
    options = {
        "kafka.bootstrap.servers": cfg.bootstrap_servers,
        "subscribe": cfg.topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }
    if cfg.security_protocol != "PLAINTEXT":
        prefix = "kafkashaded." if runtime == "databricks" else ""
        jaas = (
            f"{prefix}org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{cfg.api_key}" password="{cfg.api_secret}";'
        )
        options.update({
            "kafka.security.protocol": cfg.security_protocol,
            "kafka.sasl.mechanism": cfg.sasl_mechanism,
            "kafka.sasl.jaas.config": jaas,
        })
    return options