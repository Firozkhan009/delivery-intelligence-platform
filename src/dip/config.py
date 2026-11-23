from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    partitions: int
    replication_factor: int
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    api_key: str | None = field(default=None, repr=False)
    api_secret: str | None = field(default=None, repr=False)

    def client_config(self) -> dict[str, str]:
        conf = {"bootstrap.servers": self.bootstrap_servers}
        if self.security_protocol != "PLAINTEXT":
            conf.update({
                "security.protocol": self.security_protocol,
                "sasl.mechanisms": self.sasl_mechanism,
                "sasl.username": self.api_key or "",
                "sasl.password": self.api_secret or "",
            })
        return conf


@dataclass(frozen=True)
class GeneratorConfig:
    orders_per_second: float
    time_scale: float
    seed: int | None
    max_orders: int
    duplicate_rate: float
    late_rate: float
    null_rate: float
    cancel_rate: float


@dataclass(frozen=True)
class SparkConfig:
    data_root: str
    watermark: str
    shuffle_partitions: int

    @property
    def bronze_path(self) -> str:
        return f"{self.data_root}/delta/bronze/order_events"

    @property
    def silver_path(self) -> str:
        return f"{self.data_root}/delta/silver/order_events"

    @property
    def checkpoint_dir(self) -> str:
        return f"{self.data_root}/checkpoints"


@dataclass(frozen=True)
class AppConfig:
    kafka: KafkaConfig
    generator: GeneratorConfig
    spark: SparkConfig


def load_config(path: str | None = None) -> AppConfig:
    cfg_path = Path(path or os.getenv("DIP_CONFIG", "config/config.yaml"))
    raw = yaml.safe_load(cfg_path.read_text())

    kafka = dict(raw["kafka"])
    kafka["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"])
    kafka["security_protocol"] = os.getenv("KAFKA_SECURITY_PROTOCOL",
                                           kafka.get("security_protocol", "PLAINTEXT"))
    kafka["api_key"] = os.getenv("KAFKA_API_KEY")
    kafka["api_secret"] = os.getenv("KAFKA_API_SECRET")

    spark = dict(raw["spark"])
    spark["data_root"] = os.getenv("DIP_DATA_ROOT", spark["data_root"])

    return AppConfig(
        kafka=KafkaConfig(**kafka),
        generator=GeneratorConfig(**raw["generator"]),
        spark=SparkConfig(**spark),
    )