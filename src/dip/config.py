from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    partitions: int
    replication_factor: int


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
class AppConfig:
    kafka: KafkaConfig
    generator: GeneratorConfig


def load_config(path: str | None = None) -> AppConfig:
    cfg_path = Path(path or os.getenv("DIP_CONFIG", "config/config.yaml"))
    raw = yaml.safe_load(cfg_path.read_text())
    kafka = dict(raw["kafka"])
    kafka["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"])
    return AppConfig(kafka=KafkaConfig(**kafka), generator=GeneratorConfig(**raw["generator"]))