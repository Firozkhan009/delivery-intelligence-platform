from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dip.streaming.schema import EVENT_SCHEMA

VALID_EVENT_TYPES = [
    "order_created", "restaurant_accepted", "courier_assigned",
    "order_picked_up", "order_delivered", "order_cancelled",
]


def parse_kafka_events(raw: DataFrame) -> DataFrame:
    """Kafka source frame -> typed bronze frame (keeps raw payload and Kafka lineage)."""
    parsed = raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).withColumn("event", F.from_json("raw_value", EVENT_SCHEMA))
    return (
        parsed.select("raw_value", "kafka_partition", "kafka_offset", "kafka_timestamp", "event.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingested_at", F.current_timestamp())
    )


def clean_events(bronze: DataFrame) -> DataFrame:
    """Bronze -> silver rows: validate and derive columns. Deduplication happens in the MERGE."""
    valid = (
        bronze
        .filter(F.col("event_id").isNotNull() & F.col("order_id").isNotNull()
                & F.col("event_time").isNotNull())
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))
        .filter(F.col("order_value").isNull() | (F.col("order_value") >= 0))
    )
    return (
        valid
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn(
            "order_value_missing",
            F.col("order_value").isNull() & (F.col("event_type") == "order_created"),
        )
        .drop("raw_value")
    )


def dedupe_batch(batch: DataFrame) -> DataFrame:
    """Remove duplicate events inside one micro-batch. Cross-batch dedupe is the MERGE's job."""
    return batch.dropDuplicates(["event_id"])
