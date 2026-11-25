from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from dip.config import AppConfig, load_config
from dip.logging_setup import setup_logging
from dip.streaming.kafka_options import spark_kafka_options
from dip.streaming.session import build_spark
from dip.streaming.transforms import clean_events, dedupe_batch, parse_kafka_events

log = logging.getLogger("dip.streaming")


def _trigger(mode: str) -> dict:
    if mode == "available-now":
        return {"availableNow": True}
    return {"processingTime": "10 seconds"}


def _kafka_stream(spark: SparkSession, cfg: AppConfig) -> DataFrame:
    return spark.readStream.format("kafka").options(**spark_kafka_options(cfg.kafka)).load()


def make_silver_upsert(silver_path: str) -> Callable[[DataFrame, int], None]:
    """Idempotent upsert: insert only events whose event_id is not already in silver."""

    def upsert(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        batch = dedupe_batch(batch_df)
        unique = batch.count()
        if not DeltaTable.isDeltaTable(spark, silver_path):
            batch.write.format("delta").mode("append").partitionBy("event_date").save(silver_path)
        else:
            (
                DeltaTable.forPath(spark, silver_path).alias("t")
                .merge(batch.alias("s"), "t.event_id = s.event_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        log.info("silver batch %d: unique_events=%d", batch_id, unique)

    return upsert


def start_bronze(spark: SparkSession, cfg: AppConfig, mode: str) -> StreamingQuery:
    df = parse_kafka_events(_kafka_stream(spark, cfg))
    return (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/bronze")
        .trigger(**_trigger(mode))
        .start(cfg.spark.bronze_path)
    )


def start_silver(spark: SparkSession, cfg: AppConfig, mode: str) -> StreamingQuery:
    bronze = spark.readStream.format("delta").load(cfg.spark.bronze_path)
    silver = clean_events(bronze)
    return (
        silver.writeStream
        .foreachBatch(make_silver_upsert(cfg.spark.silver_path))
        .option("checkpointLocation", f"{cfg.spark.checkpoint_dir}/silver")
        .trigger(**_trigger(mode))
        .start()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a streaming layer")
    parser.add_argument("layer", choices=["bronze", "silver"])
    parser.add_argument("--mode", choices=["available-now", "continuous"], default="available-now")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config()
    spark = build_spark(f"dip-{args.layer}", with_kafka=(args.layer == "bronze"),
                        shuffle_partitions=cfg.spark.shuffle_partitions)
    started = time.monotonic()
    try:
        starter = start_bronze if args.layer == "bronze" else start_silver
        query = starter(spark, cfg, args.mode)
        log.info("%s started (mode=%s)", args.layer, args.mode)
        query.awaitTermination()
        batches = query.recentProgress
        rows = sum((p["numInputRows"] or 0) for p in batches)
        log.info("%s finished: batches=%d input_rows=%d elapsed=%.1fs",
                 args.layer, len(batches), rows, time.monotonic() - started)
    except KeyboardInterrupt:
        log.info("%s stopped by user", args.layer)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
