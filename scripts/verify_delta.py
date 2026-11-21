from dip.config import load_config
from dip.streaming.session import build_spark


def main() -> None:
    cfg = load_config()
    spark = build_spark("dip-verify", shuffle_partitions=cfg.spark.shuffle_partitions)
    bronze = spark.read.format("delta").load(cfg.spark.bronze_path)
    silver = spark.read.format("delta").load(cfg.spark.silver_path)

    b_total = bronze.count()
    b_distinct = bronze.select("event_id").distinct().count()
    s_total = silver.count()
    s_distinct = silver.select("event_id").distinct().count()
    s_missing = silver.filter("order_value_missing").count()

    print(f"bronze rows            : {b_total}")
    print(f"bronze distinct events : {b_distinct}")
    print(f"duplicates in bronze   : {b_total - b_distinct}")
    print(f"silver rows            : {s_total}")
    print(f"duplicates in silver   : {s_total - s_distinct}  (must be 0)")
    print(f"silver == bronze distinct: {s_total == b_distinct}")
    print(f"order_created with missing order_value: {s_missing}")
    silver.groupBy("event_type").count().orderBy("event_type").show()
    silver.groupBy("city").count().orderBy("city").show()
    spark.stop()


if __name__ == "__main__":
    main()
