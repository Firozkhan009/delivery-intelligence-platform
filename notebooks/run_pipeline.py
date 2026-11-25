# Databricks notebook source
# MAGIC %md
# MAGIC # Delivery Intelligence Platform: bronze and silver on Databricks
# MAGIC Reads order events from Confluent Cloud, writes bronze and silver Delta tables.

# COMMAND ----------
import os
import sys

repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, os.path.join(repo_root, "src"))
os.environ["DIP_CONFIG"] = os.path.join(repo_root, "config", "config.yaml")
print("repo root:", repo_root)

# COMMAND ----------
# Credentials come from widgets (never from source). Clear them when finished.
dbutils.widgets.text("bootstrap", "")
dbutils.widgets.text("api_key", "")
dbutils.widgets.text("api_secret", "")

# COMMAND ----------
catalog = spark.sql("SELECT current_catalog()").first()[0]
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.default.dip")

os.environ.update({
    "KAFKA_BOOTSTRAP_SERVERS": dbutils.widgets.get("bootstrap"),
    "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
    "KAFKA_API_KEY": dbutils.widgets.get("api_key"),
    "KAFKA_API_SECRET": dbutils.widgets.get("api_secret"),
    "DIP_DATA_ROOT": f"/Volumes/{catalog}/default/dip",
    "DIP_RUNTIME": "databricks",
})
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------
from dip.config import load_config
from dip.streaming.jobs import start_bronze, start_silver

cfg = load_config()
for name, start in (("bronze", start_bronze), ("silver", start_silver)):
    query = start(spark, cfg, "available-now")
    query.awaitTermination()
    rows = sum((p["numInputRows"] or 0) for p in query.recentProgress)
    print(f"{name} finished: input_rows={rows}")

# COMMAND ----------
bronze = spark.read.format("delta").load(cfg.spark.bronze_path)
silver = spark.read.format("delta").load(cfg.spark.silver_path)
b_total = bronze.count()
b_distinct = bronze.select("event_id").distinct().count()
s_total = silver.count()
s_distinct = silver.select("event_id").distinct().count()
print("bronze rows            :", b_total)
print("bronze distinct events :", b_distinct)
print("duplicates in bronze   :", b_total - b_distinct)
print("silver rows            :", s_total)
print("duplicates in silver   :", s_total - s_distinct, "(must be 0)")
print("silver == bronze distinct:", s_total == b_distinct)
display(silver.groupBy("event_type").count().orderBy("event_type"))

# COMMAND ----------
dbutils.widgets.removeAll()
