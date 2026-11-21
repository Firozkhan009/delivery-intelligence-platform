from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

EVENT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("city", StringType()),
    StructField("zone", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("cuisine", StringType()),
    StructField("items_count", IntegerType()),
    StructField("order_value", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("courier_id", StringType()),
])
