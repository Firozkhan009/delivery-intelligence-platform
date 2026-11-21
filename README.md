# Delivery Intelligence Platform

Streaming data engineering and ML platform on simulated Dubai/Riyadh food-delivery data.

## Planned architecture
Python event generator -> Kafka -> PySpark Structured Streaming (Databricks) -> Delta Lake on S3 -> Snowflake -> dbt marts -> Power BI

Orchestration: Airflow. Data quality: Great Expectations. ML: XGBoost, PyTorch, MLflow. Serving: FastAPI + Docker. CI: GitHub Actions.

## Status
- [x] Stage 1: Local Kafka + order event generator
