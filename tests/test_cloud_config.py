from dip.config import KafkaConfig, SparkConfig, load_config
from dip.streaming.kafka_options import spark_kafka_options


def kafka(**overrides):
    base = dict(bootstrap_servers="b:9092", topic="t", partitions=3, replication_factor=1)
    base.update(overrides)
    return KafkaConfig(**base)


def test_plaintext_client_config():
    assert kafka().client_config() == {"bootstrap.servers": "b:9092"}


def test_sasl_client_config():
    conf = kafka(security_protocol="SASL_SSL", api_key="k", api_secret="s").client_config()
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanisms"] == "PLAIN"
    assert conf["sasl.username"] == "k"
    assert conf["sasl.password"] == "s"


def test_secret_not_in_repr():
    assert "topsecret" not in repr(kafka(api_secret="topsecret"))


def test_spark_options_plaintext_has_no_jaas():
    opts = spark_kafka_options(kafka(), runtime="local")
    assert "kafka.sasl.jaas.config" not in opts
    assert opts["subscribe"] == "t"


def test_spark_options_local_sasl_unshaded():
    opts = spark_kafka_options(kafka(security_protocol="SASL_SSL", api_key="k", api_secret="s"),
                               runtime="local")
    assert opts["kafka.sasl.jaas.config"].startswith("org.apache.kafka.common.security.plain")


def test_spark_options_databricks_uses_shaded_module():
    opts = spark_kafka_options(kafka(security_protocol="SASL_SSL", api_key="k", api_secret="s"),
                               runtime="databricks")
    assert opts["kafka.sasl.jaas.config"].startswith("kafkashaded.org.apache.kafka.common.security.plain")


def test_spark_paths_follow_data_root():
    s = SparkConfig(data_root="s3://bucket/dip", watermark="15 minutes", shuffle_partitions=4)
    assert s.bronze_path == "s3://bucket/dip/delta/bronze/order_events"
    assert s.checkpoint_dir == "s3://bucket/dip/checkpoints"


def test_load_config_env_overrides(monkeypatch):
    monkeypatch.setenv("DIP_DATA_ROOT", "s3://b/dip")
    monkeypatch.setenv("KAFKA_API_KEY", "k")
    cfg = load_config("config/config.yaml")
    assert cfg.spark.silver_path == "s3://b/dip/delta/silver/order_events"
    assert cfg.kafka.api_key == "k"
    