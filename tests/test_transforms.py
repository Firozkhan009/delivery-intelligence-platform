import json
from datetime import datetime

import pytest

from dip.streaming.session import build_spark
from dip.streaming.transforms import clean_events, parse_kafka_events


@pytest.fixture(scope="module")
def spark():
    session = build_spark("dip-tests", shuffle_partitions=2)
    yield session
    session.stop()


def event(**overrides):
    base = {
        "schema_version": 1, "event_id": "e1", "event_type": "order_created",
        "event_time": "2026-11-21T12:00:00.000+00:00", "order_id": "ORD-1",
        "customer_id": "CU1", "restaurant_id": "RS1", "city": "dubai", "zone": "Marina",
        "lat": 25.08, "lon": 55.14, "cuisine": "Pizza", "items_count": 2,
        "order_value": 80.5, "currency": "AED", "payment_method": "card",
        "distance_km": 3.2, "courier_id": None,
    }
    base.update(overrides)
    return json.dumps(base)


def raw_df(spark, payloads):
    rows = [(p.encode(), 0, i, datetime(2026, 11, 21, 12, 0, 0)) for i, p in enumerate(payloads)]
    return spark.createDataFrame(
        rows, "value binary, partition int, offset long, timestamp timestamp")


def clean(spark, payloads):
    return clean_events(parse_kafka_events(raw_df(spark, payloads)), "15 minutes")


def test_parse_types_and_lineage(spark):
    row = parse_kafka_events(raw_df(spark, [event()])).collect()[0]
    assert row.event_id == "e1"
    assert isinstance(row.event_time, datetime)
    assert row.order_value == 80.5
    assert row.kafka_offset == 0


def test_parse_malformed_json_keeps_raw_and_nulls_fields(spark):
    row = parse_kafka_events(raw_df(spark, ["not json"])).collect()[0]
    assert row.event_id is None
    assert row.raw_value == "not json"


def test_clean_removes_duplicates(spark):
    rows = clean(spark, [event(), event(), event(event_id="e2")]).collect()
    assert sorted(r.event_id for r in rows) == ["e1", "e2"]


def test_clean_drops_invalid_rows(spark):
    payloads = [
        event(event_id="ok"),
        event(event_id=None),
        event(event_id="bad-type", event_type="teleported"),
        event(event_id="neg", order_value=-5.0),
        "not json",
    ]
    assert [r.event_id for r in clean(spark, payloads).collect()] == ["ok"]


def test_clean_flags_missing_order_value(spark):
    rows = clean(spark, [event(event_id="n", order_value=None)]).collect()
    assert rows[0].order_value_missing is True


def test_clean_adds_event_date(spark):
    row = clean(spark, [event()]).collect()[0]
    assert row.event_date.isoformat() == "2026-11-21"
