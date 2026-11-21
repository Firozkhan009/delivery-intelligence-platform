from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dip.config import GeneratorConfig
from dip.simulator import OrderSimulator

NOW = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
EXPECTED = ["order_created", "restaurant_accepted", "courier_assigned",
            "order_picked_up", "order_delivered"]


def make_cfg(**overrides):
    base = dict(orders_per_second=5, time_scale=1.0, seed=7, max_orders=0,
                duplicate_rate=0.0, late_rate=0.0, null_rate=0.0, cancel_rate=0.0)
    base.update(overrides)
    return GeneratorConfig(**base)


def drain(cfg, orders=50):
    sim = OrderSimulator(cfg)
    for _ in range(orders):
        sim.create_order(NOW)
    return sim, sim.due_events(NOW + timedelta(hours=3))


def test_deterministic_with_seed():
    assert drain(make_cfg())[1] == drain(make_cfg())[1]


def test_full_lifecycle_per_order():
    _, events = drain(make_cfg())
    assert len(events) == 50 * 5
    by_order = defaultdict(list)
    for e in events:
        by_order[e["order_id"]].append(e)
    for evs in by_order.values():
        evs.sort(key=lambda e: datetime.fromisoformat(e["event_time"]))
        assert [e["event_type"] for e in evs] == EXPECTED


def test_cancelled_orders_never_delivered():
    _, events = drain(make_cfg(cancel_rate=1.0))
    types = {e["event_type"] for e in events}
    assert "order_cancelled" in types
    assert "order_delivered" not in types


def test_duplicates_share_event_id():
    _, events = drain(make_cfg(duplicate_rate=1.0))
    assert len(events) == 2 * len({e["event_id"] for e in events})


def test_late_events_not_due_at_event_time():
    sim = OrderSimulator(make_cfg(late_rate=1.0))
    sim.create_order(NOW)
    assert sim.due_events(NOW) == []
    assert sim.pending() == 5


def test_null_injection_on_created_only():
    _, events = drain(make_cfg(null_rate=1.0))
    created = [e for e in events if e["event_type"] == "order_created"]
    assert all(e["order_value"] is None for e in created)
    assert all(e["order_value"] is not None for e in events if e["event_type"] != "order_created")