from __future__ import annotations

import heapq
import random
import uuid
from datetime import datetime, timedelta
from typing import Any

from dip.config import GeneratorConfig

SCHEMA_VERSION = 1

ZONES = {
    "dubai": [
        ("Downtown", 25.1972, 55.2744), ("Marina", 25.0805, 55.1403),
        ("JLT", 25.0693, 55.1413), ("Deira", 25.2697, 55.3095),
        ("Business Bay", 25.1850, 55.2610),
    ],
    "riyadh": [
        ("Olaya", 24.6988, 46.6857), ("Al Malqa", 24.8150, 46.6260),
        ("Al Nakheel", 24.7480, 46.6550), ("Al Yasmin", 24.8230, 46.6570),
        ("Hittin", 24.7640, 46.5900),
    ],
}
CURRENCY = {"dubai": "AED", "riyadh": "SAR"}
UTC_OFFSET_HOURS = {"dubai": 4, "riyadh": 3}
PEAK_HOURS = {12, 13, 19, 20, 21}
CUISINES = ["Arabic", "Indian", "Pizza", "Burgers", "Shawarma", "Chinese", "Healthy", "Desserts"]
PAYMENTS = ["card", "cash", "wallet"]


class OrderSimulator:
    """Pure-Python order lifecycle simulator (no Kafka). Events are scheduled on a
    simulated clock; dirty data (late, duplicate, null) is injected at schedule time."""

    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self._heap: list[tuple[datetime, int, dict[str, Any]]] = []
        self._seq = 0
        self.orders_created = 0

    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _minutes(self, low: float, high: float) -> timedelta:
        return timedelta(minutes=self.rng.uniform(low, high))

    def _push(self, due: datetime, event: dict[str, Any]) -> None:
        self._seq += 1
        heapq.heappush(self._heap, (due, self._seq, event))

    def _schedule(self, event: dict[str, Any], event_time: datetime) -> None:
        due = event_time
        if self.rng.random() < self.cfg.late_rate:
            due = event_time + self._minutes(3, 10)  # arrives late; event_time unchanged
        self._push(due, event)
        if self.rng.random() < self.cfg.duplicate_rate:
            self._push(due + self._minutes(0, 2), dict(event))  # same event_id

    def create_order(self, now: datetime) -> None:
        r = self.rng
        self.orders_created += 1
        city = r.choice(sorted(ZONES))
        zone, lat, lon = r.choice(ZONES[city])
        items = r.randint(1, 6)
        distance_km = round(max(0.5, r.gauss(4.0, 2.0)), 2)
        order = {
            "order_id": f"ORD-{self._uuid()[:12]}",
            "customer_id": f"CU{r.randint(1, 20000):06d}",
            "restaurant_id": f"RS{r.randint(1, 300):04d}",
            "city": city,
            "zone": zone,
            "lat": round(lat + r.uniform(-0.01, 0.01), 5),
            "lon": round(lon + r.uniform(-0.01, 0.01), 5),
            "cuisine": r.choice(CUISINES),
            "items_count": items,
            "order_value": round(items * r.uniform(18, 45) + 5, 2),
            "currency": CURRENCY[city],
            "payment_method": r.choice(PAYMENTS),
            "distance_km": distance_km,
        }

        t_accept = now + self._minutes(1, 4)
        timeline: list[tuple[str, datetime, str | None]] = [
            ("order_created", now, None),
            ("restaurant_accepted", t_accept, None),
        ]
        if r.random() < self.cfg.cancel_rate:
            timeline.append(("order_cancelled", t_accept + self._minutes(1, 5), None))
        else:
            courier = f"CR{r.randint(1, 500):05d}"
            t_assign = t_accept + self._minutes(1, 6)
            t_pick = max(t_assign + self._minutes(2, 8), t_accept + self._minutes(8, 25))
            local_hour = (now.hour + UTC_OFFSET_HOURS[city]) % 24
            peak = 1.25 if local_hour in PEAK_HOURS else 1.0
            speed_kmh = r.uniform(22, 35)
            travel_min = distance_km / speed_kmh * 60 * peak + r.uniform(1, 5)
            t_deliver = t_pick + timedelta(minutes=travel_min)
            timeline += [
                ("courier_assigned", t_assign, courier),
                ("order_picked_up", t_pick, courier),
                ("order_delivered", t_deliver, courier),
            ]

        for event_type, ts, courier_id in timeline:
            event = {
                "schema_version": SCHEMA_VERSION,
                "event_id": self._uuid(),
                "event_type": event_type,
                "event_time": ts.isoformat(timespec="milliseconds"),
                **order,
                "courier_id": courier_id,
            }
            if event_type == "order_created" and r.random() < self.cfg.null_rate:
                event["order_value"] = None
            self._schedule(event, ts)

    def due_events(self, now: datetime) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        while self._heap and self._heap[0][0] <= now:
            out.append(heapq.heappop(self._heap)[2])
        return out

    def pending(self) -> int:
        return len(self._heap)