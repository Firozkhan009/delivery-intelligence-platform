from __future__ import annotations

import argparse
import logging
import signal
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from dip.config import AppConfig, load_config
from dip.logging_setup import setup_logging
from dip.producer import EventProducer
from dip.simulator import OrderSimulator

log = logging.getLogger("dip.main")


def run(cfg: AppConfig) -> None:
    gen = cfg.generator
    sim = OrderSimulator(gen)
    producer = EventProducer(cfg.kafka)
    producer.ensure_topic()

    stop = {"flag": False}

    def _stop(*_: object) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    wall_start = time.monotonic()
    sim_start = datetime.now(timezone.utc)
    next_order_at = 0.0
    last_report = wall_start
    log.info("starting: %.1f orders/s, time_scale=%.1f, max_orders=%d",
             gen.orders_per_second, gen.time_scale, gen.max_orders)

    while not stop["flag"]:
        elapsed = time.monotonic() - wall_start
        sim_now = sim_start + timedelta(seconds=elapsed * gen.time_scale)

        while elapsed >= next_order_at and not (gen.max_orders and sim.orders_created >= gen.max_orders):
            sim.create_order(sim_now)
            next_order_at += 1.0 / gen.orders_per_second

        for event in sim.due_events(sim_now):
            producer.send(event)
        producer.poll()

        if gen.max_orders and sim.orders_created >= gen.max_orders and sim.pending() == 0:
            break
        if time.monotonic() - last_report >= 10:
            log.info("orders=%d sent=%d failed=%d pending=%d",
                     sim.orders_created, producer.sent, producer.failed, sim.pending())
            last_report = time.monotonic()
        time.sleep(0.01)

    unflushed = producer.flush()
    elapsed = time.monotonic() - wall_start
    log.info("done: orders=%d sent=%d failed=%d unflushed=%d elapsed=%.1fs rate=%.0f msg/s",
             sim.orders_created, producer.sent, producer.failed, unflushed,
             elapsed, producer.sent / elapsed if elapsed else 0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delivery order event generator")
    parser.add_argument("--config", default=None)
    parser.add_argument("--max-orders", type=int)
    parser.add_argument("--time-scale", type=float)
    parser.add_argument("--orders-per-second", type=float)
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.config)
    overrides = {
        k: v for k, v in {
            "max_orders": args.max_orders,
            "time_scale": args.time_scale,
            "orders_per_second": args.orders_per_second,
        }.items() if v is not None
    }
    cfg = replace(cfg, generator=replace(cfg.generator, **overrides))
    run(cfg)


if __name__ == "__main__":
    main()