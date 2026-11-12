"""Publish a synthetic batch and verify broker delivery acknowledgements."""

import argparse
from dataclasses import asdict, dataclass
import json
import logging
from pathlib import Path
import sys
import time
from typing import Iterable

from confluent_kafka import Producer

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Settings, load_settings
from producer.batch import DEFAULT_BATCH_DIR, encode_event, load_batch, prepare_batch
from producer.event_generator import DEFAULT_SEED, SimulationSpec, generate_events


LOGGER = logging.getLogger(__name__)


@dataclass
class DeliveryStats:
    attempted: int = 0
    queued: int = 0
    acknowledged: int = 0
    failed: int = 0
    pending: int = 0


class DeliveryError(RuntimeError):
    def __init__(self, reason: str, stats: DeliveryStats):
        super().__init__(reason)
        self.stats = stats


def producer_config(settings: Settings) -> dict:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": settings.delivery_timeout_seconds * 1000,
        "socket.timeout.ms": settings.kafka_timeout_seconds * 1000,
    }


def generate_messages(customer_count: int, *, run_id=None, seed=DEFAULT_SEED, base_time=None):
    spec = SimulationSpec.create(customer_count, run_id=run_id, seed=seed, base_time=base_time)
    for event in generate_events(spec):
        yield event["customer_id"].encode("utf-8"), encode_event(event)


def publish_records(messages: Iterable[tuple[bytes | None, bytes | None]],
                    settings: Settings, *, producer=None) -> DeliveryStats:
    """Publish bytes and count delivery reports. Queue/flush waits are bounded."""
    producer = producer if producer is not None else Producer(producer_config(settings))
    stats = DeliveryStats()
    enqueue_error = None

    def delivered(error, message):
        if error is None:
            stats.acknowledged += 1
        else:
            stats.failed += 1
            LOGGER.error("Kafka delivery failed: %s", error)

    try:
        for key, value in messages:
            stats.attempted += 1
            deadline = time.monotonic() + settings.queue_timeout_seconds
            while True:
                try:
                    producer.produce(
                        settings.kafka_topic, key=key, value=value, on_delivery=delivered,
                    )
                    stats.queued += 1
                    break
                except BufferError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Producer queue remained full; stop and investigate before retrying")
                    producer.poll(0.1)
            producer.poll(0)
    except Exception as exc:
        enqueue_error = exc
    finally:
        # The extra second lets the last delivery-timeout callback be serviced.
        stats.pending = producer.flush(settings.delivery_timeout_seconds + 1)

    if enqueue_error is not None:
        raise DeliveryError(str(enqueue_error), stats) from enqueue_error
    if stats.failed or stats.pending or stats.acknowledged != stats.queued:
        raise DeliveryError("Not every queued message received a successful broker acknowledgement", stats)
    return stats


def main(*, run_id=None, seed=None, base_time=None, customer_count=None,
         output_dir=None, replay=None, prepare_only=False) -> int:
    settings = load_settings()
    try:
        if replay is not None:
            if any(value is not None for value in (run_id, seed, base_time, customer_count, output_dir)):
                raise ValueError("--replay uses saved inputs; do not combine it with generation options")
            batch = load_batch(replay)
        else:
            spec = SimulationSpec.create(
                settings.customer_count if customer_count is None else customer_count,
                run_id=run_id, seed=DEFAULT_SEED if seed is None else seed, base_time=base_time,
            )
            batch = prepare_batch(spec, DEFAULT_BATCH_DIR if output_dir is None else output_dir)
        print("Batch ready: " + json.dumps({
            "run_id": batch.manifest["simulation"]["run_id"],
            "event_count": batch.manifest["event_count"],
            "events_sha256": batch.manifest["events_sha256"],
            "directory": str(batch.directory),
        }, sort_keys=True), flush=True)
        if prepare_only:
            return 0
        stats = publish_records(batch.messages, settings)
    except DeliveryError as exc:
        print("Producer summary: " + json.dumps(asdict(exc.stats), sort_keys=True))
        LOGGER.error("%s", exc)
        return 1
    except Exception:
        LOGGER.exception("Producer failed before completion")
        return 1
    print("Producer summary: " + json.dumps(asdict(stats), sort_keys=True))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", help="Batch UUID; omitted means a fresh simulation")
    parser.add_argument("--seed", type=int, help="Random seed; defaults to 42")
    parser.add_argument("--base-time", help="ISO timestamp with timezone; defaults to current UTC time")
    parser.add_argument("--customer-count", type=int, help="Overrides BANKING_CUSTOMER_COUNT")
    parser.add_argument("--output-dir", help="Batch root; defaults to artifacts/runs relative to project root")
    parser.add_argument("--replay", help="Read an existing batch directory without regenerating events")
    parser.add_argument("--prepare-only", action="store_true", help="Prepare or validate the batch without contacting Kafka")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main(**vars(arguments)))
