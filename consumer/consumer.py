"""Commit Kafka progress only after the event or rejection is durable."""

import argparse
from dataclasses import asdict, dataclass
import json
import logging
import math
from pathlib import Path
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import Settings, load_settings
from consumer.storage import EventStore, KafkaRecord


LOGGER = logging.getLogger(__name__)


@dataclass
class ConsumerStats:
    received: int = 0
    inserted: int = 0
    duplicate: int = 0
    rejected: int = 0
    replayed: int = 0
    committed: int = 0
    stop_reason: str = "running"


def consumer_config(settings: Settings) -> dict:
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": settings.kafka_group_id,
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
        "socket.timeout.ms": settings.kafka_timeout_seconds * 1000,
    }


def validate_limits(max_messages: int | None, idle_timeout: float | None) -> None:
    if max_messages is not None and max_messages <= 0:
        raise ValueError("max_messages must be positive")
    if idle_timeout is not None and (not math.isfinite(idle_timeout) or idle_timeout <= 0):
        raise ValueError("idle_timeout must be finite and positive")


def consume(consumer, store: EventStore, *, max_messages: int | None = None,
            idle_timeout: float | None = None, stats: ConsumerStats | None = None) -> ConsumerStats:
    """Process sequentially. Any persistence/commit failure stops this run."""
    validate_limits(max_messages, idle_timeout)
    stats = stats if stats is not None else ConsumerStats()
    last_message_at = time.monotonic()
    try:
        while max_messages is None or stats.received < max_messages: #没设置最大接受数量的情况or接受数量未达上限,继续消费
            message = consumer.poll(timeout=1.0)
            if message is None:
                if idle_timeout is not None and time.monotonic() - last_message_at >= idle_timeout:
                    if max_messages is not None: #判断提前停止是否属于任务未完成
                        raise TimeoutError(f"Expected {max_messages} messages, received {stats.received} before idle timeout")
                    stats.stop_reason = "idle"
                    return stats
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            last_message_at = time.monotonic()
            stats.received += 1#统计的是 Consumer 收到的消息数量，而不一定是成功写入 DuckDB 的数量
            record = KafkaRecord(
                message.topic(), message.partition(), message.offset(),
                message.value(), message.key(),
            )
            result = store.process(record)  # DuckDB COMMIT happens inside this call.
            setattr(stats, result.outcome, getattr(stats, result.outcome) + 1)

            # A crash before this call causes a safe replay on the next run.
            offsets = consumer.commit(message=message, asynchronous=False)
            if not offsets or any(partition.error is not None for partition in offsets):
                raise RuntimeError(f"Kafka offset commit failed at {record.position}")
            if not any(
                (partition.topic, partition.partition, partition.offset)
                == (record.topic, record.partition, record.offset + 1)
                for partition in offsets
            ):
                raise RuntimeError(f"Kafka did not confirm the requested offset at {record.position}")
            stats.committed += 1
            LOGGER.info(
                "outcome=%s topic=%s partition=%s offset=%s event_id=%s error_code=%s",
                result.outcome, record.topic, record.partition, record.offset,
                result.event_id, result.error_code,
            )
        stats.stop_reason = "max_messages"
        return stats
    except KeyboardInterrupt:
        stats.stop_reason = "interrupted"
        return stats
    except Exception:
        stats.stop_reason = "error"
        raise


def main(max_messages: int | None = None, idle_timeout: float | None = None) -> int:
    settings = load_settings()
    validate_limits(max_messages, idle_timeout)
    stats = ConsumerStats()
    store = None
    consumer = None
    try:
        # A legacy database is rejected before creating the Kafka consumer.
        store = EventStore(settings.db_path)
        consumer = Consumer(consumer_config(settings))
        consumer.subscribe([settings.kafka_topic])
        LOGGER.info("Listening to %s; database=%s", settings.kafka_topic, settings.db_path)
        consume(consumer, store, max_messages=max_messages, idle_timeout=idle_timeout, stats=stats)
        return 130 if stats.stop_reason == "interrupted" and max_messages is not None else 0
    except Exception:
        stats.stop_reason = "error"
        LOGGER.exception("Ingestion stopped; Kafka progress may be unconfirmed. Restart with the same topic, group and database.")
        return 1
    finally:
        try:
            if consumer is not None:
                consumer.close()  # Auto-commit is disabled, including at shutdown.
        finally:
            if store is not None:
                store.close()
            print("Consumer summary: " + json.dumps(asdict(stats), sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-messages", type=int, help="Stop after this many Kafka records, including duplicates and rejects")
    parser.add_argument("--idle-timeout", type=float, help="Stop after this many seconds without a record; fail if a requested count is unmet")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    raise SystemExit(main(arguments.max_messages, arguments.idle_timeout))
