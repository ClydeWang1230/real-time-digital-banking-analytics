"""Verify P0.2 against local Kafka, using a unique topic, groups and databases.

The crash worker deliberately exits without cleanup AFTER a DuckDB commit but
BEFORE a Kafka commit. It only receives a newly created verification database.
"""

import argparse
from dataclasses import asdict, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import duckdb
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PROJECT_ROOT, Settings, load_settings
from consumer.consumer import ConsumerStats, consume, consumer_config
from consumer.storage import EventStore
from producer.producer import generate_messages, publish_records


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def client_for(settings: Settings):
    config = consumer_config(settings)
    # Shorten recovery only for this isolated single-consumer verification.
    config.update({"session.timeout.ms": 6000, "heartbeat.interval.ms": 2000})
    return Consumer(config)


def committed_offset(settings: Settings) -> int:
    client = client_for(settings)
    try:
        result = client.committed([TopicPartition(settings.kafka_topic, 0)], timeout=15)
        require(result[0].error is None, "Cannot read committed offset")
        return result[0].offset
    finally:
        client.close()


def run_batch(settings: Settings, count: int) -> ConsumerStats:
    with EventStore(settings.db_path) as store:
        client = client_for(settings)
        try:
            client.subscribe([settings.kafka_topic])
            return consume(client, store, max_messages=count, idle_timeout=45)
        finally:
            client.close()


def database_counts(path: Path) -> dict:
    with duckdb.connect(str(path), read_only=True) as connection:
        counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("raw_events", "rejected_events", "processed_messages")
        }
        counts["unique_event_ids"] = connection.execute(
            "SELECT COUNT(DISTINCT event_id) FROM raw_events"
        ).fetchone()[0]
        counts["sessions"] = connection.execute(
            "SELECT COUNT(DISTINCT session_id) FROM raw_events"
        ).fetchone()[0]
        counts["rejection_reasons"] = dict(connection.execute(
            "SELECT error_code, COUNT(*) FROM rejected_events GROUP BY error_code ORDER BY error_code"
        ).fetchall())
        return counts


def test_messages(customer_count: int):
    generated = list(generate_messages(customer_count))
    key, value = generated[0]
    original = json.loads(value)
    missing = dict(original)
    missing.pop("event_id")
    naive = dict(original, event_time="2026-11-06T12:00:00")
    unknown = dict(original, event_type="not_a_stage")
    conflict = dict(original, properties={**original["properties"], "campaign_id": "CONFLICT"})
    additions = [
        (key, value),  # Same event ID at a different Kafka position.
        (key, json.dumps(original, sort_keys=True).encode()),
        (key, b"{broken"), (key, b"\xff"), (key, None),
        (key, json.dumps(missing).encode()),
        (key, json.dumps(naive).encode()),
        (key, json.dumps(unknown).encode()),
        (key, json.dumps(conflict).encode()),
    ]
    # A valid final message demonstrates continued processing after bad records.
    # Keep the first generated event before its duplicates even for one customer.
    messages = generated + additions + [(key, value)]
    return messages, len(generated), 3, 7


def crash_worker(settings: Settings) -> None:
    store = EventStore(settings.db_path)
    client = client_for(settings)

    class ExitBeforeCommit:
        def poll(self, timeout):
            return client.poll(timeout)

        def commit(self, **kwargs):
            os._exit(73)  # Skip finally/close: exercise DuckDB WAL recovery too.

    try:
        client.subscribe([settings.kafka_topic])
        consume(ExitBeforeCommit(), store, max_messages=1, idle_timeout=45)
    finally:
        client.close()
        store.close()


def verify(bootstrap: str, customers: int) -> Path:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    output = PROJECT_ROOT / "artifacts" / "verification" / ("p02-" + run_id)
    output.mkdir(parents=True, exist_ok=False)
    topic = "banking-p02-" + run_id.lower()
    settings = replace(
        Settings(), kafka_bootstrap_servers=bootstrap, kafka_topic=topic,
        kafka_group_id=topic + "-crash", db_path=output / "recovery.duckdb",
    )
    report = {"status": "running", "topic": topic, "output": str(output), "customers": customers}
    report_path = output / "report.json"
    try:
        print(f"Creating isolated verification topic: {topic}", flush=True)
        admin = AdminClient({"bootstrap.servers": bootstrap})
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)],
                            request_timeout=15)[topic].result(timeout=20)
        messages, unique, duplicates, rejected = test_messages(customers)
        total = len(messages)
        report["expected"] = {"messages": total, "unique_events": unique,
                              "duplicates": duplicates, "rejected": rejected}
        delivery = publish_records(messages, settings)
        report["producer"] = asdict(delivery)
        require(delivery.acknowledged == total, "Producer acknowledgement mismatch")
        print(f"Acknowledged {total} records; forcing exit between database and Kafka commits...", flush=True)

        worker = subprocess.run([
            sys.executable, "-B", str(Path(__file__).resolve()), "--crash-worker",
            "--bootstrap", bootstrap, "--topic", topic, "--group", settings.kafka_group_id,
            "--database", str(settings.db_path),
        ], capture_output=True, timeout=75)
        require(worker.returncode == 73,
                f"Crash worker exited {worker.returncode}: {worker.stderr.decode(errors='replace')}")
        offset_after_crash = committed_offset(settings)
        require(offset_after_crash < 0, "Crash incorrectly advanced the Kafka offset")
        # The file can still have a WAL: reopening must recover the committed row.
        after_crash = database_counts(settings.db_path)
        require(after_crash["raw_events"] == after_crash["processed_messages"] == 1,
                "The pre-crash database commit was not recovered")
        report["crash"] = {"exit_code": worker.returncode, "committed_offset": offset_after_crash,
                           "database": after_crash}

        print("Restarting the same consumer group and database...", flush=True)
        recovered = run_batch(settings, total)
        report["recovery"] = asdict(recovered)
        require((recovered.inserted, recovered.duplicate, recovered.rejected, recovered.replayed)
                == (unique - 1, duplicates, rejected, 1), "Recovery counters did not reconcile")
        require(recovered.committed == total, "Not all recovered records were committed")
        require(committed_offset(settings) == total, "Group offset does not match the topic end")
        counts = database_counts(settings.db_path)
        require(counts["raw_events"] == counts["unique_event_ids"] == unique,
                "Unique-event count mismatch")
        require(counts["rejected_events"] == rejected and counts["processed_messages"] == total,
                "Quarantine or ledger count mismatch")
        require(counts["sessions"] == customers, "Session count mismatch")
        report["database"] = counts

        print("Replaying the whole topic with a new group...", flush=True)
        replay_settings = replace(settings, kafka_group_id=topic + "-replay")
        replay = run_batch(replay_settings, total)
        report["full_replay"] = asdict(replay)
        require(replay.replayed == replay.committed == total, "Full replay did not reconcile")
        require(database_counts(settings.db_path) == counts, "Replay changed stored data")

        print("Checking that a database error leaves Kafka progress unchanged...", flush=True)
        failure_settings = replace(settings, kafka_group_id=topic + "-db-failure",
                                   db_path=output / "database_failure.duckdb")
        store = EventStore(failure_settings.db_path)
        store.close()  # Real DuckDB connection failure, on an isolated test DB.
        client = client_for(failure_settings)
        failed_stats = ConsumerStats()
        try:
            client.subscribe([topic])
            try:
                consume(client, store, max_messages=1, idle_timeout=45, stats=failed_stats)
            except duckdb.ConnectionException:
                pass
            else:
                raise AssertionError("Expected a database connection failure")
        finally:
            client.close()
        require(failed_stats.received == 1 and failed_stats.committed == 0,
                "Failed storage incorrectly acknowledged a record")
        require(committed_offset(failure_settings) < 0, "DB failure advanced Kafka progress")
        retry = run_batch(failure_settings, total)
        require((retry.inserted, retry.duplicate, retry.rejected, retry.committed)
                == (unique, duplicates, rejected, total), "Retry after DB failure lost records")
        require(database_counts(failure_settings.db_path) == counts,
                "Recovered database counts differ")
        report["database_failure"] = {"failed_attempt": asdict(failed_stats), "retry": asdict(retry)}
        report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"Verification report: {report_path}", flush=True)
    print("PASS: delivery, quarantine, crash recovery, full replay and DB-failure retry.", flush=True)
    return report_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", help="Kafka address; defaults to shared settings")
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--crash-worker", action="store_true", help=argparse.SUPPRESS)
    for name in ("topic", "group", "database"):
        parser.add_argument("--" + name, help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.customers <= 0:
        parser.error("--customers must be positive")
    bootstrap = arguments.bootstrap or load_settings().kafka_bootstrap_servers
    if arguments.crash_worker:
        if not all((arguments.topic, arguments.group, arguments.database)):
            parser.error("Crash worker requires its isolated topic, group and database")
        crash_worker(replace(Settings(), kafka_bootstrap_servers=bootstrap,
                             kafka_topic=arguments.topic, kafka_group_id=arguments.group,
                             db_path=Path(arguments.database)))
    else:
        verify(bootstrap, arguments.customers)
