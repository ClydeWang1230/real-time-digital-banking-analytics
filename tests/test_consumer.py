from contextlib import redirect_stdout
from dataclasses import replace
import io
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

from confluent_kafka import TopicPartition
import duckdb

from config import load_settings
import consumer.consumer as ingestion
from consumer.storage import EventStore
from tests.helpers import FakeConsumer, record


class ConsumerTests(unittest.TestCase):
    def test_consumer_disables_automatic_progress_and_topic_creation(self):
        options = ingestion.consumer_config(load_settings({}))
        self.assertIs(options["enable.auto.commit"], False)
        self.assertIs(options["enable.auto.offset.store"], False)
        self.assertIs(options["allow.auto.create.topics"], False)

    def test_commit_happens_after_both_database_writes(self):
        client = FakeConsumer([record()])
        with EventStore(":memory:") as store:
            def commit(*, message, asynchronous):
                self.assertFalse(asynchronous)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 1)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0], 1)
                return [TopicPartition(message.topic(), message.partition(), message.offset() + 1)]

            client.commit = Mock(side_effect=commit)
            stats = ingestion.consume(client, store, max_messages=1)
            self.assertEqual((stats.inserted, stats.committed, stats.stop_reason), (1, 1, "max_messages"))

    def test_bad_message_is_quarantined_then_next_valid_message_is_processed(self):
        client = FakeConsumer([record(value=b"broken"), record(offset=1)])
        with EventStore(":memory:") as store:
            stats = ingestion.consume(client, store, max_messages=2)
            self.assertEqual((stats.rejected, stats.inserted, stats.committed), (1, 1, 2))
            self.assertEqual(client.commits, [0, 1])

    def test_database_failure_never_commits_or_processes_later_records(self):
        client = FakeConsumer([record(), record(offset=1)])
        store = Mock()
        store.process.side_effect = duckdb.IOException("write unavailable")
        with self.assertRaises(duckdb.IOException):
            ingestion.consume(client, store, max_messages=2)
        self.assertEqual(client.commits, [])
        self.assertEqual(store.process.call_count, 1)

    def test_failed_rejection_persistence_is_not_acknowledged(self):
        client = FakeConsumer([record(value=b"broken")])
        with EventStore(":memory:") as store:
            connection = store.conn
            wrapped = Mock(wraps=connection)

            def execute(sql, *args):
                if "INSERT INTO rejected_events" in sql:
                    raise duckdb.IOException("quarantine unavailable")
                return connection.execute(sql, *args)

            wrapped.execute.side_effect = execute
            store.conn = wrapped
            with self.assertRaises(duckdb.IOException):
                ingestion.consume(client, store, max_messages=1)
            self.assertEqual(client.commits, [])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0], 0)

    def test_commit_failure_stops_and_next_run_replays_without_duplicate(self):
        first, second = record(), record(offset=1)
        client = FakeConsumer([first, second])
        client.commit = Mock(side_effect=RuntimeError("commit unavailable"))
        with EventStore(":memory:") as store:
            with self.assertRaisesRegex(RuntimeError, "commit unavailable"):
                ingestion.consume(client, store, max_messages=2)
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 1)
            restarted = FakeConsumer([first, second])
            stats = ingestion.consume(restarted, store, max_messages=2)
            self.assertEqual((stats.replayed, stats.inserted, stats.committed), (1, 1, 2))
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 2)

    def test_commit_partition_error_or_wrong_offset_is_a_failure(self):
        for response in [None, [], [SimpleNamespace(error="failed")], [TopicPartition("test-topic", 0, 999)]]:
            client = FakeConsumer([record()])
            client.commit = Mock(return_value=response)
            with self.subTest(response=response), EventStore(":memory:") as store:
                with self.assertRaises(RuntimeError):
                    ingestion.consume(client, store, max_messages=1)

    def test_count_not_reached_before_idle_timeout_fails(self):
        client = Mock()
        client.poll.return_value = None
        with patch.object(ingestion.time, "monotonic", side_effect=[0, 10]):
            with self.assertRaises(TimeoutError):
                ingestion.consume(client, Mock(), max_messages=2, idle_timeout=1)
        client.commit.assert_not_called()

    def test_legacy_default_is_refused_before_kafka_starts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "old.duckdb"
            with duckdb.connect(str(path)) as connection:
                connection.execute("CREATE TABLE raw_events(id VARCHAR)")
            with patch.object(ingestion, "load_settings", return_value=replace(load_settings({}), db_path=path)), patch.object(
                ingestion, "Consumer"
            ) as client, self.assertLogs(ingestion.LOGGER, level="ERROR"), redirect_stdout(io.StringIO()):
                self.assertEqual(ingestion.main(), 1)
            client.assert_not_called()

    def test_invalid_limits_fail_before_database_is_opened(self):
        for limits in [(0, None), (None, 0), (None, float("nan"))]:
            with self.subTest(limits=limits), patch.object(ingestion, "EventStore") as store:
                with self.assertRaises(ValueError):
                    ingestion.main(*limits)
                store.assert_not_called()
