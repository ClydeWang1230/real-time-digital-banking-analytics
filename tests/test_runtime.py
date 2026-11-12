"""Regression checks for P0.1; Kafka is mocked and databases are temporary."""

from contextlib import redirect_stdout
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import duckdb
from confluent_kafka import TopicPartition

from config import PROJECT_ROOT, load_settings
import consumer.consumer as ingestion
import producer.producer as publishing
import run_analytics as reports
from consumer.storage import EventStore
from tests.helpers import make_event


class RuntimeTests(unittest.TestCase):
    def test_database_paths_are_independent_of_working_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            previous = Path.cwd()
            try:
                os.chdir(directory)
                self.assertEqual(
                    load_settings({}).db_path,
                    PROJECT_ROOT / "banking_events_analytics.duckdb",
                )
                self.assertEqual(
                    load_settings({"BANKING_DB_PATH": "separate.duckdb"}).db_path,
                    PROJECT_ROOT / "separate.duckdb",
                )
                absolute = Path(directory) / "absolute.duckdb"
                self.assertEqual(
                    load_settings({"BANKING_DB_PATH": str(absolute)}).db_path,
                    absolute.resolve(),
                )
                self.assertEqual(list(Path(directory).iterdir()), [])
            finally:
                os.chdir(previous)

    def test_imports_and_direct_script_loading_have_no_external_side_effects(self):
        code = """
import importlib
from pathlib import Path
import runpy
import sys
from unittest.mock import patch

root = Path(sys.argv[1])
sys.path.insert(0, str(root))
with patch('confluent_kafka.Producer', side_effect=AssertionError('Kafka producer opened')), \
     patch('confluent_kafka.Consumer', side_effect=AssertionError('Kafka consumer opened')), \
     patch('duckdb.connect', side_effect=AssertionError('Database opened')):
    for name in ['config', 'producer.producer', 'consumer.consumer', 'run_analytics']:
        importlib.import_module(name)
    for name in ['producer/producer.py', 'consumer/consumer.py', 'run_analytics.py']:
        runpy.run_path(str(root / name), run_name='import_check')
"""
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [sys.executable, "-B", "-c", code, str(PROJECT_ROOT)],
                cwd=directory, capture_output=True, text=True, timeout=20,
            )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_configuration_fails_before_clients_or_database_open(self):
        for count in ("0", "-1", "abc", ""):
            with self.subTest(count=count), patch.dict(
                os.environ, {"BANKING_CUSTOMER_COUNT": count}
            ), patch.object(publishing, "Producer") as producer, patch.object(
                ingestion, "EventStore"
            ) as connect:
                with self.assertRaisesRegex(ValueError, "BANKING_CUSTOMER_COUNT"):
                    publishing.main()
                with self.assertRaisesRegex(ValueError, "BANKING_CUSTOMER_COUNT"):
                    ingestion.main()
                producer.assert_not_called()
                connect.assert_not_called()

    def test_shared_settings_preserve_publish_and_insert_behavior(self):
        settings = load_settings({
            "BANKING_KAFKA_BOOTSTRAP_SERVERS": "example.invalid:9092",
            "BANKING_KAFKA_TOPIC": "isolated-test-topic",
            "BANKING_KAFKA_GROUP_ID": "isolated-test-group",
            "BANKING_DB_PATH": "unused-test-path.duckdb",
            "BANKING_CUSTOMER_COUNT": "2",
        })
        producer = Mock()
        producer.flush.return_value = 0
        producer.produce.side_effect = lambda *args, **kwargs: kwargs["on_delivery"](None, Mock())
        with tempfile.TemporaryDirectory() as directory, patch.object(publishing, "load_settings", return_value=settings), patch.object(
            publishing, "Producer", return_value=producer
        ) as factory, redirect_stdout(io.StringIO()):
            self.assertEqual(publishing.main(output_dir=directory), 0)
        factory.assert_called_once_with(publishing.producer_config(settings))

        events = []
        for call in producer.produce.call_args_list:
            self.assertEqual(call.args[0], settings.kafka_topic)
            event = json.loads(call.kwargs["value"])
            self.assertEqual(call.kwargs["key"], event["customer_id"].encode("utf-8"))
            events.append(event)
        self.assertEqual(len({event["customer_id"] for event in events}), 2)

        message = Mock()
        message.error.return_value = None
        message.value.return_value = json.dumps(events[0]).encode("utf-8")
        message.offset.return_value = 0
        message.topic.return_value = settings.kafka_topic
        message.partition.return_value = 0
        message.key.return_value = events[0]["customer_id"].encode("utf-8")
        consumer = Mock()
        consumer.poll.side_effect = [message, KeyboardInterrupt]
        consumer.commit.return_value = [TopicPartition(settings.kafka_topic, 0, 1)]

        with EventStore(":memory:") as store:
            connection = store.conn
            wrapped = Mock(wraps=store)
            wrapped.close = Mock()
            with patch.object(ingestion, "load_settings", return_value=settings), patch.object(
                ingestion, "EventStore", return_value=wrapped
            ) as connect, patch.object(
                ingestion, "Consumer", return_value=consumer
            ) as consumer_factory, redirect_stdout(io.StringIO()):
                self.assertEqual(ingestion.main(), 0)
            connect.assert_called_once_with(settings.db_path)
            consumer_factory.assert_called_once_with(ingestion.consumer_config(settings))
            consumer.subscribe.assert_called_once_with([settings.kafka_topic])
            consumer.close.assert_called_once()
            wrapped.close.assert_called_once()
            row = connection.execute(
                "SELECT event_id, customer_id, event_type, session_id, properties FROM raw_events"
            ).fetchone()
            self.assertEqual(row[:4], tuple(
                events[0][key] for key in ("event_id", "customer_id", "event_type", "session_id")
            ))
            self.assertEqual(json.loads(row[4]), events[0]["properties"])

    def test_consumer_closes_database_if_kafka_initialization_fails(self):
        store = Mock()
        with patch.object(ingestion, "load_settings", return_value=load_settings({})), patch.object(
            ingestion, "EventStore", return_value=store
        ), patch.object(ingestion, "Consumer", side_effect=RuntimeError("startup failed")), self.assertLogs(
            ingestion.LOGGER, level="ERROR"
        ), redirect_stdout(io.StringIO()):
            self.assertEqual(ingestion.main(), 1)
        store.close.assert_called_once()

    def test_analytics_does_not_create_a_missing_database(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.duckdb"
            settings = replace(load_settings({}), db_path=path)
            with patch.object(reports, "load_settings", return_value=settings):
                with self.assertRaisesRegex(FileNotFoundError, "Analytics database not found"):
                    reports.main()
            self.assertFalse(path.exists())

    def test_analytics_rejects_writes_and_closes_connection_after_sql_error(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.duckdb"
            with duckdb.connect(str(path)) as connection:
                connection.execute("CREATE TABLE sentinel AS SELECT 1 AS value")
                connection.execute("""CREATE TABLE raw_events (
                    event_id VARCHAR, customer_id VARCHAR, session_id VARCHAR, event_type VARCHAR,
                    event_time TIMESTAMPTZ, channel VARCHAR, device_type VARCHAR, properties JSON)""")
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            sql_dir = Path(directory) / "sql"
            sql_dir.mkdir()
            (sql_dir / "01_write.sql").write_text(
                "INSERT INTO sentinel VALUES (2)", encoding="utf-8"
            )
            settings = replace(load_settings({}), db_path=path)
            with patch.object(reports, "load_settings", return_value=settings), patch.object(
                reports, "SQL_DIR", sql_dir
            ), redirect_stdout(io.StringIO()):
                with self.assertRaises(duckdb.Error):
                    reports.main()
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)
            with duckdb.connect(str(path), read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT * FROM sentinel").fetchall(), [(1,)])

    def test_analytics_script_finds_reports_from_another_working_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.duckdb"
            with duckdb.connect(str(path)) as connection:
                event = make_event()
                connection.execute("""
                    CREATE TABLE raw_events AS
                    SELECT ?::VARCHAR AS event_id, ?::VARCHAR AS customer_id,
                           ?::VARCHAR AS event_type, ?::VARCHAR AS session_id,
                           ?::TIMESTAMPTZ AS event_time, ?::VARCHAR AS channel,
                           ?::VARCHAR AS device_type, ?::JSON AS properties
                """, [event['event_id'], event['customer_id'], event['event_type'], event['session_id'],
                       event['event_time'], event['channel'], event['device_type'], json.dumps(event['properties'])])
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            env = {key: value for key, value in os.environ.items() if not key.startswith("BANKING_")}
            env["BANKING_DB_PATH"] = str(path)
            result = subprocess.run(
                [sys.executable, "-B", str(PROJECT_ROOT / "run_analytics.py")],
                cwd=directory, env=env, capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("03_onboarding_funnel.sql", result.stdout)
            self.assertIn("legacy | 1 | onboarding_started | Onboarding started | 1 | 1 | NULL | 100.0", result.stdout)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)


if __name__ == "__main__":
    unittest.main()
