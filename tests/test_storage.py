import copy
import hashlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import duckdb

from consumer.storage import DatabaseSchemaError, EventStore, KafkaRecord, SourceRecordConflict
from tests.helpers import make_event, record


class StorageTests(unittest.TestCase):
    def test_duplicate_event_and_same_offset_replay_are_idempotent(self):
        event = make_event()
        with EventStore(":memory:") as store:
            original = record(event)
            self.assertEqual(store.process(original).outcome, "inserted")
            self.assertEqual(store.process(record(event, offset=1)).outcome, "duplicate")
            self.assertEqual(store.process(original).outcome, "replayed")
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 1)
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0], 2)
            with self.assertRaises(duckdb.ConstraintException):
                store.conn.execute("INSERT INTO raw_events SELECT * FROM raw_events")

    def test_conflicting_event_id_is_quarantined_and_original_is_kept(self):
        event = make_event()
        conflict = copy.deepcopy(event)
        conflict["properties"]["campaign_id"] = "different"
        with EventStore(":memory:") as store:
            store.process(record(event))
            result = store.process(record(conflict, offset=1))
            self.assertEqual((result.outcome, result.error_code), ("rejected", "event_id_conflict"))
            self.assertEqual(store.conn.execute("SELECT properties->>'campaign_id' FROM raw_events").fetchone(), ("CMP_001",))

    def test_invalid_bytes_and_tombstones_are_preserved_once(self):
        with EventStore(":memory:") as store:
            for offset, payload in enumerate((b"\xff", None)):
                item = KafkaRecord("test", 0, offset, payload)
                self.assertEqual(store.process(item).outcome, "rejected")
                self.assertEqual(store.process(item).outcome, "replayed")
            self.assertEqual(
                store.conn.execute("SELECT payload FROM rejected_events ORDER BY source_offset").fetchall(),
                [(b"\xff",), (None,)],
            )
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0], 2)

    def test_reused_source_position_with_different_bytes_fails_closed(self):
        with EventStore(":memory:") as store:
            store.process(record())
            with self.assertRaises(SourceRecordConflict):
                store.process(record())
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 1)

    def test_ledger_failure_rolls_back_event_insert(self):
        with EventStore(":memory:") as store:
            connection = store.conn
            wrapped = Mock(wraps=connection)

            def execute(sql, *args):
                if "INSERT INTO processed_messages" in sql:
                    raise duckdb.IOException("simulated ledger failure")
                return connection.execute(sql, *args)

            wrapped.execute.side_effect = execute
            store.conn = wrapped
            with self.assertRaises(duckdb.IOException):
                store.process(record())
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM processed_messages").fetchone()[0], 0)

    def test_ledger_failure_also_rolls_back_rejection(self):
        with EventStore(":memory:") as store:
            connection = store.conn
            wrapped = Mock(wraps=connection)

            def execute(sql, *args):
                if "INSERT INTO processed_messages" in sql:
                    raise duckdb.IOException("simulated ledger failure")
                return connection.execute(sql, *args)

            wrapped.execute.side_effect = execute
            store.conn = wrapped
            with self.assertRaises(duckdb.IOException):
                store.process(record(value=b"broken"))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM rejected_events").fetchone()[0], 0)

    def test_legacy_database_is_rejected_without_modification(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.duckdb"
            with duckdb.connect(str(path)) as connection:
                connection.execute("CREATE TABLE raw_events AS SELECT 'historical' AS event_id")
            before = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(DatabaseSchemaError, "No automatic migration"):
                EventStore(path)
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), before)
            with duckdb.connect(str(path), read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT * FROM raw_events").fetchall(), [("historical",)])

    def test_new_database_can_be_reopened_and_replayed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "new.duckdb"
            original = record()
            with EventStore(path) as store:
                store.process(original)
            with EventStore(path) as store:
                self.assertEqual(store.process(original).outcome, "replayed")
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0], 1)
