"""Transactional event persistence. Existing legacy databases are never migrated."""

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path

import duckdb

from consumer.validation import EventValidationError, validate_event


SCHEMA_VERSION = "banking-ingestion-p02-v1"
SCHEMA_SQL = """
CREATE TABLE pipeline_metadata (schema_version VARCHAR PRIMARY KEY);
CREATE TABLE raw_events (
    event_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    channel VARCHAR NOT NULL,
    device_type VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    properties JSON NOT NULL,
    payload JSON NOT NULL,
    payload_hash VARCHAR NOT NULL,
    source_topic VARCHAR NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
CREATE TABLE rejected_events (
    source_topic VARCHAR NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    message_key BLOB,
    payload BLOB,
    error_code VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source_topic, source_partition, source_offset)
);
CREATE TABLE processed_messages (
    source_topic VARCHAR NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    message_hash VARCHAR NOT NULL,
    event_id VARCHAR,
    outcome VARCHAR NOT NULL CHECK (outcome IN ('inserted', 'duplicate', 'rejected')),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (source_topic, source_partition, source_offset)
);
"""


class DatabaseSchemaError(RuntimeError):
    pass


class SourceRecordConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class KafkaRecord: #类型KafkaRecord
    topic: str
    partition: int
    offset: int
    value: bytes | None
    key: bytes | None = None

    @property #Python内置的装饰器名，这样后调用position不用加括号，而是用下面函数return
    def position(self) -> tuple[str, int, int]: #position是方法名称
        return self.topic, self.partition, self.offset


@dataclass(frozen=True)
class ProcessingResult:
    outcome: str
    event_id: str | None = None
    error_code: str | None = None


def _message_hash(record: KafkaRecord) -> str:#一个参数，参数应该是一个 KafkaRecord 类型的对象叫 record
    digest = hashlib.sha256()
    for item in (record.key, record.value): #前面定义过的KafkaRecord作为这里record的值，那里面有value等
        digest.update(b"\x00" if item is None else b"\x01" + len(item).to_bytes(8, "big") + item)
    return digest.hexdigest()


def _check_existing_schema(path: Path) -> None:
    # This connection is deliberately read-only: even a legacy empty DB is preserved.
    with duckdb.connect(str(path), read_only=True) as check:
        tables = {row[0] for row in check.execute("SHOW TABLES").fetchall()}
        expected = {"pipeline_metadata", "raw_events", "rejected_events", "processed_messages"}
        if not expected.issubset(tables):
            raise DatabaseSchemaError(
                f"Existing database has a legacy or unknown schema: {path}. "
                "Set BANKING_DB_PATH to a new database for P0.2. No automatic migration is performed."
            )
        if check.execute("SELECT schema_version FROM pipeline_metadata").fetchall() != [(SCHEMA_VERSION,)]:
            raise DatabaseSchemaError(f"Unsupported ingestion schema in {path}; database left unchanged.")
        primary_keys = check.execute("""
            SELECT constraint_column_names FROM duckdb_constraints()
            WHERE table_name='raw_events' AND constraint_type='PRIMARY KEY'
        """).fetchall()
        if primary_keys != [(["event_id"],)]:
            raise DatabaseSchemaError("raw_events must have an event_id primary key; no changes were made.")


class EventStore:
    def __init__(self, path: str | Path):
        memory = str(path) == ":memory:"
        destination = None if memory else Path(path).resolve()
        existing = destination is not None and destination.exists()
        if existing:
            _check_existing_schema(destination)
        self.conn = duckdb.connect(":memory:" if memory else str(destination))
        if not existing:
            try:
                self.conn.execute("BEGIN TRANSACTION")
                self.conn.execute(SCHEMA_SQL)
                self.conn.execute("INSERT INTO pipeline_metadata VALUES (?)", [SCHEMA_VERSION])
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.close()
                raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def close(self) -> None:
        self.conn.close()

    def process(self, record: KafkaRecord) -> ProcessingResult:
        """Persist the outcome and its source position together, then return."""
        if not record.topic or record.partition < 0 or record.offset < 0:
            raise ValueError("Kafka source position must contain a topic and non-negative partition/offset")
        message_hash = _message_hash(record)
        self.conn.execute("BEGIN TRANSACTION")
        try:
            previous = self.conn.execute("""
                SELECT message_hash, event_id FROM processed_messages
                WHERE source_topic=? AND source_partition=? AND source_offset=?
            """, record.position).fetchone()
            if previous:
                if previous[0] != message_hash:
                    raise SourceRecordConflict("A previously processed Kafka position has different bytes; stop and investigate topic reuse")
                result = ProcessingResult("replayed", previous[1])
            else:
                result = self._persist_new(record)
                self.conn.execute("""
                    INSERT INTO processed_messages (
                        source_topic, source_partition, source_offset,
                        message_hash, event_id, outcome
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (*record.position, message_hash, result.event_id, result.outcome))
            self.conn.execute("COMMIT")
            return result
        except BaseException:
            try:
                self.conn.execute("ROLLBACK")
            except duckdb.Error:
                pass  # Preserve the original failure, including an uncertain commit.
            raise

    def _persist_new(self, record: KafkaRecord) -> ProcessingResult:
        event_id = None
        try:
            validated = validate_event(record.value, record.key)
            event = validated.data
            event_id = event["event_id"]
            existing = self.conn.execute(
                "SELECT payload_hash FROM raw_events WHERE event_id=?", [event_id]
            ).fetchone()
            if existing:
                if existing[0] != validated.payload_hash:
                    raise EventValidationError("event_id_conflict", "event_id already exists with different event content")
                return ProcessingResult("duplicate", event_id)

            self.conn.execute("""
                INSERT INTO raw_events (
                    event_id, customer_id, event_type, event_time, channel,
                    device_type, session_id, properties, payload, payload_hash,
                    source_topic, source_partition, source_offset
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?::JSON, ?::JSON, ?, ?, ?, ?)
            """, (
                event_id, event["customer_id"], event["event_type"], event["event_time"],
                event["channel"], event["device_type"], event["session_id"],
                json.dumps(event["properties"], allow_nan=False),
                validated.canonical_json, validated.payload_hash, *record.position,
            ))
            return ProcessingResult("inserted", event_id)
        except EventValidationError as exc:
            self.conn.execute("""
                INSERT INTO rejected_events (
                    source_topic, source_partition, source_offset,
                    message_key, payload, error_code, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (*record.position, record.key, record.value, exc.code, str(exc)))
            return ProcessingResult("rejected", event_id, exc.code)
