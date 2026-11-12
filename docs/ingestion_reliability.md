# P0.2 — Reliable ingestion

## The idea in plain language

Think of Kafka as a numbered inbox and DuckDB as the record book. The consumer
first records what happened to a message, then tells Kafka where to resume next.
It must not mark a message as finished before its result is saved.

If the program stops after saving but before reporting progress, Kafka can send
the same message again. The database recognizes it and avoids another insertion.
This is **at-least-once consumption with idempotent persistence**: receiving a
message more than once is allowed; recording the same event twice is prevented.

## Processing order

1. Read one Kafka record, including its topic, partition, offset, key and bytes.
2. Start a DuckDB transaction.
3. Check whether this source position has already been processed.
4. For a new position, validate the payload and insert either a valid event or a
   rejection. An identical existing event ID needs no additional event row.
5. Record the source position and outcome in the same transaction.
6. Commit the DuckDB transaction.
7. Synchronously commit `offset + 1` to Kafka for that record's partition.

Both Kafka auto-commit and automatic offset storage are disabled. A storage,
polling or commit error stops the run. The consumer never processes a later
record after such an error, so it cannot skip past the failed record accidentally.
A commit timeout can have an uncertain broker outcome; restarting is safe whether
the broker recorded the offset or needs to deliver the message again.

## Database tables

| Table | Purpose |
|---|---|
| `raw_events` | One validated event per `event_id`; a primary key enforces uniqueness |
| `rejected_events` | Original key/payload bytes, source position, reason and rejection time |
| `processed_messages` | One durable outcome per `(topic, partition, offset)` |
| `pipeline_metadata` | Identifies the supported P0.2 schema |

`raw_events` retains the original eight analytics columns and adds the full
normalized JSON payload, its SHA-256 hash, source position and ingestion time.
Existing SQL selects named columns and remains compatible.

The source-position ledger also makes rejected-message replay idempotent. A hash
of the original key and payload detects a reused Kafka position with different
bytes. That condition stops processing instead of silently skipping new data.
Keep the source Kafka cluster/topic identity stable; do not delete and recreate a
topic under the same name while reusing its ingestion database.

Existing legacy databases are inspected with a read-only connection and refused
before a writable connection or Kafka consumer is opened. There is no migration
or backfill. Use a fresh database, topic and consumer group together for a new
dataset. For recovery, reuse all three from the interrupted run.

## Event contract

The payload must be UTF-8 JSON with an object at its top level. Null payloads
(Kafka tombstones), repeated JSON keys, NaN/Infinity and unsupported numeric or
Unicode values are rejected. These events are inserts; tombstones do not delete
banking records.

| Field | Rule |
|---|---|
| `event_id` | UUID, normalized to lowercase hyphenated form |
| `customer_id`, `session_id` | Nonblank strings without surrounding whitespace |
| `event_type` | One of the seven supported onboarding stages |
| `event_time` | ISO timestamp with a timezone, normalized to UTC |
| `channel`, `device_type` | `mobile_app` with `ios`/`android`; `web` with `desktop`; `branch` with `branch_terminal` |
| `properties` | JSON object with the required fields below |
| Kafka message key | Optional; when present, valid UTF-8 matching `customer_id` |

Required stage properties:

| Stage | Properties |
|---|---|
| `onboarding_started` | Nonblank `campaign_id`, `entry_point` |
| `profile_submitted` | Two-uppercase-letter `country`; nonblank `occupation_group` |
| `kyc_submitted` | Nonblank `kyc_method` |
| `kyc_verification_completed` | `status` is `passed` or `failed`; nonblank `review_type` |
| `account_opened` | UUID `account_id` |
| `card_activated` | UUID `card_id`; nonblank `card_type` |
| `first_transaction_completed` | UUID `transaction_id`; positive finite numeric `amount`; three-uppercase-letter `currency`; nonblank `transaction_type` |

Country/currency checks validate their format, not membership of an ISO list.
Extra fields are retained in the full payload. JSON key order, timezone spelling
for the same instant, and UUID spelling are normalized before hashing. If an
existing event ID arrives with different normalized content, the new record is
quarantined as `event_id_conflict`; the original event is preserved.

These are checks on individual events. A failed KYC result is a valid event; it
does not prove a successful onboarding journey. P0.4 checks cross-event stage
order and session consistency in temporary analytics models, and requires passed
KYC in its funnel. It reports problems without rewriting persisted events.
Future timestamps are allowed because the generator emits a simulated 20-minute
journey immediately. P0.3 adds controlled inputs and optional batch metadata;
see [reproducible_batches.md](reproducible_batches.md). Events without the new
metadata remain compatible. Batch fields are persisted inside the existing
full-payload JSON column, with no schema migration.

## Outcomes and counters

| Consumer outcome | Meaning |
|---|---|
| `inserted` | A new event ID was saved |
| `duplicate` | A new Kafka position contains an identical existing event |
| `rejected` | A new Kafka position was quarantined with its reason |
| `replayed` | This exact source position was already durably processed |
| `committed` | Kafka confirmed progress for this record; not a unique-event count |

On a successful run, `received = inserted + duplicate + rejected + replayed`
and `received = committed`. On a failed run, these may differ: for example, a
saved event can be counted as inserted while its Kafka commit is unconfirmed.

The producer separately reports `attempted`, `queued`, `acknowledged`, `failed`
and `pending`. It enables Kafka producer idempotence and `acks=all`, counts delivery
callbacks, bounds queue/flush waits, and exits nonzero when delivery is incomplete.
`flush()` returning zero alone is not sufficient: failed deliveries also leave
the queue. P0.3 saves batches before publishing and supports `--replay` of those
files with unchanged IDs and payloads. Running the producer with no batch options
creates a new run ID and a new synthetic population. To recover a delivery attempt,
replay the original batch directory instead.

## Failure behavior

| Situation | Durable result and next action |
|---|---|
| Invalid event | Save original bytes and reason, commit the rejection, then advance Kafka |
| Duplicate ID and identical normalized content | Preserve one event row, record the new source position, advance Kafka |
| Duplicate ID with different content | Preserve original event, quarantine the conflicting payload |
| Database or quarantine write fails | Roll back; do not attempt a Kafka commit; stop and retry after repair |
| Exit after DB commit, before Kafka commit | Resume the same group/database; recognize the redelivered source position |
| Kafka commit fails or is uncertain | Stop; the durable database result makes any replay safe |
| Same source position, changed bytes | Stop for investigation of source reuse/configuration |

Quarantine means invalid data has been saved for investigation, not repaired. No
automatic correction or redrive tool is included. A corrected event must be sent
as a new Kafka record, and an existing event's content cannot be overwritten.

## Verified locally on 2026-11-06

- 38 automated tests passed using isolated databases and mocked Kafka clients.
- A live single-partition Kafka run acknowledged 522 records: 512 unique events
  from 100 sessions, three duplicates and seven rejected records.
- A subprocess used `os._exit(73)` after its first database commit and before its
  first Kafka commit. The group had no committed offset; DuckDB recovered its one
  saved event and ledger row after reopening.
- The same group resumed: 511 new events, one replay, three duplicates and seven
  rejections; all 522 records had confirmed Kafka progress.
- A new group replayed all 522 positions against that database. Event, rejection
  and ledger counts were unchanged.
- A closed DuckDB connection caused a real connection error before progress was
  committed. Retrying with a healthy connection reconciled all records.
- SHA-256 hashes of all four historical databases remained unchanged.

Re-run the offline tests with `python -B -m unittest discover -s tests -v`, or
the live checks with `python -B scripts/verify_ingestion.py` using the project's
environment. The [README](../README.md) provides full PowerShell commands.
Live checks retain isolated databases and a JSON report under
`artifacts/verification/`; they do not reset historical offsets or data.

This evidence covers the listed local failure scenarios. It does not establish
broker high availability, machine/power-loss durability, disk-exhaustion recovery,
multi-consumer rebalance behavior or performance at scale. Kafka retention and
durable storage still matter. Analytics runs after ingestion closes the writer;
concurrent live reporting is outside the approved DuckDB MVP workflow.

API references: [Confluent Python client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)
and [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md).
