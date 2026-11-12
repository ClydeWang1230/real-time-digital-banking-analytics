from contextlib import redirect_stdout
from datetime import datetime, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from uuid import UUID

from config import PROJECT_ROOT, load_settings
from consumer.storage import EventStore, KafkaRecord
from consumer.validation import EventValidationError, validate_event
from producer.batch import encode_event, load_batch, prepare_batch
from producer.event_generator import SimulationSpec, create_customer_context, create_journey, generate_events
import producer.producer as publishing
from tests.helpers import FakeProducer, make_event


RUN_ID = "11111111-1111-4111-8111-111111111103"
BASE_TIME = "2026-11-08T00:00:00+00:00"


def sample_spec(**overrides):
    values = {"customer_count": 5, "run_id": RUN_ID, "seed": 42, "base_time": BASE_TIME}
    values.update(overrides)
    return SimulationSpec.create(**values)


class SimulationTests(unittest.TestCase):
    def test_all_events_ids_times_and_bytes_are_repeatable_without_global_random_effects(self):
        before = random.getstate()
        first = [encode_event(event) for event in generate_events(sample_spec())]
        self.assertEqual(random.getstate(), before)
        self.assertEqual(first, [encode_event(event) for event in generate_events(sample_spec())])
        self.assertTrue(first)

    def test_equivalent_timezones_produce_identical_batches(self):
        self.assertEqual(list(generate_events(sample_spec())), list(generate_events(
            sample_spec(base_time="2026-11-08T08:00:00+08:00"))))

    def test_new_run_has_distinct_customers_sessions_events_and_business_ids(self):
        first = list(generate_events(sample_spec()))
        second = list(generate_events(sample_spec(run_id="22222222-2222-4222-8222-222222222203")))
        for field in ("event_id", "customer_id", "session_id"):
            self.assertTrue({e[field] for e in first}.isdisjoint(e[field] for e in second))
        for field in ("account_id", "card_id", "transaction_id"):
            self.assertTrue({e["properties"][field] for e in first if field in e["properties"]}.isdisjoint(
                e["properties"][field] for e in second if field in e["properties"]))
        self.assertEqual([(e["channel"], e["event_type"], e["event_time"]) for e in first],
                         [(e["channel"], e["event_type"], e["event_time"]) for e in second])

    def test_journeys_are_valid_ordered_prefixes_with_one_session_per_customer(self):
        from consumer.validation import EVENT_TYPES
        events = list(generate_events(sample_spec(customer_count=100)))
        sessions = {}
        for event in events:
            validate_event(encode_event(event), event["customer_id"].encode())
            self.assertEqual(UUID(event["event_id"]).version, 5)
            sessions.setdefault(event["session_id"], []).append(event)
        self.assertEqual(len(sessions), 100)
        self.assertEqual(len({e["customer_id"] for e in events}), 100)
        for index, journey in enumerate(sessions.values()):
            self.assertEqual([e["event_type"] for e in journey], list(EVENT_TYPES[:len(journey)]))
            times = [datetime.fromisoformat(e["event_time"]) for e in journey]
            self.assertEqual(times, sorted(times))
            self.assertEqual(times[0], datetime.fromisoformat(BASE_TIME) + timedelta(minutes=index))
            self.assertEqual(len({e["customer_id"] for e in journey}), 1)

    def test_invalid_simulation_inputs_are_rejected(self):
        for inputs in ({"run_id": "bad"}, {"seed": -1}, {"seed": True}, {"seed": 2**63},
                       {"customer_count": 0}, {"customer_count": True},
                       {"base_time": "2026-11-08T00:00:00"}, {"base_time": "bad"}):
            with self.subTest(inputs=inputs), self.assertRaises(ValueError):
                sample_spec(**inputs)

    def test_legacy_helpers_and_events_still_validate(self):
        context = create_customer_context(1)
        for event in create_journey(context):
            self.assertNotIn("run_id", event)
            validate_event(encode_event(event))

    def test_partial_or_invalid_batch_metadata_is_rejected(self):
        event = next(generate_events(sample_spec()))
        for field, value in (("run_id", "bad"), ("simulation_seed", True),
                             ("simulation_seed", -1), ("simulation_base_time", "2026-11-08"),
                             ("generator_version", "")):
            with self.subTest(field=field, value=value), self.assertRaises(EventValidationError):
                validate_event(encode_event({**event, field: value}))
        del event["simulation_seed"]
        with self.assertRaisesRegex(EventValidationError, "All four"):
            validate_event(encode_event(event))
        validate_event(encode_event(make_event()))


class BatchTests(unittest.TestCase):
    def test_checked_in_sample_is_valid_and_reproducible(self):
        saved = load_batch(PROJECT_ROOT / "examples" / "p03" / RUN_ID)
        with tempfile.TemporaryDirectory() as directory:
            regenerated = prepare_batch(sample_spec(), directory)
            for name in ("events.jsonl", "manifest.json"):
                self.assertEqual((saved.directory / name).read_bytes(), (regenerated.directory / name).read_bytes())

    def test_prepare_reuses_identical_batch_without_rewriting_files(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = prepare_batch(sample_spec(), directory)
            before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in batch.directory.iterdir()}
            repeated = prepare_batch(sample_spec(), directory)
            self.assertEqual(batch.messages, repeated.messages)
            self.assertEqual(before, {p.name: (p.read_bytes(), p.stat().st_mtime_ns)
                                      for p in batch.directory.iterdir()})

    def test_changed_inputs_cannot_overwrite_same_run(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = prepare_batch(sample_spec(), directory)
            before = (batch.directory / "events.jsonl").read_bytes()
            for change in ({"seed": 7}, {"base_time": "2026-11-09T00:00:00Z"}, {"customer_count": 6}):
                with self.subTest(change=change), self.assertRaisesRegex(ValueError, "different inputs"):
                    prepare_batch(sample_spec(**change), directory)
            self.assertEqual((batch.directory / "events.jsonl").read_bytes(), before)

    def test_corrupt_replay_never_creates_a_producer_or_publishes_valid_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = prepare_batch(sample_spec(), directory)
            data_path = batch.directory / "events.jsonl"
            data = data_path.read_bytes() + b"{broken\n"
            data_path.write_bytes(data)
            for update_checksum in (False, True):
                if update_checksum:
                    manifest = dict(batch.manifest, events_sha256=hashlib.sha256(data).hexdigest())
                    (batch.directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.subTest(update_checksum=update_checksum), patch.object(publishing, "Producer") as producer, \
                     self.assertLogs(publishing.LOGGER, level="ERROR"), redirect_stdout(io.StringIO()):
                    self.assertEqual(publishing.main(replay=batch.directory), 1)
                producer.assert_not_called()

    def test_manifest_count_and_metadata_are_checked(self):
        with tempfile.TemporaryDirectory() as directory:
            batch = prepare_batch(sample_spec(), directory)
            for manifest in (
                dict(batch.manifest, event_count=batch.manifest["event_count"] + 1),
                dict(batch.manifest, simulation={**batch.manifest["simulation"], "seed": 7}),
                dict(batch.manifest, simulation={"run_id": RUN_ID}),
            ):
                (batch.directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_batch(batch.directory)

    def test_prepare_only_never_connects_to_kafka_and_replay_keeps_exact_bytes(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(publishing, "load_settings", return_value=load_settings({})):
            with patch.object(publishing, "Producer") as producer, redirect_stdout(io.StringIO()):
                self.assertEqual(publishing.main(run_id=RUN_ID, seed=42, base_time=BASE_TIME,
                                                customer_count=5, output_dir=directory, prepare_only=True), 0)
            producer.assert_not_called()
            batch = load_batch(Path(directory) / RUN_ID)
            fake = FakeProducer()
            with patch.object(publishing, "Producer", return_value=fake), redirect_stdout(io.StringIO()):
                self.assertEqual(publishing.main(replay=batch.directory), 0)
            self.assertEqual([(key, value) for _, key, value in fake.messages], batch.messages)

    def test_replay_rejects_generation_options_before_connecting(self):
        with patch.object(publishing, "Producer") as producer, self.assertLogs(publishing.LOGGER, level="ERROR"), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(publishing.main(replay="unused", seed=42), 1)
        producer.assert_not_called()

    def test_batch_replay_at_new_offsets_is_idempotent_and_new_run_is_distinct(self):
        with tempfile.TemporaryDirectory() as directory, EventStore(":memory:") as store:
            first = prepare_batch(sample_spec(), directory)
            second = prepare_batch(sample_spec(run_id="22222222-2222-4222-8222-222222222203"), directory)
            offset = 0
            for batch, expected in ((first, "inserted"), (load_batch(first.directory), "duplicate"), (second, "inserted")):
                for key, payload in batch.messages:
                    result = store.process(KafkaRecord("p03-test", 0, offset, payload, key))
                    self.assertEqual(result.outcome, expected)
                    offset += 1
            groups = store.conn.execute("""
                SELECT payload->>'run_id', COUNT(*), COUNT(DISTINCT customer_id), COUNT(DISTINCT session_id)
                FROM raw_events GROUP BY 1 ORDER BY 1
            """).fetchall()
            self.assertEqual(groups, [(RUN_ID, len(first.messages), 5, 5),
                                     (second.manifest["simulation"]["run_id"], len(second.messages), 5, 5)])
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM rejected_events").fetchone()[0], 0)

    def test_fresh_process_reproduces_the_same_bytes_from_unrelated_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            expected = prepare_batch(sample_spec(), Path(directory) / "expected")
            destination = Path(directory) / "actual"
            env = {k: v for k, v in os.environ.items() if not k.startswith("BANKING_")}
            env["PYTHONHASHSEED"] = "991"
            result = subprocess.run([
                sys.executable, "-B", str(PROJECT_ROOT / "producer" / "producer.py"),
                "--prepare-only", "--run-id", RUN_ID, "--seed", "42", "--base-time", BASE_TIME,
                "--customer-count", "5", "--output-dir", str(destination),
            ], cwd=directory, env=env, capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            for name in ("events.jsonl", "manifest.json"):
                self.assertEqual((expected.directory / name).read_bytes(), (destination / RUN_ID / name).read_bytes())
