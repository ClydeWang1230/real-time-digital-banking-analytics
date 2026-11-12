"""Hand-built journeys exercise metric semantics independently of the generator."""

import copy
import csv
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from uuid import uuid4

import duckdb

from consumer.validation import EVENT_TYPES
from run_analytics import analyze, export_snapshot, main
from tests.helpers import make_event


RUN_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
RUN_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
BASE = datetime(2026, 11, 8, tzinfo=timezone.utc)


def journey(session, *, count=7, run_id=RUN_A, channel="web", equal_times=False):
    events = []
    for stage, minutes in zip(EVENT_TYPES[:count], [0, 3, 5, 10, 12, 15, 20]):
        event = make_event(stage)
        event.update(session_id=session, customer_id="customer_" + session, channel=channel,
                     device_type={"web": "desktop", "mobile_app": "ios", "branch": "branch_terminal"}[channel],
                     event_time=(BASE + timedelta(minutes=0 if equal_times else minutes)).isoformat())
        if run_id is not None:
            event.update(run_id=run_id, generator_version="onboarding-v1", simulation_seed=42,
                         simulation_base_time=BASE.isoformat())
        events.append(event)
    return events


def write_fixture(path, events, *, with_payload=True, with_health=False):
    with duckdb.connect(str(path)) as connection:
        connection.execute("""CREATE TABLE raw_events (
            event_id VARCHAR, customer_id VARCHAR, session_id VARCHAR, event_type VARCHAR,
            event_time VARCHAR, channel VARCHAR, device_type VARCHAR, properties VARCHAR
        )""")
        if with_payload:
            connection.execute("ALTER TABLE raw_events ADD COLUMN payload VARCHAR")
        for event in events:
            values = [event.get(k) for k in ("event_id", "customer_id", "session_id", "event_type",
                                           "event_time", "channel", "device_type")]
            values.append(event.get("_properties_text", json.dumps(event.get("properties"))))
            if with_payload:
                values.append(event.get("_payload_text", json.dumps(event)))
            connection.execute("INSERT INTO raw_events VALUES (" + ",".join("?" for _ in values) + ")", values)
        if with_health:
            connection.execute("CREATE TABLE rejected_events AS SELECT 'bad bytes' AS reason")
            connection.execute("CREATE TABLE processed_messages AS SELECT 'rejected' AS outcome")


def rows(result, name):
    report = result.reports[name]
    return [dict(zip(report["columns"], row)) for row in report["rows"]]


def funnel(result):
    return [row["sessions"] for row in rows(result, "03_onboarding_funnel")]


def quality(result):
    return {row["check_name"]: row["affected_count"] for row in rows(result, "01_data_quality_checks")}


class AnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "analytics.duckdb"

    def run_events(self, events, **options):
        write_fixture(self.path, events, **options)
        return analyze(self.path)

    def test_complete_and_dropout_journeys_reconcile_with_channels(self):
        result = self.run_events(list(reversed(journey("full"))) + journey("drop", count=2, channel="branch"))
        self.assertEqual(funnel(result), [2, 2, 1, 1, 1, 1, 1])
        self.assertEqual(result.error_checks, 0)
        kpi = rows(result, "04_onboarding_kpis")[0]
        self.assertEqual(kpi["first_transaction_conversion_pct"], 50.0)
        channels = rows(result, "05_channel_performance")
        for key in ("started_sessions", "account_opened_sessions", "first_transaction_sessions"):
            self.assertEqual(sum(row[key] for row in channels), kpi[key])
        self.assertIsNone(next(row for row in channels if row["channel"] == "mobile_app")["first_transaction_conversion_pct"])

    def test_failed_kyc_cannot_qualify_an_account_opening(self):
        events = journey("failed", count=5)
        events[3]["properties"]["status"] = "failed"
        result = self.run_events(events)
        self.assertEqual(funnel(result), [1, 1, 1, 0, 0, 0, 0])
        self.assertEqual(rows(result, "04_onboarding_kpis")[0]["sessions_with_kyc_failure"], 1)
        self.assertEqual(quality(result)["stage_order_violations"], 1)

    def test_failed_then_passed_kyc_can_progress_on_a_later_pass(self):
        events = journey("retry")
        failed = copy.deepcopy(events[3])
        failed.update(event_id=str(uuid4()), event_time=(BASE + timedelta(minutes=8)).isoformat())
        failed["properties"]["status"] = "failed"
        result = self.run_events(events + [failed])
        self.assertEqual(funnel(result), [1]*7)
        self.assertEqual(result.error_checks, 0)
        self.assertEqual(quality(result)["repeated_stage_occurrences"], 1)

    def test_event_time_order_controls_progress_not_arrival_order(self):
        events = journey("bad-order")
        events[4]["event_time"] = (BASE + timedelta(minutes=9)).isoformat()
        result = self.run_events(events)
        self.assertEqual(funnel(result), [1, 1, 1, 1, 0, 0, 0])
        self.assertEqual(quality(result)["stage_order_violations"], 3)

    def test_early_stage_retry_does_not_hide_later_valid_occurrence(self):
        events = journey("early")
        early = copy.deepcopy(events[1])
        early.update(event_id=str(uuid4()), event_time=(BASE - timedelta(minutes=1)).isoformat())
        result = self.run_events([early] + events)
        self.assertEqual(funnel(result), [1]*7)
        self.assertEqual(quality(result)["stage_order_violations"], 1)
        self.assertEqual(rows(result, "06_session_journeys")[0]["profile_at"], BASE + timedelta(minutes=3))

    def test_equal_timestamps_are_allowed(self):
        result = self.run_events(journey("ties", equal_times=True))
        self.assertEqual(funnel(result), [1]*7)
        self.assertEqual(rows(result, "04_onboarding_kpis")[0]["avg_minutes_to_first_transaction"], 0)
        self.assertEqual(result.error_checks, 0)

    def test_missing_prior_stage_and_orphan_never_inflate_later_counts(self):
        missing = journey("missing")
        del missing[1]
        orphan = journey("orphan")[1:]
        result = self.run_events(missing + orphan)
        self.assertEqual(funnel(result), [1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(quality(result)["orphan_sessions"], 1)
        self.assertEqual(rows(result, "04_onboarding_kpis")[0]["first_transaction_conversion_pct"], 0)

    def test_identical_raw_duplicates_do_not_increase_funnel_counts(self):
        events = journey("dup")
        result = self.run_events(events + [copy.deepcopy(events[0])])
        self.assertEqual(funnel(result), [1]*7)
        self.assertEqual(quality(result)["duplicate_event_rows"], 1)
        self.assertEqual(result.error_checks, 0)

    def test_conflicting_event_ids_exclude_affected_sessions(self):
        events = journey("conflict")
        changed = copy.deepcopy(events[1])
        changed["properties"]["country"] = "GB"
        result = self.run_events(events + [changed])
        self.assertEqual(funnel(result), [0]*7)
        self.assertEqual(quality(result)["conflicting_event_ids"], 1)
        self.assertEqual(rows(result, "06_session_journeys")[0]["journey_status"], "excluded")

    def test_mixed_customer_channel_and_metadata_are_reported_as_exclusions(self):
        first, second, third = journey("identity"), journey("channel"), journey("metadata")
        first[1]["customer_id"] = "someone_else"
        second[1].update(channel="branch", device_type="branch_terminal")
        third[1]["simulation_seed"] = 7
        result = self.run_events(first + second + third)
        self.assertEqual(funnel(result), [0]*7)
        self.assertEqual(quality(result)["inconsistent_sessions"], 3)
        self.assertEqual(quality(result)["conflicting_run_metadata"], 1)

    def test_bad_fields_and_json_are_reported_without_crashing(self):
        events = []
        for index, change in enumerate(({"event_time": "bad"}, {"event_time": "infinity"},
                                       {"customer_id": None}, {"event_id": None}, {"session_id": None},
                                       {"_properties_text": "{broken"}, {"_payload_text": "broken"})):
            event = journey(str(index), count=1)[0]
            event.update(change)
            events.append(event)
        result = self.run_events(events)
        self.assertTrue(result.error_checks)
        reports = rows(result, "01_data_quality_checks")
        self.assertEqual(sum(r["affected_count"] for r in reports if r["check_name"] == "invalid_event_times"), 2)
        self.assertTrue(all(value == 0 for value in funnel(result)))

    def test_run_selection_and_legacy_never_merge_same_session_ids(self):
        result = self.run_events(journey("same", run_id=RUN_A) + journey("same", run_id=RUN_B, count=2)
                                 + journey("same", run_id=None, count=1))
        self.assertEqual(len(rows(result, "06_session_journeys")), 3)
        self.assertEqual(funnel(analyze(self.path, run_id=RUN_A.upper())), [1]*7)
        self.assertEqual(funnel(analyze(self.path, run_id=RUN_B)), [1, 1, 0, 0, 0, 0, 0])
        self.assertEqual(funnel(analyze(self.path, legacy_only=True)), [1, 0, 0, 0, 0, 0, 0])
        self.assertEqual(len(rows(analyze(self.path, list_runs=True), "available_runs")), 3)
        with self.assertRaisesRegex(ValueError, "not found"):
            analyze(self.path, run_id=str(uuid4()))
        with self.assertRaises(ValueError):
            analyze(self.path, run_id="' OR 1=1 --")

    def test_naive_times_and_non_string_properties_are_not_silently_coerced(self):
        naive = journey('naive', count=1)[0]
        naive['event_time'] = '2026-11-08T00:00:00'
        date_only = journey('date-only', count=1)[0]
        date_only['event_time'] = '2026-11-08'
        numeric = journey('numeric', count=1)[0]
        numeric['properties']['campaign_id'] = 123
        result = self.run_events([naive, date_only, numeric])
        self.assertEqual(quality(result)['invalid_event_times'], 2)
        self.assertEqual(quality(result)['invalid_event_fields'], 3)
        self.assertEqual(funnel(result), [0]*7)

    def test_csv_preserves_unicode_and_quoted_fields(self):
        events = journey('客户,"one"', count=1)
        result = self.run_events(events)
        directory = export_snapshot(result, Path(self.temp.name) / 'unicode-export')
        with (directory / '06_session_journeys.csv').open(encoding='utf-8', newline='') as source:
            record = next(csv.DictReader(source))
        self.assertEqual(record['session_id'], events[0]['session_id'])

    def test_cross_run_event_id_conflicts_remain_visible_when_filtering(self):
        first, second = journey("one", count=1), journey("two", count=1, run_id=RUN_B)
        second[0]["event_id"] = first[0]["event_id"]
        self.run_events(first + second)
        self.assertEqual(quality(analyze(self.path, run_id=RUN_A))["conflicting_event_ids"], 1)
        self.assertEqual(funnel(analyze(self.path, run_id=RUN_A)), [0]*7)

    def test_empty_legacy_database_has_zero_counts_and_null_rates(self):
        result = self.run_events([], with_payload=False)
        self.assertEqual(funnel(result), [0]*7)
        self.assertTrue(all(r["conversion_from_start_pct"] is None for r in rows(result, "03_onboarding_funnel")))
        self.assertIsNone(rows(result, "04_onboarding_kpis")[0]["first_transaction_conversion_pct"])
        self.assertEqual(result.error_checks, 0)
        unavailable = [r for r in rows(result, "07_ingestion_health") if not r["available"]]
        self.assertEqual(len(unavailable), 3)

    def test_ingestion_health_is_explicitly_database_wide(self):
        self.run_events(journey("one") + journey("two", run_id=RUN_B), with_health=True)
        health = rows(analyze(self.path, run_id=RUN_A), "07_ingestion_health")
        self.assertEqual({r["scope"] for r in health}, {"database"})
        self.assertEqual(next(r["value"] for r in health if r["metric"] == "raw_event_rows"), 14)
        self.assertEqual(next(r["value"] for r in health if r["metric"] == "rejected_messages"), 1)

    def test_durations_use_only_completed_stages(self):
        first, second = journey("complete"), journey("account-only", count=5)
        second[4]["event_time"] = (BASE + timedelta(minutes=20)).isoformat()
        result = self.run_events(first + second)
        kpi = rows(result, "04_onboarding_kpis")[0]
        self.assertEqual(kpi["avg_minutes_to_account_opened"], 16)
        self.assertEqual(kpi["avg_minutes_to_first_transaction"], 20)
        self.assertEqual(kpi["account_to_first_transaction_pct"], 50)

    def test_exports_reconcile_and_preserve_source_and_existing_outputs(self):
        write_fixture(self.path, journey("good", count=1, run_id=None), with_payload=False)
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        result = analyze(self.path)
        first = export_snapshot(result, Path(self.temp.name) / "export1")
        second = export_snapshot(result, Path(self.temp.name) / "export2")
        manifest = json.loads((first / "manifest.json").read_text())
        self.assertEqual(len(manifest["datasets"]), 7)
        self.assertTrue(manifest["quality_passed"])
        for dataset in manifest["datasets"]:
            path = first / dataset["file"]
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), dataset["sha256"])
            self.assertEqual(path.read_bytes(), (second / dataset["file"]).read_bytes())
            with path.open(encoding="utf-8", newline="") as source:
                self.assertEqual(len(list(csv.DictReader(source))), dataset["rows"])
        with (first / "06_session_journeys.csv").open(encoding="utf-8", newline="") as source:
            record = next(csv.DictReader(source))
            self.assertEqual(record["started_at"], BASE.isoformat())
            self.assertEqual(record["first_transaction_at"], "")
        with self.assertRaises(FileExistsError):
            export_snapshot(result, first)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        with duckdb.connect(str(self.path), read_only=True) as connection:
            self.assertEqual(connection.execute("SHOW TABLES").fetchall(), [("raw_events",)])

    def test_strict_mode_exports_failure_evidence_and_returns_two(self):
        write_fixture(self.path, journey("orphan")[1:])
        destination = Path(self.temp.name) / "failed-export"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(db_path=self.path, strict=True, export_dir=destination), 2)
        self.assertFalse(json.loads((destination / "manifest.json").read_text())["quality_passed"])
