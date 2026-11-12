"""Verify BI handoff invariants; native Tableau rendering is verified separately."""

import csv
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import xml.etree.ElementTree as ET
from zipfile import ZipFile

from config import PROJECT_ROOT
from consumer.storage import EventStore, KafkaRecord
from producer.batch import load_batch
from run_analytics import analyze, export_snapshot
from scripts.build_tableau import build, prepare_data, sha256
from tableau.workbook import make_workbook


class TableauTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = tempfile.TemporaryDirectory()
        root = Path(cls.fixture.name)
        sample = load_batch(PROJECT_ROOT / "examples/p03/11111111-1111-4111-8111-111111111103")
        database = root / "sample.duckdb"
        with EventStore(database) as store:
            for offset, (key, payload) in enumerate(sample.messages):
                store.process(KafkaRecord("tableau-tests", 0, offset, payload, key))
        cls.snapshot = export_snapshot(analyze(database), root / "snapshot")
        empty = root / "empty.duckdb"
        with EventStore(empty):
            pass
        cls.empty_snapshot = export_snapshot(analyze(empty), root / "empty_snapshot")

    @classmethod
    def tearDownClass(cls):
        cls.fixture.cleanup()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def modified_snapshot(self, filename, change):
        destination = self.root / "modified"
        shutil.copytree(self.snapshot, destination)
        file = destination / filename
        with file.open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            fields, rows = reader.fieldnames, list(reader)
        change(rows)
        with file.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        manifest_file = destination / "manifest.json"
        manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
        for dataset in manifest["datasets"]:
            if dataset["file"] == filename:
                dataset.update(sha256=sha256(file), rows=len(rows))
        manifest_file.write_text(json.dumps(manifest), encoding="utf-8")
        return destination

    def test_metrics_remain_at_independent_grains_with_fractional_rates(self):
        data, provenance = prepare_data([self.snapshot])
        self.assertEqual(len(data["kpis"]), 1)
        self.assertEqual([r["sessions"] for r in data["funnel"]], [5, 4, 4, 2, 2, 2, 1])
        self.assertEqual(data["kpis"][0]["transaction_rate"], 0.2)
        self.assertEqual(data["kpis"][0]["transaction_display"], "20.0%")
        self.assertEqual(sum(r["first_transaction_sessions"] for r in data["channels"]), 1)
        self.assertIsNone(data["funnel"][0]["dropoff_rate"])
        self.assertEqual(provenance[0]["manifest_sha256"], sha256(self.snapshot / "manifest.json"))

    def test_portable_package_has_relative_sources_and_one_parameter_for_every_sheet(self):
        directory = build([self.snapshot], self.root / "dashboard")
        with ZipFile(directory / "Digital Banking Onboarding.twbx") as archive:
            self.assertEqual(set(archive.namelist()), {"Digital Banking Onboarding.twb", "Data/kpis.csv", "Data/funnel.csv", "Data/channels.csv"})
            root = ET.fromstring(archive.read("Digital Banking Onboarding.twb"))
            self.assertFalse(root.findall(".//relation[@type='join']"))
            for connection in root.findall("datasources/datasource/connection"):
                self.assertEqual(connection.get("directory"), "Data")
                self.assertIn("Data/" + connection.get("filename"), archive.namelist())
            sheets = root.findall("worksheets/worksheet")
            self.assertEqual(len(sheets), 10)
            for sheet in sheets:
                self.assertIsNotNone(sheet.find(".//datasource-dependencies[@datasource='Parameters']/column[@name='[Batch]']"))
                self.assertTrue(any("Selected batch" in f.get("column", "") for f in sheet.findall(".//filter")))
        self.assertEqual(json.loads((directory / "manifest.json").read_text())["percentage_scale"],
                         "0 to 1; formatted as percentages in Tableau")

    def test_duplicate_run_is_refused_before_any_output_is_created(self):
        destination = self.root / "dashboard"
        with self.assertRaisesRegex(ValueError, "multiple snapshots"):
            build([self.snapshot, self.snapshot], destination)
        self.assertFalse(destination.exists())

    def test_format_attribute_names_match_the_native_tableau_vocabulary(self):
        # These misspellings passed generic XML parsing but Tableau rejected the
        # entire workbook with D2E8DA72. Reference: bundled 2026.1 sample TWBs.
        data, _ = prepare_data([self.snapshot])
        root = ET.fromstring(make_workbook(data, data["kpis"][0]["dataset_label"]))
        attributes = {node.get("attr") for node in root.findall(".//format")}
        self.assertFalse(attributes & {"font-color", "mark-size"})
        self.assertIsNotNone(root.find(".//style-rule[@element='cell']/format[@attr='color']"))
        self.assertIsNotNone(root.find(".//style-rule[@element='mark']/format[@attr='size']"))
        self.assertNotIn("source-width", root.find("windows").attrib)

    def test_corrupted_csv_is_refused_before_any_output_is_created(self):
        snapshot = self.root / "snapshot"
        shutil.copytree(self.snapshot, snapshot)
        with (snapshot / "04_onboarding_kpis.csv").open("a", encoding="utf-8") as target:
            target.write("tampered\n")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            build([snapshot], self.root / "dashboard")
        self.assertFalse((self.root / "dashboard").exists())

    def test_resigned_but_inconsistent_rate_is_refused(self):
        snapshot = self.modified_snapshot("04_onboarding_kpis.csv",
            lambda rows: rows[0].update(first_transaction_conversion_pct="99.9"))
        with self.assertRaisesRegex(ValueError, "Rate/denominator mismatch"):
            prepare_data([snapshot])

    def test_quality_failure_cannot_be_shown_as_pass_even_if_manifest_claims_pass(self):
        snapshot = self.modified_snapshot("01_data_quality_checks.csv",
            lambda rows: next(r for r in rows if r["severity"] == "error").update(affected_count="1", status="FAIL"))
        with self.assertRaisesRegex(ValueError, "Quality check failed"):
            prepare_data([snapshot])

    def test_empty_cohort_preserves_unavailable_rates_and_quality_warning(self):
        data, _ = prepare_data([self.empty_snapshot])
        kpi = data["kpis"][0]
        self.assertEqual(kpi["started_sessions"], 0)
        self.assertIsNone(kpi["transaction_rate"])
        self.assertEqual(kpi["transaction_display"], "N/A")
        self.assertTrue(kpi["quality_status"].startswith("WARN"))
        self.assertTrue(all(row["transaction_rate"] is None for row in data["channels"]))

    def test_build_preserves_input_and_refuses_existing_output(self):
        before = {p.name: sha256(p) for p in self.snapshot.iterdir()}
        destination = build([self.snapshot], self.root / "dashboard")
        package_before = sha256(destination / "Digital Banking Onboarding.twbx")
        with self.assertRaisesRegex(ValueError, "already exists"):
            build([self.snapshot], destination)
        self.assertEqual(package_before, sha256(destination / "Digital Banking Onboarding.twbx"))
        self.assertEqual(before, {p.name: sha256(p) for p in self.snapshot.iterdir()})


if __name__ == "__main__":
    unittest.main()
