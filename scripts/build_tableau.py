"""Build a portable Tableau workbook from verified P0.4 snapshots; never opens a database."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PROJECT_ROOT
from tableau.workbook import make_workbook


STAGES = ["Start onboarding", "Submit profile", "Submit KYC", "Pass KYC",
          "Open account", "Activate card", "First transaction"]
REPORTS = ("01_data_quality_checks", "03_onboarding_funnel", "04_onboarding_kpis",
           "05_channel_performance")
CHANNELS = {"mobile_app": "Mobile app", "web": "Web", "branch": "Branch"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def project_path(path):
    path = Path(path).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


def load_snapshot(directory):
    directory = project_path(directory)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    require(manifest.get("analytics_version") == "onboarding-analytics-v1", "Unsupported analytics version")
    require(manifest.get("quality_passed") is True and manifest.get("error_checks") == 0,
            "Resolve error-level quality failures before building a showcase dashboard")
    datasets = {item["file"]: item for item in manifest["datasets"]}
    require(len(datasets) == len(manifest["datasets"]), "Duplicate manifest dataset")
    result = {}
    for name in REPORTS:
        filename = name + ".csv"
        require(filename in datasets, f"Missing report: {filename}")
        path = directory / filename
        item = datasets[filename]
        require(sha256(path) == item["sha256"], f"Snapshot checksum mismatch: {path}")
        with path.open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            require(reader.fieldnames == list(item["columns"]), f"Unexpected columns: {path}")
            result[name] = list(reader)
        require(len(result[name]) == item["rows"], f"Snapshot row-count mismatch: {path}")
    return result, {"snapshot": str(directory), "manifest_sha256": sha256(manifest_path),
                    "analytics_manifest": manifest}


def number(row, field, nullable=False):
    value = row[field]
    if value == "" and nullable:
        return None
    value = float(value)
    require(math.isfinite(value), f"Nonfinite value: {field}")
    return value


def count(row, field):
    value = number(row, field)
    require(value >= 0 and value.is_integer(), f"Invalid count: {field}")
    return int(value)


def ratio(numerator, denominator):
    return numerator / denominator if denominator else None


def percent(value):
    return "N/A" if value is None else f"{value:.1%}"


def check_rate(row, field, numerator, denominator):
    observed = number(row, field, nullable=True)
    expected = ratio(numerator, denominator)
    require((expected is None and observed is None) or
            (expected is not None and observed is not None and abs(observed - 100 * expected) <= 0.051),
            f"Rate/denominator mismatch: {field}")


def prepare_data(snapshots, labels=None):
    """Retain three independent grains; rates come from counts, never averages of rates."""
    labels = labels or {}
    output = {"kpis": [], "funnel": [], "channels": []}
    provenance, seen_runs, seen_labels = [], set(), set()
    for snapshot in snapshots:
        reports, evidence = load_snapshot(snapshot)
        provenance.append(evidence)
        for kpi in reports["04_onboarding_kpis"]:
            run_id = kpi["run_id"]
            require(run_id not in seen_runs, f"Run appears in multiple snapshots: {run_id}")
            label = labels.get(run_id, "Legacy snapshot" if run_id == "legacy" else "Run " + run_id[:8])
            require(label and label not in seen_labels, "Dataset labels must be nonempty and unique")
            seen_runs.add(run_id)
            seen_labels.add(label)
            base = {"dataset_label": label, "run_id": run_id}
            funnel = sorted((r for r in reports["03_onboarding_funnel"] if r["run_id"] == run_id),
                            key=lambda r: int(r["step"]))
            require([int(r["step"]) for r in funnel] == list(range(1, 8)), "Expected seven ordered funnel stages")
            counts = [count(r, "sessions") for r in funnel]
            require(counts == sorted(counts, reverse=True), "Funnel is not monotonic")
            started, opened, transacted = (count(kpi, f) for f in
                                          ("started_sessions", "account_opened_sessions", "first_transaction_sessions"))
            require([counts[0], counts[4], counts[6]] == [started, opened, transacted], "KPI/funnel counts differ")
            for field, numerator, denominator in (
                ("account_opening_conversion_pct", opened, started),
                ("first_transaction_conversion_pct", transacted, started),
                ("account_to_first_transaction_pct", transacted, opened),
            ):
                check_rate(kpi, field, numerator, denominator)
            quality = [r for r in reports["01_data_quality_checks"] if r["run_id"] == run_id]
            require(len(quality) == 15 and len({r["check_name"] for r in quality}) == 15, "Expected 15 distinct quality checks")
            for row in quality:
                affected = count(row, "affected_count")
                require(row["severity"] in ("error", "warning"), "Unknown quality severity")
                expected = "PASS" if affected == 0 else "FAIL" if row["severity"] == "error" else "WARN"
                require(row["status"] == expected and expected != "FAIL", "Quality check failed or has inconsistent status")
            warnings = sum(r["status"] == "WARN" for r in quality)
            quality_status = f"WARN: {warnings} checks" if warnings else "PASS: 15 checks"
            for i, row in enumerate(funnel):
                previous = counts[i - 1] if i else None
                loss = previous - counts[i] if i else None
                require(number(row, "previous_sessions", nullable=True) == previous, "Previous-stage count mismatch")
                require(number(row, "dropoff_sessions", nullable=True) == loss, "Drop-off count mismatch")
                check_rate(row, "conversion_from_start_pct", counts[i], started)
                output["funnel"].append({**base, "step": i + 1,
                    "stage_display": f"{i+1:02d}  {STAGES[i]}", "sessions": counts[i],
                    "from_start_rate": ratio(counts[i], started),
                    "reach_label": f"{counts[i]}   |   {percent(ratio(counts[i], started))}",
                    "transition_display": f"{i:02d}  {STAGES[i-1]} > {STAGES[i]}" if i else "Start",
                    "previous_sessions": previous, "dropoff_sessions": loss,
                    "dropoff_rate": ratio(loss, previous) if i else None,
                    "loss_label": f"{loss}   |   {percent(ratio(loss, previous))}" if i else "",
                })
            channels = [r for r in reports["05_channel_performance"] if r["run_id"] == run_id]
            require(len(channels) == 3 and {r["channel"] for r in channels} == set(CHANNELS), "Unexpected channels")
            for field, total in (("started_sessions", started), ("account_opened_sessions", opened),
                                 ("first_transaction_sessions", transacted)):
                require(sum(count(r, field) for r in channels) == total, f"Channel total mismatch: {field}")
            for row in channels:
                n, a, t = (count(row, f) for f in ("started_sessions", "account_opened_sessions", "first_transaction_sessions"))
                require(0 <= t <= a <= n, "Invalid channel progression")
                check_rate(row, "first_transaction_conversion_pct", t, n)
                output["channels"].append({**base, "channel": CHANNELS[row["channel"]],
                    "started_sessions": n, "account_opened_sessions": a, "first_transaction_sessions": t,
                    "transaction_rate": ratio(t, n), "channel_label": f"{percent(ratio(t,n))}   |   {t} / {n}",
                })
            losses = [counts[i-1] - counts[i] for i in range(1,7)]
            peak = max(range(1,7), key=lambda i: losses[i-1])
            require(count(kpi, "not_completed_sessions") == started-transacted, "Incomplete-session count mismatch")
            insight = (f"Largest observed step loss: {STAGES[peak-1]} > {STAGES[peak]}.\n"
                       f"{losses[peak-1]} sessions did not progress ({percent(ratio(losses[peak-1], counts[peak-1]))} of the previous stage)."
                       if started else "No eligible onboarding starts in this snapshot; conversion rates are unavailable.")
            output["kpis"].append({**base, "started_sessions": started, "account_opened_sessions": opened,
                "first_transaction_sessions": transacted, "not_completed_sessions": started-transacted,
                "account_rate": ratio(opened, started), "transaction_rate": ratio(transacted, started),
                "account_display": percent(ratio(opened, started)),
                "transaction_display": percent(ratio(transacted, started)),
                "started_detail": f"{count(kpi, 'event_rows'):,} recorded events",
                "account_detail": f"{opened} of {started} started sessions",
                "transaction_detail": f"{transacted} of {started} started sessions",
                "quality_status": quality_status, "quality_warnings": warnings,
                "context_label": f"{label}  |  {count(kpi, 'event_rows'):,} events  |  Run: {run_id}",
                "quality_label": f"Data quality  {quality_status}  |  Excluded sessions: {count(kpi,'excluded_sessions')}  |  Orphans: {count(kpi,'orphan_sessions')}",
                "insight": insight,
            })
        expected_runs = {r["run_id"] for r in reports["04_onboarding_kpis"]}
        for name, rows in reports.items():
            require({r["run_id"] for r in rows} == expected_runs, f"Inconsistent run coverage: {name}")
    require(output["kpis"], "At least one snapshot is required")
    return output, provenance


def build(snapshots, output_dir, *, labels=None, default_run_id=None):
    output_dir = project_path(output_dir)
    require(not output_dir.exists(), "Output directory already exists; choose a new build directory")
    data, provenance = prepare_data(snapshots, labels)
    runs = {r["run_id"]: r["dataset_label"] for r in data["kpis"]}
    default_run_id = default_run_id or next(iter(runs))
    require(default_run_id in runs, "Default run is absent from the supplied snapshots")
    # Build XML before creating output so invalid settings cannot leave a partial package.
    workbook = make_workbook(data, runs[default_run_id])
    output_dir.mkdir(parents=True, exist_ok=False)
    data_dir = output_dir / "Data"
    data_dir.mkdir()
    datasets = []
    for name, rows in data.items():
        path = data_dir / (name + ".csv")
        with path.open("x", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        with path.open(encoding="utf-8", newline="") as source:
            actual = list(csv.DictReader(source))
        expected = [{k: "" if v is None else str(v) for k,v in r.items()} for r in rows]
        require(actual == expected, "CSV serialization differs from prepared data")
        datasets.append({"file": f"Data/{path.name}", "rows": len(rows), "sha256": sha256(path)})
    twb = output_dir / "Digital Banking Onboarding.twb"
    twb.write_bytes(workbook)
    twbx = output_dir / "Digital Banking Onboarding.twbx"
    with ZipFile(twbx, "x", compression=ZIP_DEFLATED) as archive:
        archive.write(twb, twb.name)
        for item in datasets:
            archive.write(output_dir / item["file"], item["file"])
    with ZipFile(twbx) as archive:
        require(archive.testzip() is None, "Invalid workbook archive")
        require(archive.read(twb.name) == workbook, "Packaged workbook differs")
        for item in datasets:
            require(hashlib.sha256(archive.read(item["file"])).hexdigest() == item["sha256"], "Packaged data differs")
    manifest = {"dashboard_version": "banking-onboarding-v1", "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "default_run_id": default_run_id, "runs": runs, "source_snapshots": provenance,
        "datasets": datasets, "workbook_sha256": sha256(twb), "package_sha256": sha256(twbx),
        "percentage_scale": "0 to 1; formatted as percentages in Tableau", "null_encoding": "empty CSV field",
        "grain": {"kpis": "run", "funnel": "run + step", "channels": "run + channel"},
        "validation": {"source_checksums": "passed", "count_and_rate_reconciliation": "passed",
                       "archive_readback": "passed", "tableau_desktop_rendering": "requires separate desktop verification"}}
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return output_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-list", help="JSON file with snapshots, optional labels and default_run_id")
    parser.add_argument("--snapshot", action="append", help="P0.4 snapshot directory; repeat for distinct runs")
    parser.add_argument("--output-dir", help="New output directory; defaults to a unique artifacts/tableau build")
    args = parser.parse_args()
    require(not (args.source_list and args.snapshot), "Use a source list or snapshot arguments")
    settings = {"snapshots": args.snapshot} if args.snapshot else json.loads(
        project_path(args.source_list or "tableau/review_sources.json").read_text(encoding="utf-8"))
    output = args.output_dir or f"artifacts/tableau/p05-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    result = build(settings["snapshots"], output, labels=settings.get("labels"), default_run_id=settings.get("default_run_id"))
    print(f"Built Tableau package: {result / 'Digital Banking Onboarding.twbx'}")
    print("PASS: snapshot checksums, metric reconciliation and packaged CSV read-back. Desktop validation is separate.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Dashboard build failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
