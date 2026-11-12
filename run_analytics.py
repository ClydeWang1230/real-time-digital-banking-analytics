"""Read-only, run-scoped SQL analytics with immutable CSV snapshot exports."""

import argparse
import csv
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from uuid import UUID

import duckdb

from config import PROJECT_ROOT, SQL_DIR, load_settings


ANALYTICS_VERSION = "onboarding-analytics-v1"
MODEL_PATH = PROJECT_ROOT / "sql" / "models" / "00_analytics_models.sql"
REQUIRED_COLUMNS = {"event_id", "customer_id", "session_id", "event_type", "event_time",
                    "channel", "device_type", "properties"}


@dataclass
class AnalysisResult:
    database: Path
    selection: dict
    reports: dict
    source_files: list
    query_hashes: dict

    @property
    def error_checks(self):
        report = self.reports.get("01_data_quality_checks", {"rows": [], "columns": []})
        return sum(dict(zip(report["columns"], row)).get("status") == "FAIL" for row in report["rows"])


def project_path(path):
    path = Path(path).expanduser()
    return (path if path.is_absolute() else PROJECT_ROOT / path).resolve()


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return {"file": str(path), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def prepare_models(connection, *, run_id=None, legacy_only=False):
    """All created relations are temporary; the source connection is read-only."""
    column_types = {row[0]: row[1] for row in connection.execute("DESCRIBE raw_events").fetchall()}
    columns = set(column_types)
    missing = REQUIRED_COLUMNS - columns
    if missing:
        raise ValueError("raw_events is missing required analytics columns: " + ", ".join(sorted(missing)))
    payload = "TRY_CAST(payload AS JSON)" if "payload" in columns else "NULL::JSON"
    valid_payload = "COALESCE(json_type(TRY_CAST(payload AS JSON))='OBJECT', FALSE)" if "payload" in columns else "TRUE"
    timezone_present = ("TRUE" if column_types['event_time'] == 'TIMESTAMP WITH TIME ZONE' else
                        "COALESCE(regexp_matches(CAST(event_time AS VARCHAR), '[T ][0-9]{2}:[0-9]{2}.*(Z|[+-][0-9]{2}(:?[0-9]{2})?)$'), FALSE)")
    connection.execute("SET TimeZone='UTC'")
    connection.execute(f"""
        CREATE TEMP VIEW analytics_input AS
        SELECT CAST(event_id AS VARCHAR) AS event_id, CAST(customer_id AS VARCHAR) AS customer_id,
               CAST(session_id AS VARCHAR) AS session_id, CAST(event_type AS VARCHAR) AS event_type,
               TRY_CAST(event_time AS TIMESTAMPTZ) AS event_time,
               CAST(channel AS VARCHAR) AS channel, CAST(device_type AS VARCHAR) AS device_type,
               CAST(properties AS VARCHAR) AS properties_text,
               TRY_CAST(properties AS JSON) AS properties_json, {payload} AS payload_json,
               {valid_payload} AS valid_payload, {timezone_present} AS event_timezone_present
        FROM raw_events
    """)
    connection.execute("""
        CREATE TEMP TABLE analytics_parameters AS
        SELECT ?::VARCHAR AS selected_run_id, ?::BOOLEAN AS legacy_only
    """, [run_id, legacy_only])
    tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
    rejected = "(SELECT COUNT(*) FROM rejected_events)" if "rejected_events" in tables else "NULL::BIGINT"
    processed = "(SELECT COUNT(*) FROM processed_messages)" if "processed_messages" in tables else "NULL::BIGINT"
    duplicate = "(SELECT COUNT(*) FROM processed_messages WHERE outcome='duplicate')" if "processed_messages" in tables else "NULL::BIGINT"
    connection.execute(f"""
        CREATE TEMP VIEW analytics_ingestion_health AS
        SELECT 'database' AS scope, 'raw_event_rows' AS metric, COUNT(*)::BIGINT AS value FROM raw_events
        UNION ALL SELECT 'database', 'rejected_messages', {rejected}
        UNION ALL SELECT 'database', 'processed_positions', {processed}
        UNION ALL SELECT 'database', 'duplicate_event_deliveries', {duplicate}
    """)
    model_sql = MODEL_PATH.read_text(encoding="utf-8")
    connection.execute(model_sql)
    if run_id is not None and not connection.execute(
        "SELECT EXISTS(SELECT 1 FROM analytics_all_events WHERE run_id=?)", [run_id]
    ).fetchone()[0]:
        raise ValueError(f"Run ID not found in this database: {run_id}. Use --list-runs.")
    return hashlib.sha256(model_sql.encode("utf-8")).hexdigest()


def analyze(database, *, run_id=None, legacy_only=False, sql_dir=None, list_runs=False):
    if run_id is not None:
        run_id = str(UUID(run_id))
    if run_id is not None and legacy_only:
        raise ValueError("Select either a run ID or legacy-only data")
    database = project_path(database)
    if not database.is_file():
        raise FileNotFoundError(f"Analytics database not found: {database}. Analytics will not create a database.")
    sql_dir = SQL_DIR if sql_dir is None else Path(sql_dir)
    sql_files = sorted(sql_dir.glob("*.sql"))
    if not sql_files and not list_runs:
        raise FileNotFoundError(f"No SQL reports found in {sql_dir}.")
    reports = {}
    with duckdb.connect(str(database), read_only=True) as connection:
        connection.execute("BEGIN TRANSACTION")
        model_hash = prepare_models(connection, run_id=run_id, legacy_only=legacy_only)
        queries = [(p.stem, p.read_text(encoding="utf-8")) for p in sql_files]
        if list_runs:
            queries = [("available_runs", """
                SELECT run_id, COUNT(*) AS event_rows, COUNT(DISTINCT session_id) AS sessions,
                       MIN(event_time) AS first_event_at, MAX(event_time) AS last_event_at
                FROM analytics_all_events GROUP BY run_id ORDER BY run_id
            """)]
        for name, sql in queries:
            cursor = connection.execute(sql)
            reports[name] = {"columns": [item[0] for item in cursor.description],
                             "types": [str(item[1]) for item in cursor.description], "rows": cursor.fetchall()}
        source_files = [fingerprint(database)]
        wal = Path(str(database) + ".wal")
        if wal.is_file():
            source_files.append(fingerprint(wal))
        connection.execute("COMMIT")
    query_hashes = {name: hashlib.sha256(sql.encode("utf-8")).hexdigest() for name, sql in queries}
    query_hashes["models/00_analytics_models"] = model_hash
    return AnalysisResult(database, {"run_id": run_id, "legacy_only": legacy_only,
                                    "mode": "run" if run_id else "legacy" if legacy_only else "all_runs_separately"},
                          reports, source_files, query_hashes)


def csv_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def export_snapshot(result, directory):
    directory = project_path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    datasets = []
    for name, report in result.reports.items():
        path = directory / (name + ".csv")
        with path.open("x", encoding="utf-8", newline="") as target:
            writer = csv.writer(target, lineterminator="\n")
            writer.writerow(report["columns"])
            writer.writerows([[csv_value(value) for value in row] for row in report["rows"]])
        datasets.append({"file": path.name, "rows": len(report["rows"]),
                         "scope": "database" if name == "07_ingestion_health" else "selected_runs",
                         "columns": dict(zip(report["columns"], report["types"])),
                         "sha256": fingerprint(path)["sha256"]})
    manifest = {"analytics_version": ANALYTICS_VERSION,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "source_database": str(result.database), "source_files": result.source_files,
                "sql_sha256_utf8_normalized_newlines": result.query_hashes,
                "selection": result.selection, "error_checks": result.error_checks,
                "quality_passed": result.error_checks == 0,
                "null_encoding": "empty CSV field", "timestamp_timezone": "UTC",
                "percentage_scale": "0 to 100 (not 0 to 1)", "datasets": datasets}
    with (directory / "manifest.json").open("x", encoding="utf-8") as target:
        target.write(json.dumps(manifest, indent=2) + "\n")
    return directory


def main(*, db_path=None, run_id=None, legacy_only=False, list_runs=False, export_dir=None, strict=False):
    if list_runs and (run_id is not None or legacy_only or export_dir is not None or strict):
        raise ValueError("--list-runs cannot be combined with selection, export or strict options")
    if export_dir is not None and project_path(export_dir).exists():
        raise FileExistsError("Export directory already exists; choose a new snapshot directory")
    settings = load_settings()
    result = analyze(settings.db_path if db_path is None else db_path, run_id=run_id,
                     legacy_only=legacy_only, sql_dir=SQL_DIR, list_runs=list_runs)
    print(f"Database: {result.database}")
    print("Selection: " + json.dumps(result.selection, sort_keys=True))
    for name, report in result.reports.items():
        print(f"\n=== {name}.sql ===")
        print(" | ".join(report["columns"]))
        for row in report["rows"]:
            print(" | ".join("NULL" if value is None else str(csv_value(value)) for value in row))
    if export_dir is not None:
        print(f"CSV snapshot: {export_snapshot(result, export_dir)}")
    if result.error_checks:
        print(f"Data quality: {result.error_checks} error checks failed. Inspect 01_data_quality_checks.")
    return 2 if strict and result.error_checks else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", help="Existing database; overrides BANKING_DB_PATH")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--run-id", help="Select one simulation UUID")
    group.add_argument("--legacy-only", action="store_true", help="Select events without batch metadata")
    parser.add_argument("--list-runs", action="store_true", help="List available run IDs without running reports")
    parser.add_argument("--export-dir", help="New directory for CSV files and a provenance manifest")
    parser.add_argument("--strict", action="store_true", help="Exit 2 after reporting/exporting if data quality errors exist")
    args = parser.parse_args()
    try:
        raise SystemExit(main(**vars(args)))
    except Exception as exc:
        print(f"Analytics failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
