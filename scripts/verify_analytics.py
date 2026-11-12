"""Verify SQL/CSV reconciliation on the saved sample and optional read-only sources."""

import argparse
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from uuid import UUID, uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import PROJECT_ROOT
from consumer.storage import EventStore, KafkaRecord
from consumer.validation import EVENT_TYPES
from producer.batch import load_batch
from run_analytics import analyze, csv_value, export_snapshot, fingerprint, project_path


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def records(result, report):
    data = result.reports[report]
    return [dict(zip(data['columns'], row)) for row in data['rows']]


def verify_snapshot(result, destination):
    require(result.error_checks == 0, f"Data quality errors in {result.database}")
    kpis = records(result, '04_onboarding_kpis')
    funnel = records(result, '03_onboarding_funnel')
    channels = records(result, '05_channel_performance')
    for kpi in kpis:
        run_id = kpi['run_id']
        counts = [row['sessions'] for row in funnel if row['run_id'] == run_id]
        require(len(counts) == 7 and counts == sorted(counts, reverse=True), 'Funnel is not monotonic')
        require(counts[0] == kpi['started_sessions'] and counts[4] == kpi['account_opened_sessions']
                and counts[6] == kpi['first_transaction_sessions'], 'KPI/funnel discrepancy')
        for field in ('started_sessions', 'account_opened_sessions', 'first_transaction_sessions'):
            require(sum(row[field] for row in channels if row['run_id'] == run_id) == kpi[field],
                    'Channel totals do not match the run KPI')
    export_snapshot(result, destination)
    manifest = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
    for dataset in manifest['datasets']:
        path = destination / dataset['file']
        data = result.reports[path.stem]
        with path.open(encoding='utf-8', newline='') as source:
            csv_rows = list(csv.reader(source))
        expected = [data['columns']] + [
            ['' if v is None else str(csv_value(v)) for v in row] for row in data['rows']
        ]
        require(csv_rows == expected, f'CSV/query mismatch: {path}')
        require(fingerprint(path)['sha256'] == dataset['sha256'], 'CSV checksum mismatch')
    for source_file in result.source_files:
        require(fingerprint(Path(source_file['file']))['sha256'] == source_file['sha256'],
                'Source changed during analysis/export')
    return {'database': str(result.database), 'selection': result.selection,
            'snapshot': str(destination), 'quality_error_checks': result.error_checks, 'kpis': kpis}


def verify(extra_databases):
    suffix = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
    output = PROJECT_ROOT / 'artifacts' / 'verification' / ('p04-' + suffix)
    output.mkdir(parents=True, exist_ok=False)
    report = {'status': 'running', 'snapshots': []}
    try:
        sample = load_batch(PROJECT_ROOT / 'examples' / 'p03' / '11111111-1111-4111-8111-111111111103')
        path = output / 'sample.duckdb'
        with EventStore(path) as store:
            for offset, (key, payload) in enumerate(sample.messages + sample.messages):
                outcome = store.process(KafkaRecord('p04-offline-sample', 0, offset, payload, key)).outcome
                require(outcome == ('inserted' if offset < len(sample.messages) else 'duplicate'),
                        'Sample persistence/replay mismatch')
        sample_result = analyze(path)
        # This fixture is a valid prefix generator output, so independent raw stage counts are the oracle.
        events = [json.loads(value) for _, value in sample.messages]
        expected = [len({e['session_id'] for e in events if e['event_type'] == stage}) for stage in EVENT_TYPES]
        require([row['sessions'] for row in records(sample_result, '03_onboarding_funnel')] == expected,
                'Sample stage counts differ from the saved events')
        report['sample_expected_stage_counts'] = expected
        report['snapshots'].append(verify_snapshot(sample_result, output / 'exports' / 'sample'))
        for index, database in enumerate(extra_databases, start=1):
            database = project_path(database)
            print(f'Checking source {index}: {database}', flush=True)
            result = analyze(database)
            report['snapshots'].append(verify_snapshot(result, output / 'exports' / f'source_{index}_all'))
            for run in records(result, '04_onboarding_kpis'):
                if run['run_id'] in ('legacy', 'invalid'):
                    continue
                run_id = str(UUID(run['run_id']))
                selected = analyze(database, run_id=run_id)
                require(records(selected, '04_onboarding_kpis') == [run], 'Run filtering changed the metrics')
                report['snapshots'].append(verify_snapshot(selected, output / 'exports' / f'source_{index}_{run_id}'))
        report['status'] = 'passed'
    except Exception as exc:
        report['status'] = 'failed'
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        path = output / 'report.json'
        path.write_text(json.dumps(report, indent=2, default=str) + '\n', encoding='utf-8')
        print(f'Verification report: {path}', flush=True)
    print('PASS: ordered funnel, KPI/channel reconciliation, run selection and CSV snapshots.', flush=True)
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db-path', action='append', default=[], help='Additional existing database; repeat for multiple sources')
    args = parser.parse_args()
    verify(args.db_path)
