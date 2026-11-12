# P0.4 analytics contract — onboarding-analytics-v1

## Scope and source protection

The source database is opened read-only, inside one read transaction. The runner
creates temporary relations, not persisted tables or views. It supports the
original eight-column `raw_events` and the newer full-payload schema. All eight
business columns are required; missing columns cause an explicit error. Timestamps
are interpreted/reported in UTC, and naive textual timestamps are quality errors.

Batch metadata comes from `raw_events.payload`. By default, every output metric
is partitioned by run ID, so separate simulations are not silently pooled.
`--run-id` selects one UUID; unknown IDs are errors. `--legacy-only` selects
untagged events. `legacy` is a report label, not an ID written back to the database.
Malformed batch metadata can appear in an `invalid` group and fails quality checks.
An empty selection without an explicit UUID produces a zero-count legacy report
with an empty-dataset warning; it does not invent a simulation run.

The ingestion-health report is deliberately database-wide. Rejected messages may
be invalid bytes without a usable run ID. Unknown historical ingestion counts are
NULL, not zero. These metrics cannot be assigned to a selected run or combined
with run-level denominators to calculate rejection rates.

## Session eligibility and event identity

The analytical journey key is `(run_id, session_id)`. A session is eligible only
when all its event rows pass structural/field checks and its customer, channel,
device and batch metadata are consistent. A session with invalid rows or conflicting
event IDs is excluded entirely, including from the starting denominator. Its
exclusion remains visible in the quality report, KPI exclusion count and session
audit. Missing/blank session IDs cannot be grouped; those rows remain visible in
raw volumes and quality checks, but not in the session audit.

Identical duplicate raw rows are reported as a warning and deduplicated for
journey calculations. Event IDs with different analytical content exclude affected
sessions. Conflicts are checked across the whole source before filtering by run,
so selecting one run cannot hide a cross-run conflict. Analytical content compares
run ID, customer, session, event type/time, channel, device and property text;
different JSON property text is conservatively treated as different content in
legacy files. The consumer's full canonical-payload conflict check remains the
ingestion guarantee. The analytics layer does not modify or quarantine source rows.

Run-wide metadata inconsistencies are errors. Different valid sessions can still
be displayed in such a report; strict mode and the export manifest signal that
the run's quality checks failed.

## Ordered funnel

One eligible session contributes at most one count to each stage:

1. Earliest `onboarding_started` timestamp.
2. Earliest `profile_submitted` timestamp at or after step 1.
3. Earliest `kyc_submitted` timestamp at or after step 2.
4. Earliest `kyc_verification_completed` with `properties.status = 'passed'`
   at or after step 3.
5. Earliest `account_opened` timestamp at or after step 4.
6. Earliest `card_activated` timestamp at or after step 5.
7. Earliest `first_transaction_completed` timestamp at or after step 6.

If a prerequisite is missing, later steps do not qualify. Equal timestamps are
allowed to accommodate coarse time resolution. Kafka arrival order is irrelevant.
An event before its prerequisite is flagged, but a later valid occurrence can
qualify the stage. For example, a profile at minute -1 does not hide a second
profile at minute 3 after onboarding starts at minute 0.

Repeated starts/stages are treated as occurrences within the same session, not
new journeys. Failed KYC is a valid business outcome and does not qualify step 4.
A later pass can qualify. This model measures whether an ordered successful path
exists; it is not a latest-state account authorization model. A later failure does
not revoke an earlier pass for this historical funnel. Revocations and separate
KYC attempt identities are outside this MVP.

Eligible sessions without a start are orphans and do not enter the funnel. Sessions
with a start but incomplete progression remain in the starting denominator. There
is no time-window censoring or maturity rule: drop-off/not-completed means absence
of observed valid progression in this database snapshot, not proven abandonment.

## Metrics and denominators

| Metric | Definition |
|---|---|
| Event rows / unique event IDs | Raw selected rows / distinct IDs, before session exclusion |
| Observed customers | Distinct raw customer IDs; can include identities from excluded sessions |
| Observed sessions | Nonblank grouped run/session keys, including excluded/orphan sessions |
| Started sessions | Eligible sessions with an onboarding start |
| Stage sessions | Eligible sessions reaching that stage through the ordered chain |
| Conversion from start | Stage sessions / started sessions × 100 |
| Conversion from previous | Stage sessions / preceding-stage sessions × 100 |
| Drop-off sessions | Preceding-stage sessions minus current-stage sessions |
| Drop-off percentage | Drop-off sessions / preceding-stage sessions × 100 |
| KYC pass from submission | Ordered passed-KYC sessions / ordered submitted-KYC sessions × 100 |
| Sessions with KYC failure | Started eligible sessions with a failed KYC after ordered submission; can also later pass |
| Account opening conversion | Ordered account-opened sessions / started sessions × 100 |
| First transaction conversion | Ordered first-transaction sessions / started sessions × 100 |
| Account to first transaction | Ordered first-transaction sessions / account-opened sessions × 100 |
| Not completed sessions | Started sessions minus ordered first-transaction sessions |
| Completion durations | Minutes from start to the relevant ordered account/transaction timestamp |

Rates use NULL when the denominator is zero. Step 1 has no previous-stage rate or
drop-off. Percentages are rounded to one decimal and already use the 0–100 scale.
KPI duration means/medians use only sessions reaching the relevant stage, rounded
to two decimal places. Session durations retain their precision. Do not replace
incomplete durations with zero or interpret completer-only averages as averages
for all applicants.

Channel attribution uses the consistent channel at onboarding start. Mixed-channel
sessions are excluded rather than assigned arbitrarily. The three known channels
are emitted even with zero sessions; their rates are NULL when the denominator is
zero. Channel counts reconcile to the per-run KPI. Channel differences are
descriptive synthetic results, not causal findings or A/B test evidence.

Current generation fixes account completion at minute 12 and first transaction at
minute 20. The duration metrics demonstrate correct computation, not realistic
variation, measured operational speed or Kafka processing latency.

## Quality checks and strict mode

Report 01 emits 15 checks per selected run. Error checks cover missing event,
customer or session IDs; malformed/naive/nonfinite times; invalid event fields;
incomplete or invalid batch metadata; conflicting event IDs; inconsistent or
excluded sessions; orphan sessions; stage-order violations; and conflicting run
metadata. Warnings cover empty datasets, duplicate raw rows and repeated stages.

Each check states its unit. Counts overlap: an orphan can also have order violations,
and a conflicting ID can exclude a session. Do not sum affected counts to infer
the number of bad messages. Stage-order checks apply to otherwise eligible events;
excluded sessions are reported separately. These diagnostics also do not replace
the consumer's input validation or prove completeness against an external source.

`--strict` returns 2 when at least one error check fails. Reports and requested
exports are still created for investigation. Without strict mode, successful
execution returns 0 while clearly reporting errors. Warnings alone do not fail
strict mode. `quality_passed` in the manifest means no error-level checks on the
selected raw-event scope; it does not mean that the whole database has no rejected
Kafka messages. Review report 07 separately.

## Export datasets and grains

| CSV | Grain | Intended use |
|---|---|---|
| `01_data_quality_checks.csv` | Run + check | Quality status and investigation |
| `02_event_counts_by_type.csv` | Run + observed event type | Raw volumes; includes excluded events |
| `03_onboarding_funnel.csv` | Run + step | Ordered funnel, conversion and drop-off |
| `04_onboarding_kpis.csv` | Run | KPI cards and per-run overview |
| `05_channel_performance.csv` | Run + channel | Channel comparisons |
| `06_session_journeys.csv` | Run + nonblank session | Stage times and exclusions |
| `07_ingestion_health.csv` | Database + metric | Global persisted ingestion counters |

These have different grains. For Tableau, start with separate datasets or correctly
configured relationships; a physical join of funnel and channel rows on run ID
alone creates multiple matches and inflates sums. Use `stage_label` for display
and `step` for funnel ordering. Stage 4 is labelled **KYC passed**, although its
underlying event type remains `kyc_verification_completed`.

CSV files use UTF-8, a header row, standard quoting, LF endings, empty fields for
NULL and ISO timestamps in UTC. Do not apply a percent display that multiplies a
0–100 rate by 100 again. The manifest includes DuckDB result types to help set
field types when importing. Actual Tableau workbook construction/import verification
is P0.5 and has not been performed in this milestone.

Exports use a new directory and never overwrite a previous snapshot. All queries
complete before export begins. The manifest is written last and contains the
analytics version, selection, timestamp, source database/WAL checksums, normalized
SQL checksums, CSV types, row counts, checksums and quality status. If an export
fails partway, keep it for diagnosis and use a new output directory; a partial
directory is not a completed snapshot.

## Commands

Use PowerShell from the repository root. Neither Kafka nor a running consumer is
needed for analytics; close the database writer first.

```powershell
.\.venv\Scripts\python.exe -B run_analytics.py --db-path banking_events_analytics.duckdb --list-runs
.\.venv\Scripts\python.exe -B run_analytics.py --db-path banking_events_analytics.duckdb --legacy-only --strict --export-dir exports/legacy-review-001
```

For the retained P0.3 database, this selects the actual seed-42 run:

```powershell
.\.venv\Scripts\python.exe -B run_analytics.py --db-path artifacts/verification/p03-20261108T110003Z-759b7c17/simulation.duckdb --run-id 4a10a289-b783-462b-8614-8011bf8a929f --strict --export-dir exports/seed42-review-001
```

That source is a local retained artifact, not a checked-in dependency. On a fresh
checkout, run the self-contained saved-sample verification instead:

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -B scripts/verify_analytics.py
```

The script writes a new sample database and CSV snapshots under
`artifacts/verification/p04-.../`; no Kafka connection is made. Add repeated
`--db-path` arguments to reconcile additional existing sources, including run
filtering and independent snapshots. SQL report files depend on the temporary
models created by the runner, rather than being standalone raw-table queries.
