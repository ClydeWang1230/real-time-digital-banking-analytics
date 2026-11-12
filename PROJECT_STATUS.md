# Project status — 2026-11-09

## Accepted architecture

- Keep DuckDB for the current MVP.
- Finish batch ingestion and close the consumer before running analytics.
- Use exported snapshots for the Tableau dashboard.
- Implement at-least-once delivery with idempotent event persistence in P0.2.
- Preserve existing databases and historical data.

## P0.1 — Reproducible baseline and shared configuration

Implementation:

- Added shared immutable settings and environment overrides in `config.py`.
- Preserved the existing broker, topic, group, database and 100-customer defaults.
- Made relative database paths independent of the working directory.
- Added explicit entry points; imports do not open clients or databases.
- Preserved module and direct-script launch options.
- Kept the existing table schema, generator logic and SQL queries.
- Kept analytics read-only; missing databases and SQL reports raise clear errors.
- Added pinned dependencies, including DuckDB and timestamp-conversion support.
- Added scoped regression checks, a PowerShell runbook and local-artifact ignore rules.

Verification completed on 2026-11-06:

| Check | Result |
|---|---|
| Fresh isolated Python 3.11 environment, installed from `requirements.txt` | Passed |
| Dependency consistency (`pip check`), fresh and existing environments | Passed |
| Eight regression tests, fresh and existing environments | Passed |
| All three SQL reports against the saved analytics database | Exact match with the pre-change results |
| DuckDB `TIMESTAMPTZ` retrieval as a timezone-aware Python value | Passed after adding the pinned `pytz` dependency |
| SHA-256 comparison of all four existing database files | Unchanged |
| Git whitespace check | Passed |

The tests cover working-directory-independent paths, side-effect-free imports,
invalid configuration before I/O, shared client settings, JSON publishing and
in-memory insertion, resource cleanup, missing-database handling and read-only
analytics. Kafka clients are mocked in these checks. No live producer/consumer
run, schema migration, offset reset or database deletion was performed.

P0.1 is complete. At that milestone, live delivery, crash recovery and replay
guarantees were still unverified; P0.2 evidence follows.

## P0.2 — Reliable ingestion

Implementation:

- Validate UTF-8/JSON, required fields, identifiers, timestamps, channel/device
  combinations and stage-specific properties.
- Keep invalid bytes and rejection reasons in `rejected_events`.
- Enforce `event_id` uniqueness; skip identical duplicates and quarantine
  conflicting content without overwriting the original event.
- Save an event/rejection and its Kafka source position in one DuckDB transaction.
- Disable auto-commit/auto-offset-storage and commit Kafka progress only after
  persistence succeeds; stop on storage, polling or commit failures.
- Report producer delivery acknowledgements, failures and pending messages, with
  bounded queue and final-flush waits.
- Add finite batch consumption, idle timeout, outcome counters and logging.
- Refuse legacy databases without modifying their schema or content. The default
  historical analytics path is unchanged; fresh ingestion requires a new path.
- Add isolated unit tests, a live Kafka verification script and recovery documentation.

Verification completed on 2026-11-06:

| Check | Observed result |
|---|---|
| Offline test suite | 38 passed |
| Producer delivery callbacks | 522 queued, 522 acknowledged, zero failures/pending |
| New 100-customer dataset | 512 unique events, 100 sessions |
| Duplicate/conflict/invalid handling | Three duplicates skipped; seven records quarantined |
| Hard process exit between DB and Kafka commits | One committed DB row recovered; Kafka group still had no committed offset |
| Restart using same database and group | 511 new rows + one replay; all 522 records confirmed |
| Full replay with a new group | 522 replays; zero additional event/rejection/ledger rows |
| Real DuckDB connection error | Zero Kafka commits; healthy retry processed all 522 records |
| Historical databases | SHA-256 hashes of all four files unchanged |
| Existing SQL on legacy and P0.2 databases | All three reports ran read-only; legacy results unchanged |
| Dependency and Git whitespace checks | Passed |

Live test topic: `banking-p02-20261106t130712z-53f7b26d`. Its test groups and data
are separate from the original analytics topic/group. The retained local
[verification report](artifacts/verification/p02-20261106T130712Z-53f7b26d/report.json)
records expected counts, producer/consumer counters, rejection reasons and failure
recovery results. Local artifacts are ignored by Git; these observed counts are
documented here for the project record. Future runs have different random counts.

The new test dataset's stage reach was 100, 91, 78, 74, 69, 61, 39; the first
transaction conversion was 39.0%. These numbers belong to the new isolated run,
not the historical 505-event baseline.

P0.2 is implemented and verified for these local scenarios. This is not a claim
of atomic Kafka/DuckDB transactions, multi-broker availability or production-scale
testing. The actual DB-error integration test closes a connection; disk-full and
power-failure tests were not performed. See
[ingestion_reliability.md](docs/ingestion_reliability.md) for the contract and limits.

## Saved analytics baseline

The existing database was queried read-only on 2026-11-06:

- 505 rows and 505 distinct event IDs.
- 100 sessions and 100 customers.
- Stage reach: 100, 89, 84, 72, 66, 56, 38.
- First-transaction conversion: 38.0%.
- Historical debug database: 2,162 rows; two other historical databases: zero rows.

These are results from existing local files, not a new deterministic generation.

## P0.3 — Reproducible simulation and saved-file replay

Implemented on 2026-11-08:

- Added validated batch inputs: run UUID, seed, timezone-aware base time and
  customer count, with generator version `onboarding-v1`.
- Isolated random choices from global random state. UUID5 produces stable
  customer/session/event and business-object identities within a run; new run IDs
  identify independent synthetic populations, one session per customer.
- Kept the seven stages, dropout weights and stage timings. Customer starts are
  now deterministically staggered by 60 seconds.
- Added `run_id`, `generator_version`, `simulation_seed` and
  `simulation_base_time` to new event payloads. Validation accepts either complete
  metadata or legacy events without it. Data stays in the existing payload JSON
  column: no schema migration or changes to existing databases were required.
- Added complete JSONL/manifest preparation before Kafka publication, `--prepare-only`
  and `--replay`. Checksum, event/metadata and count checks run before publishing.
  Saved bytes/IDs are preserved on replay; existing artifact files are not overwritten.
- Added a five-customer, 20-event example, 16 additional tests, and an isolated
  live verification script. Preserved module/direct-script launch and legacy helpers.

Verification completed on 2026-11-08:

| Check | Result |
|---|---|
| Offline regression suite | 54 passed (38 existing plus 16 new) |
| Fresh-process regeneration from an unrelated working directory | JSONL and manifest bytes identical |
| Saved example | 5 customers/sessions, 20 events; checked-in bytes reproduced |
| Run ID reused with changed inputs in the same artifact root | Refused; original files unchanged |
| Corrupt replay input or invalid event at the end of the file | Refused before a Kafka producer was created |
| First live run, seed 42 | 508 acknowledged and inserted, 100 customers/sessions |
| Replay saved file using the same group at new Kafka offsets | 508 acknowledged/committed duplicates, zero additional event rows |
| Second live run, seed 7 and new run ID | 481 acknowledged and inserted, 100 additional customers/sessions |
| Final isolated database | 989 unique events, 200 customers, 200 sessions, 0 rejected, 1,497 processed positions |
| Dependency and Git whitespace checks | Passed |
| Original 505-event SQL reports and four historical database hashes | Unchanged |

Live test topic: `banking-p03-20261108t110003z-759b7c17`. The local
[verification report](artifacts/verification/p03-20261108T110003Z-759b7c17/report.json)
contains both batch manifests, checksums and per-phase acknowledgement, consumer
and database counters. The
[saved example manifest](examples/p03/11111111-1111-4111-8111-111111111103/manifest.json)
is included in the repository; its event-file SHA-256 is
`4b87a6cabf5f000d5bac28a664d7ca8debe0389d5abdbc35f09f96d8cd5c17f6`.

P0.3 is complete for the local MVP scope. Regeneration assumes the same inputs,
generator version and pinned Python runtime; replay uses the actual saved bytes.
The manifest is input evidence, not proof of successful ingestion. No shared
cross-directory batch registry or returning-customer model is implemented.
At the end of P0.3, SQL reports still aggregated the selected database. P0.4 below
adds reusable per-run selection and an ordered funnel.

## P0.4 — Ordered analytics and review snapshots

Implemented and verified on 2026-11-08:

- Added temporary shared SQL models over a read-only source transaction. Both the
  historical eight-column schema and the current payload schema are supported.
- Partitioned metrics by `(run_id, session_id)`, with explicit run selection,
  legacy selection and run listing. Separate simulations are never silently pooled.
- Defined the funnel as an ordered event-time chain; stage four requires KYC
  **passed**. Equal timestamps and later valid retries are supported. Invalid or
  conflicting sessions are excluded with visible reasons, rather than counted as
  business drop-off. Duplicate raw rows do not inflate stage counts.
- Added 15 quality checks per run, KPI/denominator definitions, channel comparison,
  session audit and database-wide ingestion health. Historical unavailable ingestion
  counters remain NULL. Strict mode returns 2 for error-level quality failures.
- Exported seven report datasets per snapshot, with CSV schema, row-count and
  checksum evidence, selection, source hashes and SQL hashes in a manifest.
  Existing output directories are never overwritten.
- Added 20 analytics tests, offline reconciliation verification, metric definitions
  and a Chinese review guide. No persistence schema migration was required.

| Verification | Observed result |
|---|---|
| Full offline suite, rerun after interruption | **74 tests passed** in 9.512 seconds |
| Saved 5-customer sample | 20 events; stage counts 5, 4, 4, 2, 2, 2, 1; replay adds no events |
| Existing review package | All 5 snapshots complete: 35 analytical CSVs and 5 manifests |
| Read-only re-query after resuming | Every saved CSV value, header, row count and type matched; SQL/source/CSV hashes matched |
| Funnel, KPI and channel reconciliation | Passed for the sample, historical baseline and both P0.3 runs |
| Per-run filtering | Both individual-run results match their corresponding rows in the all-runs snapshot |
| Quality checks on reviewed datasets | Zero error-level failures |
| Original source-row review copies | 505 historical + 508 seed-42 + 481 seed-7 rows; all columns retained and CSV read-back verified |
| Data preservation | Four historical database hashes unchanged; P0.3/sample source hashes and all 42 pre-existing package files unchanged |

| Dataset | Events | Started sessions | Account opened | First transaction | First-transaction conversion |
|---|---:|---:|---:|---:|---:|
| Historical baseline | 505 | 100 | 66 | 38 | 38.0% |
| P0.3 seed 42 | 508 | 100 | 63 | 37 | 37.0% |
| P0.3 seed 7 | 481 | 100 | 58 | 30 | 30.0% |

The retained local [review package](artifacts/verification/p04-20261108T115412Z-17d45672/REVIEW.md)
contains the existing analytical snapshots, three additional source-event CSVs,
the original [verification report](artifacts/verification/p04-20261108T115412Z-17d45672/report.json)
and [continuation verification](artifacts/verification/p04-20261108T115412Z-17d45672/continuation_verification.json).
The source-event manifest records the exact read-only queries, parameters, schemas
and checksums used for those review copies. The original 35 analytical CSVs were
verified in place, not regenerated or replaced. No Kafka replay was needed.

Local artifacts remain ignored by Git. On a fresh checkout,
`python -B scripts/verify_analytics.py` builds the checked-in sample in a new
database and exports its seven analytical reports. Optional repeatable `--db-path`
arguments add read-only analysis of available local sources; historical files are
not included in a fresh checkout. The additional source-row copies belong to this
review package and are not extra analytics reports emitted by the runner.

P0.4 is complete for this local MVP scope. See the
[review guide](docs/p04_review.md) and [metric contract](docs/analytics_contract.md).
Incomplete journeys describe the current snapshot, not proven permanent
abandonment. Fixed synthetic timings do not demonstrate real operational speed;
channel differences are descriptive, not causal. At that milestone, Tableau
workbook construction and import validation remained P0.5; progress follows.

## P0.5 — Tableau workbook complete and locally verified

Implemented on 2026-11-09:

- Added a repeatable workbook builder over immutable P0.4 CSV snapshots. It verifies
  checksums, run coverage, quality status, counts and rate denominators before
  writing output. Existing output directories are refused.
- Kept three independent grains: one row per run, run/stage and run/channel.
  A single Tableau batch parameter filters all worksheets. No physical joins or
  sums across independent simulations are introduced.
- Generated four KPI cards, the seven-stage ordered funnel, six stage transitions,
  channel conversion, a descriptive bottleneck statement and a quality summary.
  Numeric rates are converted to fractions for percentage formatting; unavailable
  rates remain NULL/N/A.
- Created a local `.twb` and `.twbx` review workbook with bundled UTF-8 CSV files
  under `tableau/review/`. Sources are the historical 505-event baseline and the
  P0.3 508/481-event runs. The generated workbook opens no database and requires
  no running Kafka process.
- Added eight BI handoff tests and a Chinese guide with per-batch expected values,
  rebuild commands, metric definitions and a desktop acceptance checklist.

Verification so far:

| Check | Result |
|---|---|
| Full regression suite after native-format regression fix | 83 tests passed (74 existing + 9 BI tests), 14.427 seconds |
| Prepared data grains | 3 KPI rows, 21 stage rows, 9 channel rows |
| Counts and rate denominators | Reconciled to P0.4 for all three datasets |
| Source/CSV checksums and packaged ZIP read-back | Passed |
| XML source paths and shared parameter/filter coverage | Checked; no absolute CSV connection paths or physical joins |
| Tableau installed application | Tableau 2026.1 launched successfully |
| Actual workbook rendering, labels and parameter interaction | Passed in Tableau Desktop 2026.1 for historical baseline, Simulation A and Simulation B |

After activation, native opening rejected the first candidate with D2E8DA72:
`font-color`, `mark-size`, and `windows/source-width` were not valid XML attributes
in their respective contexts. The generator now uses the installed native sample
workbooks' `color` and `size` conventions and omits unsupported window metadata.
A ninth BI regression test covers this failure. Native inspection also exposed
clipped KPI labels, narrow headers and whitespace-only label runs; those were
corrected before acceptance. The earlier 82-test result covered data/packaging,
not native compatibility, and is retained as historical evidence.

The [verified workbook](tableau/validated/Digital%20Banking%20Onboarding.twbx)
was opened in Desktop and all three batch selections were reconciled to P0.4:
historical account/transaction rates 66%/38%, A 63%/37%, B 58%/30%. Ordered funnel
counts, six drop-off transitions, channel numerators/denominators, context,
quality status and bottleneck text changed together. KPI line breaks, titles and
labels were visually checked. The package includes all three CSVs and can open
without Kafka or a DuckDB connection.

[Verification evidence](tableau/validated/verification.json) records hashes,
tests and the desktop checks; [screenshots](tableau/validated/screenshots/) retain
one native rendering per dataset. `tableau/review/` and intermediate candidates
are preserved, but the original review candidate must not be used. P0.5 is
complete for this local MVP scope. The [guide](docs/tableau_dashboard.md) records
exact acceptance values. No Tableau Public/Server/Cloud publication was performed.

## Still outstanding

- Release preparation: review/stage/commit the complete working tree and validate
  a fresh remote clone; this has not been performed automatically.
- P1: Kafka readiness/topic initialization and a stable named-volume/recovery plan.

## P0.6 — completed local verification, 2026-11-10

Actual repository audit and existing verification scripts were reused. Both the
original environment and a fresh Python 3.11.9 virtual environment passed 83 tests.
Live Kafka reliability tests reconciled 518 messages into 508 unique events,
3 duplicates and 7 rejections, including crash-before-offset, restart, replay
and database-failure recovery. Fresh two-batch simulation produced 989 unique
events and 1497 processed Kafka positions including replay. SQL verification
checked 8 snapshots and 56 CSVs, including the retained historical/A/B data.

A fresh source-directory demo ran generator → Kafka → consumer → DuckDB → SQL →
CSV → Tableau: 20 events, 5 sessions, 40% account opening, 20% first transaction.
Tableau Desktop 2026.1 rendered this package and all three retained dataset views.
Original saved data/workbooks were unchanged; only one automatic .twbr recovery
file changed among 120 protected files. No old topics were deleted or reset.

See the [final report](docs/p06_final_verification.md) for Passed/Failed/Not Tested/
Known Limitations, commands and evidence, and [presentation notes](docs/project_presentation.md)
for the demo and CV bullets. Fresh OS installation, remote clone, broker storage
loss and production-scale behaviour remain untested. P0.6 changes documentation
and evidence only (plus an ignore rule for recovery files), not business logic.

## Repository state

The repository contained staged, unstaged and untracked work before P0.1. The
generator, analytics runner and three SQL files had empty staged versions even
though their working files contained implementations. P0.1 does not stage files,
commit changes, rewrite history or remove historical databases. Review the complete
working diff before creating the next project commit. P0.2 likewise does not
stage or commit files; it retains the user's existing configuration comments and
does not alter the generator, SQL queries, Compose file or historical databases.
P0.3 changes the generator/producer and extends validation, while preserving the
user's existing explanatory comments. It does not change the persistence schema,
consumer offset logic, SQL files, Compose configuration, dependencies or historical
databases, and it does not stage or commit files.
P0.4 changes the analytics runner, SQL layer, analytics/runtime tests and related
documentation, and adds an offline verification script. It does not change the
producer/consumer ingestion logic, database schema, Compose configuration or
dependencies. The resumed work verified existing outputs and completed review
materials without staging, committing or altering existing data.
P0.5 adds the Tableau builder, presentation definitions, synthetic review package,
tests and documentation. Ingestion, SQL, dependencies and existing data are unchanged.
