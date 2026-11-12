# P0.3 — Reproducible batches and replay

## What a batch means

A **run** is one immutable synthetic dataset. It contains one customer population
and one onboarding session per customer. A **delivery attempt** publishes that
dataset to Kafka. Several attempts can deliver the same run without creating new
business events. A Kafka **consumer group** records reading progress; it is not
the simulation's run ID.

| Input | Source/default | Purpose |
|---|---|---|
| `run_id` | `--run-id`; new UUID if omitted | Identify a batch and namespace all its customer/session/event IDs |
| `seed` | `--seed`; 42 | Control channels, devices and dropout choices with a local random generator |
| `base_time` | `--base-time`; current UTC time | Start the first customer's simulated journey |
| `customer_count` | `--customer-count`, then `BANKING_CUSTOMER_COUNT`, then 100 | Number of customers and onboarding sessions |
| `generator_version` | Code constant `onboarding-v1` | Identify the generation rules used |

Seeds must be integers from 0 through `2**63 - 1`. Counts must be positive.
Run IDs must be UUIDs. Base times must include a timezone; equivalent timezone
representations normalize to the same UTC timestamp. Relative artifact paths
resolve from the project root, as database paths already do.

The same complete inputs generate the same JSONL bytes in the pinned Python 3.11
environment. A seed alone does not fix timestamps or IDs. For later code/runtime
versions, saved-file replay is the source of truth; do not assume regeneration
across changed algorithms. Bump the generator version when generation rules change
and retain the old artifacts. The loader currently supports `onboarding-v1`.

## Customer and session definitions

Within a run, customer slot 1 always resolves to the same synthetic customer and
session; all of that session's events share them. UUID5 derives stable identities
from a namespace and a label. Event and account/card/transaction IDs are also
derived from the session, so replay/regeneration cannot accidentally mint new IDs.

A new run ID creates a separate population. With the same seed and time, the
journey patterns match but the identities are different. This MVP does not yet
model returning customers across runs; cross-run retention would require a shared
customer population and additional sessions.

Customers start 60 seconds apart. Stage offsets remain 0, 3, 5, 10, 12, 15 and 20
minutes. Final-stage weights remain `[5, 10, 10, 10, 10, 15, 40]`; these describe
where journeys stop. Events are emitted immediately, even when their simulated
timestamps are later than the wall clock. This is not a latency benchmark.

## Saved files and lifecycle

By default, a producer invocation prepares:

```text
artifacts/runs/<run_id>/
    events.jsonl   # one complete event per line, in generation order
    manifest.json # simulation inputs, event count and file SHA-256
```

Preparation completes before the Kafka producer is created. The manifest is
written last. An incomplete directory or corrupted file cannot be published.
An existing batch is reused only when it matches the newly generated manifest;
different inputs/events under the same output root cause an error, without
overwriting files. Do not reuse a run ID for changed inputs in another output root
either: the run ID denotes the original dataset, and conflicting event content
will be quarantined by ingestion. There is no central cross-directory run registry.

`--replay` loads the entire file, verifies its checksum, event count, metadata,
event validity, unique event IDs and customer/session counts, then publishes the
saved payload bytes in order. It does not regenerate events, replace timestamps
or assign new IDs. Kafka keys are reconstructed from the saved `customer_id`.
Generation options cannot be combined with `--replay`.

If publishing fails or its acknowledgements are uncertain, retain the batch and
replay its directory after resolving the problem. Already persisted events are
deduplicated. Keep the consumer's existing topic/group/database pairing for
recovery. A new empty database with an old advanced group can miss earlier data.

For simplicity, preparation and replay hold a batch in memory. This is suitable
for the small local MVP datasets; large-file streaming and a distributed batch
registry are outside this milestone. The manifest is not an acknowledgement or
proof of database ingestion: delivery and database counters provide that evidence.

## Prepare or inspect without Kafka

From the project root in PowerShell:

```powershell
.\.venv\Scripts\python.exe -B -m producer.producer --prepare-only --run-id 11111111-1111-4111-8111-111111111103 --seed 42 --base-time 2026-11-08T00:00:00Z --customer-count 5 --output-dir artifacts/reproduction
```

Expected: 20 events and SHA-256
`4b87a6cabf5f000d5bac28a664d7ca8debe0389d5abdbc35f09f96d8cd5c17f6`.
Compare it with the checked-in sample in
`examples/p03/11111111-1111-4111-8111-111111111103/`. Git attributes preserve its
LF line endings so the checksum remains valid on Windows checkouts.

Inspect the sample manifest and validate the file without publishing:

```powershell
Get-Content examples/p03/11111111-1111-4111-8111-111111111103/manifest.json
.\.venv\Scripts\python.exe -B -m producer.producer --replay examples/p03/11111111-1111-4111-8111-111111111103 --prepare-only
```

If a preparation was interrupted, keep the incomplete directory for diagnosis.
Regenerate using the manifest inputs in a separate output root; never silently
overwrite an incomplete batch. For a new dataset, use a new run ID instead.

## Publish and replay the sample with an isolated database

Start Kafka using the README, then run the following in one PowerShell terminal.
Each invocation of this setup chooses a fresh topic/group/database. No historical
database or original topic is used.

```powershell
$batchSuffix = [guid]::NewGuid().ToString('N')
$env:BANKING_KAFKA_TOPIC = "banking-p03-demo-$batchSuffix"
$env:BANKING_KAFKA_GROUP_ID = "banking-p03-demo-$batchSuffix"
$env:BANKING_DB_PATH = "banking_p03_demo_$batchSuffix.duckdb"
$batchPath = 'examples/p03/11111111-1111-4111-8111-111111111103'
$manifest = Get-Content "$batchPath/manifest.json" -Raw | ConvertFrom-Json

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $env:BANKING_KAFKA_TOPIC --partitions 1 --replication-factor 1
if ($LASTEXITCODE -ne 0) { throw 'Topic creation failed.' }
.\.venv\Scripts\python.exe -B -m producer.producer --replay $batchPath
if ($LASTEXITCODE -ne 0) { throw 'Publishing failed; keep the batch for retry.' }
.\.venv\Scripts\python.exe -B -m consumer.consumer --max-messages $manifest.event_count --idle-timeout 60
if ($LASTEXITCODE -ne 0) { throw 'Ingestion failed; inspect the error before continuing.' }
```

Expected: 20 acknowledged messages, then 20 inserted/committed, zero rejections.
After that consumer exits, resend the file using the same configuration:

```powershell
.\.venv\Scripts\python.exe -B -m producer.producer --replay $batchPath
if ($LASTEXITCODE -ne 0) { throw 'Replay publishing failed.' }
.\.venv\Scripts\python.exe -B -m consumer.consumer --max-messages $manifest.event_count --idle-timeout 60
if ($LASTEXITCODE -ne 0) { throw 'Replay ingestion failed.' }
.\.venv\Scripts\python.exe -B run_analytics.py
```

Expected: 20 duplicates, zero inserted/rejected; analytics still sees 20 events
and five customers/sessions. The ledger now contains 40 source positions. This
is **file replay at new Kafka offsets**. Re-reading existing Kafka offsets with
a new group instead yields `replayed` outcomes; both keep event totals stable.
Do not rerun the setup block between the initial delivery and its replay.

## Query batches without migrating the database

P0.2 already stores the full event JSON. P0.3 uses that column for the four added
fields: `run_id`, `generator_version`, `simulation_seed`, `simulation_base_time`.
If any is present, all four must be valid. Old events with none remain valid.
No schema migration is needed; existing legacy databases are still protected.

With the consumer stopped, these read-only queries work on a P0.2/P0.3 database:

```sql
SELECT payload->>'run_id' AS run_id,
       COUNT(*) AS events,
       COUNT(DISTINCT customer_id) AS customers,
       COUNT(DISTINCT session_id) AS sessions
FROM raw_events
GROUP BY 1
ORDER BY 1;

SELECT event_id, event_type, event_time, customer_id, session_id
FROM raw_events
WHERE (payload->>'run_id') = '11111111-1111-4111-8111-111111111103'
ORDER BY event_time, session_id;
```

P0.2 events without metadata appear in a NULL batch group. The older eight-column
historical database does not have `payload`, so these batch queries do not apply
to it. The P0.4 runner adapts both schemas automatically, provides `--run-id`,
`--legacy-only` and `--list-runs`, and produces ordered funnel/KPI/channel datasets.
See [analytics_contract.md](analytics_contract.md) for definitions and exports.

## Verification

Offline regression and reproducibility checks:

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
```

Live verification in unique resources:

```powershell
.\.venv\Scripts\python.exe -B scripts/verify_simulation.py
```

On 2026-11-08, 54 offline tests passed. The live verification regenerated
byte-identical JSONL/manifests, ingested 508 events from seed 42, republished those
508 saved events with no new event rows, then ingested 481 events from a distinct
run with seed 7. The new database contained 989 unique events, 200 customers and
200 sessions, zero rejections and 1,497 processed source positions. Kafka confirmed
all progress. The original historical databases were not used for this test.
