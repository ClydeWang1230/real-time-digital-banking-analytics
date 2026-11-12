# Project presentation and interview notes

## 60-second introduction

This personal project simulates a digital banking onboarding journey, from starting an application to completing a first transaction. Python publishes reproducible synthetic events to Kafka. A consumer validates each message and persists valid events or rejection details to DuckDB before committing Kafka progress. SQL then calculates an ordered onboarding funnel, quality checks and channel conversion metrics. Tableau displays exported batch snapshots. The focus is correctness, recovery and explainable business metrics, rather than production scale.

## Five-minute demonstration

1. Show the architecture in the README. Explain ingestion versus batch reporting.
2. Run the README's five-customer demo with a fresh topic/group/database. Reconcile 20 produced, acknowledged, consumed and inserted events.
3. Show the 15 quality checks and ordered funnel 5 → 4 → 4 → 2 → 2 → 2 → 1. Explain that KYC completion requires a pass, and conversion uses started sessions as the denominator.
4. Open the newly built Tableau package. Show 40% account-opening and 20% first-transaction conversion. For the retained portfolio workbook, switch Historical Baseline, A and B and explain snapshot dates/batches.
5. Show the P0.6 ingestion report: crash after persistence, restart, replay and no duplicate business rows. Explain the limits of a single broker and a local database.

Pre-install Docker/Python/Tableau before a time-limited interview. Keep the validated workbook and test evidence as a clearly labelled fallback if the interviewer has no Docker or Tableau; do not present a saved screenshot as a live run.

## Decisions and reasons

| Decision | Reason and trade-off |
|---|---|
| Kafka + Python consumer | Demonstrates event transport, delivery acknowledgements and offset handling with a small inspectable implementation. No distributed processing engine is required for this data volume. |
| DuckDB | Direct SQL and easy local reproduction; accept single-writer and batch reporting constraints. PostgreSQL remains unnecessary for this MVP. |
| Commit database before offset | Avoid acknowledging an event that was not saved. A crash between commits can redeliver it, so idempotency is essential. |
| event_id plus source-position ledger | Event IDs prevent business duplication; topic/partition/offset records identify a previously processed Kafka message. Same ID with conflicting content is rejected rather than silently replacing data. |
| Rejected-events table | Preserve the original bad input and reason, keep valid subsequent messages moving, and make investigation possible. |
| Seed + run ID + base time | Reproduce a simulation and isolate batches. A seed alone does not identify the dataset or fix timestamps. |
| Explicit ordered SQL funnel | Require prerequisite stages in time order and KYC passed; avoid counting isolated later-stage events as successful journeys. |
| CSV snapshots for Tableau | Portable, inspectable and independent of an active DuckDB writer. Updates require another batch/export; not a live dashboard. |

## English CV bullet points

- Built a Kafka-based digital banking analytics pipeline in Python, validating synthetic onboarding events and persisting accepted and rejected messages in DuckDB with idempotent writes and commit-after-persistence offset handling.
- Developed batch-scoped SQL analytics for a seven-stage ordered onboarding funnel, 15 data-quality checks and channel conversion KPIs, exporting CSV snapshots to a Tableau dashboard with three independently selectable datasets.
- Verified ingestion and recovery through 83 automated tests and live Kafka crash/replay scenarios; reconciled 518 test messages into 508 unique events, 3 duplicates and 7 rejected messages without duplicating persisted business events.

These statements describe a local synthetic-data MVP. They do not claim enterprise throughput, production deployment, business uplift, Spark, PostgreSQL or continuous Tableau refresh. Evidence: [P0.6 report](p06_final_verification.md).

## Concepts to explain without memorising every line

- Follow one message: generate → publish → validate → database transaction → offset commit → later SQL aggregation.
- Explain why an event ID differs from a Kafka offset, and why duplicates can reappear after a crash.
- Explain rejected versus duplicate versus replayed counters and reconcile them with unique rows.
- Explain denominator, stage ordering, KYC-pass rule, and why incomplete does not necessarily mean permanently abandoned.
- Be transparent about AI-assisted implementation: describe what you reviewed, what you tested and the limitations. Do not claim to have independently written code you cannot explain.
