# P0.6 evidence index

Collected on 2026-11-10. See [the final report](../../p06_final_verification.md)
for interpretation, exact commands and untested boundaries.

- `ingestion.json`, `simulation.json`, `analytics.json`: copies of actual existing
  verification-script reports. `provenance.json` identifies originals and their
  SHA256 hashes. Local repository-root paths are replaced with `<repo>` in copies.
- `unit-tests.txt`, `fresh-unit-tests.txt`: actual unittest output; 83 tests each.
- `fresh-install.txt`, `pip-check.txt`: fresh environment dependency evidence.
- `quickstart-*.txt`: direct CLI run logs from the fresh source directory.
- `fresh_quickstart.png`: native Tableau rendering of that new 20-event dataset.
- `historical_baseline.png`, `simulation_a.png`, `simulation_b.png`: native
  Tableau Desktop screenshots taken after switching the retained workbook's
  dataset parameter. These are saved batch snapshots, not live streaming views.
- `preservation_before.json`, `preservation.json`: before-file hashes and comparison;
  one auto-generated Tableau recovery file changed, no saved dataset/workbook did.

The original databases, raw logs, fresh virtual environment and generated outputs
remain in ignored local `artifacts/verification/` directories. This small evidence
directory is intended for review and inclusion in a future Git commit; no commit
or publication was performed by this verification stage.
