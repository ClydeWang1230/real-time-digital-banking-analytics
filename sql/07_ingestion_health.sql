-- Database-wide context, deliberately not attributed to a selected run.
-- Invalid bytes may have no run ID. Missing historical tables yield NULL, not a misleading zero.
SELECT scope, metric, value, value IS NOT NULL AS available
FROM analytics_ingestion_health ORDER BY metric;
