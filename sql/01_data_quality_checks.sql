-- Counts overlap; they identify issues, not an additive rejected-event total.
WITH totals AS (
    SELECT r.run_id, COUNT(e.run_id) AS event_rows,
        COUNT(*) FILTER (WHERE e.run_id IS NOT NULL AND NULLIF(TRIM(event_id),'') IS NULL) AS missing_event_ids,
        COUNT(*) FILTER (WHERE e.run_id IS NOT NULL AND NULLIF(TRIM(customer_id),'') IS NULL) AS missing_customer_ids,
        COUNT(*) FILTER (WHERE e.run_id IS NOT NULL AND NULLIF(TRIM(session_id),'') IS NULL) AS missing_session_ids,
        COUNT(*) FILTER (WHERE e.run_id IS NOT NULL AND (event_time IS NULL OR NOT isfinite(event_time) OR NOT event_timezone_present)) AS invalid_event_times,
        COUNT(*) FILTER (WHERE e.run_id IS NOT NULL AND NOT valid_event_fields) AS invalid_event_fields,
        COUNT(*) FILTER (WHERE e.run_id IS NOT NULL AND NOT valid_batch_metadata) AS invalid_batch_metadata,
        COUNT(event_id)-COUNT(DISTINCT event_id) AS duplicate_event_rows,
        COUNT(DISTINCT event_id) FILTER (WHERE conflicting_event_id) AS conflicting_event_ids,
        GREATEST(COUNT(DISTINCT struct_pack(version:=payload_json->>'generator_version',
            seed:=payload_json->>'simulation_seed', base_time:=payload_json->>'simulation_base_time'))
            FILTER (WHERE has_batch_metadata)-1, 0) AS extra_run_metadata_versions
    FROM analytics_runs r LEFT JOIN analytics_selected_events e ON e.run_id=r.run_id GROUP BY r.run_id
), sessions AS (
    SELECT run_id,
        COUNT(*) FILTER (WHERE NOT is_eligible) AS excluded_sessions,
        COUNT(*) FILTER (WHERE customer_variants<>1 OR channel_variants<>1 OR device_variants<>1 OR metadata_variants<>1) AS inconsistent_sessions,
        COUNT(*) FILTER (WHERE is_eligible AND started_at IS NULL) AS orphan_sessions
    FROM analytics_journeys GROUP BY run_id
), ordering AS (
    SELECT run_id, COUNT(*) FILTER (WHERE step>1 AND (prerequisite_at IS NULL OR event_time<prerequisite_at)) AS invalid_order_events
    FROM analytics_event_order GROUP BY run_id
), repeated AS (
    SELECT run_id, COUNT(*) AS repeated_stage_pairs
    FROM (SELECT run_id, session_id, event_type FROM analytics_events
          GROUP BY run_id, session_id, event_type HAVING COUNT(*)>1) GROUP BY run_id
), checks AS (
    SELECT t.run_id, c.*
    FROM totals t
    LEFT JOIN sessions s USING(run_id) LEFT JOIN ordering o USING(run_id) LEFT JOIN repeated d USING(run_id)
    CROSS JOIN LATERAL (VALUES
        ('empty_dataset', 'warning', 'datasets', (t.event_rows=0)::BIGINT),
        ('missing_event_ids', 'error', 'rows', t.missing_event_ids),
        ('missing_customer_ids', 'error', 'rows', t.missing_customer_ids),
        ('missing_session_ids', 'error', 'rows', t.missing_session_ids),
        ('invalid_event_times', 'error', 'rows', t.invalid_event_times),
        ('invalid_event_fields', 'error', 'rows', t.invalid_event_fields),
        ('invalid_batch_metadata', 'error', 'rows', t.invalid_batch_metadata),
        ('duplicate_event_rows', 'warning', 'rows', t.duplicate_event_rows),
        ('conflicting_event_ids', 'error', 'event IDs', t.conflicting_event_ids),
        ('inconsistent_sessions', 'error', 'sessions', COALESCE(s.inconsistent_sessions,0)),
        ('excluded_sessions', 'error', 'sessions', COALESCE(s.excluded_sessions,0)),
        ('orphan_sessions', 'error', 'sessions', COALESCE(s.orphan_sessions,0)),
        ('stage_order_violations', 'error', 'events', COALESCE(o.invalid_order_events,0)),
        ('repeated_stage_occurrences', 'warning', 'session-stage pairs', COALESCE(d.repeated_stage_pairs,0)),
        ('conflicting_run_metadata', 'error', 'extra versions', t.extra_run_metadata_versions)
    ) c(check_name, severity, unit, affected_count)
)
SELECT 'selected_run' AS scope, run_id, check_name, severity, unit, affected_count,
       CASE WHEN affected_count=0 THEN 'PASS' WHEN severity='error' THEN 'FAIL' ELSE 'WARN' END AS status
FROM checks ORDER BY run_id, check_name;
