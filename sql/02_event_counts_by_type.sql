-- Observed raw volumes, including records excluded from the ordered funnel.
SELECT e.run_id, e.event_type, COUNT(*) AS event_count,
       COUNT(DISTINCT e.event_id) AS unique_event_ids,
       COUNT(DISTINCT e.session_id) AS sessions_with_event,
       COUNT(*) FILTER (WHERE NOT e.valid_event_fields OR NOT e.valid_batch_metadata) AS invalid_rows
FROM analytics_selected_events e LEFT JOIN analytics_stages s USING(event_type)
GROUP BY e.run_id, e.event_type, s.step
ORDER BY e.run_id, COALESCE(s.step,99), e.event_type;
