-- Audit grain: one row per run/session, including excluded and orphan sessions.
SELECT j.run_id, j.session_id, j.customer_id, j.channel, j.device_type, j.raw_event_rows,
       j.is_eligible,
       concat_ws('; ',
           CASE WHEN j.invalid_rows>0 THEN 'invalid event fields or batch metadata' END,
           CASE WHEN j.conflicting_rows>0 THEN 'conflicting event ID' END,
           CASE WHEN j.customer_variants<>1 THEN 'mixed customer identity' END,
           CASE WHEN j.channel_variants<>1 OR j.device_variants<>1 THEN 'mixed channel/device' END,
           CASE WHEN j.metadata_variants<>1 THEN 'mixed batch metadata' END) AS exclusion_reason,
       CASE WHEN NOT j.is_eligible THEN 'excluded'
            WHEN j.started_at IS NULL THEN 'orphan'
            WHEN j.first_transaction_at IS NOT NULL THEN 'completed'
            ELSE 'incomplete' END AS journey_status,
       j.stages_reached, s.stage_label AS last_valid_stage,
       j.started_at, j.profile_at, j.kyc_submitted_at, j.kyc_passed_at, j.kyc_failed_at,
       j.account_opened_at, j.card_activated_at, j.first_transaction_at,
       j.minutes_to_account_opened, j.minutes_to_first_transaction
FROM analytics_journeys j LEFT JOIN analytics_stages s ON s.step=j.stages_reached
ORDER BY j.run_id,j.session_id;
