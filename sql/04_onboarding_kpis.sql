WITH cohorts AS (
    SELECT r.run_id, COUNT(j.session_id) AS observed_sessions,
        COUNT(j.session_id) FILTER (WHERE NOT j.is_eligible) AS excluded_sessions,
        COUNT(j.session_id) FILTER (WHERE j.is_eligible AND j.started_at IS NULL) AS orphan_sessions,
        COUNT(j.started_at) AS started_sessions, COUNT(j.kyc_submitted_at) AS kyc_submitted_sessions,
        COUNT(j.kyc_passed_at) AS kyc_passed_sessions, COUNT(j.kyc_failed_at) AS sessions_with_kyc_failure,
        COUNT(j.account_opened_at) AS account_opened_sessions, COUNT(j.card_activated_at) AS card_activated_sessions,
        COUNT(j.first_transaction_at) AS first_transaction_sessions,
        AVG(j.minutes_to_account_opened) AS avg_minutes_to_account_opened,
        MEDIAN(j.minutes_to_account_opened) AS median_minutes_to_account_opened,
        AVG(j.minutes_to_first_transaction) AS avg_minutes_to_first_transaction,
        MEDIAN(j.minutes_to_first_transaction) AS median_minutes_to_first_transaction
    FROM analytics_runs r LEFT JOIN analytics_journeys j ON j.run_id=r.run_id GROUP BY r.run_id
), volumes AS (
    SELECT run_id, COUNT(*) AS event_rows, COUNT(DISTINCT event_id) AS unique_event_ids,
           COUNT(DISTINCT customer_id) AS observed_customers
    FROM analytics_selected_events GROUP BY run_id
)
SELECT c.run_id, COALESCE(v.event_rows,0) AS event_rows, COALESCE(v.unique_event_ids,0) AS unique_event_ids,
       COALESCE(v.observed_customers,0) AS observed_customers,
       c.observed_sessions, c.excluded_sessions, c.orphan_sessions,
       c.started_sessions, c.kyc_submitted_sessions, c.kyc_passed_sessions, c.sessions_with_kyc_failure,
       c.account_opened_sessions, c.card_activated_sessions, c.first_transaction_sessions,
       c.started_sessions-c.first_transaction_sessions AS not_completed_sessions,
       ROUND(100.0*c.kyc_passed_sessions/NULLIF(c.kyc_submitted_sessions,0),1) AS kyc_pass_from_submission_pct,
       ROUND(100.0*c.account_opened_sessions/NULLIF(c.started_sessions,0),1) AS account_opening_conversion_pct,
       ROUND(100.0*c.first_transaction_sessions/NULLIF(c.started_sessions,0),1) AS first_transaction_conversion_pct,
       ROUND(100.0*c.first_transaction_sessions/NULLIF(c.account_opened_sessions,0),1) AS account_to_first_transaction_pct,
       ROUND(c.avg_minutes_to_account_opened,2) AS avg_minutes_to_account_opened,
       ROUND(c.median_minutes_to_account_opened,2) AS median_minutes_to_account_opened,
       ROUND(c.avg_minutes_to_first_transaction,2) AS avg_minutes_to_first_transaction,
       ROUND(c.median_minutes_to_first_transaction,2) AS median_minutes_to_first_transaction
FROM cohorts c LEFT JOIN volumes v USING(run_id) ORDER BY c.run_id;
