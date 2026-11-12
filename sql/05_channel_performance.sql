-- Stable session channel, attributed at onboarding start; invalid/mixed sessions are excluded.
WITH channels AS (SELECT * FROM (VALUES ('mobile_app'), ('web'), ('branch')) c(channel)),
counts AS (
    SELECT r.run_id, c.channel, COUNT(j.started_at) AS started_sessions,
           COUNT(j.account_opened_at) AS account_opened_sessions,
           COUNT(j.first_transaction_at) AS first_transaction_sessions,
           AVG(j.minutes_to_first_transaction) AS avg_minutes_to_first_transaction
    FROM analytics_runs r CROSS JOIN channels c
    LEFT JOIN analytics_journeys j ON j.run_id=r.run_id AND j.channel=c.channel AND j.started_at IS NOT NULL
    GROUP BY r.run_id,c.channel
)
SELECT *, started_sessions-first_transaction_sessions AS not_completed_sessions,
       ROUND(100.0*account_opened_sessions/NULLIF(started_sessions,0),1) AS account_opening_conversion_pct,
       ROUND(100.0*first_transaction_sessions/NULLIF(started_sessions,0),1) AS first_transaction_conversion_pct
FROM counts ORDER BY run_id,channel;
