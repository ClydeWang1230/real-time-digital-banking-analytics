-- Temporary models only; the runner opens the source read-only.
CREATE TEMP MACRO analytics_text(doc, json_path) AS (
    COALESCE(json_type(doc, json_path)='VARCHAR'
      AND LENGTH(TRIM(json_extract_string(doc, json_path)))>0
      AND json_extract_string(doc, json_path)=TRIM(json_extract_string(doc, json_path)), FALSE)
);

CREATE TEMP VIEW analytics_stages AS
SELECT * FROM (VALUES
    (1, 'onboarding_started', 'Onboarding started'),
    (2, 'profile_submitted', 'Profile submitted'),
    (3, 'kyc_submitted', 'KYC submitted'),
    (4, 'kyc_verification_completed', 'KYC passed'),
    (5, 'account_opened', 'Account opened'),
    (6, 'card_activated', 'Card activated'),
    (7, 'first_transaction_completed', 'First transaction completed')
) AS stages(step, event_type, stage_label);

CREATE TEMP TABLE analytics_all_events AS
WITH normalized AS (
    SELECT *, COALESCE(
        json_exists(payload_json, '$.run_id') OR json_exists(payload_json, '$.generator_version')
        OR json_exists(payload_json, '$.simulation_seed') OR json_exists(payload_json, '$.simulation_base_time'), FALSE
    ) AS has_batch_metadata
    FROM analytics_input
)
SELECT *,
    COALESCE(CAST(TRY_CAST(payload_json->>'run_id' AS UUID) AS VARCHAR),
             CASE WHEN has_batch_metadata OR NOT valid_payload THEN 'invalid' ELSE 'legacy' END) AS run_id,
    COALESCE(valid_payload AND (NOT has_batch_metadata OR (
        TRY_CAST(payload_json->>'run_id' AS UUID) IS NOT NULL
        AND analytics_text(payload_json, '$.generator_version')
        AND json_type(payload_json, '$.simulation_seed') IN ('BIGINT', 'UBIGINT')
        AND TRY_CAST(payload_json->>'simulation_seed' AS BIGINT) >= 0
        AND analytics_text(payload_json, '$.simulation_base_time')
        AND TRY_CAST(payload_json->>'simulation_base_time' AS TIMESTAMPTZ) IS NOT NULL
        AND regexp_matches(payload_json->>'simulation_base_time', '(Z|[+-][0-9]{2}:[0-9]{2})$')
    )), FALSE) AS valid_batch_metadata,
    COALESCE(
        TRY_CAST(event_id AS UUID) IS NOT NULL
        AND LENGTH(TRIM(customer_id)) > 0 AND customer_id=TRIM(customer_id)
        AND LENGTH(TRIM(session_id)) > 0 AND session_id=TRIM(session_id)
        AND event_time IS NOT NULL AND isfinite(event_time) AND event_timezone_present
        AND event_type IN (SELECT event_type FROM analytics_stages)
        AND ((channel='mobile_app' AND device_type IN ('ios','android'))
          OR (channel='web' AND device_type='desktop')
          OR (channel='branch' AND device_type='branch_terminal'))
        AND json_type(properties_json)='OBJECT'
        AND CASE event_type
            WHEN 'onboarding_started' THEN analytics_text(properties_json, '$.campaign_id')
                                           AND analytics_text(properties_json, '$.entry_point')
            WHEN 'profile_submitted' THEN regexp_full_match(properties_json->>'country', '[A-Z]{2}')
                                          AND analytics_text(properties_json, '$.occupation_group')
            WHEN 'kyc_submitted' THEN analytics_text(properties_json, '$.kyc_method')
            WHEN 'kyc_verification_completed' THEN (properties_json->>'status') IN ('passed','failed')
                                                   AND analytics_text(properties_json, '$.review_type')
            WHEN 'account_opened' THEN TRY_CAST(properties_json->>'account_id' AS UUID) IS NOT NULL
            WHEN 'card_activated' THEN TRY_CAST(properties_json->>'card_id' AS UUID) IS NOT NULL
                                      AND analytics_text(properties_json, '$.card_type')
            WHEN 'first_transaction_completed' THEN TRY_CAST(properties_json->>'transaction_id' AS UUID) IS NOT NULL
                AND json_type(properties_json, '$.amount') IN ('BIGINT','UBIGINT','DOUBLE')
                AND isfinite(TRY_CAST(properties_json->>'amount' AS DOUBLE))
                AND TRY_CAST(properties_json->>'amount' AS DOUBLE)>0
                AND regexp_full_match(properties_json->>'currency', '[A-Z]{3}')
                AND analytics_text(properties_json, '$.transaction_type')
            ELSE FALSE END,
        FALSE) AS valid_event_fields
FROM normalized;

-- ID conflicts are detected across the entire source, even in a filtered report.
-- Property text differences are conservatively treated as different legacy content.
CREATE TEMP TABLE analytics_id_integrity AS
SELECT event_id, COUNT(*) AS copies,
       COUNT(DISTINCT struct_pack(run_id:=run_id, customer_id:=customer_id, session_id:=session_id,
             event_type:=event_type, event_time:=event_time, channel:=channel,
             device_type:=device_type, properties:=properties_text))>1 AS conflicting
FROM analytics_all_events WHERE event_id IS NOT NULL GROUP BY event_id;

CREATE TEMP TABLE analytics_selected_events AS
SELECT e.*, COALESCE(i.conflicting, FALSE) AS conflicting_event_id
FROM analytics_all_events e LEFT JOIN analytics_id_integrity i USING(event_id)
CROSS JOIN analytics_parameters p
WHERE (p.selected_run_id IS NULL OR e.run_id=p.selected_run_id)
  AND (NOT p.legacy_only OR e.run_id='legacy');

CREATE TEMP VIEW analytics_runs AS
SELECT DISTINCT run_id FROM analytics_selected_events
UNION ALL
SELECT COALESCE(selected_run_id, 'legacy') FROM analytics_parameters
WHERE NOT EXISTS(SELECT 1 FROM analytics_selected_events);

CREATE TEMP TABLE analytics_session_quality AS
WITH audit AS (
    SELECT run_id, session_id, COUNT(*) AS raw_event_rows,
           COUNT(DISTINCT customer_id) AS customer_variants,
           COUNT(DISTINCT channel) AS channel_variants,
           COUNT(DISTINCT device_type) AS device_variants,
           COUNT(DISTINCT struct_pack(version:=payload_json->>'generator_version',
                seed:=payload_json->>'simulation_seed', base_time:=payload_json->>'simulation_base_time')) AS metadata_variants,
           MIN(customer_id) AS customer_id, MIN(channel) AS channel, MIN(device_type) AS device_type,
           COUNT(*) FILTER (WHERE NOT valid_event_fields OR NOT valid_batch_metadata) AS invalid_rows,
           COUNT(*) FILTER (WHERE conflicting_event_id) AS conflicting_rows
    FROM analytics_selected_events
    WHERE LENGTH(TRIM(session_id))>0 GROUP BY run_id, session_id
)
SELECT *, invalid_rows=0 AND conflicting_rows=0 AND customer_variants=1
          AND channel_variants=1 AND device_variants=1 AND metadata_variants=1 AS is_eligible
FROM audit;

CREATE TEMP TABLE analytics_events AS
SELECT DISTINCT e.run_id, e.session_id, e.customer_id, e.event_id, e.event_type,
       e.event_time, e.channel, e.device_type, e.properties_json
FROM analytics_selected_events e JOIN analytics_session_quality s USING(run_id,session_id)
WHERE s.is_eligible;

-- Earliest feasible chain; an early retry does not hide a later valid occurrence.
-- Equal timestamps are allowed. Kafka arrival order is not event-time order.
CREATE TEMP TABLE analytics_journeys AS
SELECT s.*, a.started_at, b.profile_at, c.kyc_submitted_at, d.kyc_passed_at,
       e.account_opened_at, f.card_activated_at, g.first_transaction_at, k.kyc_failed_at,
       (a.started_at IS NOT NULL)::INTEGER + (b.profile_at IS NOT NULL)::INTEGER
       + (c.kyc_submitted_at IS NOT NULL)::INTEGER + (d.kyc_passed_at IS NOT NULL)::INTEGER
       + (e.account_opened_at IS NOT NULL)::INTEGER + (f.card_activated_at IS NOT NULL)::INTEGER
       + (g.first_transaction_at IS NOT NULL)::INTEGER AS stages_reached,
       epoch(e.account_opened_at-a.started_at)/60.0 AS minutes_to_account_opened,
       epoch(g.first_transaction_at-a.started_at)/60.0 AS minutes_to_first_transaction
FROM analytics_session_quality s
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS started_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='onboarding_started'
) a ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS profile_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='profile_submitted' AND event_time>=a.started_at
) b ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS kyc_submitted_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='kyc_submitted' AND event_time>=b.profile_at
) c ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS kyc_passed_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='kyc_verification_completed'
      AND (properties_json->>'status')='passed' AND event_time>=c.kyc_submitted_at
) d ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS account_opened_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='account_opened' AND event_time>=d.kyc_passed_at
) e ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS card_activated_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='card_activated' AND event_time>=e.account_opened_at
) f ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS first_transaction_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='first_transaction_completed' AND event_time>=f.card_activated_at
) g ON TRUE
LEFT JOIN LATERAL (
    SELECT MIN(event_time) AS kyc_failed_at FROM analytics_events x
    WHERE x.run_id=s.run_id AND x.session_id=s.session_id AND event_type='kyc_verification_completed'
      AND (properties_json->>'status')='failed' AND event_time>=c.kyc_submitted_at
) k ON TRUE;

CREATE TEMP VIEW analytics_event_order AS
SELECT e.*, s.step,
       CASE s.step WHEN 2 THEN j.started_at WHEN 3 THEN j.profile_at
            WHEN 4 THEN j.kyc_submitted_at WHEN 5 THEN j.kyc_passed_at
            WHEN 6 THEN j.account_opened_at WHEN 7 THEN j.card_activated_at END AS prerequisite_at
FROM analytics_events e JOIN analytics_stages s USING(event_type)
JOIN analytics_journeys j USING(run_id,session_id);

CREATE TEMP VIEW analytics_funnel_counts AS
SELECT r.run_id, s.step, s.event_type AS stage, s.stage_label,
       COUNT(CASE s.step WHEN 1 THEN j.started_at WHEN 2 THEN j.profile_at
             WHEN 3 THEN j.kyc_submitted_at WHEN 4 THEN j.kyc_passed_at
             WHEN 5 THEN j.account_opened_at WHEN 6 THEN j.card_activated_at
             WHEN 7 THEN j.first_transaction_at END) AS sessions
FROM analytics_runs r CROSS JOIN analytics_stages s
LEFT JOIN analytics_journeys j ON j.run_id=r.run_id
GROUP BY r.run_id, s.step, s.event_type, s.stage_label;
