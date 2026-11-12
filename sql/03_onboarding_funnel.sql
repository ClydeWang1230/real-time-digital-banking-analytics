-- 100 个开始开户的客户中，有多少人提交资料、通过 KYC、开户并完成首笔交易？
-- 按 run_id + session_id 计算：每一步必须存在时间不早于前一步的有效事件。
-- 第四阶段明确为 KYC passed；原始审核完成（包括失败）另见事件数量报告。
WITH metrics AS (
    SELECT *, FIRST_VALUE(sessions) OVER (PARTITION BY run_id ORDER BY step) AS started_sessions,
           LAG(sessions) OVER (PARTITION BY run_id ORDER BY step) AS previous_sessions
    FROM analytics_funnel_counts
)
SELECT run_id, step, stage, stage_label, sessions, started_sessions, previous_sessions,
       ROUND(100.0*sessions/NULLIF(started_sessions,0),1) AS conversion_from_start_pct,
       ROUND(100.0*sessions/NULLIF(previous_sessions,0),1) AS conversion_from_previous_pct,
       previous_sessions-sessions AS dropoff_sessions,
       ROUND(100.0*(previous_sessions-sessions)/NULLIF(previous_sessions,0),1) AS dropoff_from_previous_pct
FROM metrics ORDER BY run_id, step;
