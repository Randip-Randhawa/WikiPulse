-- Overall bot vs. human edit share over the last 24 hours.
SELECT
    COUNT(*) FILTER (WHERE NOT is_bot)                  AS total_human_edits,
    COUNT(*) FILTER (WHERE is_bot)                      AS total_bot_edits,
    ROUND(100.0 * COUNT(*) FILTER (WHERE NOT is_bot) / NULLIF(COUNT(*), 0), 2) AS human_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_bot) / NULLIF(COUNT(*), 0), 2)     AS bot_pct
FROM edit_events
WHERE event_time >= now() - interval '24 hours';
