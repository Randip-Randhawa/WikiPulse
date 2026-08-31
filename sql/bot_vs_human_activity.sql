-- Overall bot vs. human edit share over the last 24 hours.
SELECT
    SUM(human_edits)                                   AS total_human_edits,
    SUM(bot_edits)                                      AS total_bot_edits,
    ROUND(
        100.0 * SUM(human_edits) / NULLIF(SUM(human_edits) + SUM(bot_edits), 0), 2
    )                                                    AS human_pct,
    ROUND(
        100.0 * SUM(bot_edits) / NULLIF(SUM(human_edits) + SUM(bot_edits), 0), 2
    )                                                    AS bot_pct
FROM page_activity_windows
WHERE window_start >= now() - interval '24 hours';
