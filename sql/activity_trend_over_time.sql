-- Hourly edit-activity trend across all pages, last 48 hours.
SELECT
    date_trunc('hour', window_start) AS hour_bucket,
    SUM(edit_count)                  AS total_edits,
    SUM(human_edits)                 AS human_edits,
    SUM(bot_edits)                   AS bot_edits,
    COUNT(DISTINCT page_title)       AS distinct_pages
FROM page_activity_windows
WHERE window_start >= now() - interval '48 hours'
GROUP BY hour_bucket
ORDER BY hour_bucket;
