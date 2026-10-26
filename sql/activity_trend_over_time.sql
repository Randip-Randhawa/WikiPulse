-- Hourly edit-activity trend across all pages, last 48 hours. Counted from
-- edit_events rather than the overlapping page_activity_windows.
SELECT
    date_trunc('hour', event_time)        AS hour_bucket,
    COUNT(*)                              AS total_edits,
    COUNT(*) FILTER (WHERE NOT is_bot)    AS human_edits,
    COUNT(*) FILTER (WHERE is_bot)        AS bot_edits,
    COUNT(DISTINCT page_title)            AS distinct_pages
FROM edit_events
WHERE event_time >= now() - interval '48 hours'
GROUP BY hour_bucket
ORDER BY hour_bucket;
