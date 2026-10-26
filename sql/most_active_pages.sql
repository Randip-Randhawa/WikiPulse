-- Most active pages in the last 24 hours, by number of edits. Counted from
-- edit_events: page_activity_windows overlap (each edit appears in every
-- sliding window that covers it), so summing their edit_count overcounts.
SELECT
    wiki,
    page_title,
    COUNT(*)                     AS total_edits,
    COUNT(DISTINCT "user")       AS distinct_editors,
    COUNT(*) FILTER (WHERE is_revert) AS reverts,
    MAX(event_time)              AS most_recent_edit
FROM edit_events
WHERE event_time >= now() - interval '24 hours'
GROUP BY wiki, page_title
ORDER BY total_edits DESC
LIMIT 25;
