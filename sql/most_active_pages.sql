-- Most active pages in the last N hours, by total edit count across windows.
-- Adjust the interval as needed.
SELECT
    wiki,
    page_title,
    SUM(edit_count)        AS total_edits,
    SUM(unique_editors)    AS total_unique_editor_observations,
    MAX(edit_velocity)     AS peak_edit_velocity,
    MAX(window_end)        AS most_recent_window
FROM page_activity_windows
WHERE window_start >= now() - interval '24 hours'
GROUP BY wiki, page_title
ORDER BY total_edits DESC
LIMIT 25;
