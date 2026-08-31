-- Pages currently showing the highest edit velocity (edits/minute),
-- looking at the most recent window per page.
WITH latest_window AS (
    SELECT DISTINCT ON (wiki, page_title)
        wiki, page_title, window_start, window_end, edit_velocity, edit_count
    FROM page_activity_windows
    ORDER BY wiki, page_title, window_start DESC
)
SELECT *
FROM latest_window
WHERE window_start >= now() - interval '30 minutes'
ORDER BY edit_velocity DESC
LIMIT 25;
