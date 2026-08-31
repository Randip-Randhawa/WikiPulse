-- Top editors (human and bot) by total edit count, last 24 hours.
SELECT
    wiki,
    "user",
    is_bot,
    SUM(edit_count)      AS total_edits,
    SUM(revert_count)    AS total_reverts,
    SUM(pages_touched)   AS total_pages_touched
FROM user_aggregates
WHERE window_start >= now() - interval '24 hours'
GROUP BY wiki, "user", is_bot
ORDER BY total_edits DESC
LIMIT 25;
