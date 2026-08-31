-- Pages with the highest cumulative mutual (reciprocal) revert counts,
-- the core signal for edit-war detection (see Lab 2 Section 6/19: a raw
-- revert count alone is NOT the same as reciprocal edit-war behavior).
SELECT
    wiki,
    page_title,
    SUM(mutual_revert_count)   AS total_mutual_reverts,
    COUNT(*) FILTER (WHERE flag) AS flagged_windows,
    MAX(conflict_score)        AS peak_conflict_score,
    MAX(window_end)            AS most_recent_window
FROM edit_war_signals
WHERE window_start >= now() - interval '7 days'
GROUP BY wiki, page_title
HAVING SUM(mutual_revert_count) > 0
ORDER BY total_mutual_reverts DESC
LIMIT 25;
