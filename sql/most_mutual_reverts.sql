-- Pages with the highest cumulative mutual (reciprocal) revert counts,
-- the core signal for edit-war detection (a raw revert count alone is not
-- the same as reciprocal edit-war behavior). PostgreSQL keeps the last
-- POSTGRES_RETENTION_HOURS (72 by default); older signals are in Parquet.
SELECT
    wiki,
    page_title,
    SUM(mutual_revert_count)   AS total_mutual_reverts,
    COUNT(*) FILTER (WHERE flag) AS flagged_windows,
    MAX(conflict_score)        AS peak_conflict_score,
    MAX(window_end)            AS most_recent_window
FROM edit_war_signals
WHERE window_start >= now() - interval '72 hours'
GROUP BY wiki, page_title
HAVING SUM(mutual_revert_count) > 0
ORDER BY total_mutual_reverts DESC
LIMIT 25;
