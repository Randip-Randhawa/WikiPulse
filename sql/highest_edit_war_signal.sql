-- Pages currently showing the strongest edit-war/conflict signal.
SELECT
    wiki,
    page_title,
    window_start,
    window_end,
    mutual_revert_count,
    editor_a,
    editor_b,
    total_reverts,
    edit_burst_count,
    conflict_score,
    flag
FROM edit_war_signals
WHERE window_start >= now() - interval '24 hours'
ORDER BY conflict_score DESC
LIMIT 25;
