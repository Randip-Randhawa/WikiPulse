-- Most recent anomaly events, newest first.
SELECT
    wiki,
    page_title,
    window_start,
    window_end,
    edit_count,
    baseline_median,
    baseline_mad,
    anomaly_score,
    anomaly_type,
    threshold_used
FROM anomaly_events
ORDER BY window_start DESC
LIMIT 50;
