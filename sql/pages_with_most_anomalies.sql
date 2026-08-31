-- Pages that have triggered the most activity anomalies (median/MAD or
-- count-based fallback) in the last N days.
SELECT
    wiki,
    page_title,
    COUNT(*)                                   AS anomaly_count,
    COUNT(*) FILTER (WHERE anomaly_type = 'median_mad')     AS median_mad_count,
    COUNT(*) FILTER (WHERE anomaly_type = 'count_fallback') AS count_fallback_count,
    MAX(anomaly_score)                         AS peak_anomaly_score,
    MAX(window_end)                            AS most_recent_anomaly
FROM anomaly_events
WHERE window_start >= now() - interval '7 days'
GROUP BY wiki, page_title
ORDER BY anomaly_count DESC
LIMIT 25;
