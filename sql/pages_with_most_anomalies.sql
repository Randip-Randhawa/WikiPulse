-- Pages that have triggered the most activity anomalies (median/MAD or
-- count-based fallback) within PostgreSQL's retention window (72 hours by
-- default).
SELECT
    wiki,
    page_title,
    COUNT(*)                                   AS anomaly_count,
    COUNT(*) FILTER (WHERE anomaly_type = 'median_mad')     AS median_mad_count,
    COUNT(*) FILTER (WHERE anomaly_type = 'count_fallback') AS count_fallback_count,
    MAX(anomaly_score)                         AS peak_anomaly_score,
    MAX(window_end)                            AS most_recent_anomaly
FROM anomaly_events
WHERE window_start >= now() - interval '72 hours'
GROUP BY wiki, page_title
ORDER BY anomaly_count DESC
LIMIT 25;
