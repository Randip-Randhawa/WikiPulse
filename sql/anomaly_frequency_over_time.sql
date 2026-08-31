-- Hourly anomaly frequency, split by detection type (median/MAD vs.
-- count-based fallback), last 48 hours.
SELECT
    date_trunc('hour', window_start) AS hour_bucket,
    anomaly_type,
    COUNT(*)                         AS anomaly_count
FROM anomaly_events
WHERE window_start >= now() - interval '48 hours'
GROUP BY hour_bucket, anomaly_type
ORDER BY hour_bucket, anomaly_type;
