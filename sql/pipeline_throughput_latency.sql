-- Pipeline health summary: latest recorded metrics per component, plus a
-- rolling 15-minute average throughput.
SELECT DISTINCT ON (component)
    component,
    recorded_at,
    events_received,
    events_parsed,
    events_rejected,
    events_published,
    anomalies_detected,
    edit_wars_detected,
    throughput_per_sec,
    processing_latency_ms,
    last_event_time,
    error_count
FROM pipeline_metrics
ORDER BY component, recorded_at DESC;

-- Rolling 15-minute average throughput per component:
-- SELECT component, AVG(throughput_per_sec) AS avg_throughput_per_sec
-- FROM pipeline_metrics
-- WHERE recorded_at >= now() - interval '15 minutes'
-- GROUP BY component;
