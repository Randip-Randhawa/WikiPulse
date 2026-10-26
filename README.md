# WikiPulse — Real-Time Wikipedia Edit-War & Trend Anomaly Engine

> **This is an academic/prototype data-engineering project**, built to demonstrate a
> complete real-time pipeline end-to-end. It is **not** a production Wikimedia
> moderation system, does not take any automated action on Wikipedia, and its
> anomaly/edit-war signals are heuristic indicators for a dashboard demo, not
> ground truth.

## What WikiPulse is

WikiPulse continuously consumes Wikimedia's live Recent Changes feed and surfaces:

1. Pages experiencing unusually high editing activity (**activity anomalies**).
2. Pages showing potential edit-war behavior, based on **reciprocal reverts**
   between editors (**edit-war signals**) — kept explicitly separate from #1.
3. Human vs. bot editing activity.
4. Page-level editing velocity and volume.
5. Edit-quality/damage-risk information via Wikimedia's **Lift Wing** service.
6. Historical metrics, browsable through pre-written SQL and Parquet files.
7. Live alerts through a **Streamlit** dashboard.
8. Basic pipeline health metrics (throughput, latency, error counts).

## Problem being solved

Wikipedia is edited thousands of times per minute across many wikis. A small
fraction of that activity represents either a coordinated/controversial
editing spike (breaking news, vandalism waves) or a slow-burning conflict
between editors repeatedly reverting each other. Manually watching Recent
Changes for these patterns doesn't scale. WikiPulse builds the streaming
infrastructure — ingestion, buffering, windowed stream processing, robust
statistics, and storage — needed to surface these patterns automatically and
present them live.

## Architecture

```
                 Wikimedia EventStreams (SSE, recentchange)
                                 │
                                 ▼
                 ┌───────────────────────────────┐
                 │   ingestion/sse_consumer.py    │  reconnect + backoff,
                 │   ingestion/normalizer.py      │  null-safe normalization
                 └───────────────┬────────────────┘
                                 │ normalized JSON events
                                 ▼
                 ┌───────────────────────────────┐
                 │      Kafka: wikipedia-edits    │  buffering / decoupling
                 │   (+ wikipedia-edits-dlq)      │
                 └───────────────┬────────────────┘
                                 │
                                 ▼
                 ┌───────────────────────────────────────────┐
                 │   processing/streaming_job.py (PySpark)    │
                 │   - 5-minute sliding window aggregation    │
                 │   - anomaly_detector.py (median/MAD +      │
                 │     count-based fallback)                  │
                 │   - edit_war_detector.py (mutual reverts)  │
                 └───────┬─────────────────────────┬──────────┘
                         │                          │
                         ▼                          ▼
        ┌───────────────────────────┐   ┌───────────────────────────┐
        │  enrichment/liftwing_..   │   │   storage/database.py      │
        │  (bounded, async-ish,     │   │   storage/parquet_writer   │
        │   disableable)            │   │   PostgreSQL + Parquet     │
        └───────────────────────────┘   └───────────┬────────────────┘
                                                      │
                                                      ▼
                                     ┌───────────────────────────────┐
                                     │   dashboard/app.py (Streamlit) │
                                     │   health / activity / anomalies│
                                     │   / edit-wars / trends         │
                                     └───────────────────────────────┘
```

`scripts/generate_synthetic_events.py` can publish directly into Kafka so the
entire pipeline downstream of ingestion can be demonstrated without relying
on live Wikimedia traffic.

Alongside the main flow:

- Ingestion and processing each write a row to `pipeline_metrics` (about
  every 30 seconds while data flows) with counts, latencies and per-stage
  timings; the dashboard's System Health panel reads them.
- The `maintenance` service (`scripts/maintain_partitions.py`) creates
  upcoming daily partitions and enforces the 72-hour PostgreSQL retention.
- `benchmark/` runs seeded synthetic workloads through the pipeline's
  components and writes JSON and Markdown results.

## Technology stack

| Layer            | Technology                                    |
|-------------------|-----------------------------------------------|
| Ingestion         | Python, `sseclient-py`, `requests`             |
| Message broker    | Apache Kafka (KRaft mode, via Docker Compose)  |
| Stream processing | PySpark / Spark Structured Streaming           |
| Enrichment        | Wikimedia Lift Wing (`httpx`, bounded threads)  |
| Storage           | PostgreSQL + Parquet (via `pyarrow`/`pandas`)  |
| Dashboard         | Streamlit + Plotly                              |
| Config            | Environment variables / `.env` (`python-dotenv`)|
| Tests             | `pytest`                                        |

## Kafka topic structure

- `wikipedia-edits` — normalized `WikipediaEvent` JSON, keyed by `page_title`
  (keeps per-page ordering on a partition for stateful downstream logic).
- `wikipedia-edits-dlq` — dead-letter topic for malformed/unparseable raw
  SSE payloads, so they are never silently dropped. Each message carries
  the original payload and the headers `reason` (the error), `stage`
  (`json_decode` or `normalize`) and `failed_at` (UTC, ISO 8601).

## Event schema

See `common/models.py` (`WikipediaEvent`). Key fields: `event_id`,
`timestamp`, `wiki`, `event_type`, `page_title`, `page_id`, `namespace`,
`revision_id`, `previous_revision_id`, `user`, `user_id`, `is_bot`,
`comment`, `byte_length_new/old/delta`, `is_revert`,
`revert_target_revision_id`, `ingestion_timestamp`. Every field Wikimedia
does not guarantee is `Optional` and normalized safely from missing/null
raw data (`ingestion/normalizer.py`).

### Timestamps and lineage

Three clocks are kept apart:

| Time | Where it comes from | Stored as |
|---|---|---|
| Event time | Wikimedia's `timestamp` (whole seconds, UTC) | `timestamp`; `edit_events.event_time` |
| Ingestion time | wall clock when the ingestion service normalized the event | `ingestion_timestamp`; `edit_events.ingestion_time` |
| Processing time | wall clock when the Spark job wrote the event to PostgreSQL | `edit_events.ingested_at` (row insert time) |

Windows, the watermark and retention all use event time. Every stored
event keeps its Wikimedia `event_id` and the Kafka `kafka_partition` /
`kafka_offset` it was read from (in `edit_events` and Parquet `raw_events`),
so any row can be traced back to its Kafka message.

## Spark processing & the five-minute window

`processing/streaming_job.py` reads the Kafka topic, applies a watermark
(`SPARK_WATERMARK_DELAY`, default 2 minutes) for late-event tolerance, and
groups events into a five-minute **sliding** window
(`SPARK_WINDOW_DURATION` / `SPARK_SLIDE_DURATION`, default 5 min / 1 min) per
`(wiki, page_title)`. Spark handles the windowing and per-window raw-record
collection; a `foreachBatch` callback then runs the (pure-Python, unit
tested) anomaly and edit-war detectors per group and persists results. This
keeps the statistical/graph logic simple, testable, and Spark-independent,
at the deliberate cost of not scaling to a distributed multi-node cluster
with externally sharded state — an appropriate simplification for a local,
academic-scale deployment (see code comments in `streaming_job.py` and
`anomaly_detector.py` for the full rationale).

The windowed query runs in **append** mode: each window reaches the
detectors exactly once, with its complete set of edits, after the watermark
passes the window's end. With the defaults that is 2 minutes (watermark)
plus up to one trigger interval (30 seconds) after the window closes in
event time. An earlier version used update mode, which re-emitted a window
on every trigger while it filled; the anomaly history then recorded partial
counts and `anomaly_events` / `edit_war_signals` received duplicate rows.
Both tables now have a unique key on `(wiki, page_title, window_start,
window_end)`; upgrading an existing database keeps the newest row of each
duplicated window.

With a 1-minute slide every edit falls into five overlapping windows; a
30-second slide doubles that to ten. In the benchmark
(`python -m benchmark.runner --slide-seconds 30`) it evaluated exactly
twice as many windows and the detection stage took 2.0–2.3 times as long,
it doubles the `page_activity_windows` rows written, and it halves the time
span the 48-window anomaly history covers. The default stays at 1 minute. Because windows
overlap, summing `page_activity_windows.edit_count` overcounts edits; edit
counts in the dashboard and in `sql/` come from `edit_events`.

## Median/MAD anomaly detection (and why not z-score)

A plain z-score is not used because a handful of extremely active windows
(breaking-news edits, a vandalism wave) would inflate a mean/stddev
baseline and hide later spikes. Instead, `processing/anomaly_detector.py`
computes, per page:

```
median = median(recent window edit counts)
MAD    = median(|edit_count_i - median|)
score  = 0.6745 * (current_edit_count - median) / MAD
```

An anomaly is flagged when `score >= ANOMALY_MAD_THRESHOLD` (default 3.5).
The `0.6745` constant is the standard scaling factor that makes this
"modified z-score" comparable to a classic z-score under normality.

The MAD used in the score is never smaller than `ANOMALY_MAD_EPSILON`
(default 1 edit). Edit counts are whole numbers, and most pages are edited
rarely, so their history is often all 1s and the MAD is 0; with the
previous floor of `1e-6` a window of 2 edits on such a page scored about
674,500 and was flagged. In the benchmark's `sustained` workload (default
flags) that produced 4,475 median/MAD anomalies in 77,007 windows; with the
1-edit floor it produces 9, and the injected burst and edit war are still
detected. Compare with `ANOMALY_MAD_EPSILON=1e-6 make benchmark`.

### Count-based fallback for insufficient history

A brand-new or rarely-edited page has no reliable statistical baseline. If
a page has fewer than `ANOMALY_MIN_HISTORY_WINDOWS` (default 8) prior
observations, WikiPulse instead uses a simple, configurable count-based
rule: flag if `edit_count >= ANOMALY_FALLBACK_EDIT_COUNT_THRESHOLD` (default
15). It is implemented and unit-tested separately from the median/MAD path.

### Page history in memory and its TTL

The per-page history behind the median (`PageHistoryStore`) lives in the
Spark driver's memory: at most `ANOMALY_HISTORY_LOOKBACK_WINDOWS` (48)
counts per page. Without eviction it would keep every page ever edited. A
page with no finalized window for `ANOMALY_HISTORY_TTL_SECONDS` (default 2
hours) is now evicted; recording a window sweeps for expired pages at most
every `ANOMALY_HISTORY_CLEANUP_INTERVAL_SECONDS` (default 60). Pages are
kept in least-recently-updated order, so a sweep stops at the first page
that is still active instead of scanning them all.

- Inactivity is measured on a monotonic clock in the driver, not in event
  time, so a replayed backlog or a wall-clock jump does not evict early.
- Only finalized windows are recorded here; windows still filling up are
  Spark state, which the watermark bounds. The TTL must be at least
  `SPARK_WINDOW_DURATION + SPARK_WATERMARK_DELAY` (checked at start-up), so
  a page edited at least once per window is never evicted.
- An evicted page that comes back starts with an empty history and is
  judged by the count-based fallback until it has 8 windows again; no
  baseline is invented for it.
- Memory is bounded by the number of distinct pages edited within one TTL.
  The benchmark measures about 2.7 KB of Python heap per page with a full
  history. The history is lost when the job restarts.

Page count, evictions and stored window counts appear on the dashboard.

## Edit-war detection (reciprocal reverts, not raw revert counts)

`processing/edit_war_detector.py` deliberately does **not** define an edit
war as "many reverts on a page" — a single editor reverting several
unrelated vandalism edits would look identical under that rule. Instead it
looks for **reciprocal** behavior within the five-minute window: editor A
reverts a revision authored by editor B, and editor B later reverts a
revision authored by editor A back. Each such pair increases
`mutual_revert_count` and the page's `conflict_score`
(`EDIT_WAR_SCORE_MUTUAL_WEIGHT` per mutual pair, plus a smaller weight for
a same-window burst of negative-byte-delta edits). A page is flagged
(`flag`) when `mutual_revert_count >= EDIT_WAR_MUTUAL_REVERT_THRESHOLD`, or
when there is at least one reciprocal revert and `conflict_score >=
EDIT_WAR_FLAG_SCORE_THRESHOLD`. The burst term alone can reach the score
threshold in a busy window, so a window without any reciprocal revert is
never flagged. This is a **signal**, not a definitive judgement, and
WikiPulse never automatically reverts anything on Wikipedia.

High activity, a statistical anomaly, and an edit-war signal are three
distinct concepts, kept in three separate tables/records
(`page_activity_windows`, `anomaly_events`, `edit_war_signals`) — a page can
be highly active without being anomalous, anomalous without being an edit
war, and so on.

## Lift Wing integration

`enrichment/liftwing_client.py` calls Wikimedia's maintained
`revertrisk-language-agnostic` Lift Wing model for a per-revision
damage/quality-risk probability — no custom classifier is trained, per Lab
2. The integration is:

- **Disableable**: `LIFTWING_ENABLED=false` (the default) skips all network
  calls entirely — useful for offline/local development.
- **Bounded**: at most one revision per page/window is sampled for scoring,
  processed through a small thread pool
  (`LIFTWING_MAX_CONCURRENT_REQUESTS`), so a slow or unavailable endpoint
  degrades enrichment coverage rather than blocking the streaming pipeline.
- **Fault-tolerant**: timeouts, HTTP errors, and unexpected response shapes
  are caught and logged; a failure never propagates into the main
  pipeline.

## PostgreSQL schema

See `storage/schema.sql`. Tables: `edit_events` (bounded recent raw-event
slice), `page_activity_windows`, `anomaly_events`, `edit_war_signals`,
`user_aggregates`, `revision_quality_scores`, `pipeline_metrics`. Indexes
are chosen for the query patterns in `sql/*.sql` and the dashboard
(recent-first, page-scoped, and score-ranked lookups).

`edit_events` also has a partial index on `(wiki, page_title, event_time)`
for revert events only, which the dashboard uses to show the reverts
behind an edit-war signal.

### Partitioning

`edit_events` (one row per edit) and `page_activity_windows` (one row per
page per window) are range-partitioned by UTC day on `event_time` and
`window_start`: `edit_events_p20261026` holds 2026-10-26T00:00Z up to the
next midnight. Each also has a `_default` partition for rows outside every
daily range (very late or far-future events), so an insert never fails for
want of a partition. The other tables get a few rows per minute and are not
partitioned.

PostgreSQL requires unique keys on a partitioned table to include the
partition column, so the keys are `(id, event_time)` and `(event_id,
event_time)`; an event's time never changes, so `event_id` still identifies
one event and replayed events are still ignored.

`Database.init_schema` (run by `make init-db`, at processing start-up and by
the maintenance service) creates any missing daily partitions from the day
holding the retention cutoff through `POSTGRES_PARTITION_HORIZON_DAYS`
(default 3) days ahead. It is idempotent and takes a PostgreSQL advisory
lock, so concurrent runs cannot create the same partition twice. If rows for
a day already sit in the default partition (for example after maintenance
was down past the horizon), they are moved into the new daily partition when
it is created.

### Retention

`Database.purge_old_data` (`make purge`, and every
`POSTGRES_MAINTENANCE_INTERVAL_SECONDS` in the maintenance service) removes
everything older than `POSTGRES_RETENTION_HOURS` (default 72) from every
table:

1. Daily partitions that lie entirely before the cutoff are dropped (cheap,
   no row-by-row delete). The partition containing the cutoff is kept.
2. Rows older than the cutoff that remain — in the partition holding the
   cutoff, the default partitions and the unpartitioned tables
   (`anomaly_events`, `edit_war_signals`, `user_aggregates`,
   `revision_quality_scores`, `pipeline_metrics`) — are deleted, so the
   cutoff is exact to the second.

Every dropped partition and the deleted row counts are logged. All tables use
the same cutoff on their own time column, so an anomaly is never kept after
the windows it refers to, or the other way round. Parquet files are never
touched. Dropping a partition briefly takes an exclusive lock on the parent
table, which is fine at this scale.

`POSTGRES_RETENTION_DAYS` (default 14) is no longer read. Set
`POSTGRES_RETENTION_HOURS=336` to keep the old 14-day behaviour.

### Upgrading an existing database

No manual step is needed. The first `init_schema` after upgrading (any of
`make init-db`, starting the processing job, or the maintenance service)
converts the old unpartitioned `edit_events` and `page_activity_windows` in
one transaction: it renames each old table (with its indexes and id
sequence), creates the partitioned table and its partitions, copies every
row across, continues the id sequence and drops the old table. Copied rows
older than the retention window land in the default partition and are
deleted by the next purge — back up first if you want to keep them in
PostgreSQL (they are already in Parquet). The same run collapses duplicate
`anomaly_events` / `edit_war_signals` rows left by the old update-mode
query.

## Parquet storage

`storage/parquet_writer.py` writes `raw_events`, `activity_windows`,
`anomaly_events`, and `edit_war_signals` datasets to
`PARQUET_BASE_PATH/<dataset>/wiki=<wiki>/date=<YYYY-MM-DD>/hour=<HH>/part-*.parquet`
(UTC), Snappy-compressed, partitioned for cheap wiki/day/hour-scoped reads
without touching PostgreSQL. Files written before the `hour=` level was
added sit directly under `date=`; readers using Hive partitioning (such as
`pyarrow.dataset`) read both layouts. Parquet history is independent of the
PostgreSQL retention.

## Pipeline metrics

Both services write cumulative counters plus per-stage measurements to
`pipeline_metrics`; the `details` JSON column holds the stage measurements.
Latency summaries are p50 / p95 / p99 / max / mean in milliseconds. A
measurement that was not taken is stored as null and shown as "unknown",
never as 0.

`component = 'ingestion'`, written every 30 seconds by
`ingestion/sse_consumer.py`:

- `events_received` (SSE messages), `events_parsed` (normalized),
  `events_rejected` (malformed, sent to the DLQ), `events_published`
  (delivered, as confirmed by Kafka); `details.events_ignored` (filtered by
  wiki or type), `events_enqueued`, `events_dead_lettered`,
  `dead_letter_rate` (dead-lettered ÷ received), `delivery_failures`,
  `publish_failures`, `dead_letter_failures`, `producer_retries`,
  `reconnects`.
- `details.normalize_ms`: time to normalize one event.
- `details.ingestion_latency_ms`: ingestion time minus event time.
- `error_count`: delivery, publish and dead-letter failures.

`component = 'spark'`, written after each raw-event micro-batch by
`processing/streaming_job.py`:

- Counters and `throughput_per_sec` (events stored per second since the
  previous row); `processing_latency_ms` is the batch's median processing
  latency.
- `details.raw_batch`: rows, rejected (unparseable Kafka messages), time to
  collect / write PostgreSQL / write Parquet, Parquet files, bytes and
  compressed vs. uncompressed column bytes, and three latency summaries:
  `ingestion_latency_ms`, `processing_latency_ms` (ingestion time to the
  PostgreSQL write; includes Kafka buffering and waiting for the next
  trigger, so up to `SPARK_TRIGGER_INTERVAL`) and `end_to_end_latency_ms`
  (event time to the PostgreSQL write). Latencies are only recorded for
  batches whose PostgreSQL write succeeded.
- `details.window_batch`: windows evaluated, anomalies, edit-war signals,
  evaluation and persistence time, and `window_emit_delay_ms` (time from a
  window's end, in event time, to its results being stored — dominated by
  the watermark delay).
- `details.page_history`: active pages, evicted pages, stored window counts.
- `details.kafka_offsets_behind`: max/avg offsets behind the latest, as
  reported by Spark's Kafka source every 10 seconds; absent until Spark
  reports it.
- `details.driver_rss_mb`: resident memory of the Python driver process
  (the JVM is a separate process and not included).
- `details.errors_by_stage`: failures per write stage since start-up.

Limitations: Wikimedia timestamps have one-second resolution, so ingestion
and end-to-end latencies carry up to a second of rounding (and can be
slightly negative). Ingestion and processing latencies compare clocks of
two processes; on separate hosts, clock skew adds to them. Counters reset
when a service restarts, and a micro-batch Spark replays after a crash is
counted again.

## Dashboard

`make dashboard` (`python -m streamlit run dashboard/app.py`; plain
`streamlit run` does not put the project root on the import path) serves
the dashboard at http://localhost:8501. The sidebar sets the time range (15
minutes to 72 hours), wiki, a page-title search and human/bot editors; all
panels apply them together. Queries are cached for
`DASHBOARD_REFRESH_SECONDS` and re-run on that interval while
"Auto-refresh" is on. Times are UTC.

- System Health: one column each for ingestion, processing and detection
  state, from the latest `pipeline_metrics` rows — status (reporting,
  stale after 2 minutes of silence, or no data), throughput, dead-letter
  rate, latencies, Kafka offsets behind, PostgreSQL / Parquet write times
  and errors, page-history size and evictions — plus throughput and latency
  over time.
- Live Activity: edits per minute (human vs. bot), most active pages, and
  each page's edit velocity in its latest finalized window.
- Activity Anomalies: anomalous windows with their baseline median, MAD,
  score and detection type, anomalies per hour, and the highest median/MAD
  scores. Count-fallback scores are raw counts and are not ranked against
  median/MAD scores.
- Edit-War Signals: signals with both editors, mutual and total reverts and
  conflict score, signals per hour, and the revert events behind a chosen
  page.
- Historical Trends: edits per hour, a comparison of selected pages, and
  events per day from the Parquet history (counted from file footers, so no
  row data is loaded).

Every panel handles the "no data yet" case gracefully — a quiet pipeline
with no current anomalies or edit wars is a valid, correctly-displayed
state, not an error. If PostgreSQL is unreachable the page says so and
stops; a single failing query shows a warning in its panel only. All
queries are bounded by the time range and aggregated in PostgreSQL. The
Docker service mounts `./data/parquet` read-only for the history chart.

## Running locally (without Docker for the app services)

```bash
cp .env.example .env

# 1. Start infrastructure (Kafka + Postgres) via Docker Compose
make infra-up

# 2. Install Python dependencies
python -m venv .venv && source .venv/bin/activate
make setup

# 3. Initialize the database schema
make init-db

# 4. In separate terminals:
make ingest       # Wikimedia SSE -> Kafka
make process       # Spark Structured Streaming job
make dashboard     # Streamlit dashboard at http://localhost:8501

# 5. Periodically (the Docker setup does this for you):
make maintain-partitions   # create upcoming partitions, apply retention

# 6. (Optional) demo without live Wikimedia traffic:
make synthetic-normal
make synthetic-spike
make synthetic-editwar
```

## Running fully with Docker Compose

```bash
cp .env.example .env
docker compose up -d --build
# Dashboard: http://localhost:8501
```

The `processing` container needs a JVM (bundled in its Dockerfile) since
PySpark requires one; the compose file mounts `./data` so Parquet output
and Spark checkpoints persist across restarts. The `maintenance` container
runs partition maintenance and retention every
`POSTGRES_MAINTENANCE_INTERVAL_SECONDS`; it is the only scheduler, and
because every run starts from the database's current state, a restart
simply catches up. The ingestion container now also needs PostgreSQL, for
its metrics.

## Running tests

```bash
make setup
make test
# or directly:
pytest -v
```

Tests cover: event normalization and malformed-event handling
(`test_normalizer.py`), median/MAD calculations, the insufficient-history
fallback and page-history TTL eviction (`test_anomaly_detector.py`),
reciprocal-revert detection (`test_edit_war_detector.py`), bot/human
classification and window aggregation (`test_aggregations.py`),
configuration loading and validation (`test_config.py`), the event JSON
wire format (`test_models.py`), the Lift Wing disable switch
(`test_liftwing_client.py`), the streaming duration parser
(`test_duration_parsing.py`), window evaluation, latency and metrics
helpers (`test_streaming_job.py`), ingestion routing and metrics
(`test_sse_consumer.py`), percentiles (`test_stats.py`), Parquet layout and
statistics (`test_parquet_writer.py`), partition date arithmetic
(`test_partitions.py`), dashboard queries (`test_dashboard_queries.py`) and
the benchmark package (`test_benchmark.py`). These use hand-built inputs,
an injectable clock and fakes for Kafka and PostgreSQL, and need nothing
running.

Two groups need infrastructure and are skipped when it is missing:

- `postgres` (`test_database_integration.py`,
  `test_dashboard_integration.py`): partition creation and idempotency,
  retention, migration of an unpartitioned database, duplicate handling,
  every dashboard query, and full renders of the Streamlit page. They need
  a disposable database whose name ends in `_test`; the tests drop and
  recreate its `public` schema.

  ```bash
  createdb wikipulse_test
  WIKIPULSE_TEST_POSTGRES_DB=wikipulse_test pytest -m postgres
  ```

  Host, port, user and password come from the usual `POSTGRES_*` variables.

- `spark` (`test_streaming_windows.py`): runs the real windowed query on a
  local Spark session and checks each window reaches the writer once with
  its final count. Needs Java 8, 11 or 17. On Windows, Spark's local file
  source also needs Hadoop's `winutils.exe`, so run it on Linux/macOS or in
  a container.

## Generating synthetic events

```bash
python -m scripts.generate_synthetic_events --scenario normal --duration 120
python -m scripts.generate_synthetic_events --scenario spike --page "Synthetic Demo Page" --count 40
python -m scripts.generate_synthetic_events --scenario edit_war --page "Synthetic Demo Page" --rounds 6
```

- `spike` publishes a fast burst of edits on one page — should trigger
  activity-anomaly detection once enough windows exist.
- `edit_war` publishes alternating reverts between two synthetic editors on
  one page — should trigger the mutual-revert edit-war detector within a
  single five-minute window.

## Benchmarks

```bash
make benchmark                                   # all scenarios, in-process stages
make benchmark BENCHMARK_ARGS="--postgres"       # also time PostgreSQL writes
make benchmark BENCHMARK_ARGS="--kafka"          # also time a Kafka round trip
python -m benchmark.runner --scenarios burst edit_war --events 5000 --slide-seconds 30
```

`benchmark/runner.py` generates seeded synthetic workloads
(`benchmark/workloads.py`) as raw Wikimedia payloads and runs them through
the project's own code, stage by stage:

| Stage | Code exercised |
|---|---|
| ingestion | JSON decode, wiki/type filter, normalization, dead-letter routing, JSON serialization (`WikimediaSSEConsumer.process_sse_data`, in-memory publisher) |
| detection | `evaluate_windows`: aggregation, median/MAD, reciprocal reverts, page-history store with TTL |
| parquet | `ParquetWriter.write_records` in micro-batch-sized batches |
| postgres (opt-in) | `Database.upsert_edit_events` / `upsert_activity_windows` against the configured database; rows use `wiki = benchmarkwiki` and are deleted afterwards |
| kafka (opt-in) | `EventPublisher` to `<KAFKA_TOPIC_EDITS>-benchmark` and back through a consumer |

Scenarios: `normal` (steady edits over 2,000 pages), `burst` (plus 200 edits
on one page in a minute), `edit_war` (plus two editors reverting each other
30 times), `multi_page` (Zipf-distributed popularity over 5,000 pages),
`malformed` (10% broken payloads, 10% filtered), and `sustained` (half the
edits on never-seen pages, spread over three page-history TTLs of simulated
time to exercise eviction). `--events`, `--rate` (simulated events per
second), `--seed`, `--batch-size` and `--slide-seconds` default to the
`BENCHMARK_*` settings and `SPARK_SLIDE_DURATION`.

Method and limits:

- These are component benchmarks. They do not use the live SSE connection,
  Spark, or the network between containers, so their throughput is not the
  pipeline's end-to-end throughput, and nothing here says anything about
  live Wikimedia traffic. End-to-end latency of a running pipeline is in
  `pipeline_metrics` (see Pipeline metrics), for example while
  `make synthetic-normal` feeds it.
- Event time is simulated and nothing sleeps. The detection stage emulates
  Spark's sliding windows, watermark and append mode in Python
  (`benchmark/windows.py`) and runs the page-history TTL on simulated time.
- Each run checks that the `burst` and `edit_war` patterns are detected and
  that no window is evaluated twice.
- Elapsed times use `time.perf_counter`. Memory per page is measured with
  `tracemalloc` on a separate store; process RSS before and after the
  detection stage is reported as-is (it includes everything else the
  process holds).
- The workload start is aligned to a whole hour, so the same seed and flags
  give the same windows and detections on every run.
- Per-event warnings from the ingestion code (one per malformed payload)
  are not logged during the run, so console output does not dominate the
  timings.

Each run writes `results.json` and `summary.md` to
`BENCHMARK_OUTPUT_DIR/<run id>/` (default `./data/benchmarks`). A run is
compared with the most recent earlier run, or with `--baseline
path/to/results.json`, only when their configuration is identical; the
summary lists the change in throughput and p95 latencies and notes whether
the environment differed. On the development machine two identical runs
differed by up to about 8% in throughput and much more in PostgreSQL p95
batch time, so treat smaller changes as noise.

## Analytical SQL

See `sql/`: most active pages, pages with most anomalies, highest edit
velocity, most mutual reverts, highest edit-war signal, bot vs. human
activity, activity trend over time, anomaly frequency over time, top
editors by activity, recent anomalies, and pipeline throughput/latency —
run any of them directly against the `wikipulse` database (e.g.
`psql -f sql/most_active_pages.sql`).

## Troubleshooting

- **`docker compose up` fails on Kafka health check** — give the container
  more time on first boot (KRaft cluster metadata initialization); rerun
  `docker compose up -d` after ~30s.
- **No data in the dashboard** — check `make ingest` and `make process` are
  both running, and that `pipeline_metrics` has rows
  (`select * from pipeline_metrics order by recorded_at desc limit 5;`). Use
  the synthetic generator scenarios to rule out a live-traffic-specific
  issue.
- **Spark can't find the Kafka connector** — the streaming job requests
  `spark-sql-kafka-0-10` via `spark.jars.packages`; the first run needs
  internet access to fetch it from Maven, and it's cached afterward.
- **Lift Wing scores never appear** — this is expected when
  `LIFTWING_ENABLED=false` (the default). Set it to `true` in `.env` to
  enable it; failures there are logged but never fatal to the pipeline.
- **Anomalies never fire** — a page needs at least
  `ANOMALY_MIN_HISTORY_WINDOWS` (default 8) prior five-minute windows of
  history before the median/MAD path activates; before that, it uses the
  count-based fallback threshold instead. Use `--scenario spike` to force a
  visible signal quickly via the fallback path.
- **PostgreSQL growing too large** — check the `maintenance` container is
  running (`docker compose logs maintenance`), or run `make purge` to apply
  `POSTGRES_RETENTION_HOURS` now; the full history remains in Parquet
  regardless.
- **Rows piling up in `edit_events_default`** — partition maintenance has
  not run for longer than `POSTGRES_PARTITION_HORIZON_DAYS`. Run
  `make maintain-partitions`; it creates the missing partitions and moves
  the rows into them.
- **No window results for a few minutes after start-up** — expected: a
  window is evaluated only after the watermark passes its end, about
  `SPARK_WINDOW_DURATION + SPARK_WATERMARK_DELAY` after its first edit.
- **`ModuleNotFoundError: No module named 'common'` from the dashboard** —
  start it with `make dashboard` (`python -m streamlit run ...`) from the
  project root, not `streamlit run`.
- **Processing refuses to start with an `ANOMALY_HISTORY_TTL_SECONDS`
  error** — the TTL must be at least the window duration plus the
  watermark delay.
