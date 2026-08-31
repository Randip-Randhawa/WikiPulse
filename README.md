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
  SSE payloads, so they are never silently dropped.

## Event schema

See `common/models.py` (`WikipediaEvent`). Key fields: `event_id`,
`timestamp`, `wiki`, `event_type`, `page_title`, `page_id`, `namespace`,
`revision_id`, `previous_revision_id`, `user`, `user_id`, `is_bot`,
`comment`, `byte_length_new/old/delta`, `is_revert`,
`revert_target_revision_id`, `ingestion_timestamp`. Every field Wikimedia
does not guarantee is `Optional` and normalized safely from missing/null
raw data (`ingestion/normalizer.py`).

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

## Median/MAD anomaly detection (and why not z-score)

The original project draft (Lab 1) proposed a plain z-score threshold. Lab 2
explicitly replaces this because a handful of extremely active pages (e.g.
breaking-news articles) would otherwise distort a mean/stddev-based
baseline for every other page. Instead, `processing/anomaly_detector.py`
computes, per page:

```
median = median(recent window edit counts)
MAD    = median(|edit_count_i - median|)
score  = 0.6745 * (current_edit_count - median) / MAD
```

An anomaly is flagged when `score >= ANOMALY_MAD_THRESHOLD` (default 3.5).
The `0.6745` constant is the standard scaling factor that makes this
"modified z-score" comparable to a classic z-score under normality.

### Count-based fallback for insufficient history

A brand-new or rarely-edited page has no reliable statistical baseline. If
a page has fewer than `ANOMALY_MIN_HISTORY_WINDOWS` (default 8) prior
observations, WikiPulse instead uses a simple, configurable count-based
rule: flag if `edit_count >= ANOMALY_FALLBACK_EDIT_COUNT_THRESHOLD` (default
15). This is explicitly required by Lab 2 (see project spec Section 5) and
is implemented and unit-tested separately from the median/MAD path.

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
(`edit_war_flag`) when `mutual_revert_count >= EDIT_WAR_MUTUAL_REVERT_THRESHOLD`
or `conflict_score >= EDIT_WAR_FLAG_SCORE_THRESHOLD`. This is a **signal**,
not a definitive judgement, and WikiPulse never automatically reverts
anything on Wikipedia.

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

A retention/purge mechanism (`storage/database.py:Database.purge_old_data`,
runnable via `make purge` / `scripts/purge_old_data.py`) deletes rows older
than `POSTGRES_RETENTION_DAYS` (default 14) from every high-volume table, so
PostgreSQL stays bounded for live dashboard queries. Long-term history is
retained in Parquet instead.

## Parquet storage

`storage/parquet_writer.py` writes `raw_events`, `activity_windows`,
`anomaly_events`, and `edit_war_signals` datasets to
`PARQUET_BASE_PATH/<dataset>/wiki=<wiki>/date=<YYYY-MM-DD>/part-*.parquet`,
partitioned for cheap date/wiki-scoped reads without touching PostgreSQL.

## Dashboard

`streamlit run dashboard/app.py` shows five panels: System Health, Live
Activity, Anomalies, Edit Wars, and Historical Trends. Every panel handles
the "no data yet" case gracefully — a quiet pipeline with no current
anomalies or edit wars is a valid, correctly-displayed state, not an error.

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

# 5. (Optional) demo without live Wikimedia traffic:
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
and Spark checkpoints persist across restarts.

## Running tests

```bash
make setup
make test
# or directly:
pytest -v
```

Tests cover: event normalization and malformed-event handling
(`test_normalizer.py`), median/MAD calculations and the insufficient-history
fallback (`test_anomaly_detector.py`), reciprocal-revert detection
(`test_edit_war_detector.py`), bot/human classification and window
aggregation (`test_aggregations.py`), configuration loading
(`test_config.py`), the event JSON wire format (`test_models.py`), the
Lift Wing disable switch (`test_liftwing_client.py`), and the streaming
duration parser (`test_duration_parsing.py`). All tests run against
deterministic, hand-built inputs — none depend on the live Wikimedia stream
or a running Kafka/Postgres instance.

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
- **PostgreSQL growing too large** — run `make purge` (or schedule
  `scripts/purge_old_data.py`) to enforce `POSTGRES_RETENTION_DAYS`; the
  full history remains in Parquet regardless.
