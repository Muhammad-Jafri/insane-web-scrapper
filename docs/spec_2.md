# Spec 2 — Async Web Scraper Architecture

---

## System overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   Clients   │────▶│  API Layer  │────▶│    Redis Queue      │
└─────────────┘     └─────────────┘     └─────────────────────┘
                           │                        │
                    ┌──────▼──────┐         ┌───────▼────────┐
                    │  PostgreSQL │         │  Worker Pool   │
                    │  (job state)│◀────────│  (N processes) │
                    └─────────────┘         │  (M coroutines │
                                            │   per process) │
                                            └───────┬────────┘
                                                    │
                                         ┌──────────▼──────────┐
                                         │   S3 / MinIO        │
                                         │  (HTML + images)    │
                                         └─────────────────────┘
```

---

## Design decisions and rationale

### 1. Async from day one — no sync phase

**Decision:** Workers are async (`asyncio`) from the first line of code.

**Why:** The original spec staged sync in phase 4a and async in 4b, treating async as an upgrade.
But rewriting a sync worker to async is not additive — it touches every I/O call. Building sync
first just creates throwaway code. Since the workload is almost entirely I/O-bound (HTTP fetches,
S3 uploads, Postgres writes), async is the right model from the start.

---

### 2. M coroutines per worker process, set via env var

**Decision:** Each worker process spawns `WORKER_CONCURRENCY` coroutines (default: 10), all
running the same `while True` pop-and-process loop concurrently.

**Why:** A single coroutine processes one job at a time — while it awaits an HTTP response, the
process sits idle. M coroutines overlap that idle time: coroutine 2 pops and starts fetching while
coroutine 1 is mid-flight waiting on a server response. This multiplies throughput without
multiplying processes (no extra memory, no extra Postgres connections per slot).

`WORKER_CONCURRENCY` is an env var rather than hardcoded because the right value depends on
network latency and target site response times — tunable without a code change.

```
process (1 OS process):
  coroutine 1: pop → await HTTP... → await S3... → await DB...
  coroutine 2:       pop → await HTTP...          → await S3... → await DB...
  coroutine 3:             pop → await HTTP...                  → await S3...
  ...
```

---

### 3. Domain concurrency — two-gate approach

**Decision:** When a domain is at its concurrency limit, the coroutine waits rather than
re-enqueueing the job.

**Why:** Re-enqueue has a safety hole — the job is already popped from Redis when we discover the
domain is busy. If the process crashes between pop and re-enqueue, the job is silently lost.
Waiting avoids this: the job stays claimed by the coroutine and Postgres already has it in
`RUNNING` state.

Two gates enforce the cap at different scopes:

**Gate 1 — local `asyncio.Semaphore` (in-process):**
Prevents any single worker process from exceeding `MAX_CONCURRENCY_PER_DOMAIN` on its own.
Fast — no network round-trip. Coroutine suspends here if the process is already at the cap,
freeing the event loop to run other coroutines on different domains.

**Gate 2 — Redis counter (cross-process):**
Enforces the global cap across all N worker containers. Uses a Lua script to atomically
increment and check the counter so no two workers can race past the limit. A TTL of
`fetch_timeout × 2` auto-releases the slot if a worker crashes without decrementing.
If Redis says the domain is at capacity, the coroutine polls every 1s until a slot is free.

```python
async with local_semaphore[domain]:              # gate 1 — in-process
    while not await acquire_domain_slot(domain): # gate 2 — cross-process
        await asyncio.sleep(1)
    try:
        ... process job ...
    finally:
        await release_domain_slot(domain)
```

The `release_domain_slot` call is in a `finally` block — the Redis counter is always decremented
even if the job fails or raises unexpectedly. A guard in `release_domain_slot` also resets the
counter if it drifts below zero (e.g. from a double-release edge case).

---

### 4. S3 via boto3 + asyncio.to_thread

**Decision:** Use the standard `boto3` library wrapped in `asyncio.to_thread()` rather than
`aioboto3`.

**Why:** `aioboto3` is a thin community wrapper around `boto3` that lags behind on releases and
adds a dependency with a small maintenance surface. `asyncio.to_thread` offloads the blocking
boto3 call to the default thread pool — the event loop is not blocked and continues running other
coroutines while the upload happens in a thread.

This is not truly non-blocking at the OS level (a thread is still sitting waiting on the socket),
but it is non-blocking from the event loop's perspective, which is what matters. At realistic
concurrency (10–20 simultaneous uploads), the default thread pool size
(`min(32, cpu_count + 4)`) is never a bottleneck.

```python
await asyncio.to_thread(s3_client.put_object, Bucket=bucket, Key=key, Body=html)
```

---

### 5. Native async for everything else

**Decision:** All other I/O uses natively async libraries.

| Concern     | Library         | Why |
|-------------|-----------------|-----|
| HTTP        | `httpx`         | Native async client, same API as requests, built-in timeout/retry hooks |
| PostgreSQL  | `asyncpg`       | Fastest async Postgres driver for Python, binary protocol |
| Redis       | `redis.asyncio` | Official async support in the redis-py package, no extra dep |
| S3 / MinIO  | `boto3` + `asyncio.to_thread` | See decision 4 above |

Unlike `boto3`, these libraries expose proper `async/await` interfaces — no thread is spawned,
the event loop handles the socket directly via epoll/selectors. This is the right model for
operations that happen on every job (HTTP fetch, DB write) where spawning threads would add up.

---

## Components in detail

### API server

Stateless FastAPI app. Responsibilities:
- Validate incoming job requests
- Check queue depth — reject `429` if above `QUEUE_DEPTH_LIMIT`
- Atomically insert job row in Postgres + push to Redis (Postgres is source of truth)
- Serve job status queries

The API never touches scraper code. It knows nothing about HTTP fetching or HTML parsing.

### Redis

Two purposes:

**Job queue** — a Redis `LIST`. Workers `BRPOP` from the left, API `RPUSH` to the right.
FIFO by default. `BRPOP` is atomic — two coroutines (even across processes) cannot pop the
same job.

**Per-domain counter** — a Redis `STRING` with expiry used as a cross-process concurrency
counter. Incremented before fetching, decremented after. Works in tandem with the local
`asyncio.Semaphore` (local gate runs first, Redis gate enforces the global cap across all
worker processes).

### Worker pool

N worker processes, each running M coroutines. A background `_queue_depth_poller` coroutine
updates the Prometheus queue depth gauge every 5s. Every job coroutine runs this loop:

```python
async def coroutine_worker(...):
    while True:
        raw = await redis.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT)
        if raw is None:
            continue

        job = await get_job(pool, job_id)
        domain = urlparse(job.url).hostname

        async with local_semaphore[domain]:          # gate 1 — in-process
            while not await acquire_domain_slot(...): # gate 2 — cross-process Redis
                await asyncio.sleep(1)

            try:
                active_jobs.inc()
                html = await fetch_url(http_client, job.url)
                key  = await upload_html(s3, html, job.id)
                data = parse_html(html, job.url)      # extracts title/links/word_count/image_urls
                image_keys = await upload_images(s3, http_client, data.pop("image_urls"), job.id)
                data["image_keys"] = image_keys
                await mark_job_done(pool, job.id, key, data)
            except RetryableError as e:
                await _handle_retry(...)
            except FatalError as e:
                await mark_job_dead(...)
            finally:
                active_jobs.dec()
                await release_domain_slot(...)
```

### PostgreSQL

Two tables (see data model). Postgres is the single source of truth. If Redis dies, the queue
is reconstructable from rows with `status = PENDING`.

### MinIO (local) / S3 (production)

Workers upload two types of blobs after scraping. Postgres stores the object keys, not the content.
MinIO is S3-compatible — swapping to real S3 requires only changing the endpoint URL.

| Blob | Key pattern | Content-Type |
|------|-------------|--------------|
| Raw HTML | `html/{job_id}.html` | `text/html` |
| Page images | `images/{job_id}/{n}.{ext}` | from response header |

Images are capped at 20 per page. Data URLs (`data:image/...`) are skipped. If an individual
image fetch fails it is silently skipped — the job does not fail because of a broken image.

---

## Data model

### `jobs` table

```sql
CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url             TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'PENDING',
                    -- PENDING | RUNNING | DONE | FAILED | DEAD
    priority        INTEGER NOT NULL DEFAULT 0,
    retries         INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    next_retry_at   TIMESTAMPTZ,
    error_message   TEXT,
    raw_html_key    TEXT,
    result          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    worker_id       TEXT
);

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_next_retry_at ON jobs(next_retry_at) WHERE status = 'PENDING';
```

Status transitions:

```
PENDING → RUNNING → DONE
                 ↘
                  FAILED → (retry) → PENDING
                         → (max retries exceeded) → DEAD
```

### `job_events` table (audit log)

```sql
CREATE TABLE job_events (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID NOT NULL REFERENCES jobs(id),
    event       TEXT NOT NULL,
    worker_id   TEXT,
    message     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_events_job_id ON job_events(job_id);
```

---

## API contract

### Submit a job
```
POST /jobs
{ "url": "https://example.com/page", "priority": 0 }

→ 202 Accepted
{ "job_id": "...", "status": "PENDING", "created_at": "..." }

→ 429 Too Many Requests   (queue depth exceeded)
→ 422 Unprocessable       (invalid URL)
```

### Poll job status
```
GET /jobs/{job_id}

→ 200 OK
{
  "job_id": "...",
  "url": "...",
  "status": "DONE",
  "retries": 1,
  "result": { "title": "...", "links": [...], "word_count": 1420, "image_keys": ["images/..."] },
  "created_at": "...",
  "started_at": "...",
  "completed_at": "..."
}
```

### Bulk submit
```
POST /jobs/bulk
{ "urls": ["https://...", "https://..."] }

→ 202 Accepted
{ "job_ids": ["...", "..."], "enqueued": 2, "rejected": 0 }
```

### Queue health
```
GET /internal/health
{
  "queue_depth": 142,
  "workers_active": 8,
  "jobs_last_60s": { "done": 94, "failed": 3 },
  "queue_limit": 10000
}
```

---

## Worker retry logic

```python
BACKOFF = [5, 25, 125]  # seconds

async def _handle_retry(pool, redis_client, job, error_msg, worker_id):
    if job.retries >= job.max_retries:
        await mark_job_dead(pool, job.id, error_msg)
    else:
        delay = BACKOFF[min(job.retries, len(BACKOFF) - 1)]
        await mark_job_pending_retry(pool, job.id, error_msg, next_retry_at)
        asyncio.create_task(_delayed_reenqueue(redis_client, pool, job.id, delay))
```

`_delayed_reenqueue` sleeps for `delay` seconds, resets status to `PENDING`, then pushes the
job back to Redis. This is a phase 2 in-process implementation — if the worker crashes during
the sleep the retry is lost. Phase 3 replaces this with a durable Redis `ZADD` sorted set.

Retryable: connection timeout, 429, 500, 502, 503.
Fatal: 404, 403, 410, any other 4xx/5xx, unparseable HTML.

---

## Environment variables

| Variable                    | Default | Purpose |
|-----------------------------|---------|---------|
| `WORKER_CONCURRENCY`        | `10`    | Coroutines per worker process |
| `MAX_CONCURRENCY_PER_DOMAIN`| `3`     | Local semaphore cap per hostname |
| `QUEUE_DEPTH_LIMIT`         | `10000` | API rejects submissions above this |
| `BRPOP_TIMEOUT`             | `5`     | Seconds a coroutine blocks waiting for work |
| `FETCH_TIMEOUT`             | `15`    | HTTP request timeout in seconds |
| `METRICS_PORT`              | `9090`  | Prometheus metrics HTTP server port (worker) |
| `DATABASE_URL`              | —       | asyncpg connection string |
| `REDIS_URL`                 | —       | Redis connection string |
| `S3_ENDPOINT_URL`           | —       | MinIO endpoint (omit for real AWS S3) |
| `S3_BUCKET`                 | —       | Bucket name for raw HTML and images |
| `S3_ACCESS_KEY`             | —       | MinIO / S3 access key |
| `S3_SECRET_KEY`             | —       | MinIO / S3 secret key |

---

## Logging

Structured JSON line logging via `app/logging_config.py`. Called once at process start from
`main.py` (API) and `worker.py` (worker).

| Handler | Level | Destination |
|---------|-------|-------------|
| `RotatingFileHandler` | INFO | `logs/app.log`, rotates at 10 MB, keeps 5 backups |
| `StreamHandler` | WARNING | stdout/stderr |

Every log line is a JSON object with at minimum `ts`, `level`, `logger`, `msg`. Context fields
are passed via `extra={}` and appear flat in the JSON:

```json
{"ts": "2026-05-16T10:00:00Z", "level": "INFO", "logger": "app.worker", "msg": "job done", "job_id": "...", "total_ms": 423}
```

Noisy library loggers (`asyncpg`, `httpx`, `boto3`, etc.) are set to WARNING to prevent
log noise.

---

## Prometheus metrics

The API mounts a `/metrics` endpoint via `prometheus_client.make_asgi_app()`. The worker runs
a separate `start_http_server(METRICS_PORT)` in a background thread (default port 9090).
Prometheus scrapes both every 5 seconds.

| Metric | Type | Description |
|--------|------|-------------|
| `scraper_jobs_total{status}` | Counter | Jobs by final status: `done`, `retried`, `dead` |
| `scraper_jobs_enqueued_total` | Counter | Jobs submitted via the API |
| `scraper_job_duration_seconds` | Histogram | End-to-end job processing time |
| `scraper_fetch_duration_seconds` | Histogram | HTTP fetch duration per job |
| `scraper_domain_wait_seconds` | Histogram | Time waiting for a domain concurrency slot |
| `scraper_queue_depth` | Gauge | Current Redis queue depth (polled every 5s) |
| `scraper_active_jobs` | Gauge | Jobs currently being processed |

Grafana datasource and dashboard are both auto-provisioned on container start via files in
`monitoring/grafana/provisioning/`.

**Multi-worker scraping:** Prometheus uses Docker SD (`docker_sd_configs` with
`unix:///var/run/docker.sock`) to auto-discover all running worker containers. Targets
are filtered by the `com.docker.compose.service=worker` label and each container's
hostname is used as the `instance` label. No config change is needed when scaling N up
or down.

### Grafana dashboard — panels

Dashboard uid: `scraper-main`. Auto-refreshes every 10s. Layout is a 24-column grid.

**Row 1 — Overview stats (h=4)**

| Panel | Type | Query |
|-------|------|-------|
| Active Jobs | Stat | `scraper_active_jobs` |
| Queue Depth | Stat | `scraper_queue_depth` |
| Total Enqueued | Stat | `scraper_jobs_enqueued_total` |
| Dead Jobs Total | Stat (red threshold ≥1) | `scraper_jobs_total{status="dead"}` |

**Row 2 — Queue & throughput (h=8)**

| Panel | Type | Query |
|-------|------|-------|
| Queue Depth Over Time | Time series | `scraper_queue_depth` |
| Job Rates (per second) | Time series | `rate(scraper_jobs_total{status=~"done\|retried\|dead"}[2m])` |

**Row 3 — Latency (h=8)**

| Panel | Type | Query |
|-------|------|-------|
| Fetch Duration | Time series | `histogram_quantile(0.5/0.95/0.99, rate(...bucket[2m]))` |
| Job Duration | Time series | `histogram_quantile(0.5/0.95/0.99, rate(...bucket[2m]))` |

**Row 4 — Domain concurrency (h=8)**

| Panel | Type | Query |
|-------|------|-------|
| Domain Concurrency Wait (p95) | Time series | `histogram_quantile(0.95, rate(scraper_domain_wait_seconds_bucket[2m]))` |

---

## Scaling model

| Knob | What it controls |
|---|---|
| `NUM_WORKERS` (N) | Number of worker containers (`deploy.replicas` in docker-compose) |
| `WORKER_CONCURRENCY` (M) | Coroutines per worker process — tune for network latency |
| `MAX_CONCURRENCY_PER_DOMAIN` | Politeness cap per hostname (local + Redis gate) |
| API instances | HTTP read/write throughput |
| `QUEUE_DEPTH_LIMIT` | Backpressure at the API layer |

Total concurrent in-flight fetches = N containers × M coroutines. Scale M first (cheap,
no extra Postgres connections), then N when M hits diminishing returns (CPU-bound HTML
parsing becomes the bottleneck). Each container is identified by its Docker hostname
(`socket.gethostname()`) in logs and job events.

---

## Build phases

| Phase | What gets built |
|---|---|
| ✅ 1 | docker-compose (Postgres + Redis + MinIO), DB schema, migrations |
| ✅ 2 | Async worker — M coroutines, two-gate domain lock (local + Redis), basic retry, FastAPI routes, structured logging, image scraping |
| ✅ 3 (partial) | Docker SD for multi-worker Prometheus scraping, `NUM_WORKERS` horizontal scaling via `deploy.replicas` |
| 3 (remaining) | Delayed retry queue (Redis `ZADD` sorted set by timestamp), `'retried'` job event |
| ✅ 4 | Prometheus `/metrics` on API + worker, queue depth gauge, job counters/histograms, Grafana + Prometheus in docker-compose |
