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

### 3. Domain semaphore — local asyncio.Semaphore, not re-enqueue

**Decision:** When a domain is at its concurrency limit, the coroutine waits
(`await semaphore.acquire()`) rather than pushing the job back to Redis and moving on.

**Why:** The alternative (re-enqueue) has a safety hole — the job is already popped from Redis
when we discover the domain is busy. Between the pop and the re-enqueue, if the process crashes,
the job is silently lost. Waiting avoids this: the job stays claimed by the coroutine, Postgres
already has it in `RUNNING` state, and recovery is straightforward.

The wait is not expensive — a suspended coroutine releases the event loop immediately. Other
coroutines (on different domains) keep running normally. The only downside is that if all M
coroutines happen to be waiting on the same domain, no new jobs get popped. This is mitigated by
setting M high enough relative to your domain diversity, and addressed properly with priority
queues in a later phase if needed.

Domain semaphores are local (in-process) `asyncio.Semaphore` objects, keyed by hostname.

**Current implementation (phase 2):** only the local `asyncio.Semaphore` is in place. This works
correctly with a single worker process. With N worker containers, each process has its own
semaphore, so the effective per-domain concurrency becomes `N × MAX_CONCURRENCY_PER_DOMAIN`
rather than the intended cap. Running `docker-compose up --scale worker=3` with a cap of 3 would
allow 9 simultaneous requests to the same domain.

**Planned (spec 3):** a Redis `INCR`/`DECR` counter with TTL will be added as a cross-process
gate. The local semaphore remains as a fast in-process first check; the Redis counter enforces the
global cap across all worker processes.

```python
# per-process, per-domain
domain_semaphores: dict[str, asyncio.Semaphore] = {}

def get_semaphore(domain: str) -> asyncio.Semaphore:
    if domain not in domain_semaphores:
        domain_semaphores[domain] = asyncio.Semaphore(MAX_CONCURRENCY_PER_DOMAIN)
    return domain_semaphores[domain]
```

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

N worker processes, each running M coroutines. Every coroutine runs this loop:

```python
async def coroutine_worker(worker_id: str, semaphores: dict):
    while True:
        raw = await redis.brpop(QUEUE_KEY, timeout=5)
        if raw is None:
            continue

        job = parse_job(raw)
        domain = extract_domain(job.url)
        semaphore = get_or_create_semaphore(semaphores, domain)

        async with semaphore:
            await update_status(job.id, "RUNNING", worker_id)
            try:
                html = await fetch(job.url)
                key  = await upload_html(html, job.id)
                data = parse(html)
                await mark_done(job.id, key, data)
            except RetryableError as e:
                await handle_retry(job, e)
            except FatalError as e:
                await mark_dead(job.id, str(e))


async def run_worker(worker_id: str):
    semaphores: dict[str, asyncio.Semaphore] = {}
    await asyncio.gather(*[
        coroutine_worker(worker_id, semaphores)
        for _ in range(WORKER_CONCURRENCY)
    ])
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

async def handle_retry(job, error):
    if job.retries >= job.max_retries:
        await mark_dead(job.id, str(error))
    else:
        delay = BACKOFF[job.retries]
        await mark_failed_for_retry(job.id, str(error), delay)
        # re-enqueued by the delayed retry scheduler (phase 3)
```

Retryable: connection timeout, 429, 500, 503.
Fatal: 404, 403, malformed URL, unparseable HTML.

---

## Environment variables

| Variable                    | Default | Purpose |
|-----------------------------|---------|---------|
| `WORKER_CONCURRENCY`        | `10`    | Coroutines per worker process |
| `MAX_CONCURRENCY_PER_DOMAIN`| `3`     | Local semaphore cap per hostname |
| `QUEUE_DEPTH_LIMIT`         | `10000` | API rejects submissions above this |
| `BRPOP_TIMEOUT`             | `5`     | Seconds a coroutine blocks waiting for work |
| `FETCH_TIMEOUT`             | `15`    | HTTP request timeout in seconds |
| `DATABASE_URL`              | —       | asyncpg connection string |
| `REDIS_URL`                 | —       | Redis connection string |
| `S3_ENDPOINT_URL`           | —       | MinIO endpoint (omit for real AWS S3) |
| `S3_BUCKET`                 | —       | Bucket name for raw HTML |

---

## Scaling model

| Knob | What it controls |
|---|---|
| API instances | HTTP read/write throughput |
| Worker processes (N) | Parallelism across CPU cores; also multiplies total coroutines |
| `WORKER_CONCURRENCY` (M) | In-flight jobs per process — tune for network latency |
| `MAX_CONCURRENCY_PER_DOMAIN` | Politeness cap per hostname |
| `QUEUE_DEPTH_LIMIT` | Backpressure at the API layer |

Total concurrent in-flight fetches = N processes × M coroutines. Scale M first (cheap),
then N when M hits diminishing returns (CPU-bound HTML parsing becomes the bottleneck).

---

## Build phases

| Phase | What gets built |
|---|---|
| 1 | docker-compose (Postgres + Redis + MinIO), DB schema, migrations |
| 2 | Async worker — M coroutines, domain semaphore, basic retry, FastAPI routes |
| 3 | Delayed retry queue (Redis `ZADD` sorted set by timestamp), Redis cross-process domain counter, `'retried'` job event |
| 4 | Prometheus `/metrics`, queue depth alerts, worker heartbeat |
