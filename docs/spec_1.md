Let's design this properly. Full architecture — components, data model, API contract, worker internals, and scaling model.

---

## System overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────┐
│   Clients   │────▶│  API Layer  │────▶│    Redis Queue      │
└─────────────┘     └─────────────┘     └─────────────────────┘
                           │                        │
                    ┌──────▼──────┐         ┌───────▼────────┐
                    │  PostgreSQL │         │  Worker Pool   │
                    │  (job state)│◀────────│  (N instances) │
                    └─────────────┘         └───────┬────────┘
                                                    │
                                         ┌──────────▼──────────┐
                                         │   S3 / Object Store │
                                         │   (raw HTML blobs)  │
                                         └─────────────────────┘
```

---

## Components in detail

### API server
Stateless FastAPI app. Multiple instances sit behind a load balancer (nginx or an ALB). Responsibilities:
- Validate incoming job requests
- Check queue depth — reject with `429` if above threshold
- Atomically insert job row + push to Redis in a single logical operation (use Postgres as source of truth, Redis as the signal)
- Serve job status queries

Critically: **the API never touches the scraper code**. It knows nothing about HTTP fetching or HTML parsing.

### Redis (queue + coordination)
Two purposes:

**Job queue** — a Redis `LIST`. Workers `BRPOP` from the left, API `RPUSH` to the right. FIFO by default. `BRPOP` is atomic — two workers cannot pop the same job. This is your horizontal scaling primitive.

**Per-domain semaphore** — a Redis `STRING` with expiry used as a counter. Before fetching a domain, a worker does `INCR domain:semaphore:{domain}` and checks the value. If it exceeds your concurrency limit (e.g. 3), the job is re-enqueued with a delay. This prevents hammering a single site.

### Worker pool
N identical, stateless worker processes. Each runs this loop forever:

```
while true:
    job = BRPOP queue (blocking, timeout 5s)
    if no job: continue

    acquire domain semaphore
    update job status → RUNNING in Postgres
    fetch URL (httpx, async, timeout 15s)
    parse HTML (BeautifulSoup)
    upload raw HTML → S3
    store extracted data → Postgres
    update job status → DONE
    release domain semaphore
```

Workers are completely independent. No shared in-process state. You can kill and restart any worker at any time without data loss because job state lives in Postgres.

### PostgreSQL
Two tables — see data model below. Postgres is the single source of truth. Redis is a performance layer, not a database. If Redis dies, you can reconstruct the queue from jobs with `status = PENDING`.

### S3 / object store
Workers upload raw HTML blobs here after scraping. Postgres stores the S3 key, not the content. This keeps your DB rows small and lets you reprocess raw HTML later without re-scraping.

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
    raw_html_key    TEXT,          -- S3 object key
    result          JSONB,         -- extracted data
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    worker_id       TEXT           -- which worker instance claimed this
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
    event       TEXT NOT NULL,   -- 'enqueued' | 'started' | 'completed' | 'failed' | 'retried' | 'dead'
    worker_id   TEXT,
    message     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_job_events_job_id ON job_events(job_id);
```

This gives you a full history per job. Invaluable for debugging "why did this job fail 3 times."

---

## API contract

### Submit a job
```
POST /jobs
Content-Type: application/json

{ "url": "https://example.com/page", "priority": 0 }

→ 202 Accepted
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PENDING",
  "created_at": "2026-05-16T10:00:00Z"
}

→ 429 Too Many Requests   (queue depth exceeded)
→ 422 Unprocessable       (invalid URL)
```

### Poll job status
```
GET /jobs/{job_id}

→ 200 OK
{
  "job_id": "...",
  "url": "https://example.com/page",
  "status": "DONE",
  "retries": 1,
  "result": { "title": "...", "links": [...], "word_count": 1420 },
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

### Queue health (internal/ops)
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

## Worker internals — retry + backoff

```python
BACKOFF = [5, 25, 125]  # seconds: 5s, 25s, 2min

async def process_job(job):
    try:
        html = await fetch(job.url, timeout=15)
        key  = await upload_to_s3(html, job.id)
        data = parse(html)
        await mark_done(job.id, key, data)

    except RetryableError as e:
        if job.retries >= job.max_retries:
            await mark_dead(job.id, str(e))
        else:
            delay = BACKOFF[job.retries]
            await mark_failed_for_retry(job.id, str(e), delay)
            # re-enqueue after delay via a scheduler or
            # a separate delayed-queue pattern in Redis

    except FatalError as e:
        await mark_dead(job.id, str(e))   # don't retry (e.g. 404, 403)
```

Retryable errors: connection timeout, 429, 500, 503.
Fatal errors: 404, 403, invalid HTML, malformed URL.

---

## Scaling model

| Knob | What it controls |
|---|---|
| API instances | Read/write throughput at the HTTP layer |
| Worker instances | Scraping throughput — scale this for more jobs/sec |
| `max_concurrency_per_domain` | Politeness — Redis semaphore cap per hostname |
| `queue_depth_limit` | Backpressure — reject submissions when queue is saturated |
| `BRPOP` timeout | How quickly idle workers notice new work |
| Worker `asyncio` concurrency | Each worker can run M coroutines, so 1 process ≠ 1 job at a time |

A single async worker process can comfortably handle 10–20 concurrent in-flight HTTP requests (I/O bound, waiting on network). With 10 worker containers × 10 coroutines each, you have 100 concurrent fetches. Throughput scales linearly until you hit a downstream bottleneck (Postgres write rate, S3 upload bandwidth, or domain rate limits).

---

## What we build in each phase

| Phase | What gets added |
|---|---|
| 4a | Single worker, Redis queue, Postgres jobs table, basic retry |
| 4b | N workers via docker-compose scale, domain semaphore, DLQ |
| 4c | Delayed retry queue (Redis `ZADD` sorted set by timestamp) |
| 4d | Prometheus `/metrics`, queue depth alerts, worker heartbeat |

---

Ready to start coding? I'd suggest we begin with the Postgres schema + Redis setup, then the worker loop, then wire up the API. Or if you want to use Go instead of Python for the workers, say the word — the architecture is identical but the concurrency model is even cleaner with goroutines.