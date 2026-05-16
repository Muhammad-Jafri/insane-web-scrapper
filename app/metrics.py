from prometheus_client import Counter, Gauge, Histogram

jobs_total = Counter(
    "scraper_jobs_total",
    "Total scrape jobs by final status",
    ["status"],  # done | retried | dead
)

jobs_enqueued_total = Counter(
    "scraper_jobs_enqueued_total",
    "Total jobs submitted via the API",
)

job_duration_seconds = Histogram(
    "scraper_job_duration_seconds",
    "End-to-end job processing time",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120],
)

fetch_duration_seconds = Histogram(
    "scraper_fetch_duration_seconds",
    "HTTP fetch duration per job",
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30],
)

domain_wait_seconds = Histogram(
    "scraper_domain_wait_seconds",
    "Time spent waiting for a domain concurrency slot",
    buckets=[0.01, 0.1, 0.5, 1, 5, 10, 30],
)

queue_depth = Gauge(
    "scraper_queue_depth",
    "Current number of jobs waiting in the Redis queue",
)

active_jobs = Gauge(
    "scraper_active_jobs",
    "Jobs currently being processed across all coroutines",
)
