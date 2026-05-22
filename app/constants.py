QUEUE_KEY = "scraper:queue"
RETRY_QUEUE_KEY = (
    "scraper:retry_queue"  # sorted set: score = unix timestamp to re-enqueue at
)
BACKOFF = [5, 25, 125]  # retry delays in seconds: 5s, 25s, ~2min
