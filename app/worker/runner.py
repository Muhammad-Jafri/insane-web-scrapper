import asyncio
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from uuid import UUID

import asyncpg
import httpx
import redis.asyncio as aioredis

from app.config import settings
from app.constants import BACKOFF, QUEUE_KEY
from app.db.pool import create_pool
from app.db.queries import (
    get_job,
    insert_job_event,
    mark_job_dead,
    mark_job_done,
    mark_job_pending,
    mark_job_pending_retry,
    update_job_running,
)
from app.worker.errors import FatalError, RetryableError
from app.worker.fetch import fetch_url
from app.worker.parse import parse_html
from app.worker.storage import ensure_bucket, make_s3_client, upload_html


async def _delayed_reenqueue(
    redis_client, pool: asyncpg.Pool, job_id: UUID, delay: int
) -> None:
    await asyncio.sleep(delay)
    await mark_job_pending(pool, job_id)
    await redis_client.rpush(QUEUE_KEY, str(job_id))


async def _handle_retry(
    pool: asyncpg.Pool,
    redis_client,
    job: dict,
    error_msg: str,
    worker_id: str,
) -> None:
    if job["retries"] >= job["max_retries"]:
        await mark_job_dead(pool, job["id"], error_msg)
        await insert_job_event(pool, job["id"], "dead", worker_id, error_msg)
        return

    delay = BACKOFF[min(job["retries"], len(BACKOFF) - 1)]
    next_retry_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
    await mark_job_pending_retry(pool, job["id"], error_msg, next_retry_at)
    await insert_job_event(pool, job["id"], "failed", worker_id, error_msg)
    asyncio.create_task(_delayed_reenqueue(redis_client, pool, job["id"], delay))


async def _coroutine_worker(
    cid: int,
    worker_id: str,
    pool: asyncpg.Pool,
    redis_client,
    s3_client,
    http_client: httpx.AsyncClient,
    domain_semaphores: dict,
) -> None:
    while True:
        try:
            raw = await redis_client.brpop(QUEUE_KEY, timeout=settings.brpop_timeout)
            if raw is None:
                continue

            _, job_id_bytes = raw
            job_id = UUID(job_id_bytes.decode())

            job = await get_job(pool, job_id)
            if job is None or job["status"] not in ("PENDING", "FAILED"):
                continue

            domain = urlparse(job["url"]).hostname or job["url"]
            if domain not in domain_semaphores:
                domain_semaphores[domain] = asyncio.Semaphore(
                    settings.max_concurrency_per_domain
                )

            async with domain_semaphores[domain]:
                await update_job_running(pool, job_id, worker_id)
                await insert_job_event(pool, job_id, "started", worker_id)

                try:
                    html = await fetch_url(http_client, job["url"])
                    key = await upload_html(s3_client, html, job_id)
                    data = parse_html(html)
                    await mark_job_done(pool, job_id, key, data)
                    await insert_job_event(pool, job_id, "completed", worker_id)

                except RetryableError as e:
                    await _handle_retry(pool, redis_client, job, str(e), worker_id)

                except FatalError as e:
                    await mark_job_dead(pool, job_id, str(e))
                    await insert_job_event(pool, job_id, "dead", worker_id, str(e))

                except Exception as e:
                    await _handle_retry(
                        pool, redis_client, job, f"Unexpected: {e}", worker_id
                    )

        except Exception as e:
            print(f"[{worker_id}:{cid}] loop error: {e}")
            await asyncio.sleep(1)


async def run_worker() -> None:
    worker_id = f"worker-{os.getpid()}"
    pool = await create_pool()
    redis_client = aioredis.from_url(settings.redis_url)
    s3_client = make_s3_client()

    await ensure_bucket(s3_client)

    domain_semaphores: dict[str, asyncio.Semaphore] = {}

    print(f"[{worker_id}] starting {settings.worker_concurrency} coroutines")

    async with httpx.AsyncClient(timeout=settings.fetch_timeout) as http_client:
        await asyncio.gather(
            *[
                _coroutine_worker(
                    i,
                    worker_id,
                    pool,
                    redis_client,
                    s3_client,
                    http_client,
                    domain_semaphores,
                )
                for i in range(settings.worker_concurrency)
            ]
        )

    await pool.close()
    await redis_client.aclose()
