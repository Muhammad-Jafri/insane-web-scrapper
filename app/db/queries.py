from datetime import datetime
from uuid import UUID

import asyncpg


async def insert_job(pool: asyncpg.Pool, url: str, priority: int = 0) -> dict:
    row = await pool.fetchrow(
        """
        INSERT INTO jobs (url, priority)
        VALUES ($1, $2)
        RETURNING id, status, created_at
        """,
        url,
        priority,
    )
    return dict(row)


async def get_job(pool: asyncpg.Pool, job_id: UUID) -> dict | None:
    row = await pool.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
    return dict(row) if row else None


async def update_job_running(pool: asyncpg.Pool, job_id: UUID, worker_id: str) -> None:
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'RUNNING', started_at = now(), worker_id = $2
        WHERE id = $1
        """,
        job_id,
        worker_id,
    )


async def mark_job_done(
    pool: asyncpg.Pool, job_id: UUID, raw_html_key: str, result: dict
) -> None:
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'DONE', completed_at = now(), raw_html_key = $2, result = $3
        WHERE id = $1
        """,
        job_id,
        raw_html_key,
        result,
    )


async def mark_job_pending_retry(
    pool: asyncpg.Pool, job_id: UUID, error_msg: str, next_retry_at: datetime
) -> None:
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'FAILED', retries = retries + 1,
            error_message = $2, next_retry_at = $3
        WHERE id = $1
        """,
        job_id,
        error_msg,
        next_retry_at,
    )


async def mark_job_pending(pool: asyncpg.Pool, job_id: UUID) -> None:
    await pool.execute(
        "UPDATE jobs SET status = 'PENDING', next_retry_at = NULL WHERE id = $1",
        job_id,
    )


async def mark_job_dead(pool: asyncpg.Pool, job_id: UUID, error_msg: str) -> None:
    await pool.execute(
        """
        UPDATE jobs
        SET status = 'DEAD', completed_at = now(), error_message = $2
        WHERE id = $1
        """,
        job_id,
        error_msg,
    )


async def insert_job_event(
    pool: asyncpg.Pool,
    job_id: UUID,
    event: str,
    worker_id: str | None = None,
    message: str | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO job_events (job_id, event, worker_id, message)
        VALUES ($1, $2, $3, $4)
        """,
        job_id,
        event,
        worker_id,
        message,
    )


async def count_running_jobs(pool: asyncpg.Pool) -> int:
    return await pool.fetchval("SELECT COUNT(*) FROM jobs WHERE status = 'RUNNING'")


async def get_jobs_last_60s(pool: asyncpg.Pool) -> dict:
    row = await pool.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE event = 'completed') AS done,
            COUNT(*) FILTER (WHERE event = 'dead')      AS failed
        FROM job_events
        WHERE created_at > now() - interval '60 seconds'
        """
    )
    return {"done": row["done"], "failed": row["failed"]}
