import logging
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from app.config import settings
from app.constants import QUEUE_KEY
from app.db import queries as db
from app.models import (
    BulkJobCreate,
    BulkJobCreatedResponse,
    JobCreate,
    JobCreatedResponse,
    JobResponse,
)

router = APIRouter()
logger = logging.getLogger("app.api")


@router.post("/jobs", status_code=202, response_model=JobCreatedResponse)
async def submit_job(body: JobCreate, request: Request):
    pool = request.app.state.pool
    redis_client = request.app.state.redis

    queue_depth = await redis_client.llen(QUEUE_KEY)
    if queue_depth >= settings.queue_depth_limit:
        logger.warning(
            "job rejected — queue full",
            extra={"queue_depth": queue_depth, "limit": settings.queue_depth_limit},
        )
        raise HTTPException(status_code=429, detail="Queue is full")

    row = await db.insert_job(pool, str(body.url), body.priority)
    await db.insert_job_event(pool, row["id"], "enqueued")
    await redis_client.rpush(QUEUE_KEY, str(row["id"]))

    logger.info("job enqueued", extra={"job_id": str(row["id"]), "url": str(body.url)})

    return JobCreatedResponse(
        job_id=row["id"],
        status=row["status"],
        created_at=row["created_at"],
    )


@router.post("/jobs/bulk", status_code=202, response_model=BulkJobCreatedResponse)
async def submit_bulk(body: BulkJobCreate, request: Request):
    pool = request.app.state.pool
    redis_client = request.app.state.redis

    queue_depth = await redis_client.llen(QUEUE_KEY)
    available = max(0, settings.queue_depth_limit - queue_depth)
    urls = [str(u) for u in body.urls][:available]
    rejected = len(body.urls) - len(urls)

    job_ids = []
    for url in urls:
        row = await db.insert_job(pool, url)
        await db.insert_job_event(pool, row["id"], "enqueued")
        job_ids.append(row["id"])

    if job_ids:
        await redis_client.rpush(QUEUE_KEY, *[str(jid) for jid in job_ids])

    logger.info(
        "bulk enqueued",
        extra={"enqueued": len(job_ids), "rejected": rejected},
    )

    return BulkJobCreatedResponse(
        job_ids=job_ids,
        enqueued=len(job_ids),
        rejected=rejected,
    )


@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: UUID, request: Request):
    pool = request.app.state.pool
    job = await db.get_job(pool, job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobResponse(
        job_id=job["id"],
        url=job["url"],
        status=job["status"],
        retries=job["retries"],
        result=job["result"],
        error_message=job["error_message"],
        created_at=job["created_at"],
        started_at=job["started_at"],
        completed_at=job["completed_at"],
    )
