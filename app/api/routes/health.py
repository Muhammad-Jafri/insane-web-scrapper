from fastapi import APIRouter, Request

from app.config import settings
from app.constants import QUEUE_KEY
from app.db import queries as db

router = APIRouter()


@router.get("/internal/health")
async def health(request: Request):
    pool = request.app.state.pool
    redis_client = request.app.state.redis

    queue_depth = await redis_client.llen(QUEUE_KEY)
    workers_active = await db.count_running_jobs(pool)
    jobs_60s = await db.get_jobs_last_60s(pool)

    return {
        "queue_depth": queue_depth,
        "workers_active": workers_active,
        "jobs_last_60s": jobs_60s,
        "queue_limit": settings.queue_depth_limit,
    }
