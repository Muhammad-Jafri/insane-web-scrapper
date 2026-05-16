from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from prometheus_client import make_asgi_app

from app.api.routes import health, jobs
from app.config import settings
from app.db.pool import create_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await create_pool()
    app.state.redis = aioredis.from_url(settings.redis_url)
    yield
    await app.state.pool.close()
    await app.state.redis.aclose()


app = FastAPI(title="insane-web-scrapper", lifespan=lifespan)
app.include_router(jobs.router)
app.include_router(health.router)
app.mount("/metrics", make_asgi_app())
