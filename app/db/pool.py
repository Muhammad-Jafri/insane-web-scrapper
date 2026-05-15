import json

import asyncpg

from app.config import settings


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(settings.database_url, init=_init_connection)
