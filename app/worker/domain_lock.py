import logging

from app.config import settings

logger = logging.getLogger("app.worker.domain_lock")

_KEY_PREFIX = "domain:lock:"

# Atomically increment counter, cap at limit, set TTL on first acquire.
# Returns 1 if slot granted, 0 if at capacity.
_LUA_ACQUIRE = """
local key   = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl   = tonumber(ARGV[2])
local val   = redis.call('INCR', key)
if val > limit then
    redis.call('DECR', key)
    return 0
end
if val == 1 then
    redis.call('EXPIRE', key, ttl)
end
return 1
"""


async def acquire_domain_slot(redis_client, domain: str) -> bool:
    ttl = settings.fetch_timeout * 2
    result = await redis_client.eval(
        _LUA_ACQUIRE,
        1,
        f"{_KEY_PREFIX}{domain}",
        settings.max_concurrency_per_domain,
        ttl,
    )
    return bool(result)


async def release_domain_slot(redis_client, domain: str) -> None:
    key = f"{_KEY_PREFIX}{domain}"
    new_val = await redis_client.decr(key)
    if new_val < 0:
        # counter drifted below zero (e.g. double-release) — reset to 0
        await redis_client.set(key, 0)
        logger.warning(
            "domain lock counter went negative — reset", extra={"domain": domain}
        )
