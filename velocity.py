"""An atomic, rolling five-minute count of distinct evaluation attempts."""

import hashlib

from redis.asyncio import Redis


WINDOW_SECONDS = 300

# Redis TIME avoids relying on the clocks of individual payment processes.
# The result key preserves the original count on retries within the window.
# A retry after this window expires is a fresh check if it never completed.
VELOCITY_LUA = """
local previous = redis.call('GET', KEYS[2])
if previous then
    return tonumber(previous)
end

local clock = redis.call('TIME')
local now_ms = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local window_ms = tonumber(ARGV[2]) * 1000
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms - window_ms)
redis.call('ZADD', KEYS[1], 'NX', now_ms, ARGV[1])
local count = redis.call('ZCARD', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], count, 'EX', ARGV[2])
return count
"""


def velocity_keys(customer_id: str, attempt_id) -> tuple[str, str]:
    customer_hash = hashlib.sha256(customer_id.encode()).hexdigest()
    prefix = f"sentinel:velocity:{{{customer_hash}}}"
    return f"{prefix}:attempts", f"{prefix}:result:{attempt_id}"


async def record_attempt(redis: Redis, customer_id: str, attempt_id) -> int:
    attempts_key, result_key = velocity_keys(customer_id, attempt_id)
    return int(await redis.eval(
        VELOCITY_LUA, 2, attempts_key, result_key, str(attempt_id), WINDOW_SECONDS
    ))
