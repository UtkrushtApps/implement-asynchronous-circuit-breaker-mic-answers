import asyncio
import time
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Any
from redis.asyncio import Redis
import aioredis
import httpx
import uuid

REDIS_URL = "redis://localhost"
CIRCUIT_BREAKER_PREFIX = "cb:"
CACHE_PREFIX = "payment:cache:"
# Time to keep circuit open after last failure (in sec)
CIRCUIT_BREAKER_TIMEOUT = 30
FAILURE_THRESHOLD = 3
CACHE_TTL = 60  # Cache API result for 1 minute

app = FastAPI()
redis: Redis = None

class PaymentRequest(BaseModel):
    provider: str
    payload: dict


@app.on_event("startup")
async def startup():
    global redis
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)

@app.on_event("shutdown")
async def shutdown():
    await redis.close()

# Lua script for circuit breaker state transitions
LUA_CIRCUIT_BREAKER = """
local key = KEYS[1]
local status = redis.call('HGET', key, 'status')
local failure_count = redis.call('HGET', key, 'failure_count')
local now = tonumber(ARGV[1])
local circuit_timeout = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
if (not status) then status = 'closed' end
if (not failure_count) then failure_count = 0 end
failure_count = tonumber(failure_count)
if status == 'open' then
    local last_failure = redis.call('HGET', key, 'last_failure')
    if last_failure and now - tonumber(last_failure) > circuit_timeout then
        redis.call('HMSET', key, 'status', 'half-open', 'failure_count', 0, 'last_failure', 0)
        return 'half-open'
    else
        return 'open'
    end
elseif status == 'half-open' then
    return 'half-open'
else
    return 'closed'
end
"""

# Lua script to atomically mark failure and trip the circuit if threshold exceeded
LUA_REGISTER_FAILURE = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local threshold = tonumber(ARGV[2])
local failure_count = redis.call('HINCRBY', key, 'failure_count', 1)
redis.call('HSET', key, 'last_failure', now)
if tonumber(failure_count) >= threshold then
    redis.call('HSET', key, 'status', 'open')
    return 'open'
else
    redis.call('HSET', key, 'status', 'closed')
    return 'closed'
end
"""

LUA_REGISTER_SUCCESS = """
local key = KEYS[1]
redis.call('HMSET', key, 'status', 'closed', 'failure_count', 0, 'last_failure', 0)
return 'closed'
"""

def provider_circuit_key(provider: str) -> str:
    return f"{CIRCUIT_BREAKER_PREFIX}{provider}"

def cache_key(provider: str, payload: dict) -> str:
    # Deterministically serialize payload for support of caching
    import hashlib, json
    raw = json.dumps(payload, sort_keys=True)
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return f"{CACHE_PREFIX}{provider}:{digest}"


async def get_circuit_state(provider: str) -> str:
    key = provider_circuit_key(provider)
    now = int(time.time())
    # status: open/closed/half-open
    # Use Lua to read/transition on timeout
    script = redis.register_script(LUA_CIRCUIT_BREAKER)
    state = await script(keys=[key], args=[now, CIRCUIT_BREAKER_TIMEOUT, FAILURE_THRESHOLD])
    return state

async def mark_failure(provider: str):
    key = provider_circuit_key(provider)
    now = int(time.time())
    script = redis.register_script(LUA_REGISTER_FAILURE)
    await script(keys=[key], args=[now, FAILURE_THRESHOLD])

async def mark_success(provider: str):
    key = provider_circuit_key(provider)
    script = redis.register_script(LUA_REGISTER_SUCCESS)
    await script(keys=[key], args=[])

async def check_cache(provider: str, payload: dict) -> Any:
    key = cache_key(provider, payload)
    result = await redis.get(key)
    return result

async def set_cache(provider: str, payload: dict, value: str):
    key = cache_key(provider, payload)
    await redis.set(key, value, ex=CACHE_TTL)

async def fake_external_api_call(provider: str, payload: dict) -> str:
    # Simulate random failures and success
    await asyncio.sleep(0.25)  # Simulate network latency
    import random
    if random.random() < 0.5:
        # 50% failure rate to exercise CB
        raise Exception("Payment API Failure!")
    # Return fake response
    return f"result-for-{provider}-{uuid.uuid4()}"

async def async_ping_provider(provider: str):
    payload = {"ping": True}
    try:
        await fake_external_api_call(provider, payload)
        await mark_success(provider)
        return True
    except Exception:
        await mark_failure(provider)
        return False

# Robust async background task runner
background_tasks_active = set()
async def run_ping_background(provider: str):
    task_id = uuid.uuid4().hex
    background_tasks_active.add(task_id)
    try:
        await async_ping_provider(provider)
    finally:
        background_tasks_active.remove(task_id)

@app.post("/pay")
async def pay(req: PaymentRequest, background_tasks: BackgroundTasks):
    provider = req.provider
    payload = req.payload
    # Check circuit state atomically
    state = await get_circuit_state(provider)
    if state == "open":
        raise HTTPException(503, f"Circuit for {provider} is open. Try again later.")
    # Try cache first
    cached = await check_cache(provider, payload)
    if cached:
        return {"provider": provider, "cached": True, "result": cached}
    try:
        # Normally would call actual external API here
        result = await fake_external_api_call(provider, payload)
    except Exception as exc:
        await mark_failure(provider)
        # In half-open, trip circuit if failure
        if state == 'half-open':
            # No-op, already handled via mark_failure
            pass
        raise HTTPException(502, f"Payment provider failure: {exc}")
    # On success
    await mark_success(provider)
    await set_cache(provider, payload, result)
    return {"provider": provider, "cached": False, "result": result}

@app.get("/circuit/{provider}")
async def circuit_status(provider: str):
    key = provider_circuit_key(provider)
    status = await redis.hgetall(key)
    return status

@app.post("/circuit/{provider}/ping")
async def test_ping(provider: str):
    # Launch async background task
    asyncio.create_task(run_ping_background(provider))
    return {"msg": f"Ping launched for {provider}."}

# Simple status route
@app.get("/status")
async def status():
    return {
        "active_background": len(background_tasks_active),
        "redis_ok": await redis.ping()
    }
