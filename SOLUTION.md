# Solution Steps

1. Initialize a new FastAPI app and connect to Redis asynchronously using `aioredis`.

2. Define configuration constants: Redis URL, circuit breaker and cache key prefixes, threshold values, timeouts, and cache TTL.

3. Implement utility functions for generating provider circuit keys and cache keys (using deterministic hashing of payloads).

4. Write Redis Lua scripts for circuit breaker atomic transitions: one to check and update state (`LUA_CIRCUIT_BREAKER`), one to register failures and trip circuits (`LUA_REGISTER_FAILURE`), and one to reset circuit on success (`LUA_REGISTER_SUCCESS`).

5. Create async helper functions: get_circuit_state(), mark_failure(), mark_success(), check_cache(), set_cache(). These interact with Redis and use the Lua scripts for atomic updates.

6. Design a simulated external API call function that randomly fails to mimic real provider failures (to exercise the circuit breaker logic).

7. Implement async background task runner for test-ping of the payment provider (with robust task tracking in a set).

8. Create and wire FastAPI endpoints: `/pay` (core endpoint with circuit breaker and caching logic), `/circuit/{provider}` (get circuit status), `/circuit/{provider}/ping` (launches async background ping), and `/status` (reports health info and active background tasks).

9. Ensure all Redis and HTTP calls are fully async and that key background tasks use `asyncio.create_task()` for real concurrency.

10. Double-check that Redis cache keys include a TTL (expiry), are deterministic, and consistently used for both set/get.

11. Use Lua scripts for all circuit state transitions and failures to guarantee atomic operations and prevent race conditions across concurrent requests.

12. Test endpoints by running the app and using the `/pay` and `/circuit/...` endpoints under load to confirm robust, async, race-free circuit breaker behavior and reliable caching.

