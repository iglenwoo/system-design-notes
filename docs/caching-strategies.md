# Caching Strategies for System Design

Beyond cache-aside: write-through, write-behind, read-through, and request coalescing.
All code examples use **FastAPI + MySQL (aiomysql) + Redis (redis-py asyncio)**.

---

## Table of Contents

1. [Cache-Aside (Lazy Loading)](#1-cache-aside-lazy-loading)
2. [Write-Through Caching](#2-write-through-caching)
3. [Write-Behind (Write-Back) Caching](#3-write-behind-write-back-caching)
4. [Read-Through Caching](#4-read-through-caching)
5. [Request Coalescing / Single-Flight](#5-request-coalescing--single-flight)
6. [Comparison Table](#6-comparison-table)

---

## Shared Setup

Every example below assumes this wiring.

```python
# app/deps.py
import os
import aiomysql
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TTL_SECONDS = 300

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

_pool: aiomysql.Pool | None = None


async def init_pool() -> None:
    global _pool
    _pool = await aiomysql.create_pool(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        db=os.getenv("MYSQL_DB", "app"),
        autocommit=False,
        minsize=1,
        maxsize=10,
    )


async def close_pool() -> None:
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()


def pool() -> aiomysql.Pool:
    assert _pool is not None, "init_pool() not called"
    return _pool
```

```python
# app/main.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.deps import init_pool, close_pool


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    yield
    await close_pool()


app = FastAPI(lifespan=lifespan)
```

Table used throughout:

```sql
CREATE TABLE users (
  id          BIGINT PRIMARY KEY,
  email       VARCHAR(255) NOT NULL,
  display_name VARCHAR(255) NOT NULL,
  updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```

---

## 1. Cache-Aside (Lazy Loading)

The application is the orchestrator. On read: check cache → on miss, query DB → write result back into cache. On write: update DB, then **invalidate** (not update) the cache key.

The cache never talks to the database. It is a dumb key-value box that the app happens to consult first.

```
READ:   app → cache (miss) → DB → app writes cache → return
WRITE:  app → DB → app DELETEs cache key
```

```python
# app/cache_aside.py
import json
import aiomysql
from fastapi import APIRouter, HTTPException
from app.deps import redis_client, pool, TTL_SECONDS

router = APIRouter()


def user_key(user_id: int) -> str:
    return f"user:{user_id}"


async def load_user_from_db(user_id: int) -> dict | None:
    async with pool().acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT id, email, display_name FROM users WHERE id = %s",
                (user_id,),
            )
            return await cur.fetchone()


@router.get("/users/{user_id}")
async def get_user(user_id: int) -> dict:
    key = user_key(user_id)

    cached = await redis_client.get(key)
    if cached is not None:
        return json.loads(cached)

    row = await load_user_from_db(user_id)
    if row is None:
        raise HTTPException(status_code=404, detail="user not found")

    await redis_client.set(key, json.dumps(row), ex=TTL_SECONDS)
    return row
```

Write path invalidates rather than updates, so a concurrent stale read cannot resurrect an old value:

```python
@router.put("/users/{user_id}")
async def update_user(user_id: int, display_name: str) -> dict:
    async with pool().acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE users SET display_name = %s WHERE id = %s",
                (display_name, user_id),
            )
        await conn.commit()

    await redis_client.delete(user_key(user_id))
    return {"id": user_id, "display_name": display_name}
```

> **When to use:** Default choice for read-heavy workloads with tolerable staleness — user profiles, product catalogs, feature flags. Cheap to reason about, resilient (cache down ⇒ degraded latency, not an outage). Costs one slow request per key per TTL expiry, and is the pattern most vulnerable to stampedes (see §5).

---

## 2. Write-Through Caching

Conceptually, the application writes **only to the cache**. The cache is responsible for synchronously persisting to the database before it acknowledges the write. Reads therefore always hit a cache that is guaranteed to reflect committed state.

```
WRITE:  app → cache → (synchronous) DB → ack ← cache ← app
READ:   app → cache (always warm for written keys)
```

Redis has no native write-through to MySQL. In practice you implement it in the application's write path: a single function that owns "DB commit, then cache update" as one atomic-looking unit. The important property is not *where the code lives* — it's that no write path exists that touches the DB without also refreshing the cache.

### Ordering: DB first, then cache

Write DB first, then cache. The inverse (cache first) means a DB failure leaves the cache advertising a value that was never persisted — a lie that survives until TTL.

```python
# app/write_through.py
import json
import aiomysql
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.deps import redis_client, pool, TTL_SECONDS

router = APIRouter()


class SettingsUpdate(BaseModel):
    email: str
    display_name: str


@router.put("/users/{user_id}/settings")
async def write_through_update(user_id: int, body: SettingsUpdate) -> dict:
    record = {"id": user_id, "email": body.email, "display_name": body.display_name}

    # 1. Synchronous durable write. If this raises, nothing is cached.
    async with pool().acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO users (id, email, display_name)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    email = VALUES(email),
                    display_name = VALUES(display_name)
                """,
                (user_id, body.email, body.display_name),
            )
        await conn.commit()

    # 2. Cache refresh, same request. Failure here must not 500 the write —
    #    the durable copy already landed. Degrade to invalidation.
    key = f"user:{user_id}"
    try:
        await redis_client.set(key, json.dumps(record), ex=TTL_SECONDS)
    except Exception:
        try:
            await redis_client.delete(key)
        except Exception:
            pass  # next read falls through to DB; TTL bounds the damage

    return record
```

### The dual-write consistency risk

Two systems, one logical write, no shared transaction. Three failure shapes:

| Failure | Result | Mitigation |
|---|---|---|
| DB commit fails | Cache untouched, request errors | Correct by construction — DB is written first |
| Cache write fails after DB commit | Cache holds stale value | Fall back to `DELETE`; TTL caps the staleness window |
| Process dies between commit and cache write | Cache holds stale value | Bounded TTL; or drive cache updates from the DB binlog (CDC) instead |

For hard consistency requirements, the durable fix is **not** more careful application code — it is to stop dual-writing. Emit cache updates from MySQL's binlog via Debezium/Maxwell, so the cache is a derived view of the committed log rather than a second write target.

Concurrent writers to the same key can also interleave (writer A commits, writer B commits, B caches, A caches ⇒ cache shows A's older value). Guard with a per-key lock or a version/`updated_at` check on the cache write if that matters.

> **When to use:** Data where a stale read is a correctness bug, not an annoyance — account settings, billing and financial records, inventory counts, permission/entitlement checks. You pay full DB write latency on every write plus a cache round-trip, and you cache data that may never be read. Worth it when reads are frequent and staleness is expensive.

---

## 3. Write-Behind (Write-Back) Caching

The application writes only to the cache and returns immediately. A background worker drains the buffered writes into the database in batches, asynchronously.

```
WRITE:  app → cache (ack immediately)
                ↓
          [Redis list/stream buffer]
                ↓  (background worker, batched)
              MySQL
```

Write latency becomes one Redis round-trip. Throughput improves further because N individual `INSERT`s collapse into one multi-row `INSERT` — far fewer round-trips, fewer fsyncs, less lock contention.

### Producer: the FastAPI endpoint

```python
# app/write_behind.py
import json
import time
from fastapi import APIRouter
from pydantic import BaseModel
from app.deps import redis_client

router = APIRouter()

QUEUE_KEY = "queue:events"


class Event(BaseModel):
    user_id: int
    name: str
    value: float


@router.post("/events", status_code=202)
async def record_event(event: Event) -> dict:
    payload = json.dumps({
        "user_id": event.user_id,
        "name": event.name,
        "value": event.value,
        "ts": time.time(),
    })

    pipe = redis_client.pipeline()
    pipe.rpush(QUEUE_KEY, payload)
    # Serve reads from the cache immediately, before the DB knows anything.
    pipe.hincrby(f"stats:{event.user_id}", event.name, 1)
    await pipe.execute()

    return {"status": "accepted"}
```

`202 Accepted` is the honest status code here. The write is buffered, not durable.

### Consumer: the flush worker

Runs as a separate process (not a FastAPI background task — it must survive independently of the web process).

```python
# worker/flush.py
import asyncio
import json
import logging
import aiomysql
from app.deps import redis_client, pool, init_pool, close_pool

QUEUE_KEY = "queue:events"
INFLIGHT_KEY = "queue:events:inflight"
BATCH_SIZE = 500
FLUSH_INTERVAL = 2.0

log = logging.getLogger("flush")


async def claim_batch(limit: int) -> list[dict]:
    """Move items to an in-flight list so a crash mid-flush loses nothing."""
    items = []
    for _ in range(limit):
        raw = await redis_client.lmove(QUEUE_KEY, INFLIGHT_KEY, "LEFT", "RIGHT")
        if raw is None:
            break
        items.append(json.loads(raw))
    return items


async def write_batch(rows: list[dict]) -> None:
    async with pool().acquire() as conn:
        async with conn.cursor() as cur:
            await cur.executemany(
                """
                INSERT INTO events (user_id, name, value, created_at)
                VALUES (%s, %s, %s, FROM_UNIXTIME(%s))
                """,
                [(r["user_id"], r["name"], r["value"], r["ts"]) for r in rows],
            )
        await conn.commit()


async def recover_inflight() -> None:
    """On startup, push anything stranded by a previous crash back to the queue."""
    while await redis_client.lmove(INFLIGHT_KEY, QUEUE_KEY, "RIGHT", "LEFT"):
        pass


async def run() -> None:
    await init_pool()
    await recover_inflight()
    try:
        while True:
            batch = await claim_batch(BATCH_SIZE)
            if not batch:
                await asyncio.sleep(FLUSH_INTERVAL)
                continue
            try:
                await write_batch(batch)
                await redis_client.ltrim(INFLIGHT_KEY, len(batch), -1)
            except Exception:
                log.exception("flush failed, returning %d rows to queue", len(batch))
                await recover_inflight()
                await asyncio.sleep(FLUSH_INTERVAL)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(run())
```

Two details that matter:

- **`LMOVE` to an in-flight list, not `LPOP`.** A plain `LPOP` followed by a crash before `commit()` silently drops the batch. The in-flight list plus `recover_inflight()` gives at-least-once delivery — so make the DB write idempotent (natural key + `INSERT IGNORE` or `ON DUPLICATE KEY UPDATE`) if duplicates would be harmful.
- **Backpressure.** Monitor `LLEN queue:events`. If producers outrun the worker indefinitely, Redis memory grows until eviction or OOM. Alert on queue depth and shed load or scale workers.

### The data-loss risk

This is the pattern's defining tradeoff and it cannot be engineered away, only bounded. Between the ack and the flush, the only copy of the data lives in Redis. If Redis dies in that window, those writes are gone.

Bounding levers:

- Short flush interval — shrinks the exposure window (at the cost of smaller batches).
- Redis AOF with `appendfsync everysec` — caps loss at ~1s of writes on a Redis crash, though not on a host/disk loss.
- Redis replication + persistence for host-failure survival.
- Use Redis **Streams** (`XADD` / consumer groups) instead of lists when you want per-consumer acknowledgment and replay built in.

If losing a window of writes is unacceptable, this pattern is the wrong choice — use write-through.

> **When to use:** High write throughput where individual records are cheap and eventual consistency is fine — metrics, view/click counters, analytics events, audit logs, activity feeds, rate-limit counters. Never for money, orders, or anything a user will notice missing.

---

## 4. Read-Through Caching

The read-side counterpart of write-through. The application asks the **cache** for a key and stops there. On a miss, the *cache* loads from the database, populates itself, and returns the value. The app has no DB code in its read path at all.

```
Cache-aside:   app → cache (miss) → app → DB → app → cache
Read-through:  app → cache (miss → cache loads from DB itself) → app
```

The behavior is nearly identical to cache-aside; the difference is **where the loading logic lives**. Read-through centralizes it, so every caller gets the same TTL, serialization, negative-caching, and stampede protection for free.

Redis cannot do this natively — it has no MySQL connector. So app-level "read-through" with Redis means a library or wrapper that owns the loader function:

```python
# app/read_through.py
from typing import Awaitable, Callable
import json
from app.deps import redis_client, TTL_SECONDS


class ReadThroughCache:
    """The cache owns the loader. Callers never see the DB."""

    def __init__(self, loader: Callable[[str], Awaitable[dict | None]], ttl: int = TTL_SECONDS):
        self._loader = loader
        self._ttl = ttl

    async def get(self, key: str) -> dict | None:
        cached = await redis_client.get(key)
        if cached is not None:
            return None if cached == "\x00" else json.loads(cached)

        value = await self._loader(key)
        if value is None:
            # Negative caching, short TTL: stops repeated misses hammering the DB.
            await redis_client.set(key, "\x00", ex=30)
            return None

        await redis_client.set(key, json.dumps(value), ex=self._ttl)
        return value


user_cache = ReadThroughCache(loader=lambda k: load_user_from_db(int(k.split(":")[1])))
```

```python
@router.get("/users/{user_id}")
async def get_user(user_id: int) -> dict:
    user = await user_cache.get(f"user:{user_id}")
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    return user
```

`aiocache`'s `@cached` decorator is essentially this pattern packaged.

### Where read-through actually shines

Application-level Redis rarely needs the ceremony — cache-aside gives you the same result with less indirection, and the extra abstraction mostly pays off once you have many cached entities and want one place to enforce policy.

Read-through is the *native* model for infrastructure that sits in front of an origin:

- **CDNs** (CloudFront, Cloudflare, Fastly) — edge miss triggers an origin fetch by the CDN itself.
- **Reverse-proxy caches** (Varnish, nginx `proxy_cache`).
- **Caching libraries with loaders** — Caffeine's `LoadingCache`, Guava, EHCache read-through.
- **Managed caches with backing stores** — DynamoDB DAX, Amazon ElastiCache with a data-loading layer.

> **When to use:** When you want loading policy in exactly one place across many call sites, or when the caching layer is infrastructure you don't control (CDN, proxy). For a handful of Redis-cached entities in a FastAPI app, plain cache-aside is usually clearer.

---

## 5. Request Coalescing / Single-Flight

### The problem: cache stampede

A hot key expires. In the microseconds before anything repopulates it, every in-flight request misses simultaneously — and every one of them queries the database.

```
t=0.000  key "user:42" TTL expires
t=0.001  1,000 concurrent requests → all MISS
t=0.002  1,000 identical SELECTs hit MySQL
t=0.050  MySQL connection pool exhausted → cascading timeouts
```

Also called the *thundering herd* or *dog-piling*. It is worst exactly where you'd least like it: on your most popular keys, under your heaviest traffic. And it is self-amplifying — the DB slows down, so requests take longer, so more of them pile up behind the same miss.

**Single-flight** fixes it: exactly one request executes the expensive load; every other request for that key waits and shares the result.

### (a) Single-process coalescing — `asyncio.Future`

Within one Python process, a dict of in-flight futures keyed by cache key is enough.

```python
# app/singleflight.py
import asyncio
import json
from typing import Awaitable, Callable
from app.deps import redis_client, TTL_SECONDS

_inflight: dict[str, asyncio.Future] = {}


async def get_single_flight(
    key: str,
    loader: Callable[[], Awaitable[dict | None]],
    ttl: int = TTL_SECONDS,
) -> dict | None:
    cached = await redis_client.get(key)
    if cached is not None:
        return json.loads(cached)

    # Someone is already loading this key — await their result.
    existing = _inflight.get(key)
    if existing is not None:
        return await asyncio.shield(existing)

    fut: asyncio.Future = asyncio.get_running_loop().create_future()
    _inflight[key] = fut

    try:
        value = await loader()
        if value is not None:
            await redis_client.set(key, json.dumps(value), ex=ttl)
        fut.set_result(value)
        return value
    except Exception as exc:
        fut.set_exception(exc)
        raise
    finally:
        _inflight.pop(key, None)
```

```python
@router.get("/users/{user_id}")
async def get_user(user_id: int) -> dict:
    user = await get_single_flight(
        f"user:{user_id}",
        lambda: load_user_from_db(user_id),
    )
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    return user
```

Notes:

- Register the future in `_inflight` **before** the first `await` inside the critical section. asyncio gives you no preemption between those two statements, which is what makes the check-then-set safe here.
- `asyncio.shield` prevents a waiter's own cancellation (client disconnect) from cancelling the shared future for everyone else.
- Waiters inherit the leader's exception. That's usually right — but it means one DB error fails N requests at once. Retry at the caller if that's not what you want.

**Limitation:** the dict lives in one process. Run 8 Uvicorn workers across 3 pods and you get 24 concurrent DB queries instead of 1000. Better, not solved.

### (b) Multi-instance coalescing — Redis distributed lock (`SET NX EX`)

To coalesce across processes and hosts, the lock has to live where everyone can see it: Redis.

```python
# app/distributed_singleflight.py
import asyncio
import json
import uuid
from typing import Awaitable, Callable
from app.deps import redis_client, TTL_SECONDS

LOCK_TTL = 10          # seconds; must exceed worst-case loader runtime
POLL_INTERVAL = 0.05   # 50 ms
POLL_TIMEOUT = 5.0     # give up waiting and load it ourselves

# Only release a lock we still own — otherwise a slow holder whose lock
# already expired would delete the *next* holder's lock.
_RELEASE = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


async def get_coalesced(
    key: str,
    loader: Callable[[], Awaitable[dict | None]],
    ttl: int = TTL_SECONDS,
) -> dict | None:
    cached = await redis_client.get(key)
    if cached is not None:
        return json.loads(cached)

    lock_key = f"lock:{key}"
    token = uuid.uuid4().hex

    # SET NX EX: atomic "acquire if absent, auto-expire".
    # The EX is the deadlock guard — if we crash holding it, it frees itself.
    acquired = await redis_client.set(lock_key, token, nx=True, ex=LOCK_TTL)

    if acquired:
        try:
            value = await loader()
            if value is not None:
                await redis_client.set(key, json.dumps(value), ex=ttl)
            return value
        finally:
            release = redis_client.register_script(_RELEASE)
            await release(keys=[lock_key], args=[token])

    # We lost the race. Poll for the leader's result.
    deadline = asyncio.get_running_loop().time() + POLL_TIMEOUT
    while asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        cached = await redis_client.get(key)
        if cached is not None:
            return json.loads(cached)

        # Lock gone but no value: leader crashed or the loader returned None.
        # Try to become the new leader.
        if not await redis_client.exists(lock_key):
            if await redis_client.set(lock_key, token, nx=True, ex=LOCK_TTL):
                try:
                    value = await loader()
                    if value is not None:
                        await redis_client.set(key, json.dumps(value), ex=ttl)
                    return value
                finally:
                    release = redis_client.register_script(_RELEASE)
                    await release(keys=[lock_key], args=[token])

    # Timed out waiting. Fall back to loading directly rather than erroring —
    # a slow response beats a 500, and this path is rare by construction.
    return await loader()
```

The failure modes this handles, and why each line is there:

| Scenario | Behavior |
|---|---|
| Leader loads normally | Waiters poll, see the value appear, return it. One DB query total. |
| Leader crashes mid-load | Lock TTL expires → a waiter acquires it and retries the load. |
| Loader is slower than `LOCK_TTL` | Lock expires while the leader still runs; a second loader starts. Duplicated work, not corruption. Set `LOCK_TTL` above p99 loader latency, or extend the lock via a watchdog. |
| Waiter's poll budget exhausted | Falls through to a direct load. Degrades to no-coalescing rather than failing. |
| Leader's lock expired, then it releases | The Lua CAS-delete refuses, since the token no longer matches. Prevents deleting someone else's lock. |
| Key legitimately has no value | Leader caches nothing; waiters spin until timeout. **Fix: negative-cache the miss** (§4) so waiters get a definitive answer. |

**Composition note:** (a) and (b) stack well. Use the in-process future to collapse a pod's concurrent requests down to one, and the Redis lock to collapse across pods. The local layer absorbs the majority of the herd without any network round-trips.

### Don't hand-roll this in production

Distributed locking has more edge cases than the table above. Prefer maintained implementations:

- **`redis-py`'s built-in `lock()`** — supports blocking acquire with timeout, ownership tokens, `extend()` for watchdog renewal, and context-manager release. It's the same primitive with the sharp edges already sanded down:

  ```python
  lock = redis_client.lock(f"lock:{key}", timeout=10, blocking_timeout=5)
  if await lock.acquire():
      try:
          value = await loader()
          await redis_client.set(key, json.dumps(value), ex=TTL_SECONDS)
      finally:
          await lock.release()
  ```

- **`aiocache`** — `@cached` decorators with pluggable Redis backends and built-in lock plugins (`RedLock`, `OptimisticLock`) that wrap exactly this pattern.

- **`redis.asyncio.lock.Lock` in Redis Cluster** — single-node locks are not safe across a failover. If you need correctness under node failure, understand the Redlock debate before relying on it; for stampede protection specifically, a duplicated query on failover is harmless, so single-node locking is fine.

### Complementary mitigations

Coalescing is one tool. These attack the same problem from other angles and cost far less:

- **TTL jitter** — `ex=300 + random.randint(0, 60)`. Stops keys populated together from expiring together. Nearly free, and it prevents the synchronized-expiry stampede outright.
- **Early/probabilistic recomputation (XFetch)** — refresh a key *before* it expires, with probability rising as expiry nears. No key is ever cold.
- **Stale-while-revalidate** — serve the expired value immediately while one background task refreshes it. Zero-latency reads, bounded staleness.

> **When to use:** Any cache key that is both expensive to compute and hot enough that many requests can miss simultaneously — homepage feeds, trending lists, aggregate counts, expensive joins, third-party API responses. Start with TTL jitter; add single-flight when a single key's load is heavy enough that even a handful of concurrent misses hurts.

---

## 6. Comparison Table

| | **Cache-Aside** | **Write-Through** | **Write-Behind** | **Read-Through** | **Single-Flight** |
|---|---|---|---|---|---|
| **Who talks to the DB** | App (both paths) | App/cache layer, synchronously on write | Background worker, asynchronously | Cache layer, on miss | App, but exactly one request per key |
| **Write latency** | DB write + cache delete | DB write + cache write (slowest) | Redis write only (fastest) | N/A — read-side only | N/A — read-side only |
| **Read latency** | Fast on hit; DB round-trip on miss | Fast — written keys are always warm | Fast — cache is the source of truth | Fast on hit; DB round-trip on miss | Fast on hit; one shared load on miss |
| **Consistency** | Eventual, bounded by TTL | Strong for written keys, modulo dual-write failure | Eventual — DB lags the cache by up to one flush interval | Eventual, bounded by TTL | Unchanged; orthogonal to consistency |
| **Risk of data loss** | None — DB is written first | None — DB commit precedes ack | **High** — unflushed buffer lost if Redis dies | None — read-side only | None — read-side only |
| **Main failure mode** | Stampede on expiry; stale reads | Dual-write skew if cache write fails | Buffer loss; unbounded queue growth | Same as cache-aside | Lock expiry ⇒ duplicated load |
| **Cache down ⇒** | Degraded latency | Degraded latency (writes still commit) | **Writes lost / rejected** | Degraded latency | Degraded to no coalescing |
| **Extra moving parts** | None | None | Separate worker process + monitoring | Loader abstraction | Redis lock or in-process registry |
| **Typical use cases** | Profiles, catalogs, feature flags | Account settings, billing, inventory, permissions | Metrics, counters, analytics events, audit logs | CDNs, reverse proxies, loader-based libraries | Hot keys: feeds, trending, aggregates |

### Choosing, in one pass

- **Read-heavy, staleness tolerable** → cache-aside. This is the default; start here.
- **Stale read is a correctness bug** → write-through, ordered DB-then-cache. Consider CDC from the binlog if the dual-write window is unacceptable.
- **Write-heavy, records individually cheap** → write-behind, with in-flight tracking and queue-depth alerts.
- **Loading policy needs one home across many entities** → read-through wrapper. In front of an origin server, it's the only model available.
- **A hot key is expensive to build** → add single-flight on top of whichever read pattern you chose. Add TTL jitter regardless — it's the cheapest win here.

These compose. A realistic service runs cache-aside for most entities, write-through for the few that must not go stale, write-behind for its telemetry, and single-flight on the three keys that actually get hammered.
