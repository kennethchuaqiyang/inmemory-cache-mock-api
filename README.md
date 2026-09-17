# inmemory-cache-mock-api

A mock user API demonstrating the cache-aside pattern backed by a plain in-process map, sitting in front of a real Postgres database.

**Live:** https://inmemory-cache-mock-api.onrender.com
**CI:** tested by [go-api-cache-mock-tests](https://github.com/kennethchuaqiyang/go-api-cache-mock-tests) and [playwright-api-cache-mock-tests](https://github.com/kennethchuaqiyang/playwright-api-cache-mock-tests)

## What this demonstrates

No external cache service at all — the cache is a `map[string]cacheEntry` protected by a `sync.RWMutex`, living entirely inside the running process's memory. Two consequences worth knowing:

- **Lazy expiration**: unlike Redis's active/server-side TTL, a plain Go map has no built-in expiry. Each entry carries a `cachedAt` timestamp, and staleness is checked on every read (`time.Since(cachedAt) < TTL`) — an expired entry isn't proactively removed, it's just not served as a hit until the next write overwrites it.
- **No shared or persistent state**: the cache dies with the process and isn't visible across replicas. On Render's free tier this also means every cold start (after the service spins down from inactivity) wipes the cache clean — the first request after any idle period is always a `MISS`, even if it was a `HIT` moments before the service slept.

This is the simplest of the three cache backends in this set ([Redis](https://github.com/kennethchuaqiyang/redis-cache-mock-api), [ELK](https://github.com/kennethchuaqiyang/elk-cache-mock-api)), useful as a baseline for comparing what a real cache service buys you.

## Endpoints

Same contract as the Redis and ELK variants:

### `GET /api/user?user_id={id}`
Cache-aside read; `X-Cache: HIT`/`MISS`, `X-User-Id`, `X-Browser` headers, `Set-Cookie: cache_key=...` with remaining TTL.

### `PUT /api/user/update`
Body: `{"user_id": 2, "salary": 9000}` → `"Success"` (deletes the cache entry) / `"No update"` (value unchanged) / `"No Success"` (DB error).

## Environment variables

| Var | Purpose | Default |
|---|---|---|
| `DATABASE_URL` | Postgres connection string (Neon) | required |
| `PORT` | HTTP port | `8080` |
| `CACHE_TTL_SECONDS` | cache TTL | `30` |

## Running locally

```bash
go mod tidy
go run main.go
```

No external cache service to start — just Postgres connectivity is required.

## Deployment

Deployed on Render (Oregon, free tier).