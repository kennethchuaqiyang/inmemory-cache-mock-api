package main

// inmemory-cache-mock-api
//
// Same two endpoints and cache-aside contract as redis-cache-mock-api and
// elk-cache-mock-api, but the cache is a plain in-process map — no
// external cache service at all. Source of truth is still the same
// Postgres DB (mock-borrow-api-db on Neon).
//
// Like the ELK version, a plain Go map has no native TTL, so this does
// LAZY expiration: each entry carries a cachedAt timestamp, and on read
// we compare time.Since(cachedAt) against the TTL ourselves. An expired
// entry isn't proactively removed — it just isn't served as a HIT; the
// next MISS overwrites it. Compare this to Redis's active/server-side
// expiration (EXPIRE), which removes keys on its own without a read.
//
// Because this cache lives in the process's own memory, it does NOT
// survive a restart and is NOT shared across multiple instances — worth
// calling out explicitly if this ever runs behind more than one replica
// (e.g. on Render's free tier this is a non-issue since there's one
// instance, but it's exactly why real deployments use Redis/ELK instead).
//
//   GET  /api/user?user_id=1
//     - entry missing, or present but stale (cachedAt older than TTL) -> MISS:
//       read Postgres, store a fresh entry
//     - entry present and within TTL                                  -> HIT:
//       serve straight from the map
//
//   PUT /api/user/update   body: {"user_id": 1, "salary": 5000}
//     - if new salary == current DB value -> {"message":"No update"}
//     - if DB write fails                  -> {"message":"No Success"}
//     - if DB write succeeds                -> delete the map entry,
//                                              {"message":"Success"}
//
// Env vars:
//   DATABASE_URL        Postgres connection string (Neon)
//   PORT                for Render compatibility (defaults to 8080)
//   CACHE_TTL_SECONDS   defaults to 30
//
// Dependencies (go.mod):
//   github.com/jackc/pgx/v5/pgxpool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	dbPool   *pgxpool.Pool
	cacheTTL time.Duration
	cache    = newInMemoryCache()
)

type user struct {
	UserID   int    `json:"user_id"`
	Username string `json:"username"`
	Location string `json:"location"`
	Salary   int    `json:"salary"`
}

type cacheEntry struct {
	user     user
	cachedAt time.Time
}

// inMemoryCache is a mutex-protected map — the concurrency-safety a real
// cache client gives you for free, we have to provide ourselves here.
type inMemoryCache struct {
	mu   sync.RWMutex
	data map[string]cacheEntry
}

func newInMemoryCache() *inMemoryCache {
	return &inMemoryCache{data: make(map[string]cacheEntry)}
}

// get returns (entry, found). found=false covers both "never cached" and
// "key evicted after a delete" — same treatment as an expired entry from
// the caller's point of view; the TTL check itself happens in the handler
// so it can decide what "fresh enough" means.
func (c *inMemoryCache) get(key string) (cacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.data[key]
	return e, ok
}

func (c *inMemoryCache) set(key string, u user) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = cacheEntry{user: u, cachedAt: time.Now()}
}

func (c *inMemoryCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
}

type updateRequest struct {
	UserID int `json:"user_id"`
	Salary int `json:"salary"`
}

type updateResponse struct {
	Message string `json:"message"`
}

func main() {
	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	var err error
	dbPool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("failed to connect to Postgres: %v", err)
	}
	defer dbPool.Close()

	cacheTTL = 30 * time.Second
	if v := os.Getenv("CACHE_TTL_SECONDS"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			cacheTTL = time.Duration(secs) * time.Second
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/user", getUserHandler)
	mux.HandleFunc("/api/user/update", updateUserHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("inmemory-cache-mock-api listening on :%s (ttl=%s)", port, cacheTTL)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func cacheKey(userID int) string {
	return fmt.Sprintf("user:%d", userID)
}

func echoBrowserHeader(w http.ResponseWriter, r *http.Request) {
	browser := r.Header.Get("X-Browser")
	if browser == "" {
		browser = r.Header.Get("User-Agent")
	}
	w.Header().Set("X-Browser", browser)
}

func getUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	idStr := r.URL.Query().Get("user_id")
	userID, err := strconv.Atoi(idStr)
	if err != nil {
		http.Error(w, "user_id is required and must be an integer", http.StatusBadRequest)
		return
	}

	echoBrowserHeader(w, r)
	w.Header().Set("X-User-Id", idStr)
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()
	key := cacheKey(userID)

	if entry, found := cache.get(key); found && time.Since(entry.cachedAt) < cacheTTL {
		remaining := cacheTTL - time.Since(entry.cachedAt)
		setCacheCookie(w, key, remaining)
		w.Header().Set("X-Cache", "HIT")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(entry.user)
		return
	}

	// Either never cached, or stale — both are a MISS here, exactly like
	// the ELK version's lazy check (and unlike Redis, nothing removed
	// the stale entry for us; we're about to overwrite it below).
	u, err := fetchUserFromDB(ctx, userID)
	if err != nil {
		// pgxpool returns its OWN not-found sentinel (pgx.ErrNoRows), not
		// database/sql's sql.ErrNoRows — checking for the latter here never
		// matched, so every missing user fell through to the generic 500
		// below instead of this intended 404. Fixed 2026-10-16.
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}
		http.Error(w, "database error", http.StatusInternalServerError)
		return
	}

	cache.set(key, *u)

	setCacheCookie(w, key, cacheTTL)
	w.Header().Set("X-Cache", "MISS")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(u)
}

func setCacheCookie(w http.ResponseWriter, key string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name:   "cache_key",
		Value:  key,
		MaxAge: int(ttl.Seconds()),
		Path:   "/",
	})
}

func fetchUserFromDB(ctx context.Context, userID int) (*user, error) {
	row := dbPool.QueryRow(ctx,
		`SELECT user_id, username, location, salary FROM users WHERE user_id = $1`,
		userID,
	)
	var u user
	if err := row.Scan(&u.UserID, &u.Username, &u.Location, &u.Salary); err != nil {
		return nil, err
	}
	return &u, nil
}

func updateUserHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req updateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	echoBrowserHeader(w, r)
	w.Header().Set("X-User-Id", strconv.Itoa(req.UserID))
	w.Header().Set("Content-Type", "application/json")

	ctx := r.Context()

	current, err := fetchUserFromDB(ctx, req.UserID)
	if err != nil {
		// Same fix as getUserHandler: pgx.ErrNoRows, not sql.ErrNoRows.
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}
		writeUpdateResult(w, http.StatusInternalServerError, "No Success")
		return
	}

	if current.Salary == req.Salary {
		writeUpdateResult(w, http.StatusOK, "No update")
		return
	}

	tag, err := dbPool.Exec(ctx,
		`UPDATE users SET salary = $1 WHERE user_id = $2`,
		req.Salary, req.UserID,
	)
	if err != nil || tag.RowsAffected() == 0 {
		writeUpdateResult(w, http.StatusInternalServerError, "No Success")
		return
	}

	// Invalidate: same reasoning as ELK — no passive expiry to lean on,
	// so an explicit delete is required or a reader could get a HIT on
	// stale data until TTL happens to lapse on its own.
	cache.delete(cacheKey(req.UserID))

	writeUpdateResult(w, http.StatusOK, "Success")
}

func writeUpdateResult(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(updateResponse{Message: message})
}
