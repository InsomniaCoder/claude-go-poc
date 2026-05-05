# Activity Feed — Design Spec

**Date:** 2026-05-05  
**Status:** Approved  
**Audience:** Personal reference — patterns to pull from in future projects

---

## Purpose

A reference Go project demonstrating Claude Code best practices end-to-end. The app itself is an activity feed: clients publish events via HTTP, events flow through Kafka, a worker consumes them into ClickHouse, and the timeline is served back via HTTP. The codebase is intentionally simple so the tooling and workflow layer is the real deliverable.

---

## Architecture

Two binaries sharing an `internal/` library:

- **`cmd/server`** — HTTP API (chi router). Accepts `POST /events`, validates and enriches the event, publishes to Kafka. Serves `GET /timeline` by reading directly from ClickHouse. Also exposes `GET /healthz`.
- **`cmd/worker`** — Kafka consumer. Reads from the `activity-events` topic, deserializes events, writes to ClickHouse. Runs as a long-lived process.

Both binaries are built from a single multi-stage `Dockerfile`. `docker-compose.yml` brings up Redpanda (Kafka-compatible, no ZooKeeper), ClickHouse, and both services.

---

## Repository Structure

```
claude-go/
├── cmd/
│   ├── server/             # HTTP API + Kafka producer
│   └── worker/             # Kafka consumer + ClickHouse writer
├── internal/
│   ├── api/                # HTTP handlers, router (chi)
│   ├── kafka/              # producer + consumer wrappers (franz-go)
│   ├── store/              # ClickHouse queries (clickhouse-go/v2)
│   ├── event/              # shared Event type and validation
│   └── config/             # env-based config (envconfig)
├── docs/
│   └── superpowers/
│       ├── specs/          # brainstorm outputs
│       └── workflows/      # Claude Code workflow guides
├── .claude/
│   ├── settings.json       # allowlisted commands, MCP config
│   └── CLAUDE.md           # project-level Claude instructions
├── .github/
│   └── workflows/
│       └── ci.yml          # lint → test → build
├── docker-compose.yml
├── Dockerfile              # multi-stage, distroless final image
├── Makefile
└── go.mod
```

---

## Data Model

### Go Event Type

```go
// internal/event/event.go
type Event struct {
    ID        string    `json:"id"`
    ActorID   string    `json:"actor_id"`
    Action    string    `json:"action"`     // e.g. "user.followed", "post.liked"
    TargetID  string    `json:"target_id"`
    Payload   string    `json:"payload"`    // arbitrary JSON string
    CreatedAt time.Time `json:"created_at"`
}
```

### ClickHouse Table

```sql
CREATE TABLE IF NOT EXISTS events (
    id         String,
    actor_id   String,
    action     LowCardinality(String),
    target_id  String,
    payload    String,
    created_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at, actor_id);
```

`LowCardinality(String)` on `action` is intentional — action types repeat heavily and ClickHouse optimises storage and queries for low-cardinality columns.

---

## Data Flow

```
Client
  │  POST /events  {"actor_id":"u1","action":"post.liked","target_id":"p42"}
  ▼
cmd/server
  │  adds UUID id + created_at timestamp
  │  publishes JSON to Kafka topic: activity-events
  ▼
Kafka (Redpanda)
  │  consumer group: activity-feed-worker
  ▼
cmd/worker
  │  deserializes event
  │  inserts into ClickHouse events table
  ▼
ClickHouse

cmd/server (independent read path)
  │  GET /timeline?actor_id=u1&limit=20
  │  SELECT ... FROM events ORDER BY created_at DESC
  ▼
Client  [{"id":"...","action":"post.liked",...}, ...]
```

---

## API Surface

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Publish an event. Body: `actor_id`, `action`, `target_id`, `payload` (optional). Returns the enriched event. |
| `GET` | `/timeline` | Query params: `actor_id` (optional filter), `action` (optional filter), `limit` (default 50), `before` (optional cursor, ISO8601 timestamp). Returns paginated events newest-first. Omitting filters returns all events. |
| `GET` | `/healthz` | Returns `{"status":"ok"}`. Used by docker-compose healthcheck. |

---

## Kafka

- **Topic:** `activity-events`
- **Serialization:** JSON — readable, no schema registry needed for a reference project
- **Consumer group:** `activity-feed-worker`
- **Library:** `franz-go` — pure Go, no CGo, clean API

---

## Storage

- **Library:** `clickhouse-go/v2` — official driver, native protocol
- **Migrations:** Plain `.sql` files in `internal/store/migrations/`, applied via a `make migrate` target that runs a small Go migration runner
- **No ORM** — raw SQL queries in `internal/store/`, typed with Go structs

---

## Tooling

### Dependencies

| Purpose | Library |
|---------|---------|
| HTTP router | `github.com/go-chi/chi/v5` |
| Kafka | `github.com/twmb/franz-go` |
| ClickHouse | `github.com/ClickHouse/clickhouse-go/v2` |
| Config | `github.com/kelseyhightower/envconfig` |
| Logging | `log/slog` (stdlib) |
| UUIDs | `github.com/google/uuid` |
| Linting | `golangci-lint` |

### Makefile Targets

```makefile
make build     # build both binaries to ./bin/
make run       # docker-compose up --build
make down      # docker-compose down
make test      # go test ./...
make lint      # golangci-lint run
make migrate   # apply ClickHouse migrations
make gen       # codegen (mocks etc.) — placeholder for future use
```

### Docker

- **`Dockerfile`:** Multi-stage. Builder stage uses `golang:1.23-alpine`. Final stage uses `gcr.io/distroless/static-debian12` — no shell, minimal attack surface.
- **`docker-compose.yml`:** Services: `redpanda` (Kafka-compatible), `clickhouse`, `server`, `worker`. Server and worker depend on healthchecks passing on infrastructure services.

### CI (GitHub Actions)

```yaml
# .github/workflows/ci.yml
# Triggers: push and PR to main
# Steps: make lint → make test → make build
```

---

## Claude Code Workflow Layer

This is the meta-layer that makes the repo a reference for Claude Code best practices.

### `.claude/settings.json`

Pre-allowlists safe commands so Claude doesn't prompt on every build:
- `go build`, `go test`, `go vet`, `go mod tidy`
- `make build`, `make test`, `make lint`
- `docker-compose up/down`
- `golangci-lint run`

### `CLAUDE.md` Contents

1. **Project overview** — what the app does, two-binary structure
2. **How to build/test/lint** — exact make targets
3. **MCP guidance:**
   - Serena → symbol navigation, rename, find references
   - Context7 → franz-go, clickhouse-go, chi API docs
   - Tavily → current Go ecosystem news, CVEs
   - Sequential → multi-component debugging, architecture questions
4. **Skill invocation map:**
   - Starting a feature → `brainstorming` → `writing-plans`
   - Before writing code → `test-driven-development`
   - Before committing → `verification-before-completion`
   - After a feature → `requesting-code-review`
   - Bug encountered → `systematic-debugging`
   - Opening a PR → follow `docs/superpowers/workflows/code-review.md`
5. **PR checklist** — enforces the full loop

### `docs/superpowers/workflows/`

Three workflow guides committed to the repo:

- **`new-feature.md`** — full lifecycle: brainstorm → plan → TDD → implement → verify → PR
- **`debugging.md`** — systematic-debugging skill walkthrough with a concrete Kafka consumer example
- **`code-review.md`** — requesting-code-review + `gh pr create` flow

---

## Error Handling

- Server validates required fields (`actor_id`, `action`) and returns `400` with a descriptive message
- Kafka publish failures return `503` — the event was not accepted
- Worker logs deserialization errors and skips the message (dead-letter handling is out of scope)
- ClickHouse write failures in the worker are retried up to 3 times with exponential backoff, then logged and skipped
- All errors use `slog` with structured fields: `slog.Error("msg", "err", err, "event_id", id)`

---

## Testing Strategy

- `internal/event` — pure unit tests, no external deps
- `internal/api` — handler tests using `httptest.NewRecorder`, mock store and producer interfaces
- `internal/store` — integration tests against a real ClickHouse instance (docker-compose test profile)
- `internal/kafka` — integration tests against real Redpanda (docker-compose test profile)
- `cmd/server`, `cmd/worker` — smoke tests only (main wiring)

Interfaces defined in each package boundary so mocks are straightforward. No mocking frameworks — hand-written mocks implement the interface.

---

## Out of Scope

- Authentication/authorization
- Schema registry / Avro / Protobuf serialization
- Dead-letter queue
- Multiple Kafka topics or partitioning strategy
- ClickHouse replication
- Frontend / UI
