# Activity Feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a two-binary Go activity feed — an HTTP server that publishes events to Kafka and serves a ClickHouse-backed timeline, and a worker that consumes events from Kafka and writes them to ClickHouse — while embedding Claude Code best-practice tooling throughout.

**Architecture:** `cmd/server` handles HTTP (`POST /events`, `GET /timeline`, `GET /healthz`) and publishes to Kafka; `cmd/worker` consumes from Kafka and writes to ClickHouse. Both share `internal/` packages. A third tiny `cmd/migrate` runs ClickHouse schema migrations.

**Tech Stack:** Go 1.25, franz-go (Kafka), clickhouse-go/v2, chi (HTTP), envconfig, slog, golangci-lint, Redpanda, ClickHouse, Docker multi-stage builds, GitHub Actions.

---

## File Map

| File | Responsibility |
|------|----------------|
| `go.mod` | Module definition and dependencies |
| `internal/config/config.go` | Env-based config struct |
| `internal/event/event.go` | Event type, CreateRequest, Validate, New |
| `internal/event/event_test.go` | Unit tests for validation and construction |
| `internal/store/store.go` | Store interface + TimelineQuery struct |
| `internal/store/clickhouse.go` | ClickHouse implementation of Store |
| `internal/store/migrate.go` | Embedded SQL migration runner |
| `internal/store/migrations/001_create_events.sql` | Schema DDL |
| `internal/kafka/producer.go` | Producer interface + franz-go implementation |
| `internal/kafka/consumer.go` | Consumer struct + Run loop |
| `internal/api/handler.go` | HTTP handlers (Health, PublishEvent, Timeline) |
| `internal/api/handler_test.go` | Unit tests with mock Store + Producer |
| `internal/api/router.go` | chi router wiring |
| `cmd/server/main.go` | Server entrypoint: config → store → producer → HTTP |
| `cmd/worker/main.go` | Worker entrypoint: config → store → consumer → Run |
| `cmd/migrate/main.go` | Migration-only entrypoint |
| `Dockerfile` | Multi-stage build, `ARG BINARY` selects server/worker |
| `docker-compose.yml` | Redpanda + ClickHouse + server + worker |
| `Makefile` | build, run, down, test, lint, migrate |
| `.golangci.yml` | Linter configuration |
| `.github/workflows/ci.yml` | lint → test → build on push/PR |
| `CLAUDE.md` | Project-level Claude Code instructions |
| `.claude/settings.json` | Allowlisted commands, MCP config |
| `docs/superpowers/workflows/new-feature.md` | Feature lifecycle walkthrough |
| `docs/superpowers/workflows/debugging.md` | Systematic debugging guide |
| `docs/superpowers/workflows/code-review.md` | PR creation guide |

---

## Task 1: Repository skeleton + Go module

**Files:**
- Create: `go.mod`
- Create: directory structure

- [ ] **Step 1: Create directory tree**

```bash
mkdir -p cmd/server cmd/worker cmd/migrate
mkdir -p internal/config internal/event internal/store/migrations internal/kafka internal/api
mkdir -p .claude .github/workflows docs/superpowers/workflows
```

- [ ] **Step 2: Initialize Go module**

```bash
go mod init github.com/InsomniaCoder/claude-go-poc
```

- [ ] **Step 3: Add all dependencies**

```bash
go get github.com/go-chi/chi/v5@latest
go get github.com/twmb/franz-go@latest
go get github.com/ClickHouse/clickhouse-go/v2@latest
go get github.com/kelseyhightower/envconfig@latest
go get github.com/google/uuid@latest
go mod tidy
```

Expected: `go.sum` created, `go.mod` has `require` block with all five deps.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum
git commit -m "chore: initialize go module with dependencies"
```

---

## Task 2: Config package

**Files:**
- Create: `internal/config/config.go`

- [ ] **Step 1: Write config struct**

```go
// internal/config/config.go
package config

import "github.com/kelseyhightower/envconfig"

type Config struct {
	KafkaBrokers string `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	KafkaTopic   string `envconfig:"KAFKA_TOPIC"   default:"activity-events"`
	KafkaGroupID string `envconfig:"KAFKA_GROUP_ID" default:"activity-feed-worker"`
	ClickHouseDSN string `envconfig:"CLICKHOUSE_DSN" default:"clickhouse://localhost:9000/default"`
	HTTPAddr      string `envconfig:"HTTP_ADDR"      default:":8080"`
}

func Load() (Config, error) {
	var c Config
	return c, envconfig.Process("", &c)
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/config/...
```

Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add internal/config/config.go
git commit -m "feat: add env-based config package"
```

---

## Task 3: Event type + validation (TDD)

**Files:**
- Create: `internal/event/event.go`
- Create: `internal/event/event_test.go`

- [ ] **Step 1: Write failing tests**

```go
// internal/event/event_test.go
package event_test

import (
	"testing"

	"github.com/InsomniaCoder/claude-go-poc/internal/event"
)

func TestValidate_MissingActorID(t *testing.T) {
	r := event.CreateRequest{Action: "post.liked"}
	if err := r.Validate(); err == nil {
		t.Fatal("expected error for missing actor_id")
	}
}

func TestValidate_MissingAction(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1"}
	if err := r.Validate(); err == nil {
		t.Fatal("expected error for missing action")
	}
}

func TestValidate_Valid(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked"}
	if err := r.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNew_SetsFields(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked", TargetID: "p42"}
	e := event.New(r)
	if e.ID == "" {
		t.Error("ID should not be empty")
	}
	if e.ActorID != "u1" {
		t.Errorf("ActorID = %q, want %q", e.ActorID, "u1")
	}
	if e.Action != "post.liked" {
		t.Errorf("Action = %q, want %q", e.Action, "post.liked")
	}
	if e.TargetID != "p42" {
		t.Errorf("TargetID = %q, want %q", e.TargetID, "p42")
	}
	if e.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestNew_UniqueIDs(t *testing.T) {
	r := event.CreateRequest{ActorID: "u1", Action: "post.liked"}
	e1 := event.New(r)
	e2 := event.New(r)
	if e1.ID == e2.ID {
		t.Error("expected unique IDs for each New call")
	}
}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
go test ./internal/event/...
```

Expected: `FAIL` with `cannot find package` or `undefined`.

- [ ] **Step 3: Write the implementation**

```go
// internal/event/event.go
package event

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        string    `json:"id"`
	ActorID   string    `json:"actor_id"`
	Action    string    `json:"action"`
	TargetID  string    `json:"target_id"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateRequest struct {
	ActorID  string `json:"actor_id"`
	Action   string `json:"action"`
	TargetID string `json:"target_id"`
	Payload  string `json:"payload"`
}

func (r CreateRequest) Validate() error {
	if r.ActorID == "" {
		return errors.New("actor_id is required")
	}
	if r.Action == "" {
		return errors.New("action is required")
	}
	return nil
}

func New(r CreateRequest) Event {
	return Event{
		ID:        uuid.New().String(),
		ActorID:   r.ActorID,
		Action:    r.Action,
		TargetID:  r.TargetID,
		Payload:   r.Payload,
		CreatedAt: time.Now().UTC(),
	}
}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
go test ./internal/event/...
```

Expected: `ok  github.com/InsomniaCoder/claude-go-poc/internal/event`

- [ ] **Step 5: Commit**

```bash
git add internal/event/
git commit -m "feat: add event type with validation (TDD)"
```

---

## Task 4: Store package (interface, ClickHouse impl, migrations)

**Files:**
- Create: `internal/store/store.go`
- Create: `internal/store/migrations/001_create_events.sql`
- Create: `internal/store/migrate.go`
- Create: `internal/store/clickhouse.go`

- [ ] **Step 1: Write the Store interface**

```go
// internal/store/store.go
package store

import (
	"context"
	"time"

	"github.com/InsomniaCoder/claude-go-poc/internal/event"
)

type TimelineQuery struct {
	ActorID string
	Action  string
	Limit   int
	Before  time.Time // zero value means no cursor
}

type Store interface {
	Insert(ctx context.Context, e event.Event) error
	Timeline(ctx context.Context, q TimelineQuery) ([]event.Event, error)
	Close() error
}
```

- [ ] **Step 2: Write the migration SQL**

```sql
-- internal/store/migrations/001_create_events.sql
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

- [ ] **Step 3: Write the embedded migration runner**

```go
// internal/store/migrate.go
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func (s *ClickHouseStore) Migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sql, err := fs.ReadFile(migrationsFS, "migrations/"+entry.Name())
		if err != nil {
			return fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		if err := s.conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("exec %s: %w", entry.Name(), err)
		}
	}
	return nil
}
```

- [ ] **Step 4: Write the ClickHouse implementation**

```go
// internal/store/clickhouse.go
package store

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/InsomniaCoder/claude-go-poc/internal/event"
)

type ClickHouseStore struct {
	conn driver.Conn
}

func NewClickHouse(dsn string) (*ClickHouseStore, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	return &ClickHouseStore{conn: conn}, nil
}

func (s *ClickHouseStore) Insert(ctx context.Context, e event.Event) error {
	return s.conn.Exec(ctx,
		`INSERT INTO events (id, actor_id, action, target_id, payload, created_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.ID, e.ActorID, e.Action, e.TargetID, e.Payload, e.CreatedAt,
	)
}

func (s *ClickHouseStore) Timeline(ctx context.Context, q TimelineQuery) ([]event.Event, error) {
	query := `SELECT id, actor_id, action, target_id, payload, created_at
	          FROM events WHERE 1=1`
	args := []any{}

	if q.ActorID != "" {
		query += ` AND actor_id = ?`
		args = append(args, q.ActorID)
	}
	if q.Action != "" {
		query += ` AND action = ?`
		args = append(args, q.Action)
	}
	if !q.Before.IsZero() {
		query += ` AND created_at < ?`
		args = append(args, q.Before)
	}

	limit := q.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC LIMIT %d`, limit)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var events []event.Event
	for rows.Next() {
		var e event.Event
		if err := rows.Scan(&e.ID, &e.ActorID, &e.Action, &e.TargetID, &e.Payload, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}
```

- [ ] **Step 5: Verify ClickHouseStore satisfies Store interface**

```bash
go build ./internal/store/...
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add internal/store/
git commit -m "feat: add store interface, ClickHouse implementation, and SQL migrations"
```

---

## Task 5: Kafka producer

**Files:**
- Create: `internal/kafka/producer.go`

- [ ] **Step 1: Write the Producer interface and franz-go implementation**

```go
// internal/kafka/producer.go
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer interface {
	Publish(ctx context.Context, topic string, value any) error
	Close()
}

type KafkaProducer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*KafkaProducer, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &KafkaProducer{client: client}, nil
}

func (p *KafkaProducer) Publish(ctx context.Context, topic string, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	record := &kgo.Record{Topic: topic, Value: b}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	return nil
}

func (p *KafkaProducer) Close() {
	p.client.Close()
}
```

- [ ] **Step 2: Verify KafkaProducer satisfies Producer interface**

```bash
go build ./internal/kafka/...
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add internal/kafka/producer.go
git commit -m "feat: add kafka producer with franz-go"
```

---

## Task 6: Kafka consumer

**Files:**
- Create: `internal/kafka/consumer.go`

- [ ] **Step 1: Write the consumer**

```go
// internal/kafka/consumer.go
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/InsomniaCoder/claude-go-poc/internal/event"
)

type EventHandler func(ctx context.Context, e event.Event) error

type Consumer struct {
	client  *kgo.Client
	handler EventHandler
}

func NewConsumer(brokers []string, groupID, topic string, handler EventHandler) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer: %w", err)
	}
	return &Consumer{client: client, handler: handler}, nil
}

// Run polls Kafka until ctx is cancelled. Returns nil on clean shutdown.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("fetch error", "err", e.Err)
			}
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			var e event.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				slog.Error("unmarshal event", "err", err, "offset", r.Offset)
				return
			}
			if err := c.handler(ctx, e); err != nil {
				slog.Error("handle event", "err", err, "event_id", e.ID)
			}
		})
	}
}

func (c *Consumer) Close() {
	c.client.Close()
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/kafka/...
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add internal/kafka/consumer.go
git commit -m "feat: add kafka consumer with franz-go"
```

---

## Task 7: API handlers (TDD)

**Files:**
- Create: `internal/api/handler.go`
- Create: `internal/api/handler_test.go`

- [ ] **Step 1: Write the failing tests with inline mocks**

```go
// internal/api/handler_test.go
package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/InsomniaCoder/claude-go-poc/internal/api"
	"github.com/InsomniaCoder/claude-go-poc/internal/event"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

type mockStore struct {
	insertFn   func(context.Context, event.Event) error
	timelineFn func(context.Context, store.TimelineQuery) ([]event.Event, error)
}

func (m *mockStore) Insert(ctx context.Context, e event.Event) error {
	return m.insertFn(ctx, e)
}
func (m *mockStore) Timeline(ctx context.Context, q store.TimelineQuery) ([]event.Event, error) {
	return m.timelineFn(ctx, q)
}
func (m *mockStore) Close() error { return nil }

type mockProducer struct {
	publishFn func(context.Context, string, any) error
}

func (m *mockProducer) Publish(ctx context.Context, topic string, value any) error {
	if m.publishFn != nil {
		return m.publishFn(ctx, topic, value)
	}
	return nil
}
func (m *mockProducer) Close() {}

func TestHealth(t *testing.T) {
	h := api.NewHandler(&mockStore{}, &mockProducer{}, "t")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.Health(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestPublishEvent_MissingActorID(t *testing.T) {
	h := api.NewHandler(&mockStore{}, &mockProducer{}, "t")
	body, _ := json.Marshal(event.CreateRequest{Action: "post.liked"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPublishEvent_MissingAction(t *testing.T) {
	h := api.NewHandler(&mockStore{}, &mockProducer{}, "t")
	body, _ := json.Marshal(event.CreateRequest{ActorID: "u1"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestPublishEvent_ProducerError_Returns503(t *testing.T) {
	h := api.NewHandler(
		&mockStore{},
		&mockProducer{publishFn: func(_ context.Context, _ string, _ any) error {
			return errors.New("broker unavailable")
		}},
		"t",
	)
	body, _ := json.Marshal(event.CreateRequest{ActorID: "u1", Action: "post.liked"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestPublishEvent_Success(t *testing.T) {
	var publishedTopic string
	h := api.NewHandler(
		&mockStore{},
		&mockProducer{publishFn: func(_ context.Context, topic string, _ any) error {
			publishedTopic = topic
			return nil
		}},
		"activity-events",
	)
	body, _ := json.Marshal(event.CreateRequest{ActorID: "u1", Action: "post.liked", TargetID: "p42"})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	h.PublishEvent(rec, req)
	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusCreated)
	}
	if publishedTopic != "activity-events" {
		t.Errorf("topic = %q, want %q", publishedTopic, "activity-events")
	}
	var e event.Event
	if err := json.NewDecoder(rec.Body).Decode(&e); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if e.ID == "" {
		t.Error("response event ID should not be empty")
	}
	if e.ActorID != "u1" {
		t.Errorf("ActorID = %q, want %q", e.ActorID, "u1")
	}
}

func TestTimeline_ReturnsEvents(t *testing.T) {
	want := []event.Event{{ID: "abc", ActorID: "u1", Action: "post.liked", CreatedAt: time.Now()}}
	h := api.NewHandler(
		&mockStore{timelineFn: func(_ context.Context, q store.TimelineQuery) ([]event.Event, error) {
			if q.ActorID != "u1" {
				t.Errorf("ActorID filter = %q, want %q", q.ActorID, "u1")
			}
			return want, nil
		}},
		&mockProducer{},
		"t",
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/timeline?actor_id=u1", nil)
	h.Timeline(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	var got []event.Event
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 1 || got[0].ID != "abc" {
		t.Errorf("events = %v, want 1 event with ID=abc", got)
	}
}

func TestTimeline_EmptyReturnsEmptyArray(t *testing.T) {
	h := api.NewHandler(
		&mockStore{timelineFn: func(_ context.Context, _ store.TimelineQuery) ([]event.Event, error) {
			return nil, nil
		}},
		&mockProducer{},
		"t",
	)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/timeline", nil)
	h.Timeline(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if body != "[]\n" {
		t.Errorf("body = %q, want %q", body, "[]\n")
	}
}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
go test ./internal/api/...
```

Expected: `FAIL` — `api.NewHandler` undefined.

- [ ] **Step 3: Write the handler implementation**

```go
// internal/api/handler.go
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/InsomniaCoder/claude-go-poc/internal/event"
	"github.com/InsomniaCoder/claude-go-poc/internal/kafka"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

type Handler struct {
	store    store.Store
	producer kafka.Producer
	topic    string
}

func NewHandler(s store.Store, p kafka.Producer, topic string) *Handler {
	return &Handler{store: s, producer: p, topic: topic}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (h *Handler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	var req event.CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	e := event.New(req)
	if err := h.producer.Publish(r.Context(), h.topic, e); err != nil {
		slog.Error("publish event", "err", err, "event_id", e.ID)
		http.Error(w, "failed to publish event", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(e)
}

func (h *Handler) Timeline(w http.ResponseWriter, r *http.Request) {
	q := store.TimelineQuery{
		ActorID: r.URL.Query().Get("actor_id"),
		Action:  r.URL.Query().Get("action"),
		Limit:   50,
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			q.Limit = n
		}
	}
	if b := r.URL.Query().Get("before"); b != "" {
		if t, err := time.Parse(time.RFC3339, b); err == nil {
			q.Before = t
		}
	}

	events, err := h.store.Timeline(r.Context(), q)
	if err != nil {
		slog.Error("query timeline", "err", err)
		http.Error(w, "failed to query timeline", http.StatusInternalServerError)
		return
	}
	if events == nil {
		events = []event.Event{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
go test ./internal/api/...
```

Expected: `ok  github.com/InsomniaCoder/claude-go-poc/internal/api`

- [ ] **Step 5: Commit**

```bash
git add internal/api/handler.go internal/api/handler_test.go
git commit -m "feat: add HTTP handlers with TDD (Health, PublishEvent, Timeline)"
```

---

## Task 8: API router

**Files:**
- Create: `internal/api/router.go`

- [ ] **Step 1: Write the chi router**

```go
// internal/api/router.go
package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(h *Handler) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/healthz", h.Health)
	r.Post("/events", h.PublishEvent)
	r.Get("/timeline", h.Timeline)
	return r
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./internal/api/...
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add internal/api/router.go
git commit -m "feat: add chi router"
```

---

## Task 9: cmd/server

**Files:**
- Create: `cmd/server/main.go`

- [ ] **Step 1: Write the server entrypoint**

```go
// cmd/server/main.go
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/InsomniaCoder/claude-go-poc/internal/api"
	"github.com/InsomniaCoder/claude-go-poc/internal/config"
	"github.com/InsomniaCoder/claude-go-poc/internal/kafka"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	st, err := store.NewClickHouse(cfg.ClickHouseDSN)
	if err != nil {
		slog.Error("connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(context.Background()); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	producer, err := kafka.NewProducer(strings.Split(cfg.KafkaBrokers, ","))
	if err != nil {
		slog.Error("create producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	h := api.NewHandler(st, producer, cfg.KafkaTopic)
	router := api.NewRouter(h)

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: router}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("server starting", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down server")
	srv.Shutdown(context.Background())
}
```

- [ ] **Step 2: Verify it compiles**

```bash
go build ./cmd/server/...
```

Expected: no output.

- [ ] **Step 3: Commit**

```bash
git add cmd/server/main.go
git commit -m "feat: add cmd/server entrypoint"
```

---

## Task 10: cmd/worker + cmd/migrate

**Files:**
- Create: `cmd/worker/main.go`
- Create: `cmd/migrate/main.go`

- [ ] **Step 1: Write the worker entrypoint**

```go
// cmd/worker/main.go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/InsomniaCoder/claude-go-poc/internal/config"
	"github.com/InsomniaCoder/claude-go-poc/internal/event"
	"github.com/InsomniaCoder/claude-go-poc/internal/kafka"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	st, err := store.NewClickHouse(cfg.ClickHouseDSN)
	if err != nil {
		slog.Error("connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	handler := func(ctx context.Context, e event.Event) error {
		if err := st.Insert(ctx, e); err != nil {
			slog.Error("insert event", "err", err, "event_id", e.ID)
			return err
		}
		slog.Info("event stored", "event_id", e.ID, "action", e.Action)
		return nil
	}

	consumer, err := kafka.NewConsumer(
		strings.Split(cfg.KafkaBrokers, ","),
		cfg.KafkaGroupID,
		cfg.KafkaTopic,
		handler,
	)
	if err != nil {
		slog.Error("create consumer", "err", err)
		os.Exit(1)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("worker starting", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	if err := consumer.Run(ctx); err != nil {
		slog.Error("consumer error", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 2: Write the migrate entrypoint**

```go
// cmd/migrate/main.go
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/InsomniaCoder/claude-go-poc/internal/config"
	"github.com/InsomniaCoder/claude-go-poc/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	st, err := store.NewClickHouse(cfg.ClickHouseDSN)
	if err != nil {
		slog.Error("connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	if err := st.Migrate(context.Background()); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}
	slog.Info("migrations complete")
}
```

- [ ] **Step 3: Verify both compile**

```bash
go build ./cmd/worker/... && go build ./cmd/migrate/...
```

Expected: no output.

- [ ] **Step 4: Run all tests**

```bash
go test ./...
```

Expected: all packages pass (store + kafka skip integration tests since no running infra).

- [ ] **Step 5: Commit**

```bash
git add cmd/worker/main.go cmd/migrate/main.go
git commit -m "feat: add cmd/worker and cmd/migrate entrypoints"
```

---

## Task 11: Dockerfile + docker-compose

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`

- [ ] **Step 1: Write the multi-stage Dockerfile**

```dockerfile
# Dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG BINARY=server
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/app ./cmd/${BINARY}

FROM gcr.io/distroless/static-debian12
COPY --from=builder /bin/app /app
ENTRYPOINT ["/app"]
```

- [ ] **Step 2: Write docker-compose.yml**

```yaml
# docker-compose.yml
version: "3.8"

services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda
      - start
      - --smp=1
      - --memory=512M
      - --overprovisioned
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://redpanda:9092
    ports:
      - "9092:9092"
    healthcheck:
      test: ["CMD-SHELL", "rpk cluster info --brokers localhost:9092 || exit 1"]
      interval: 5s
      timeout: 5s
      retries: 15

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 5s
      timeout: 5s
      retries: 10

  server:
    build:
      context: .
      args:
        BINARY: server
    environment:
      KAFKA_BROKERS: redpanda:9092
      CLICKHOUSE_DSN: clickhouse://clickhouse:9000/default
      HTTP_ADDR: ":8080"
    ports:
      - "8080:8080"
    depends_on:
      redpanda:
        condition: service_healthy
      clickhouse:
        condition: service_healthy

  worker:
    build:
      context: .
      args:
        BINARY: worker
    environment:
      KAFKA_BROKERS: redpanda:9092
      CLICKHOUSE_DSN: clickhouse://clickhouse:9000/default
      KAFKA_GROUP_ID: activity-feed-worker
      KAFKA_TOPIC: activity-events
    depends_on:
      redpanda:
        condition: service_healthy
      clickhouse:
        condition: service_healthy
```

- [ ] **Step 3: Verify Dockerfile builds locally (server binary)**

```bash
docker build --build-arg BINARY=server -t activity-feed-server .
```

Expected: build succeeds, `activity-feed-server` image created.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile docker-compose.yml
git commit -m "chore: add multi-stage Dockerfile and docker-compose with Redpanda + ClickHouse"
```

---

## Task 12: Makefile + linter config

**Files:**
- Create: `Makefile`
- Create: `.golangci.yml`

- [ ] **Step 1: Write the Makefile**

```makefile
# Makefile
.PHONY: build run down test lint migrate gen

build:
	go build -o bin/server ./cmd/server
	go build -o bin/worker ./cmd/worker

run:
	docker compose up --build

down:
	docker compose down

test:
	go test ./...

lint:
	golangci-lint run

migrate:
	go run ./cmd/migrate

gen:
	go generate ./...
```

- [ ] **Step 2: Write the linter config**

```yaml
# .golangci.yml
version: "1"

linters:
  enable:
    - errcheck
    - govet
    - staticcheck
    - gosimple
    - ineffassign
    - unused

linters-settings:
  errcheck:
    check-type-assertions: true

issues:
  exclude-rules:
    - path: "_test.go"
      linters:
        - errcheck
```

- [ ] **Step 3: Run lint**

```bash
golangci-lint run
```

Expected: no issues, or fix any flagged (typically unused imports or unchecked errors).

- [ ] **Step 4: Run make build**

```bash
make build
```

Expected: `bin/server` and `bin/worker` created.

- [ ] **Step 5: Commit**

```bash
git add Makefile .golangci.yml
git commit -m "chore: add Makefile and golangci-lint config"
```

---

## Task 13: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the CI workflow**

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  ci:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-go@v5
        with:
          go-version: "1.25"
          cache: true

      - name: Lint
        uses: golangci/golangci-lint-action@v6
        with:
          version: latest
          args: --timeout=5m

      - name: Test
        run: go test ./...

      - name: Build
        run: make build
```

- [ ] **Step 2: Verify the YAML is valid**

```bash
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "valid"
```

Expected: `valid`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions lint → test → build pipeline"
```

---

## Task 14: Claude Code workflow layer

**Files:**
- Create: `CLAUDE.md`
- Create: `.claude/settings.json`
- Create: `docs/superpowers/workflows/new-feature.md`
- Create: `docs/superpowers/workflows/debugging.md`
- Create: `docs/superpowers/workflows/code-review.md`

- [ ] **Step 1: Write CLAUDE.md at project root**

```markdown
# CLAUDE.md

## Project Overview

Activity feed with two binaries:
- `cmd/server` — HTTP API (`POST /events`, `GET /timeline`, `GET /healthz`) + Kafka producer
- `cmd/worker` — Kafka consumer → ClickHouse writer

Shared internal packages: `event`, `store`, `kafka`, `api`, `config`.

## Build / Test / Lint

```bash
make build    # builds bin/server and bin/worker
make test     # go test ./...
make lint     # golangci-lint run
make run      # docker compose up --build (full stack)
make migrate  # run ClickHouse migrations standalone
```

## MCP Guidance

| Task | Tool |
|------|------|
| Find a symbol, rename a function, trace references | **Serena** (`find_symbol`, `find_referencing_symbols`, `rename_symbol`) |
| Look up franz-go, clickhouse-go, or chi API | **Context7** |
| Search for recent Go ecosystem news or CVEs | **Tavily** |
| Debug a multi-component issue (Kafka + ClickHouse + HTTP) | **Sequential** |

## Skill Invocation Map

| Moment | Skill |
|--------|-------|
| Starting any new feature | `superpowers:brainstorming` → `superpowers:writing-plans` |
| Before writing implementation code | `superpowers:test-driven-development` |
| Before any `git commit` or PR | `superpowers:verification-before-completion` |
| After implementing a feature | `superpowers:requesting-code-review` |
| Bug encountered | `superpowers:systematic-debugging` |
| Opening a PR | See `docs/superpowers/workflows/code-review.md` |

## Conventions

- Errors: `fmt.Errorf("context: %w", err)` — always wrap with context
- Logging: `slog.Error("verb noun", "key", val, ...)` — structured, no format strings
- Interfaces: defined in the package that owns the abstraction (e.g., `store.Store`, `kafka.Producer`)
- Mocks: inline in `_test.go` files, implement the interface, no mocking frameworks
- No ORM — raw SQL in `internal/store/clickhouse.go`

## PR Checklist

1. `make lint` passes
2. `make test` passes
3. `make build` passes
4. Invoke `superpowers:verification-before-completion`
5. Invoke `superpowers:requesting-code-review`
6. `gh pr create` with summary and test plan
```

- [ ] **Step 2: Write `.claude/settings.json`**

```json
{
  "permissions": {
    "allow": [
      "Bash(go build:*)",
      "Bash(go test:*)",
      "Bash(go vet:*)",
      "Bash(go mod:*)",
      "Bash(go run:*)",
      "Bash(make build:*)",
      "Bash(make test:*)",
      "Bash(make lint:*)",
      "Bash(make migrate:*)",
      "Bash(golangci-lint:*)",
      "Bash(docker compose ps:*)",
      "Bash(docker compose logs:*)",
      "Bash(git status:*)",
      "Bash(git diff:*)",
      "Bash(git log:*)"
    ]
  }
}
```

- [ ] **Step 3: Write new-feature workflow guide**

```markdown
# New Feature Workflow

This is the standard lifecycle for any feature in this repo.

## 1. Brainstorm

```
/brainstorming
```

Answer the skill's questions. Output: approved design in `docs/superpowers/specs/`.

## 2. Write a Plan

The brainstorming skill invokes this automatically:

```
/writing-plans
```

Output: implementation plan in `docs/superpowers/plans/`.

## 3. Implement with TDD

Before writing implementation code, invoke:

```
/test-driven-development
```

Follow the plan task by task. Each task:
1. Write the failing test
2. Run it — confirm it fails
3. Write minimal implementation
4. Run it — confirm it passes
5. Commit

## 4. Verify Before Committing

Before the final commit or PR:

```
/verification-before-completion
```

This runs `make lint`, `make test`, `make build` and confirms all pass.

## 5. Open a PR

See `docs/superpowers/workflows/code-review.md`.
```

- [ ] **Step 4: Write debugging workflow guide**

```markdown
# Debugging Workflow

When something is broken, invoke systematic-debugging **before** proposing fixes.

## Invoke the skill

```
/systematic-debugging
```

## Example: Kafka consumer not receiving events

Symptoms: `GET /timeline` returns empty, events were published via `POST /events`.

### Step 1 — Reproduce

```bash
# Start the stack
make run

# Publish an event
curl -s -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{"actor_id":"u1","action":"post.liked","target_id":"p42"}'

# Check timeline immediately
curl -s http://localhost:8080/timeline
```

### Step 2 — Isolate the layer

Use Sequential MCP to reason through the flow: HTTP → Kafka → ClickHouse → HTTP read.

```bash
# Check if event hit Kafka (Redpanda console)
docker compose exec redpanda rpk topic consume activity-events --num 1

# Check if event hit ClickHouse
docker compose exec clickhouse clickhouse-client --query \
  "SELECT count() FROM events"

# Check worker logs
docker compose logs worker --tail=50
```

### Step 3 — Serena for symbol tracing

If the consumer handler is suspect, use Serena to find all references:

```
Use mcp__serena__find_referencing_symbols to find all callers of EventHandler
```

### Step 4 — Fix and re-run tests

After identifying the root cause, write a regression test before fixing.
```

- [ ] **Step 5: Write code-review + PR workflow guide**

```markdown
# Code Review & PR Workflow

## Before opening a PR

### 1. Run verification skill

```
/verification-before-completion
```

This must pass: `make lint`, `make test`, `make build`.

### 2. Request code review

```
/requesting-code-review
```

Follow the skill's output. Fix any issues it surfaces.

### 3. Open the PR

```bash
gh pr create \
  --title "feat: short description under 70 chars" \
  --body "$(cat <<'EOF'
## Summary
- What this PR does (2-3 bullets)

## Test plan
- [ ] make lint passes
- [ ] make test passes
- [ ] make build passes
- [ ] Manually tested with make run + curl

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

## Reviewing a PR

```
/review
```

Or use the Serena MCP to navigate changed symbols:

```
Use mcp__serena__find_referencing_symbols on any changed interface to verify all callers were updated
```
```

- [ ] **Step 6: Verify all new files are present**

```bash
ls CLAUDE.md .claude/settings.json docs/superpowers/workflows/
```

Expected: all three workflow files listed.

- [ ] **Step 7: Run the full test suite one final time**

```bash
make test
```

Expected: all packages pass.

- [ ] **Step 8: Commit**

```bash
git add CLAUDE.md .claude/settings.json docs/superpowers/workflows/
git commit -m "docs: add CLAUDE.md, settings.json, and Claude Code workflow guides"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|-----------------|------|
| Two binaries: server + worker | Tasks 9, 10 |
| POST /events, GET /timeline, GET /healthz | Task 7, 8 |
| Kafka producer (franz-go) | Task 5 |
| Kafka consumer (franz-go) | Task 6 |
| ClickHouse store (clickhouse-go/v2) | Task 4 |
| Event type with UUID + timestamp | Task 3 |
| Env-based config | Task 2 |
| Multi-stage Dockerfile + docker-compose | Task 11 |
| Makefile (build, run, down, test, lint, migrate) | Task 12 |
| golangci-lint | Task 12 |
| GitHub Actions CI | Task 13 |
| CLAUDE.md with MCP guidance + skill map | Task 14 |
| .claude/settings.json with allowlist | Task 14 |
| Workflow docs (new-feature, debugging, code-review) | Task 14 |
| LowCardinality(String) on action column | Task 4 |
| Graceful shutdown (signal handling) | Tasks 9, 10 |
| Empty timeline returns [] not null | Task 7 |

All requirements covered. No gaps.
