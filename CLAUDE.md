# CLAUDE.md

## Prerequisites

This repo uses the **superpowers skill plugin** for Claude Code. Skills referenced below
(`brainstorming`, `writing-plans`, `test-driven-development`, `verification-before-completion`,
`requesting-code-review`, `systematic-debugging`) are installed via the plugin, not stored in this repo.

Install: in Claude Code, run `/install-plugin` and search for `superpowers` (or ask Claude to install it).
Skills live at: `~/.claude/plugins/cache/claude-plugins-official/superpowers/`

Without the plugin the workflow docs still describe the *intent* of each step — you can follow
them manually (run the described commands yourself) even without the skills.

---

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
- Linting: `make lint` must pass before any commit — runs golangci-lint with errcheck, govet, staticcheck, ineffassign, unused

## PR Checklist

1. `make lint` passes
2. `make test` passes
3. `make build` passes
4. Invoke `superpowers:verification-before-completion`
5. Invoke `superpowers:requesting-code-review`
6. `gh pr create` with summary and test plan
