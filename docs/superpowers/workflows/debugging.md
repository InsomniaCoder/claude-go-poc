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
