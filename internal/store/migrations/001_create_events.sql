CREATE TABLE IF NOT EXISTS events (
    id         String,
    actor_id   String,
    action     LowCardinality(String),
    target_id  String,
    payload    String,
    created_at DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (created_at, actor_id);
