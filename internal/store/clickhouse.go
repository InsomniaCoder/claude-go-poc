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
