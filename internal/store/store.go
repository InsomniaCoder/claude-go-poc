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
