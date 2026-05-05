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
