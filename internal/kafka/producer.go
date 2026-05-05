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

// compile-time interface assertion
var _ Producer = (*KafkaProducer)(nil)

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
