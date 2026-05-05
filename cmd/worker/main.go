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
		// deferred consumer.Close() and st.Close() run on return
		return
	}
	slog.Info("worker shutting down")
}
