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
