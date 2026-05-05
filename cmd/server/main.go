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
	"time"

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

	errCh := make(chan error, 1)
	go func() {
		slog.Info("server starting", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		stop() // call immediately so a second signal forces hard exit
		slog.Info("shutting down server")
	case err := <-errCh:
		slog.Error("server error", "err", err)
		// deferred cleanup (st.Close, producer.Close) runs on return
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "err", err)
	}
}
