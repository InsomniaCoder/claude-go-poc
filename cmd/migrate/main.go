// cmd/migrate/main.go
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/InsomniaCoder/claude-go-poc/internal/config"
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
	slog.Info("migrations complete")
}
