package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pj-hoakari/jetstream-http-push/internal/infra/httpapi"
	"github.com/pj-hoakari/jetstream-http-push/internal/logging"
	"github.com/pj-hoakari/jetstream-http-push/internal/relay"
)

const (
	defaultAddr       = ":8080"
	defaultLogLevel   = "info"
	shutdownTimeout   = 10 * time.Second
	readHeaderTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("relay failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	level, err := logging.ParseLevel(getenv("LOG_LEVEL", defaultLogLevel))
	if err != nil {
		return fmt.Errorf("read LOG_LEVEL: %w", err)
	}

	slog.SetDefault(logging.NewLogger(os.Stdout, logging.Options{Level: level, AddSource: false, ProjectID: ""}))

	cfg, err := relay.LoadConfig(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	rel, err := relay.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := getenv("SERVER_ADDR", defaultAddr)
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewHandler(httpapi.HealthRoutes(rel.ReadinessChecks()...)),
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(slog.Default().Handler(), slog.LevelError),
	}

	serveErr := make(chan error, 1)

	go func() {
		slog.Info("server listening", "addr", addr)

		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}

		serveErr <- err

		stop()
	}()

	runErr := rel.Run(ctx)

	slog.Info("relay shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	return errors.Join(runErr, httpServer.Shutdown(shutdownCtx), <-serveErr)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
