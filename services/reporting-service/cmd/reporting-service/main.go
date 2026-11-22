// Command reporting-service serves merchant reports on turnover and conversion from the ClickHouse views
// (SRS 4.1.3, FR-11). It only reads. Configuration is read from the environment, see
// services/reporting-service/AGENTS.md.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/reporting-service/internal/api"
	"github.com/m5tshift/weigate/services/reporting-service/internal/config"
	"github.com/m5tshift/weigate/services/reporting-service/internal/jwks"
	"github.com/m5tshift/weigate/services/reporting-service/internal/store"
)

const serviceName = "reporting-service"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("reporting-service stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	tel, err := telemetry.New(ctx, telemetry.ConfigFromEnv(serviceName))
	if err != nil {
		return err
	}
	defer shutdown(log, "telemetry", tel.Shutdown)

	reports, err := store.Open(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer shutdown(log, "clickhouse", reports.Close)

	health := telemetry.NewHealth(log, 0)
	health.AddReadiness("clickhouse", reports.Ready)
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	keys := jwks.New(cfg.JWKSURL, jwks.Config{
		Client:          &http.Client{Timeout: cfg.JWKSTimeout},
		RefreshInterval: cfg.JWKSRefresh, MinInterval: cfg.JWKSMinInterval, Logger: log,
	})
	if err := api.Register(mux, api.Options{
		Reports: reports, JWTIssuer: cfg.JWTIssuer, Keys: keys, MaxDays: cfg.MaxDays,
		QueryTimeout: cfg.QueryTimeout, RetryAfter: cfg.RetryAfter, Logger: log,
	}); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      cfg.QueryTimeout + 5*time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.InfoContext(ctx, "listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err = <-errc:
	}
	stop()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
	defer cancel()
	if serr := srv.Shutdown(sctx); serr != nil {
		log.Error("http shutdown", "error", serr)
	}
	return err
}

func shutdown(log *slog.Logger, name string, f func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f(ctx); err != nil {
		log.Error("shutdown", "component", name, "error", err)
	}
}
