// Command merchant-mock plays a merchant endpoint for webhook-service: it verifies and journals webhooks and
// fails or slows down on command (SRS 4.1.3, 4.1.10, 8.2б). Configuration is read from the environment,
// see testkit/merchant-mock/README.md.
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

	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/testkit/merchant-mock/internal/api"
	"github.com/m5tshift/slopay/testkit/merchant-mock/internal/config"
	"github.com/m5tshift/slopay/testkit/merchant-mock/internal/receiver"
)

const serviceName = "merchant-mock"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("merchant-mock stopped", "error", err)
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
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tel.Shutdown(sctx); err != nil {
			log.Warn("telemetry shutdown", "error", err)
		}
	}()

	rcv := receiver.New(receiver.Options{
		TimestampTolerance: cfg.TimestampTolerance, PreviousSecretTTL: cfg.PreviousSecretTTL,
		JournalSize: cfg.JournalSize, SeenEvents: cfg.SeenEvents, Registerer: tel.Registry,
	})
	mux := http.NewServeMux()
	telemetry.Register(mux, telemetry.NewHealth(log, 0), tel)
	api.Register(mux, rcv, log)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		// An injected delay is at most receiver.MaxDelay; the answer must still be written after it.
		WriteTimeout: receiver.MaxDelay + 15*time.Second,
		IdleTimeout:  60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- fmt.Errorf("http server: %w", err)
		}
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}
