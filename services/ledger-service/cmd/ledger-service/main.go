// Command ledger-service keeps the double-entry ledger of receipts and the platform fee (SRS 4.1.3, FR-12):
// it posts every transfer with N confirmations from invoice.events, publishes ledger.events through the
// transactional outbox and serves the statement API to reconciliation-service. Configuration is read from
// the environment, see services/ledger-service/AGENTS.md.
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

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/outbox"
	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/services/ledger-service/internal/api"
	"github.com/m5tshift/slopay/services/ledger-service/internal/config"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
	"github.com/m5tshift/slopay/services/ledger-service/internal/store"
	"github.com/m5tshift/slopay/services/ledger-service/migrations"
)

const serviceName = "ledger-service"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("ledger-service stopped", "error", err)
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

	pg, err := storage.OpenPostgres(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer pg.Close()
	n, err := pg.Migrate(ctx, migrations.FS)
	if err != nil {
		return err
	}
	log.InfoContext(ctx, "migrations applied", "count", n)

	codec, err := kafka.NewCodec()
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(codec, cfg.KafkaBrokers, kgo.ClientID(serviceName))
	if err != nil {
		return err
	}
	defer producer.Close()
	relay, err := outbox.NewRelay(outbox.Config{Pool: pg.Pool(), Publisher: producer, Logger: log, MeterProvider: tel.Meter})
	if err != nil {
		return err
	}

	svc, err := ledger.NewService(store.New(pg.Pool(), codec), ledger.Config{FeeBps: cfg.FeeBps}, log)
	if err != nil {
		return err
	}
	consumer, err := kafka.NewConsumer(codec, log, cfg.KafkaBrokers, cfg.ConsumerGroup,
		[]string{ledger.TopicInvoices}, kgo.ClientID(serviceName))
	if err != nil {
		return err
	}
	defer consumer.Close()

	health := telemetry.NewHealth(log, 0)
	health.AddReadiness("postgresql", pg.Ready)
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	if err := api.Register(mux, api.Options{Service: svc, Logger: log}); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 3)
	go func() {
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errc <- fmt.Errorf("outbox relay: %w", err)
		}
	}()
	go func() {
		if err := consumer.Run(ctx, ledger.Handler(svc, ledger.DefaultBackoff, log)); err != nil && ctx.Err() == nil {
			errc <- fmt.Errorf("invoice.events consumer: %w", err)
		}
	}()
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
