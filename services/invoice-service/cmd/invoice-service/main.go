// Command invoice-service creates invoices with a fixed rate and a derived address, serves their status and
// list to merchants and the public payment page (SRS 4.1.3, 4.1.4, 4.1.7). Configuration is read from the
// environment, see services/invoice-service/AGENTS.md.
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
	"github.com/valkey-io/valkey-go"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/outbox"
	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/services/invoice-service/internal/api"
	"github.com/m5tshift/slopay/services/invoice-service/internal/cache"
	"github.com/m5tshift/slopay/services/invoice-service/internal/config"
	"github.com/m5tshift/slopay/services/invoice-service/internal/invoice"
	"github.com/m5tshift/slopay/services/invoice-service/internal/projection"
	"github.com/m5tshift/slopay/services/invoice-service/internal/rates"
	"github.com/m5tshift/slopay/services/invoice-service/internal/store"
	"github.com/m5tshift/slopay/services/invoice-service/migrations"
)

const serviceName = "invoice-service"

// valkeyRetry is the pause between connection attempts while Valkey is unavailable.
const valkeyRetry = 5 * time.Second

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("invoice-service stopped", "error", err)
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

	// The status cache starts empty and attaches Valkey when it is reachable; until then statuses are read
	// from PostgreSQL (SRS 4.1.7).
	statusCache := cache.New(cfg.StatusCacheTTL, cfg.ValkeyTimeout, log)
	defer statusCache.Close()
	openValkey := func(ctx context.Context) (valkey.Client, error) {
		v, err := storage.OpenValkey(ctx, cfg.Valkey)
		if err != nil {
			return nil, err
		}
		return v.Client(), nil
	}

	rateClient, err := rates.New(cfg.RateServiceURL, cfg.RateServiceTimeout)
	if err != nil {
		return err
	}
	svc, err := invoice.NewService(store.New(pg.Pool(), codec), rateClient, statusCache, invoice.Config{
		InvoiceTTL: cfg.InvoiceTTL, RateMaxAge: cfg.RateMaxAge,
	}, log)
	if err != nil {
		return err
	}
	consumer, err := projection.NewConsumer(codec, svc, log, cfg.KafkaBrokers, cfg.ConsumerGroup, kgo.ClientID(serviceName))
	if err != nil {
		return err
	}
	defer consumer.Close()

	health := telemetry.NewHealth(log, 0)
	// Valkey is a cache and rate-service is needed only for new invoices, so neither is a readiness check.
	health.AddReadiness("postgresql", pg.Ready)
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	if err := api.Register(mux, api.Options{
		Service: svc, JWTIssuer: cfg.JWTIssuer, RetryAfter: cfg.RateRetryAfter, Logger: log,
	}); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 3)
	go statusCache.Connect(ctx, openValkey, valkeyRetry)
	go func() {
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errc <- fmt.Errorf("outbox relay: %w", err)
		}
	}()
	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			errc <- fmt.Errorf("merchant.events consumer: %w", err)
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
