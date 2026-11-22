// Command webhook-service delivers signed webhooks to merchants (SRS 4.1.3, 4.1.7, 4.1.9, 4.1.10; FR-07): a webhook
// for every invoice event, repeats through the retry topics, then the DLQ, a Circuit Breaker per merchant endpoint.
// Configuration is read from the environment, see services/webhook-service/AGENTS.md.
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

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/webhook-service/internal/config"
	"github.com/m5tshift/weigate/services/webhook-service/internal/consume"
	"github.com/m5tshift/weigate/services/webhook-service/internal/dedup"
	"github.com/m5tshift/weigate/services/webhook-service/internal/delivery"
	"github.com/m5tshift/weigate/services/webhook-service/internal/journal"
	"github.com/m5tshift/weigate/services/webhook-service/internal/merchants"
	"github.com/m5tshift/weigate/services/webhook-service/internal/secrets"
)

const (
	serviceName = "webhook-service"
	// valkeyRetry is the pause between attempts to connect to Valkey.
	valkeyRetry = 10 * time.Second
)

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("webhook-service stopped", "error", err)
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

	codec, err := kafka.NewCodec()
	if err != nil {
		return err
	}
	producer, err := kafka.NewProducer(codec, cfg.KafkaBrokers, kgo.ClientID(serviceName))
	if err != nil {
		return err
	}
	defer producer.Close()

	mongo, err := storage.OpenMongo(ctx, cfg.Mongo)
	if err != nil {
		return err
	}
	defer shutdown(log, "mongodb", mongo.Close)
	deliveries := journal.New(mongo, cfg.JournalTTL)
	if err := deliveries.EnsureIndexes(ctx); err != nil {
		return err
	}
	projection := merchants.New(mongo)
	bao, err := secrets.New(secrets.Config{Addr: cfg.OpenBaoAddr, TokenFile: cfg.OpenBaoTokenFile, Mount: cfg.OpenBaoMount})
	if err != nil {
		return err
	}
	deliverer, err := delivery.New(delivery.Config{
		Merchants: projection, Secrets: bao, Journal: deliveries, Publisher: producer,
		HTTP: delivery.NewHTTPClient(cfg.DeliveryTimeout), Breaker: cfg.Breaker, Logger: log,
	})
	if err != nil {
		return err
	}

	// Deduplication attaches when Valkey is reachable; until then webhooks are delivered without it (SRS 4.1.7).
	dd := dedup.New(cfg.ValkeyTimeout, log)
	var valkey *storage.Valkey
	defer func() {
		if valkey != nil {
			valkey.Close()
		}
	}()
	valkeyReady := make(chan struct{})
	go func() {
		defer close(valkeyReady)
		dd.Connect(ctx, func(ctx context.Context) (kafka.DedupStore, error) {
			v, err := storage.OpenValkey(ctx, cfg.Valkey)
			if err != nil {
				return nil, err
			}
			valkey = v
			return storage.NewValkeyDedup(v, "webhook-service:dedup:", cfg.DedupTTL), nil
		}, valkeyRetry)
	}()

	type consumer struct {
		group, topic string
		handler      kafka.Handler
	}
	consumers := []consumer{
		{cfg.ConsumerGroup + "-merchants", merchants.Topic, merchants.Handler(projection)},
		{cfg.ConsumerGroup, delivery.TopicInvoices, dd.Wrap(deliverer.Invoice, consume.RetryInitial, consume.RetryMax)},
	}
	for _, s := range delivery.Stages {
		consumers = append(consumers, consumer{cfg.ConsumerGroup + "-" + s.Topic, s.Topic,
			dd.Wrap(deliverer.Retry, consume.RetryInitial, consume.RetryMax)})
	}

	health := telemetry.NewHealth(log, 0)
	// Neither Valkey nor OpenBao is a readiness check: without Valkey webhooks go without deduplication, without
	// OpenBao they are repeated through the retry topics.
	health.AddReadiness("mongodb", mongo.Ready)
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.InfoContext(ctx, "starting", "consumer_group", cfg.ConsumerGroup, "delivery_timeout", cfg.DeliveryTimeout.String())
	errc := make(chan error, len(consumers)+1)
	for _, c := range consumers {
		loop, err := consume.New(codec, log, cfg.KafkaBrokers, c.group, []string{c.topic}, cfg.Concurrency, kgo.ClientID(serviceName))
		if err != nil {
			return err
		}
		defer loop.Close()
		go func() {
			if err := loop.Run(ctx, c.handler); err != nil && ctx.Err() == nil {
				errc <- fmt.Errorf("%s consumer: %w", c.topic, err)
			}
		}()
	}
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
	<-valkeyReady
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
