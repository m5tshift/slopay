// Command reconciliation-service reconciles the chain with the invoices and the ledger (SRS 4.1.3, UC-08, FR-15):
// every 10 minutes the leader replica reads the balances of the invoice addresses of the last 7 days at head - N
// and compares them with the transfers accounted in the invoices and with the ledger statement. Every replica keeps
// the projection of invoice.events. Configuration is read from the environment, see
// services/reconciliation-service/AGENTS.md.
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/leader"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/config"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/events"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/ledger"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/node"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/projection"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/recon"
	"github.com/m5tshift/weigate/services/reconciliation-service/internal/report"
)

const serviceName = "reconciliation-service"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("reconciliation-service stopped", "error", err)
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
	invoices := projection.New(mongo, cfg.ProjectionTTL)
	if err := invoices.EnsureIndexes(ctx); err != nil {
		return err
	}
	reports := report.New(mongo, cfg.ReportTTL)
	if err := reports.EnsureIndexes(ctx); err != nil {
		return err
	}
	consumer, err := kafka.NewConsumer(codec, log, cfg.KafkaBrokers, cfg.ConsumerGroup,
		[]string{projection.TopicInvoices}, kgo.ClientID(serviceName),
		// A new group reads only the invoices within the lookback of the runs.
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-cfg.Lookback).UnixMilli())))
	if err != nil {
		return err
	}
	defer consumer.Close()

	client, err := node.Dial(ctx, cfg.NodeURL, cfg.NodeTimeout)
	if err != nil {
		return err
	}
	defer client.Close()
	br, err := breaker.New(cfg.NodeBreaker)
	if err != nil {
		return err
	}
	ledgerClient, err := ledger.New(cfg.LedgerURL, cfg.LedgerTimeout)
	if err != nil {
		return err
	}
	rec, err := recon.New(recon.Config{
		Node: client, Breaker: br, Ledger: ledgerClient, Projection: invoices, Reports: reports,
		Publisher: events.NewPublisher(producer), ChainID: cfg.ChainID, Confirmations: cfg.Confirmations,
		Lookback: cfg.Lookback, TrackingWindow: cfg.TrackingWindow, SettleDelay: cfg.SettleDelay,
		BatchSize: cfg.BatchSize, Interval: cfg.Interval, RunTimeout: cfg.RunTimeout, Logger: log,
	})
	if err != nil {
		return err
	}
	ld, err := newLeader(cfg, log)
	if err != nil {
		return err
	}

	health := telemetry.NewHealth(log, 0)
	// Neither the node nor ledger-service is a readiness check: their failures fail or postpone a run, and the
	// replica serves no traffic.
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

	log.InfoContext(ctx, "starting", "chain_id", cfg.ChainID, "confirmations", cfg.Confirmations,
		"interval", cfg.Interval.String(), "leader_lease", cfg.LeaderLease)
	errc := make(chan error, 3)
	go func() {
		if err := consumer.Run(ctx, projection.Handler(invoices, projection.DefaultBackoff, log)); err != nil && ctx.Err() == nil {
			errc <- fmt.Errorf("invoice.events consumer: %w", err)
		}
	}()
	go func() {
		if err := ld.Run(ctx, rec.Schedule); err != nil {
			errc <- fmt.Errorf("leader election: %w", err)
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

// newLeader elects the reconciling replica through the Lease LEADER_LEASE_NAME; without it the replica always
// leads (local runs).
func newLeader(cfg config.Config, log *slog.Logger) (*leader.Leader, error) {
	lc := leader.Config{Name: serviceName, Logger: log}
	if cfg.LeaderLease != "" {
		rc, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("leader: kubernetes config: %w", err)
		}
		client, err := kubernetes.NewForConfig(rc)
		if err != nil {
			return nil, fmt.Errorf("leader: kubernetes client: %w", err)
		}
		lc.Name, lc.Namespace, lc.Identity, lc.Client = cfg.LeaderLease, cfg.PodNamespace, cfg.PodName, client
	}
	return leader.New(lc)
}

func shutdown(log *slog.Logger, name string, f func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := f(ctx); err != nil {
		log.Error("shutdown", "component", name, "error", err)
	}
}
