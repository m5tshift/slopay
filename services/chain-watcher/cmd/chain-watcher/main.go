// Command chain-watcher reads the blocks of an EVM chain and publishes chain.new_head and chain.reorg
// (SRS 4.1.3, 4.1.6, 4.1.9). Only the leader replica of the Lease reads the chain; the others wait.
// Configuration is read from the environment, see services/chain-watcher/AGENTS.md.
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

	"github.com/m5tshift/slopay/pkg/breaker"
	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/leader"
	"github.com/m5tshift/slopay/pkg/storage"
	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/config"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/events"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/node"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/store"
	"github.com/m5tshift/slopay/services/chain-watcher/internal/watcher"
)

const serviceName = "chain-watcher"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("chain-watcher stopped", "error", err)
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

	client, err := node.Dial(ctx, cfg.NodeURL, cfg.NodeTimeout)
	if err != nil {
		return err
	}
	defer client.Close()
	br, err := breaker.New(cfg.NodeBreaker)
	if err != nil {
		return err
	}
	w, err := watcher.New(watcher.Config{
		Node: client, Store: store.New(mongo), Publisher: events.NewPublisher(producer, events.TopicBlocks),
		Breaker: br, ChainID: cfg.ChainID, StartBlock: cfg.StartBlock, WindowSize: cfg.WindowSize,
		MaxReorgDepth: cfg.MaxReorgDepth, PollInterval: cfg.PollInterval, BackoffInitial: cfg.BackoffInitial,
		BackoffMax: cfg.BackoffMax, Logger: log,
	})
	if err != nil {
		return err
	}
	ld, err := newLeader(cfg, log)
	if err != nil {
		return err
	}

	health := telemetry.NewHealth(log, 0)
	// The node is not a readiness check: its failures are handled by the breaker and the backoff, and taking
	// the replica out of the Service would not help, since it serves no traffic.
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

	log.InfoContext(ctx, "starting", "chain_id", cfg.ChainID, "window_size", cfg.WindowSize,
		"reorg_max_depth", cfg.MaxReorgDepth, "leader_lease", cfg.LeaderLease)
	errc := make(chan error, 2)
	go func() {
		if err := ld.Run(ctx, w.Run); err != nil {
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

// newLeader elects the reading replica through the Lease LEADER_LEASE_NAME; without it the replica always
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
