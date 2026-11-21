// Command chain-watcher reads the blocks of an EVM chain and publishes chain.new_head, chain.reorg and the native
// ETH transfers to tracked addresses (SRS 4.1.3, 4.1.6, 4.1.7, 4.1.9). Only the leader replica of the Lease reads
// the chain; every replica keeps the projection of tracked addresses from invoice.events.
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
	"github.com/valkey-io/valkey-go"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/leader"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/chain"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/config"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/events"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/node"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/projection"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/store"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/tracked"
	"github.com/m5tshift/weigate/services/chain-watcher/internal/watcher"
)

const serviceName = "chain-watcher"

// valkeyRetry is the pause between connection attempts while Valkey is unavailable.
const valkeyRetry = 5 * time.Second

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

	// The tracked addresses: the projection in MongoDB and its cache in Valkey, which attaches when it is
	// reachable; until then the addresses are checked in MongoDB.
	addresses := projection.New(mongo)
	if err := addresses.EnsureIndexes(ctx); err != nil {
		return err
	}
	cache := tracked.NewValkey(chain.Int64(cfg.ChainID), cfg.ValkeyTimeout, log)
	defer cache.Close()
	openValkey := func(ctx context.Context) (valkey.Client, error) {
		v, err := storage.OpenValkey(ctx, cfg.Valkey)
		if err != nil {
			return nil, err
		}
		return v.Client(), nil
	}
	book, err := tracked.NewBook(addresses, cache, valkeyRetry, nil, log)
	if err != nil {
		return err
	}
	invoices, err := kafka.NewConsumer(codec, log, cfg.KafkaBrokers, cfg.ConsumerGroup,
		[]string{projection.TopicInvoices}, kgo.ClientID(serviceName),
		// A new group reads only the invoices that can still be tracked.
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-cfg.RebuildWindow).UnixMilli())))
	if err != nil {
		return err
	}
	defer invoices.Close()
	handler := projection.Handler(addresses, cfg.TrackingWindow, projection.DefaultBackoff, log)

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
		Node: client, Store: store.New(mongo), Addresses: book,
		Publisher: events.NewPublisher(producer, events.TopicBlocks, events.TopicTransfers),
		Breaker:   br, Tokens: cfg.Tokens, ChainID: cfg.ChainID, StartBlock: cfg.StartBlock, WindowSize: cfg.WindowSize,
		MaxReorgDepth: cfg.MaxReorgDepth, RescanClockSkew: cfg.RescanClockSkew, RescanLookback: cfg.RebuildWindow,
		PollInterval: cfg.PollInterval, BackoffInitial: cfg.BackoffInitial, BackoffMax: cfg.BackoffMax, Logger: log,
	})
	if err != nil {
		return err
	}
	ld, err := newLeader(cfg, log)
	if err != nil {
		return err
	}

	health := telemetry.NewHealth(log, 0)
	// Neither the node nor Valkey is a readiness check: node failures are handled by the breaker and the backoff,
	// Valkey is a cache, and taking the replica out of the Service would not help, since it serves no traffic.
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
	errc := make(chan error, 3)
	go cache.Connect(ctx, openValkey, valkeyRetry)
	go func() {
		// A lost projection is rebuilt from invoice.events of the rebuild window before the group goes on.
		rebuild := func(ctx context.Context) error {
			return projection.Rebuild(ctx, projection.RebuildConfig{
				Brokers: cfg.KafkaBrokers, Topic: projection.TopicInvoices, Since: time.Now().Add(-cfg.RebuildWindow),
				Codec: codec, Handler: handler, Log: log, Opts: []kgo.Opt{kgo.ClientID(serviceName)},
			})
		}
		if err := projection.EnsureBuilt(ctx, addresses, rebuild, projection.DefaultBackoff, log); err != nil {
			return
		}
		if err := invoices.Run(ctx, handler); err != nil && ctx.Err() == nil {
			errc <- fmt.Errorf("invoice.events consumer: %w", err)
		}
	}()
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
