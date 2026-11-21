// Command rate-service serves the latest ETH/USD and USDT/USD rates (SRS 4.1.3, 4.1.7, 4.1.9). The leader
// replica polls the sources, every replica serves GET /rates/{pair} from Valkey or from its memory, which is
// restored from rate.latest. Configuration is read from the environment, see services/rate-service/AGENTS.md.
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
	"github.com/m5tshift/weigate/services/rate-service/internal/api"
	"github.com/m5tshift/weigate/services/rate-service/internal/config"
	"github.com/m5tshift/weigate/services/rate-service/internal/events"
	"github.com/m5tshift/weigate/services/rate-service/internal/limiter"
	"github.com/m5tshift/weigate/services/rate-service/internal/poller"
	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
	"github.com/m5tshift/weigate/services/rate-service/internal/source"
	"github.com/m5tshift/weigate/services/rate-service/internal/store"
)

const serviceName = "rate-service"

// valkeyRetry is the pause between connection attempts while Valkey is unavailable.
const valkeyRetry = 5 * time.Second

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("rate-service stopped", "error", err)
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

	cache := rate.NewCache()
	reader, err := events.NewReader(codec, cache, log, cfg.KafkaBrokers, kgo.ClientID(serviceName))
	if err != nil {
		return err
	}
	defer reader.Close()

	// The service starts without Valkey and serves from memory until the connection succeeds (SRS 4.1.7).
	st := store.New(cfg.ValkeyTimeout)
	defer st.Close()
	openValkey := func(ctx context.Context) (valkey.Client, error) {
		v, err := storage.OpenValkey(ctx, cfg.Valkey)
		if err != nil {
			return nil, err
		}
		return v.Client(), nil
	}

	fetcher, lim, err := newSources(cfg, st, log)
	if err != nil {
		return err
	}
	breakers := map[rate.Source]*breaker.Breaker{}
	for src, bc := range cfg.Breakers {
		if breakers[src], err = breaker.New(bc); err != nil {
			return err
		}
	}
	pl, err := poller.New(poller.Config{
		Fetcher: fetcher, Limiter: lim, Breakers: breakers, Publisher: events.NewPublisher(producer), Store: st, Cache: cache,
		Interval: cfg.PollInterval, USDTMaxAge: cfg.USDTMaxAge, DivergenceMaxBps: cfg.DivergenceMaxBps, Logger: log,
	})
	if err != nil {
		return err
	}
	if err := poller.RegisterAgeGauge(nil, cache, time.Now); err != nil {
		return err
	}
	ld, err := newLeader(cfg, log)
	if err != nil {
		return err
	}

	health := telemetry.NewHealth(log, 0)
	// No readiness checks: a replica serves from memory without Valkey and answers 503 without a rate, so
	// taking it out of the Service would only remove capacity.
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	api.Register(mux, api.Options{
		Rates: store.NewLatest(st, cache, log), MaxAge: cfg.RateMaxAge, RetryAfter: cfg.RetryAfter,
		FaultInjection: cfg.FaultInjection, Logger: log,
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.InfoContext(ctx, "starting", "source_mode", cfg.SourceMode, "fault_injection", cfg.FaultInjection,
		"leader_lease", cfg.LeaderLease)
	errc := make(chan error, 2)
	go st.Connect(ctx, openValkey, valkeyRetry, log)
	go reader.Run(ctx)
	go func() {
		if err := ld.Run(ctx, pl.Run); err != nil {
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

// newSources builds the quote source of the configured mode. Only the exchanges mode has the outgoing rate
// limit (SRS 4.1.8): a token bucket in Valkey, in memory of the leader while Valkey is down. The nil
// *limiter.Limiter must not reach the poller as a non-nil interface, hence the explicit return.
func newSources(cfg config.Config, st *store.Valkey, log *slog.Logger) (source.Fetcher, poller.Limiter, error) {
	if cfg.SourceMode == config.ModeMock {
		f, err := source.NewMock(cfg.RateMockURL, cfg.SourceTimeout)
		if err != nil {
			return nil, nil, err
		}
		return f, nil, nil
	}
	f, err := source.NewExchanges(cfg.CoinbaseURL, cfg.OKXURL, cfg.SourceTimeout)
	if err != nil {
		return nil, nil, err
	}
	limits := map[string]limiter.Config{}
	for src, c := range cfg.Limits {
		limits[string(src)] = c
	}
	lim, err := limiter.New(limits, st.Client, cfg.ValkeyTimeout, limiter.WithLogger(log))
	if err != nil {
		return nil, nil, err
	}
	return f, lim, nil
}

// newLeader elects the polling replica through the Lease LEADER_LEASE_NAME; without it the replica always
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
