// Command merchant-service registers merchants, keeps their xpub and webhook settings, issues API keys and
// exchanges them for JWTs (SRS 4.1.2, 4.1.10). Configuration is read from the environment, see
// services/merchant-service/AGENTS.md.
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
	"github.com/m5tshift/weigate/pkg/outbox"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/pkg/telemetry"
	"github.com/m5tshift/weigate/services/merchant-service/internal/api"
	"github.com/m5tshift/weigate/services/merchant-service/internal/config"
	"github.com/m5tshift/weigate/services/merchant-service/internal/jwt"
	"github.com/m5tshift/weigate/services/merchant-service/internal/merchant"
	"github.com/m5tshift/weigate/services/merchant-service/internal/openbao"
	"github.com/m5tshift/weigate/services/merchant-service/internal/store"
	"github.com/m5tshift/weigate/services/merchant-service/migrations"
)

const serviceName = "merchant-service"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("merchant-service stopped", "error", err)
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

	// The previous signing key stays in the JWKS for a token lifetime after the key file is rotated.
	signer, err := jwt.NewFileSigner(cfg.JWTKeyFile, cfg.JWTTTL)
	if err != nil {
		return err
	}
	issuer := jwt.NewIssuer(signer, cfg.JWTIssuer, cfg.JWTTTL)
	svc := merchant.NewService(store.New(pg.Pool(), codec), merchant.Config{
		KeyRotationGrace:    cfg.KeyRotationGrace,
		MaxActiveKeys:       cfg.MaxActiveKeys,
		DefaultToleranceBps: cfg.DefaultToleranceBps,
		MaxToleranceBps:     cfg.MaxToleranceBps,
	}, log)
	if cfg.OpenBaoAddr != "" {
		bao, err := openbao.New(openbao.Config{Addr: cfg.OpenBaoAddr, TokenFile: cfg.OpenBaoTokenFile, Mount: cfg.OpenBaoMount})
		if err != nil {
			return err
		}
		svc.WithSecrets(bao)
	} else {
		log.Warn("OPENBAO_ADDR is not set: webhook secret rotation is disabled")
	}

	health := telemetry.NewHealth(log, 0)
	health.AddReadiness("postgresql", pg.Ready)
	health.AddReadiness("jwt-signing-key", func(ctx context.Context) error {
		_, err := signer.PublicKeys(ctx)
		return err
	})
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	if err := api.Register(mux, api.Options{
		Service: svc, Issuer: issuer, Signer: signer, JWKSMaxAge: cfg.JWKSMaxAge, Logger: log,
	}); err != nil {
		return err
	}
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 2)
	go func() {
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errc <- fmt.Errorf("outbox relay: %w", err)
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
