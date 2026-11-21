// Command payer-sim plays the buyer: it pays invoices with ETH in Anvil from pre-funded accounts and sends the
// stored signed transactions again after a reorganisation (SRS 4.1.3, 8.1, 8.5). Configuration is read from the
// environment, see testkit/payer-sim/README.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/testkit/payer-sim/internal/api"
	"github.com/m5tshift/slopay/testkit/payer-sim/internal/config"
	"github.com/m5tshift/slopay/testkit/payer-sim/internal/invoice"
	"github.com/m5tshift/slopay/testkit/payer-sim/internal/payer"
)

const serviceName = "payer-sim"

func main() {
	log := telemetry.NewLogger(os.Stdout, telemetry.ParseLevel(os.Getenv("LOG_LEVEL")))
	if err := run(log); err != nil {
		log.Error("payer-sim stopped", "error", err)
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
	raw, err := os.ReadFile(cfg.KeysFile)
	if err != nil {
		return fmt.Errorf("read keys file: %w", err)
	}
	keys, err := payer.ParseKeys(string(raw))
	if err != nil {
		return fmt.Errorf("keys file: %w", err)
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

	chain, err := ethclient.DialContext(ctx, cfg.RPCURL)
	if err != nil {
		return fmt.Errorf("anvil: %w", err)
	}
	defer chain.Close()
	p, err := payer.New(ctx, payer.Config{
		Chain:       chain,
		Invoices:    invoice.NewClient(cfg.InvoiceURL, &http.Client{Timeout: 5 * time.Second}),
		Keys:        keys,
		Shares:      payer.Shares{Pay: cfg.PayPercent, Underpay: cfg.UnderpayPercent, Skip: cfg.SkipPercent},
		UnderpayBps: cfg.UnderpayBps,
		Roll:        rand.IntN,
	})
	if err != nil {
		return err
	}
	log.Info("payer ready", "accounts", len(keys))

	health := telemetry.NewHealth(log, 0)
	health.AddReadiness("anvil", func(ctx context.Context) error {
		_, err := chain.BlockNumber(ctx)
		return err
	})
	mux := http.NewServeMux()
	telemetry.Register(mux, health, tel)
	api.Register(mux, p, log)
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           tel.Middleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second, // a replay of many transactions
		IdleTimeout:       60 * time.Second,
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
