// Command stand-setup registers the merchants of the stand (SRS 8.1) and
// configures them: xpub from a test mnemonic, merchant-mock webhook URL,
// tolerance. It runs from the work machine, there is no image. Re-running is
// safe: merchants already in the keys file are reused.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/m5tshift/slopay/testkit/stand-setup/internal/client"
	"github.com/m5tshift/slopay/testkit/stand-setup/internal/setup"
	"github.com/m5tshift/slopay/testkit/stand-setup/internal/wallet"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "stand-setup:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 0 && args[0] == "deploy-usdt" {
		return runDeployUSDT(args[1:], os.Stdout)
	}
	fs := flag.NewFlagSet("stand-setup", flag.ContinueOnError)
	var (
		baseURL      = fs.String("base-url", "", "merchant-service base URL, e.g. https://slopay.example.com (required)")
		mnemonicFile = fs.String("mnemonic-file", "", "file with the test mnemonic, outside the repository (required)")
		keysFile     = fs.String("keys-file", "", "output file for API keys, outside git (required)")
		webhookURL   = fs.String("webhook-url", "", "merchant-mock webhook URL, https (required)")
		count        = fs.Int("merchants", 60, "regular merchants (profile 8.1)")
		extra        = fs.Int("extra", 1, "additional merchants (acceptance 8.3)")
		tolerance    = fs.Int("tolerance-bps", 100, "tolerance in basis points, 0..500")
		prefix       = fs.String("name-prefix", "stand-merchant", "merchant name prefix")
		domain       = fs.String("email-domain", "stand.example.com", "domain of merchant emails")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	for name, v := range map[string]string{"base-url": *baseURL, "mnemonic-file": *mnemonicFile, "keys-file": *keysFile, "webhook-url": *webhookURL} {
		if v == "" {
			return fmt.Errorf("-%s is required", name)
		}
	}
	for _, p := range []string{*mnemonicFile, *keysFile} {
		in, err := setup.InsideGitRepo(p)
		if err != nil {
			return err
		}
		if in {
			return errors.New("secret files must be outside a git repository: " + p)
		}
	}
	raw, err := os.ReadFile(*mnemonicFile)
	if err != nil {
		return fmt.Errorf("read mnemonic file: %w", err)
	}
	w, err := wallet.New(string(raw))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	res, err := setup.Run(ctx, client.New(*baseURL, nil), w, setup.Config{
		Count: *count, Extra: *extra, NamePrefix: *prefix, EmailDomain: *domain,
		WebhookURL: *webhookURL, ToleranceBps: *tolerance, KeysPath: *keysFile,
	}, log)
	log.Info("done", "registered", res.Registered, "reused", res.Reused,
		"configured", res.Configured, "unchanged", res.Unchanged)
	return err
}
