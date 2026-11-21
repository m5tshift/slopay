//go:build integration

package setup_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/m5tshift/weigate/pkg/hdwallet"
	"github.com/m5tshift/weigate/testkit/stand-setup/internal/client"
	"github.com/m5tshift/weigate/testkit/stand-setup/internal/setup"
	"github.com/m5tshift/weigate/testkit/stand-setup/internal/wallet"
)

// The test runs against a real merchant-service container. The service is not
// in the repository yet, so the image is passed in MERCHANT_SERVICE_IMAGE
// (a self-contained image that serves HTTP on 8080 and has its own
// dependencies); without it the test is skipped.
func TestAgainstMerchantService(t *testing.T) {
	image := os.Getenv("MERCHANT_SERVICE_IMAGE")
	if image == "" {
		t.Skip("MERCHANT_SERVICE_IMAGE is not set: merchant-service is not built yet")
	}
	ctx := context.Background()
	ctr, err := testcontainers.Run(ctx, image,
		testcontainers.WithExposedPorts("8080/tcp"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/health/ready").WithPort("8080/tcp")),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatal(err)
	}
	base, err := ctr.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		t.Fatal(err)
	}

	w, err := wallet.New("test test test test test test test test test test test junk")
	if err != nil {
		t.Fatal(err)
	}
	cfg := setup.Config{Count: 3, Extra: 1, NamePrefix: "it", EmailDomain: "it.example.com",
		WebhookURL: "https://merchant-mock.example.com/hook", ToleranceBps: 150,
		KeysPath: filepath.Join(t.TempDir(), "keys.json")}
	api := client.New(base, nil)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	res, err := setup.Run(ctx, api, w, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	if res.Registered != 4 {
		t.Fatalf("first run: %+v", res)
	}
	res, err = setup.Run(ctx, api, w, cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	if res.Registered != 0 || res.Unchanged != 4 {
		t.Fatalf("second run is not idempotent: %+v", res)
	}

	keys, err := setup.LoadKeys(cfg.KeysPath)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range keys.Merchants {
		jwt, err := api.Token(ctx, e.APIKey)
		if err != nil {
			t.Fatal(err)
		}
		me, err := api.Me(ctx, jwt)
		if err != nil {
			t.Fatal(err)
		}
		want, _ := w.AccountXpub(uint32(i)) //nolint:gosec // small index
		if me.Xpub == nil || *me.Xpub != want || hdwallet.Validate(want) != nil {
			t.Errorf("merchant %d: xpub mismatch", i)
		}
		if me.WebhookURL == nil || *me.WebhookURL != cfg.WebhookURL || me.ToleranceBps != 150 {
			t.Errorf("merchant %d: settings %+v", i, me)
		}
	}
}
