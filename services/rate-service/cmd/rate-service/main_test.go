package main

import (
	"log/slog"
	"testing"
	"time"

	"github.com/m5tshift/slopay/services/rate-service/internal/config"
	"github.com/m5tshift/slopay/services/rate-service/internal/source"
	"github.com/m5tshift/slopay/services/rate-service/internal/store"
)

func TestNewSourcesMock(t *testing.T) {
	f, lim, err := newSources(config.Config{SourceMode: config.ModeMock, RateMockURL: "http://rate-mock:8080", SourceTimeout: time.Second},
		store.New(time.Second), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.(*source.Mock); !ok || lim != nil {
		t.Fatalf("mock mode: fetcher %T, limiter %v", f, lim)
	}
}

func TestNewSourcesExchanges(t *testing.T) {
	cfg := config.Config{
		SourceMode: config.ModeExchanges, CoinbaseURL: source.DefaultCoinbaseURL, OKXURL: source.DefaultOKXURL,
		SourceTimeout: time.Second, ValkeyTimeout: time.Second, Limits: config.DefaultLimits,
	}
	f, lim, err := newSources(cfg, store.New(time.Second), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := f.(*source.Exchanges); !ok || lim == nil {
		t.Fatalf("exchanges mode: fetcher %T, limiter %v", f, lim)
	}
	cfg.OKXURL = "nope"
	if _, _, err := newSources(cfg, store.New(time.Second), slog.Default()); err == nil {
		t.Fatal("bad URL accepted")
	}
	cfg.OKXURL = source.DefaultOKXURL
	cfg.Limits = nil
	if _, _, err := newSources(cfg, store.New(time.Second), slog.Default()); err == nil {
		t.Fatal("no limits accepted")
	}
}
