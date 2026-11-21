package config

import (
	"strings"
	"testing"
	"time"

	"github.com/m5tshift/slopay/services/rate-service/internal/rate"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("KAFKA_BROKERS", "k1:9092, k2:9092,")
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("RATE_MOCK_URL", "http://rate-mock:8080")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || len(c.KafkaBrokers) != 2 || c.SourceMode != ModeMock {
		t.Errorf("unexpected: %+v", c)
	}
	if c.PollInterval != 5*time.Second || c.RateMaxAge != 60*time.Second || c.USDTMaxAge != time.Hour {
		t.Errorf("timings: %+v", c)
	}
	if c.DivergenceMaxBps != 100 || c.RetryAfter != 5*time.Second || c.ValkeyTimeout != 200*time.Millisecond ||
		c.SourceTimeout != 2*time.Second || c.FaultInjection || c.LeaderLease != "" {
		t.Errorf("defaults: %+v", c)
	}
	for _, src := range []rate.Source{rate.Coinbase, rate.OKX} {
		b := c.Breakers[src]
		if b.Dependency != string(src) || b.FailureThreshold != 3 || b.OpenTimeout != 30*time.Second || b.HalfOpenMax != 1 {
			t.Errorf("breaker %s: %+v", src, b)
		}
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("POLL_INTERVAL", "1s")
	t.Setenv("RATE_MAX_AGE", "30s")
	t.Setenv("USDT_MAX_AGE", "10m")
	t.Setenv("DIVERGENCE_MAX_BPS", "50")
	t.Setenv("RETRY_AFTER", "2s")
	t.Setenv("FAULT_INJECTION_ENABLED", "true")
	t.Setenv("SOURCE_BREAKER_FAILURE_THRESHOLD", "5")
	t.Setenv("LEADER_LEASE_NAME", "rate-service")
	t.Setenv("POD_NAME", "rate-service-0")
	t.Setenv("POD_NAMESPACE", "apps")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9090" || c.PollInterval != time.Second || c.RateMaxAge != 30*time.Second ||
		c.USDTMaxAge != 10*time.Minute || c.DivergenceMaxBps != 50 || c.RetryAfter != 2*time.Second ||
		!c.FaultInjection || c.Breakers[rate.OKX].FailureThreshold != 5 || c.LeaderLease != "rate-service" {
		t.Errorf("overrides: %+v", c)
	}
}

func TestErrors(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"missing all", map[string]string{"KAFKA_BROKERS": "", "VALKEY_ADDRS": "", "RATE_MOCK_URL": ""},
			"KAFKA_BROKERS is required"},
		{"no valkey", map[string]string{"VALKEY_ADDRS": ""}, "VALKEY_ADDRS is required"},
		{"no mock url", map[string]string{"RATE_MOCK_URL": ""}, "RATE_MOCK_URL is required"},
		{"bad limit requests", map[string]string{"RATE_SOURCE_MODE": "exchanges", "SOURCE_LIMIT_OKX_REQUESTS": "0"}, "SOURCE_LIMIT_OKX_REQUESTS must be an integer"},
		{"bad limit window", map[string]string{"RATE_SOURCE_MODE": "exchanges", "SOURCE_LIMIT_COINBASE_WINDOW": "fast"}, "SOURCE_LIMIT_COINBASE_WINDOW"},
		{"unusable limit", map[string]string{"RATE_SOURCE_MODE": "exchanges", "SOURCE_LIMIT_COINBASE_REQUESTS": "100000"}, "not a usable limit"},
		{"bad mode", map[string]string{"RATE_SOURCE_MODE": "auto"}, "must be mock or exchanges"},
		{"lease without pod", map[string]string{"LEADER_LEASE_NAME": "rate-service"}, "POD_NAME and POD_NAMESPACE"},
		{"bad breaker", map[string]string{"SOURCE_BREAKER_OPEN_TIMEOUT": "soon"}, "SOURCE_BREAKER_OPEN_TIMEOUT"},
		{"bad duration", map[string]string{"POLL_INTERVAL": "-1s"}, "POLL_INTERVAL must be a positive duration"},
		{"max age above contract", map[string]string{"RATE_MAX_AGE": "61s"}, "RATE_MAX_AGE must not exceed"},
		{"retry after", map[string]string{"RETRY_AFTER": "500ms"}, "RETRY_AFTER must be at least 1s"},
		{"bad bps", map[string]string{"DIVERGENCE_MAX_BPS": "-1"}, "DIVERGENCE_MAX_BPS must be an integer"},
		{"bad bps text", map[string]string{"DIVERGENCE_MAX_BPS": "one"}, "DIVERGENCE_MAX_BPS must be an integer"},
		{"bad bool", map[string]string{"FAULT_INJECTION_ENABLED": "yes please"}, "FAULT_INJECTION_ENABLED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setRequired(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestExchangesMode(t *testing.T) {
	setRequired(t)
	t.Setenv("RATE_SOURCE_MODE", "exchanges")
	t.Setenv("RATE_MOCK_URL", "") // not needed in this mode
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.SourceMode != ModeExchanges || c.CoinbaseURL != "https://api.exchange.coinbase.com" || c.OKXURL != "https://www.okx.com" {
		t.Errorf("exchanges: %+v", c)
	}
	if l := c.Limits[rate.Coinbase]; l.Capacity != 10 || l.Window != time.Second {
		t.Errorf("coinbase limit: %+v", l)
	}
	if l := c.Limits[rate.OKX]; l.Capacity != 20 || l.Window != 2*time.Second {
		t.Errorf("okx limit: %+v", l)
	}

	t.Setenv("COINBASE_URL", "http://coinbase-egress:8080")
	t.Setenv("OKX_URL", "http://okx-egress:8080")
	t.Setenv("SOURCE_LIMIT_OKX_REQUESTS", "5")
	t.Setenv("SOURCE_LIMIT_OKX_WINDOW", "10s")
	c, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.CoinbaseURL != "http://coinbase-egress:8080" || c.OKXURL != "http://okx-egress:8080" ||
		c.Limits[rate.OKX].Capacity != 5 || c.Limits[rate.OKX].Window != 10*time.Second {
		t.Errorf("overrides: %+v", c)
	}
}

func TestMockModeHasNoLimits(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Limits) != 0 {
		t.Errorf("limits in mock mode: %+v", c.Limits)
	}
}
