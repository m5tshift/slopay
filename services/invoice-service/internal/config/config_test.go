package config

import (
	"strings"
	"testing"
	"time"
)

func base(t *testing.T) {
	t.Helper()
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("POSTGRES_URL", "postgres://invoice@pg:5432/invoice")
	t.Setenv("POSTGRES_PASSWORD_FILE", "/run/secrets/postgres/password")
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("RATE_SERVICE_URL", "http://rate-service")
	t.Setenv("JWKS_URL", "http://merchant-service/.well-known/jwks.json")
}

func TestDefaults(t *testing.T) {
	base(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || len(c.KafkaBrokers) != 2 || c.ConsumerGroup != "invoice-service" ||
		c.JWTIssuer != "slopay-merchant-service" || c.RateServiceURL != "http://rate-service" {
		t.Errorf("%+v", c)
	}
	// Numbers of SRS 4.1.3, 4.1.4, 4.1.7.
	if c.InvoiceTTL != 15*time.Minute || c.RateMaxAge != 60*time.Second || c.StatusCacheTTL != 30*time.Second ||
		c.RateRetryAfter != 5*time.Second || c.RateServiceTimeout != time.Second || c.ValkeyTimeout != 100*time.Millisecond {
		t.Errorf("%+v", c)
	}
	if c.JWKSURL != "http://merchant-service/.well-known/jwks.json" || c.JWKSTimeout != 2*time.Second ||
		c.JWKSRefresh != 5*time.Minute || c.JWKSMinInterval != 10*time.Second {
		t.Errorf("%+v", c)
	}
	// Numbers of SRS 3.1 and 4.1.4 on Anvil.
	if c.ChainID != 31337 || c.Confirmations != 7 || c.RevertWait != 10*time.Minute || c.TrackingWindow != 24*time.Hour ||
		c.ChainBlocks != 256 || c.BarrierPoll != 50*time.Millisecond {
		t.Errorf("%+v", c)
	}
}

func TestOverrides(t *testing.T) {
	base(t)
	t.Setenv("INVOICE_TTL", "10m")
	t.Setenv("RATE_MAX_AGE", "30s")
	t.Setenv("STATUS_CACHE_TTL", "5s")
	t.Setenv("JWT_ISSUER", "other")
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("CONFIRMATIONS", "12")
	t.Setenv("REVERT_WAIT", "1m")
	t.Setenv("TRACKING_WINDOW", "1h")
	t.Setenv("CHAIN_BLOCKS", "64")
	c, err := FromEnv()
	if err != nil || c.InvoiceTTL != 10*time.Minute || c.RateMaxAge != 30*time.Second || c.StatusCacheTTL != 5*time.Second ||
		c.JWTIssuer != "other" || c.ChainID != 11155111 || c.Confirmations != 12 || c.RevertWait != time.Minute ||
		c.TrackingWindow != time.Hour || c.ChainBlocks != 64 {
		t.Errorf("%+v %v", c, err)
	}
}

func TestInvalid(t *testing.T) {
	cases := map[string][2]string{
		"KAFKA_BROKERS":     {"KAFKA_BROKERS", " , "},
		"RATE_SERVICE_URL":  {"RATE_SERVICE_URL", "rate-service"},
		"RATE_SERVICE_URL ": {"RATE_SERVICE_URL", ""},
		"JWKS_URL":          {"JWKS_URL", ""},
		"JWKS_URL ":         {"JWKS_URL", "merchant-service"},
		"JWKS_REFRESH":      {"JWKS_REFRESH", "0"},
		"RATE_MAX_AGE":      {"RATE_MAX_AGE", "61s"},
		"INVOICE_TTL":       {"INVOICE_TTL", "-1m"},
		"STATUS_CACHE_TTL":  {"STATUS_CACHE_TTL", "soon"},
		"POSTGRES_URL":      {"POSTGRES_URL", ""},
		"VALKEY_ADDRS":      {"VALKEY_ADDRS", ""},
		"CHAIN_ID":          {"CHAIN_ID", "anvil"},
		"CONFIRMATIONS":     {"CONFIRMATIONS", "1"},
		"CONFIRMATIONS ":    {"CONFIRMATIONS", "0"},
		"REVERT_WAIT":       {"REVERT_WAIT", "0s"},
		"TRACKING_WINDOW":   {"TRACKING_WINDOW", "-1h"},
		"CHAIN_BLOCKS":      {"CHAIN_BLOCKS", "7"},
		"CHAIN_BLOCKS ":     {"CHAIN_BLOCKS", "x"},
		"BARRIER_POLL":      {"BARRIER_POLL", "0"},
	}
	for name, kv := range cases {
		t.Run(name, func(t *testing.T) {
			base(t)
			t.Setenv(kv[0], kv[1])
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), strings.TrimSpace(name)) {
				t.Errorf("%v", err)
			}
		})
	}
}
