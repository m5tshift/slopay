package config

import (
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("KAFKA_BROKERS", "k1:9092, k2:9092,")
	t.Setenv("MONGODB_URI", "mongodb://mongodb:27017")
	t.Setenv("MONGODB_USERNAME", "reconciliation-service")
	t.Setenv("MONGODB_PASSWORD_FILE", "/run/secrets/mongodb/password")
	t.Setenv("MONGODB_DATABASE", "reconciliation")
	t.Setenv("NODE_URL", "http://anvil:8545")
	t.Setenv("LEDGER_URL", "http://ledger-service:8080/")
	t.Setenv("CHAIN_ID", "31337")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || len(c.KafkaBrokers) != 2 || c.ConsumerGroup != "reconciliation-service" ||
		c.Mongo.Database != "reconciliation" || c.NodeURL != "http://anvil:8545" ||
		c.LedgerURL != "http://ledger-service:8080" || c.ChainID != 31337 || c.LeaderLease != "" {
		t.Errorf("unexpected: %+v", c)
	}
	if c.Confirmations != 7 || c.BatchSize != 100 || c.NodeTimeout != 5*time.Second || c.LedgerTimeout != 2*time.Second ||
		c.Interval != 10*time.Minute || c.RunTimeout != 5*time.Minute || c.SettleDelay != 15*time.Second ||
		c.Lookback != 168*time.Hour || c.TrackingWindow != 24*time.Hour || c.ProjectionTTL != 192*time.Hour ||
		c.ReportTTL != 720*time.Hour {
		t.Errorf("defaults: %+v", c)
	}
	b := c.NodeBreaker
	if b.Dependency != "node" || b.FailureThreshold != 5 || b.OpenTimeout != 15*time.Second || b.HalfOpenMax != 1 {
		t.Errorf("breaker: %+v", b)
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("CONFIRMATIONS", "12")
	t.Setenv("RPC_BATCH_SIZE", "20")
	t.Setenv("RUN_INTERVAL", "1m")
	t.Setenv("SETTLE_DELAY", "0s")
	t.Setenv("ADDRESS_LOOKBACK", "24h")
	t.Setenv("PROJECTION_TTL", "48h")
	t.Setenv("LEADER_LEASE_NAME", "reconciliation-service")
	t.Setenv("POD_NAME", "reconciliation-service-0")
	t.Setenv("POD_NAMESPACE", "apps")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Confirmations != 12 || c.BatchSize != 20 || c.Interval != time.Minute || c.SettleDelay != 0 ||
		c.Lookback != 24*time.Hour || c.ProjectionTTL != 48*time.Hour || c.LeaderLease != "reconciliation-service" {
		t.Errorf("overrides: %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for _, tc := range []struct{ name, key, value, want string }{
		{"no brokers", "KAFKA_BROKERS", "", "KAFKA_BROKERS"},
		{"no node", "NODE_URL", "", "NODE_URL"},
		{"node scheme", "NODE_URL", "ws://anvil:8545", "NODE_URL"},
		{"no ledger", "LEDGER_URL", "", "LEDGER_URL"},
		{"no chain", "CHAIN_ID", "", "CHAIN_ID"},
		{"zero chain", "CHAIN_ID", "0", "CHAIN_ID"},
		{"confirmations", "CONFIRMATIONS", "0", "CONFIRMATIONS"},
		{"batch", "RPC_BATCH_SIZE", "x", "RPC_BATCH_SIZE"},
		{"interval", "RUN_INTERVAL", "-1m", "RUN_INTERVAL"},
		{"settle", "SETTLE_DELAY", "-1s", "SETTLE_DELAY"},
		{"settle vs timeout", "SETTLE_DELAY", "10m", "RUN_TIMEOUT"},
		{"projection ttl", "PROJECTION_TTL", "24h", "PROJECTION_TTL"},
		{"lease without pod", "LEADER_LEASE_NAME", "reconciliation-service", "POD_NAME"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setRequired(t)
			t.Setenv(tc.key, tc.value)
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %s", err, tc.want)
			}
		})
	}
}
