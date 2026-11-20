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
	t.Setenv("MONGODB_USERNAME", "chain-watcher")
	t.Setenv("MONGODB_PASSWORD_FILE", "/run/secrets/mongodb/password")
	t.Setenv("MONGODB_DATABASE", "chain_watcher")
	t.Setenv("NODE_URL", "http://anvil:8545")
	t.Setenv("CHAIN_ID", "31337")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || len(c.KafkaBrokers) != 2 || c.Mongo.Database != "chain_watcher" ||
		c.NodeURL != "http://anvil:8545" || c.ChainID != 31337 || c.StartBlock != nil || c.LeaderLease != "" {
		t.Errorf("unexpected: %+v", c)
	}
	if c.WindowSize != 64 || c.MaxReorgDepth != 6 || c.NodeTimeout != 5*time.Second || c.PollInterval != time.Second ||
		c.BackoffInitial != 500*time.Millisecond || c.BackoffMax != 30*time.Second {
		t.Errorf("defaults: %+v", c)
	}
	b := c.NodeBreaker
	if b.Dependency != "node" || b.FailureThreshold != 5 || b.OpenTimeout != 15*time.Second || b.HalfOpenMax != 1 {
		t.Errorf("breaker: %+v", b)
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("NODE_URL", "https://sepolia.example.org/rpc")
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("CHAIN_START_BLOCK", "0")
	t.Setenv("WINDOW_SIZE", "128")
	t.Setenv("REORG_MAX_DEPTH", "12")
	t.Setenv("NODE_TIMEOUT", "2s")
	t.Setenv("POLL_INTERVAL", "12s")
	t.Setenv("BACKOFF_INITIAL", "1s")
	t.Setenv("BACKOFF_MAX", "1m")
	t.Setenv("NODE_BREAKER_FAILURE_THRESHOLD", "3")
	t.Setenv("LEADER_LEASE_NAME", "chain-watcher")
	t.Setenv("POD_NAME", "chain-watcher-0")
	t.Setenv("POD_NAMESPACE", "apps")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9090" || c.ChainID != 11155111 || c.StartBlock == nil || *c.StartBlock != 0 ||
		c.WindowSize != 128 || c.MaxReorgDepth != 12 || c.NodeTimeout != 2*time.Second ||
		c.PollInterval != 12*time.Second || c.BackoffInitial != time.Second || c.BackoffMax != time.Minute ||
		c.NodeBreaker.FailureThreshold != 3 || c.LeaderLease != "chain-watcher" || c.PodName != "chain-watcher-0" {
		t.Errorf("overrides: %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for name, tc := range map[string]struct {
		env  map[string]string
		want string
	}{
		"no brokers":        {map[string]string{"KAFKA_BROKERS": " , "}, "KAFKA_BROKERS"},
		"no mongodb":        {map[string]string{"MONGODB_URI": ""}, "MONGODB_URI"},
		"no node":           {map[string]string{"NODE_URL": ""}, "NODE_URL"},
		"node scheme":       {map[string]string{"NODE_URL": "ws://anvil:8545"}, "NODE_URL"},
		"no chain":          {map[string]string{"CHAIN_ID": ""}, "CHAIN_ID is required"},
		"zero chain":        {map[string]string{"CHAIN_ID": "0"}, "CHAIN_ID must"},
		"bad chain":         {map[string]string{"CHAIN_ID": "anvil"}, "CHAIN_ID must"},
		"bad start":         {map[string]string{"CHAIN_START_BLOCK": "-1"}, "CHAIN_START_BLOCK"},
		"bad window":        {map[string]string{"WINDOW_SIZE": "1"}, "WINDOW_SIZE"},
		"depth zero":        {map[string]string{"REORG_MAX_DEPTH": "0"}, "REORG_MAX_DEPTH must be an integer"},
		"depth too big":     {map[string]string{"REORG_MAX_DEPTH": "64"}, "smaller than WINDOW_SIZE"},
		"bad timeout":       {map[string]string{"NODE_TIMEOUT": "0s"}, "NODE_TIMEOUT"},
		"bad poll":          {map[string]string{"POLL_INTERVAL": "soon"}, "POLL_INTERVAL"},
		"backoff inverted":  {map[string]string{"BACKOFF_INITIAL": "1m", "BACKOFF_MAX": "1s"}, "BACKOFF_MAX"},
		"bad breaker":       {map[string]string{"NODE_BREAKER_OPEN_TIMEOUT": "x"}, "NODE_BREAKER_OPEN_TIMEOUT"},
		"lease without pod": {map[string]string{"LEADER_LEASE_NAME": "chain-watcher"}, "POD_NAME"},
	} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
