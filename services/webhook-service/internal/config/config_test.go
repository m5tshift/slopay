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
	t.Setenv("MONGODB_USERNAME", "webhook-service")
	t.Setenv("MONGODB_PASSWORD_FILE", "/run/secrets/mongodb/password")
	t.Setenv("MONGODB_DATABASE", "webhook_service")
	t.Setenv("VALKEY_ADDRS", "valkey:6379")
	t.Setenv("OPENBAO_ADDR", "http://openbao:8200/")
	t.Setenv("OPENBAO_TOKEN_FILE", "/run/secrets/openbao/token")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.AdminAddr != ":8081" || len(c.KafkaBrokers) != 2 || c.ConsumerGroup != "webhook-service" ||
		c.Mongo.Database != "webhook_service" || len(c.Valkey.Addrs) != 1 || c.OpenBaoAddr != "http://openbao:8200" ||
		c.OpenBaoMount != "secret" || c.OpenBaoTokenFile != "/run/secrets/openbao/token" {
		t.Errorf("unexpected: %+v", c)
	}
	if c.DeliveryTimeout != 5*time.Second || c.DedupTTL != 24*time.Hour || c.JournalTTL != 720*time.Hour ||
		c.ValkeyTimeout != 100*time.Millisecond || c.Concurrency != 64 {
		t.Errorf("defaults: %+v", c)
	}
	b := c.Breaker
	if b.FailureThreshold != 5 || b.OpenTimeout != 30*time.Second || b.HalfOpenMax != 1 {
		t.Errorf("breaker: %+v", b)
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("KAFKA_CONSUMER_GROUP", "wh")
	t.Setenv("ADMIN_ADDR", ":9091")
	t.Setenv("OPENBAO_KV_MOUNT", "kv")
	t.Setenv("DELIVERY_TIMEOUT", "2s")
	t.Setenv("DEDUP_TTL", "1h")
	t.Setenv("JOURNAL_TTL", "48h")
	t.Setenv("DELIVERY_CONCURRENCY", "4")
	t.Setenv("ENDPOINT_BREAKER_FAILURE_THRESHOLD", "3")
	t.Setenv("ENDPOINT_BREAKER_OPEN_TIMEOUT", "10s")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.ConsumerGroup != "wh" || c.AdminAddr != ":9091" || c.OpenBaoMount != "kv" || c.DeliveryTimeout != 2*time.Second || c.DedupTTL != time.Hour ||
		c.JournalTTL != 48*time.Hour || c.Concurrency != 4 || c.Breaker.FailureThreshold != 3 ||
		c.Breaker.OpenTimeout != 10*time.Second {
		t.Errorf("overrides: %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"no brokers":     {"KAFKA_BROKERS": ""},
		"no mongo":       {"MONGODB_URI": ""},
		"no valkey":      {"VALKEY_ADDRS": ""},
		"openbao url":    {"OPENBAO_ADDR": "openbao:8200"},
		"no token file":  {"OPENBAO_TOKEN_FILE": ""},
		"timeout":        {"DELIVERY_TIMEOUT": "0s"},
		"journal ttl":    {"JOURNAL_TTL": "soon"},
		"concurrency":    {"DELIVERY_CONCURRENCY": "0"},
		"breaker":        {"ENDPOINT_BREAKER_FAILURE_THRESHOLD": "0"},
		"breaker format": {"ENDPOINT_BREAKER_OPEN_TIMEOUT": "x"},
	} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := FromEnv(); err == nil || strings.TrimSpace(err.Error()) == "" {
				t.Errorf("expected an error for %v", env)
			}
		})
	}
}
