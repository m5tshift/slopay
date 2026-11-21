package config

import (
	"strings"
	"testing"
)

var allVars = []string{
	"HTTP_ADDR", "KAFKA_BROKERS", "KAFKA_CONSUMER_GROUP", "POSTGRES_URL", "POSTGRES_PASSWORD_FILE",
	"POSTGRES_MAX_CONNS", "FEE_BPS",
}

func setRequired(t *testing.T) {
	t.Helper()
	for _, v := range allVars {
		t.Setenv(v, "")
	}
	t.Setenv("KAFKA_BROKERS", "k1:9092, k2:9092,")
	t.Setenv("POSTGRES_URL", "postgres://ledger-service@pg:5432/ledger")
	t.Setenv("POSTGRES_PASSWORD_FILE", "/var/run/secrets/slopay/postgres/password")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	// SRS 4.1.3: the fee is 0.5 %.
	if c.HTTPAddr != ":8080" || c.ConsumerGroup != "ledger-service" || c.FeeBps != 50 {
		t.Errorf("defaults: %+v", c)
	}
	if len(c.KafkaBrokers) != 2 || c.KafkaBrokers[1] != "k2:9092" {
		t.Errorf("brokers = %v", c.KafkaBrokers)
	}
	if c.Postgres.URL == "" || c.Postgres.PasswordFile == "" {
		t.Errorf("postgres: %+v", c.Postgres)
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("HTTP_ADDR", ":9000")
	t.Setenv("KAFKA_CONSUMER_GROUP", "ledger-replay")
	for _, v := range []string{"0", "75", "10000"} {
		t.Setenv("FEE_BPS", v)
		c, err := FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if c.HTTPAddr != ":9000" || c.ConsumerGroup != "ledger-replay" || c.FeeBps == 50 {
			t.Errorf("FEE_BPS=%s: %+v", v, c)
		}
	}
}

func TestInvalid(t *testing.T) {
	for name, c := range map[string]struct{ key, value, want string }{
		"no brokers":       {"KAFKA_BROKERS", " , ", "KAFKA_BROKERS"},
		"no postgres":      {"POSTGRES_URL", "", "POSTGRES_URL"},
		"fee not a number": {"FEE_BPS", "0.5", "FEE_BPS"},
		"negative fee":     {"FEE_BPS", "-1", "FEE_BPS"},
		"fee above 100 %":  {"FEE_BPS", "10001", "FEE_BPS"},
	} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			t.Setenv(c.key, c.value)
			if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("got %v, want an error about %s", err, c.want)
			}
		})
	}
}
