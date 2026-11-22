package config

import (
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("CLICKHOUSE_ADDR", "clickhouse:9000")
	t.Setenv("CLICKHOUSE_USERNAME", "reporting")
	t.Setenv("CLICKHOUSE_PASSWORD_FILE", "/run/secrets/clickhouse/password")
	t.Setenv("CLICKHOUSE_DATABASE", "weigate")
	t.Setenv("JWKS_URL", "http://merchant-service/.well-known/jwks.json")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.ClickHouse.Database != "weigate" || c.ClickHouse.Username != "reporting" ||
		c.JWTIssuer != "weigate-merchant-service" || c.MaxDays != 92 {
		t.Errorf("unexpected: %+v", c)
	}
	if c.JWKSTimeout != 2*time.Second || c.JWKSRefresh != 5*time.Minute || c.JWKSMinInterval != 10*time.Second ||
		c.QueryTimeout != 10*time.Second || c.RetryAfter != 5*time.Second {
		t.Errorf("defaults: %+v", c)
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("JWT_ISSUER", "iss")
	t.Setenv("REPORT_MAX_DAYS", "31")
	t.Setenv("QUERY_TIMEOUT", "3s")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9090" || c.JWTIssuer != "iss" || c.MaxDays != 31 || c.QueryTimeout != 3*time.Second {
		t.Errorf("unexpected: %+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"no clickhouse": {"CLICKHOUSE_ADDR": ""},
		"no jwks":       {"JWKS_URL": ""},
		"jwks not http": {"JWKS_URL": "ftp://merchant-service/jwks"},
		"max days zero": {"REPORT_MAX_DAYS": "0"},
		"max days text": {"REPORT_MAX_DAYS": "many"},
		"bad duration":  {"QUERY_TIMEOUT": "soon"},
		"zero duration": {"RETRY_AFTER": "0s"},
	} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			for k, v := range env {
				t.Setenv(k, v)
			}
			if _, err := FromEnv(); err == nil {
				t.Error("want an error")
			}
		})
	}
}

func TestErrorsAreJoined(t *testing.T) {
	t.Setenv("CLICKHOUSE_ADDR", "")
	t.Setenv("JWKS_URL", "")
	_, err := FromEnv()
	if err == nil || !strings.Contains(err.Error(), "CLICKHOUSE_ADDR") || !strings.Contains(err.Error(), "JWKS_URL") {
		t.Errorf("err = %v", err)
	}
}
