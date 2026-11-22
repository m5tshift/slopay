package config

import (
	"strings"
	"testing"
	"time"
)

var allVars = []string{
	"HTTP_ADDR", "LOG_LEVEL", "KAFKA_BROKERS", "POSTGRES_URL", "POSTGRES_PASSWORD_FILE", "POSTGRES_MAX_CONNS",
	"JWT_SIGNING_KEY_FILE", "JWT_ISSUER", "JWT_TTL", "JWKS_MAX_AGE", "API_KEY_ROTATION_GRACE",
	"API_KEY_MAX_ACTIVE", "TOLERANCE_DEFAULT_BPS", "TOLERANCE_MAX_BPS",
	"OPENBAO_ADDR", "OPENBAO_TOKEN_FILE", "OPENBAO_KV_MOUNT", "JWT_TRANSIT_KEY", "OPENBAO_TRANSIT_MOUNT", "TRANSIT_KEYS_REFRESH",
}

func setRequired(t *testing.T) {
	t.Helper()
	for _, v := range allVars {
		t.Setenv(v, "")
	}
	t.Setenv("KAFKA_BROKERS", "k1:9092, k2:9092,")
	t.Setenv("POSTGRES_URL", "postgres://merchant@pg:5432/merchant")
	t.Setenv("POSTGRES_PASSWORD_FILE", "/var/run/secrets/weigate/postgres/password")
	t.Setenv("JWT_SIGNING_KEY_FILE", "/var/run/secrets/weigate/jwt/private.pem")
}

func TestDefaults(t *testing.T) {
	setRequired(t)
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.LogLevel != "info" || c.JWTIssuer != "weigate-merchant-service" {
		t.Errorf("defaults: %+v", c)
	}
	if len(c.KafkaBrokers) != 2 || c.KafkaBrokers[1] != "k2:9092" {
		t.Errorf("brokers = %v", c.KafkaBrokers)
	}
	// SRS 4.1.10 and 4.1.4.
	if c.JWTTTL != 15*time.Minute || c.KeyRotationGrace != 24*time.Hour || c.MaxActiveKeys != 2 ||
		c.DefaultToleranceBps != 100 || c.MaxToleranceBps != 500 || c.JWKSMaxAge != 5*time.Minute {
		t.Errorf("SRS defaults: %+v", c)
	}
	if c.OpenBaoAddr != "" || c.OpenBaoMount != "secret" {
		t.Errorf("openbao defaults: %+v", c)
	}
	if c.Postgres.URL == "" || c.Postgres.PasswordFile == "" {
		t.Errorf("postgres: %+v", c.Postgres)
	}
}

func TestOverrides(t *testing.T) {
	setRequired(t)
	t.Setenv("HTTP_ADDR", ":9000")
	t.Setenv("JWT_TTL", "10m")
	t.Setenv("API_KEY_ROTATION_GRACE", "1h")
	t.Setenv("API_KEY_MAX_ACTIVE", "3")
	t.Setenv("TOLERANCE_DEFAULT_BPS", "0")
	t.Setenv("TOLERANCE_MAX_BPS", "300")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9000" || c.JWTTTL != 10*time.Minute || c.KeyRotationGrace != time.Hour ||
		c.MaxActiveKeys != 3 || c.DefaultToleranceBps != 0 || c.MaxToleranceBps != 300 {
		t.Errorf("overrides: %+v", c)
	}
}

func TestOpenBao(t *testing.T) {
	setRequired(t)
	t.Setenv("OPENBAO_ADDR", "https://openbao:8200")
	t.Setenv("OPENBAO_TOKEN_FILE", "/var/run/secrets/weigate/openbao/token")
	t.Setenv("OPENBAO_KV_MOUNT", "kv")
	c, err := FromEnv()
	if err != nil || c.OpenBaoAddr != "https://openbao:8200" || c.OpenBaoTokenFile == "" || c.OpenBaoMount != "kv" {
		t.Errorf("openbao: %+v %v", c, err)
	}
}

func TestTransit(t *testing.T) {
	setRequired(t)
	t.Setenv("JWT_SIGNING_KEY_FILE", "")
	t.Setenv("JWT_TRANSIT_KEY", "merchant-jwt")
	t.Setenv("OPENBAO_ADDR", "https://openbao:8200")
	t.Setenv("OPENBAO_TOKEN_FILE", "/var/run/secrets/weigate/openbao/token")
	c, err := FromEnv()
	if err != nil || c.JWTTransitKey != "merchant-jwt" || c.OpenBaoTransitMount != "transit" || c.TransitKeysRefresh != time.Minute {
		t.Errorf("transit: %+v %v", c, err)
	}
	t.Setenv("OPENBAO_TRANSIT_MOUNT", "tr")
	t.Setenv("TRANSIT_KEYS_REFRESH", "30s")
	c, err = FromEnv()
	if err != nil || c.OpenBaoTransitMount != "tr" || c.TransitKeysRefresh != 30*time.Second {
		t.Errorf("transit overrides: %+v %v", c, err)
	}
}

func TestErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		env  map[string]string
		want string
	}{
		"no brokers":            {map[string]string{"KAFKA_BROKERS": " , "}, "KAFKA_BROKERS is required"},
		"no key file":           {map[string]string{"JWT_SIGNING_KEY_FILE": ""}, "JWT_SIGNING_KEY_FILE or JWT_TRANSIT_KEY is required"},
		"file and transit":      {map[string]string{"JWT_TRANSIT_KEY": "k", "OPENBAO_ADDR": "https://o:8200", "OPENBAO_TOKEN_FILE": "/t"}, "mutually exclusive"},
		"transit without addr":  {map[string]string{"JWT_SIGNING_KEY_FILE": "", "JWT_TRANSIT_KEY": "k"}, "JWT_TRANSIT_KEY requires OPENBAO_ADDR"},
		"bad transit refresh":   {map[string]string{"TRANSIT_KEYS_REFRESH": "0"}, "TRANSIT_KEYS_REFRESH"},
		"openbao without token": {map[string]string{"OPENBAO_ADDR": "https://openbao:8200"}, "OPENBAO_TOKEN_FILE is required"},
		"no postgres":           {map[string]string{"POSTGRES_URL": ""}, "POSTGRES_URL"},
		"bad ttl":               {map[string]string{"JWT_TTL": "soon"}, "JWT_TTL must be a positive duration"},
		"negative grace":        {map[string]string{"API_KEY_ROTATION_GRACE": "-1h"}, "API_KEY_ROTATION_GRACE"},
		"zero keys":             {map[string]string{"API_KEY_MAX_ACTIVE": "0"}, "API_KEY_MAX_ACTIVE must be an integer >= 1"},
		"bad tolerance":         {map[string]string{"TOLERANCE_DEFAULT_BPS": "x"}, "TOLERANCE_DEFAULT_BPS"},
		"max over 5 %":          {map[string]string{"TOLERANCE_MAX_BPS": "501"}, "must not exceed 500"},
		"default over max":      {map[string]string{"TOLERANCE_MAX_BPS": "50"}, "TOLERANCE_DEFAULT_BPS must not exceed"},
	} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := FromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}
