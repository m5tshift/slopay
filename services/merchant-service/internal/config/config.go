// Package config reads the merchant-service configuration from the environment. Secrets are never
// passed in variables: only paths of mounted secret files are.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/m5tshift/weigate/pkg/storage"
)

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address of the API, health and metrics (HTTP_ADDR, default :8080).
	HTTPAddr string
	// LogLevel is debug, info, warn or error (LOG_LEVEL, default info).
	LogLevel string
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// Postgres is read by storage.PostgresConfigFromEnv("POSTGRES").
	Postgres storage.PostgresConfig

	// JWTKeyFile is the mounted ECDSA P-256 private key in PEM (JWT_SIGNING_KEY_FILE, required).
	JWTKeyFile string
	// JWTIssuer is the iss claim checked by the gateway (JWT_ISSUER, default weigate-merchant-service).
	JWTIssuer string
	// JWTTTL is the token lifetime (JWT_TTL, SRS 4.1.10: 15m).
	JWTTTL time.Duration
	// JWKSMaxAge is the Cache-Control max-age of the JWKS (JWKS_MAX_AGE, default 5m).
	JWKSMaxAge time.Duration

	// KeyRotationGrace is how long the previous API key works after a new one is issued
	// (API_KEY_ROTATION_GRACE, SRS 4.1.10: 24h).
	KeyRotationGrace time.Duration
	// MaxActiveKeys is the number of API keys active at the same time (API_KEY_MAX_ACTIVE, SRS 4.1.10: 2).
	MaxActiveKeys int

	// OpenBaoAddr, OpenBaoTokenFile and OpenBaoMount configure the KV v2 store of webhook secrets
	// (OPENBAO_ADDR, OPENBAO_TOKEN_FILE, OPENBAO_KV_MOUNT, default "secret"). Without OPENBAO_ADDR the
	// rotation endpoint answers 501.
	OpenBaoAddr      string
	OpenBaoTokenFile string
	OpenBaoMount     string

	// DefaultToleranceBps is the tolerance when the merchant does not set one
	// (TOLERANCE_DEFAULT_BPS, SRS 4.1.4: 100 = 1 %).
	DefaultToleranceBps int
	// MaxToleranceBps is the highest allowed tolerance (TOLERANCE_MAX_BPS, SRS 4.1.4: 500 = 5 %).
	MaxToleranceBps int
}

// contractMaxToleranceBps is the upper bound of ToleranceBps in the OpenAPI contract and the event schema.
const contractMaxToleranceBps = 500

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:     envOr("HTTP_ADDR", ":8080"),
		LogLevel:     envOr("LOG_LEVEL", "info"),
		KafkaBrokers: splitList(os.Getenv("KAFKA_BROKERS")),
		JWTKeyFile:   os.Getenv("JWT_SIGNING_KEY_FILE"),
		JWTIssuer:    envOr("JWT_ISSUER", "weigate-merchant-service"),

		OpenBaoAddr:      os.Getenv("OPENBAO_ADDR"),
		OpenBaoTokenFile: os.Getenv("OPENBAO_TOKEN_FILE"),
		OpenBaoMount:     envOr("OPENBAO_KV_MOUNT", "secret"),
	}
	if c.OpenBaoAddr != "" && c.OpenBaoTokenFile == "" {
		errs = append(errs, errors.New("OPENBAO_TOKEN_FILE is required with OPENBAO_ADDR"))
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if c.JWTKeyFile == "" {
		errs = append(errs, errors.New("JWT_SIGNING_KEY_FILE is required"))
	}
	pg, err := storage.PostgresConfigFromEnv("POSTGRES")
	errs = append(errs, err)
	c.Postgres = pg

	c.JWTTTL, err = duration("JWT_TTL", 15*time.Minute)
	errs = append(errs, err)
	c.JWKSMaxAge, err = duration("JWKS_MAX_AGE", 5*time.Minute)
	errs = append(errs, err)
	c.KeyRotationGrace, err = duration("API_KEY_ROTATION_GRACE", 24*time.Hour)
	errs = append(errs, err)
	c.MaxActiveKeys, err = integer("API_KEY_MAX_ACTIVE", 2, 1)
	errs = append(errs, err)
	c.DefaultToleranceBps, err = integer("TOLERANCE_DEFAULT_BPS", 100, 0)
	errs = append(errs, err)
	c.MaxToleranceBps, err = integer("TOLERANCE_MAX_BPS", contractMaxToleranceBps, 0)
	errs = append(errs, err)
	if c.MaxToleranceBps > contractMaxToleranceBps {
		errs = append(errs, fmt.Errorf("TOLERANCE_MAX_BPS must not exceed %d", contractMaxToleranceBps))
	}
	if c.DefaultToleranceBps > c.MaxToleranceBps {
		errs = append(errs, errors.New("TOLERANCE_DEFAULT_BPS must not exceed TOLERANCE_MAX_BPS"))
	}
	return c, errors.Join(errs...)
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func duration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def, fmt.Errorf("%s must be a positive duration, e.g. 15m", name)
	}
	return d, nil
}

func integer(name string, def, minimum int) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < minimum {
		return def, fmt.Errorf("%s must be an integer >= %d", name, minimum)
	}
	return n, nil
}
