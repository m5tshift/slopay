// Package config reads the invoice-service configuration from the environment. Secrets are never passed in
// variables: only paths of mounted secret files are (storage.PostgresConfigFromEnv, ValkeyConfigFromEnv).
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/m5tshift/slopay/pkg/storage"
)

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address of the API, health and metrics (HTTP_ADDR, default :8080).
	HTTPAddr string
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// ConsumerGroup is the group of merchant.events (KAFKA_CONSUMER_GROUP, default invoice-service).
	ConsumerGroup string
	// Postgres is read by storage.PostgresConfigFromEnv("POSTGRES").
	Postgres storage.PostgresConfig
	// Valkey is read by storage.ValkeyConfigFromEnv("VALKEY"). The service starts without it: it is a cache.
	Valkey storage.ValkeyConfig
	// ValkeyTimeout bounds one Valkey call (VALKEY_TIMEOUT, default 100ms).
	ValkeyTimeout time.Duration
	// StatusCacheTTL is the lifetime of a status cache entry (STATUS_CACHE_TTL, SRS 4.1.7: 30s).
	StatusCacheTTL time.Duration

	// RateServiceURL is the base URL of rate-service (RATE_SERVICE_URL, required).
	RateServiceURL string
	// RateServiceTimeout bounds one call to rate-service (RATE_SERVICE_TIMEOUT, default 1s; the mesh timeout
	// of 500 ms is shorter, ADR 0003).
	RateServiceTimeout time.Duration
	// RateMaxAge is the oldest rate an invoice is created with (RATE_MAX_AGE, SRS 4.1.3: 60s).
	RateMaxAge time.Duration
	// RateRetryAfter is Retry-After of 503 when rate-service suggested none (RATE_RETRY_AFTER, default 5s).
	RateRetryAfter time.Duration
	// InvoiceTTL is the lifetime of an invoice (INVOICE_TTL, SRS 4.1.4: 15m).
	InvoiceTTL time.Duration

	// JWTIssuer is the iss claim a merchant token must carry (JWT_ISSUER, default slopay-merchant-service).
	JWTIssuer string
}

// maxRateAge is the bound of SRS 4.1.3 and NFR-08: a larger RATE_MAX_AGE would break the requirement.
const maxRateAge = 60 * time.Second

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:       envOr("HTTP_ADDR", ":8080"),
		KafkaBrokers:   splitList(os.Getenv("KAFKA_BROKERS")),
		ConsumerGroup:  envOr("KAFKA_CONSUMER_GROUP", "invoice-service"),
		RateServiceURL: os.Getenv("RATE_SERVICE_URL"),
		JWTIssuer:      envOr("JWT_ISSUER", "slopay-merchant-service"),
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if u, err := url.Parse(c.RateServiceURL); c.RateServiceURL == "" || err != nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, errors.New("RATE_SERVICE_URL must be an http(s) URL, e.g. http://rate-service"))
	}
	var err error
	c.Postgres, err = storage.PostgresConfigFromEnv("POSTGRES")
	errs = append(errs, err)
	c.Valkey, err = storage.ValkeyConfigFromEnv("VALKEY")
	errs = append(errs, err)

	c.ValkeyTimeout, err = duration("VALKEY_TIMEOUT", 100*time.Millisecond)
	errs = append(errs, err)
	c.StatusCacheTTL, err = duration("STATUS_CACHE_TTL", 30*time.Second)
	errs = append(errs, err)
	c.RateServiceTimeout, err = duration("RATE_SERVICE_TIMEOUT", time.Second)
	errs = append(errs, err)
	c.RateMaxAge, err = duration("RATE_MAX_AGE", maxRateAge)
	errs = append(errs, err)
	if c.RateMaxAge > maxRateAge {
		errs = append(errs, fmt.Errorf("RATE_MAX_AGE must not exceed %s", maxRateAge))
	}
	c.RateRetryAfter, err = duration("RATE_RETRY_AFTER", 5*time.Second)
	errs = append(errs, err)
	c.InvoiceTTL, err = duration("INVOICE_TTL", 15*time.Minute)
	errs = append(errs, err)
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
		return def, fmt.Errorf("%s must be a positive duration, e.g. 30s", name)
	}
	return d, nil
}
