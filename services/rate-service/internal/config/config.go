// Package config reads the rate-service configuration from the environment. Secrets are never passed in
// variables: only paths of mounted secret files are.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/rate-service/internal/limiter"
	"github.com/m5tshift/weigate/services/rate-service/internal/rate"
	"github.com/m5tshift/weigate/services/rate-service/internal/source"
)

// Source modes (SRS 4.1.3). There is no automatic switch between them.
const (
	ModeMock      = "mock"
	ModeExchanges = "exchanges"
)

// contractMaxAge is the largest age of a served rate in the OpenAPI contract (ageSeconds maximum).
const contractMaxAge = 60 * time.Second

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address of the API, health and metrics (HTTP_ADDR, default :8080).
	HTTPAddr string
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// Valkey is read by storage.ValkeyConfigFromEnv("VALKEY").
	Valkey storage.ValkeyConfig
	// ValkeyTimeout bounds one Valkey call; on timeout a replica serves from memory (VALKEY_TIMEOUT, 200ms).
	ValkeyTimeout time.Duration

	// SourceMode is mock or exchanges (RATE_SOURCE_MODE, default mock).
	SourceMode string
	// RateMockURL is the base URL of rate-mock (RATE_MOCK_URL, required in mock mode).
	RateMockURL string
	// CoinbaseURL and OKXURL are the base URLs of the exchanges in exchanges mode (COINBASE_URL, OKX_URL).
	CoinbaseURL string
	OKXURL      string
	// Limits are the outgoing limits to the exchanges, a token bucket per source:
	// SOURCE_LIMIT_<SOURCE>_REQUESTS per SOURCE_LIMIT_<SOURCE>_WINDOW (exchanges mode).
	Limits map[rate.Source]limiter.Config
	// SourceTimeout bounds one request to a source (SOURCE_TIMEOUT, default 2s).
	SourceTimeout time.Duration
	// Breakers are the Circuit Breakers of the sources: SOURCE_BREAKER_FAILURE_THRESHOLD (3),
	// SOURCE_BREAKER_OPEN_TIMEOUT (30s), SOURCE_BREAKER_HALF_OPEN_MAX (1), the same for every source.
	Breakers map[rate.Source]breaker.Config

	// PollInterval is the polling period of the leader (POLL_INTERVAL, SRS 4.1.7: 5s).
	PollInterval time.Duration
	// RateMaxAge is the age after which a rate is stale (RATE_MAX_AGE, SRS 4.1.7: 60s, at most 60s).
	RateMaxAge time.Duration
	// USDTMaxAge is how long the USDT/USD rate converts OKX prices (USDT_MAX_AGE, SRS 4.1.9: 1h).
	USDTMaxAge time.Duration
	// DivergenceMaxBps is the largest divergence of the sources for a reliable rate
	// (DIVERGENCE_MAX_BPS, SRS 4.1.9: 100 = 1 %).
	DivergenceMaxBps int64
	// RetryAfter is the Retry-After of a 503 (RETRY_AFTER, default 5s, whole seconds).
	RetryAfter time.Duration

	// FaultInjection enables /test/fault (FAULT_INJECTION_ENABLED, default false; SRS 8.2а).
	FaultInjection bool

	// LeaderLease is the Lease name (LEADER_LEASE_NAME); empty means this replica always leads (local runs).
	LeaderLease string
	// PodName and PodNamespace identify the replica in the Lease (POD_NAME, POD_NAMESPACE).
	PodName      string
	PodNamespace string
}

// DefaultBreaker are the breaker thresholds of a source without SOURCE_BREAKER_* variables.
var DefaultBreaker = breaker.Config{FailureThreshold: 3, OpenTimeout: 30 * time.Second, HalfOpenMax: 1}

// DefaultLimits are the published public limits of the exchanges (SRS 4.1.9: the limits are not exceeded):
// Coinbase Exchange 10 requests per second per IP, OKX ticker 20 requests per 2 seconds.
var DefaultLimits = map[rate.Source]limiter.Config{
	rate.Coinbase: {Capacity: 10, Window: time.Second},
	rate.OKX:      {Capacity: 20, Window: 2 * time.Second},
}

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:     envOr("HTTP_ADDR", ":8080"),
		KafkaBrokers: splitList(os.Getenv("KAFKA_BROKERS")),
		SourceMode:   envOr("RATE_SOURCE_MODE", ModeMock),
		RateMockURL:  os.Getenv("RATE_MOCK_URL"),
		CoinbaseURL:  envOr("COINBASE_URL", source.DefaultCoinbaseURL),
		OKXURL:       envOr("OKX_URL", source.DefaultOKXURL),
		LeaderLease:  os.Getenv("LEADER_LEASE_NAME"),
		PodName:      os.Getenv("POD_NAME"),
		PodNamespace: os.Getenv("POD_NAMESPACE"),
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	var err error
	c.Valkey, err = storage.ValkeyConfigFromEnv("VALKEY")
	errs = append(errs, err)

	switch c.SourceMode {
	case ModeMock:
		if c.RateMockURL == "" {
			errs = append(errs, errors.New("RATE_MOCK_URL is required in mock mode"))
		}
	case ModeExchanges:
		c.Limits = map[rate.Source]limiter.Config{}
		for src, def := range DefaultLimits {
			l, err := limit(string(src), def)
			errs = append(errs, err)
			c.Limits[src] = l
		}
	default:
		errs = append(errs, fmt.Errorf("RATE_SOURCE_MODE must be %s or %s", ModeMock, ModeExchanges))
	}
	if c.LeaderLease != "" && (c.PodName == "" || c.PodNamespace == "") {
		errs = append(errs, errors.New("POD_NAME and POD_NAMESPACE are required with LEADER_LEASE_NAME"))
	}

	c.Breakers = map[rate.Source]breaker.Config{}
	for _, src := range []rate.Source{rate.Coinbase, rate.OKX} {
		b, err := breaker.ConfigFromEnv("SOURCE_BREAKER", string(src), DefaultBreaker)
		errs = append(errs, err)
		c.Breakers[src] = b
	}

	c.ValkeyTimeout, err = duration("VALKEY_TIMEOUT", 200*time.Millisecond)
	errs = append(errs, err)
	c.SourceTimeout, err = duration("SOURCE_TIMEOUT", 2*time.Second)
	errs = append(errs, err)
	c.PollInterval, err = duration("POLL_INTERVAL", 5*time.Second)
	errs = append(errs, err)
	c.RateMaxAge, err = duration("RATE_MAX_AGE", contractMaxAge)
	errs = append(errs, err)
	if c.RateMaxAge > contractMaxAge {
		errs = append(errs, fmt.Errorf("RATE_MAX_AGE must not exceed %s", contractMaxAge))
	}
	c.USDTMaxAge, err = duration("USDT_MAX_AGE", time.Hour)
	errs = append(errs, err)
	c.RetryAfter, err = duration("RETRY_AFTER", 5*time.Second)
	errs = append(errs, err)
	if c.RetryAfter < time.Second {
		errs = append(errs, errors.New("RETRY_AFTER must be at least 1s"))
	}
	bps, err := integer("DIVERGENCE_MAX_BPS", 100, 0)
	errs = append(errs, err)
	c.DivergenceMaxBps = int64(bps)
	c.FaultInjection, err = boolean("FAULT_INJECTION_ENABLED")
	errs = append(errs, err)
	return c, errors.Join(errs...)
}

// limit reads SOURCE_LIMIT_<SOURCE>_REQUESTS and SOURCE_LIMIT_<SOURCE>_WINDOW on top of def.
func limit(src string, def limiter.Config) (limiter.Config, error) {
	prefix := "SOURCE_LIMIT_" + strings.ToUpper(src)
	n, err1 := integer(prefix+"_REQUESTS", def.Capacity, 1)
	w, err2 := duration(prefix+"_WINDOW", def.Window)
	c := limiter.Config{Capacity: n, Window: w}
	if err := errors.Join(err1, err2); err != nil {
		return c, err
	}
	if err := c.Validate(); err != nil {
		return c, fmt.Errorf("%s: %w", prefix, err)
	}
	return c, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for p := range strings.SplitSeq(s, ",") {
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
		return def, fmt.Errorf("%s must be a positive duration, e.g. 5s", name)
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

func boolean(name string) (bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return b, nil
}
