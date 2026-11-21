// Package config reads the invoice-service configuration from the environment. Secrets are never passed in
// variables: only paths of mounted secret files are (storage.PostgresConfigFromEnv, ValkeyConfigFromEnv).
package config

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/m5tshift/weigate/pkg/storage"
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
	// USDTContract is the address of the test USDT contract in lower case (USDT_CONTRACT, FR-10). Empty:
	// USDT invoices are refused.
	USDTContract string
	// USDTDecimals is the decimals of the token (USDT_DECIMALS, default 6).
	USDTDecimals int
	// InvoiceTTL is the lifetime of an invoice (INVOICE_TTL, SRS 4.1.4: 15m).
	InvoiceTTL time.Duration

	// JWTIssuer is the iss claim a merchant token must carry (JWT_ISSUER, default weigate-merchant-service).
	JWTIssuer string

	// JWKSURL is the JWKS of merchant-service the token signatures are verified with (JWKS_URL, required).
	JWKSURL string
	// JWKSTimeout bounds one JWKS request (JWKS_TIMEOUT, default 2s).
	JWKSTimeout time.Duration
	// JWKSRefresh is the age after which the cached JWKS is reloaded (JWKS_REFRESH, default 5m).
	JWKSRefresh time.Duration
	// JWKSMinInterval is the shortest time between two JWKS requests, including the reload on an unknown kid
	// (JWKS_MIN_INTERVAL, default 10s).
	JWKSMinInterval time.Duration

	// ChainID is the chain whose chain.blocks and chain.transfers are applied (CHAIN_ID, default 31337: Anvil).
	ChainID int64
	// Confirmations is N of SRS 4.1.4 (CONFIRMATIONS, default 7 on Anvil). It must exceed the deepest handled
	// reorg (6, SRS 4.1.9).
	Confirmations int
	// RevertWait is the wait for the same txHash after a revert past expiresAt (REVERT_WAIT, SRS 4.1.4: 10m).
	RevertWait time.Duration
	// TrackingWindow is the address tracking window (TRACKING_WINDOW, SRS 4.1.4: 24h).
	TrackingWindow time.Duration
	// ChainBlocks is how many recent canonical blocks are kept (CHAIN_BLOCKS, default 256, more than N and
	// the deepest reorg).
	ChainBlocks uint64
	// BarrierPoll is the poll interval of the wait for chain.transfers before a block (BARRIER_POLL, default 50ms).
	BarrierPoll time.Duration
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
		JWTIssuer:      envOr("JWT_ISSUER", "weigate-merchant-service"),
		JWKSURL:        os.Getenv("JWKS_URL"),
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	if u, err := url.Parse(c.RateServiceURL); c.RateServiceURL == "" || err != nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, errors.New("RATE_SERVICE_URL must be an http(s) URL, e.g. http://rate-service"))
	}
	if u, err := url.Parse(c.JWKSURL); c.JWKSURL == "" || err != nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, errors.New("JWKS_URL must be an http(s) URL, e.g. http://merchant-service/.well-known/jwks.json"))
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
	c.USDTContract = strings.ToLower(os.Getenv("USDT_CONTRACT"))
	if c.USDTContract != "" && !usdtAddress.MatchString(c.USDTContract) {
		errs = append(errs, errors.New("USDT_CONTRACT must be a 0x-prefixed 20-byte hex address"))
	}
	d, err := nonNegative("USDT_DECIMALS", 6, 77)
	errs = append(errs, err)
	c.USDTDecimals = int(d)
	c.InvoiceTTL, err = duration("INVOICE_TTL", 15*time.Minute)
	errs = append(errs, err)

	c.JWKSTimeout, err = duration("JWKS_TIMEOUT", 2*time.Second)
	errs = append(errs, err)
	c.JWKSRefresh, err = duration("JWKS_REFRESH", 5*time.Minute)
	errs = append(errs, err)
	c.JWKSMinInterval, err = duration("JWKS_MIN_INTERVAL", 10*time.Second)
	errs = append(errs, err)

	c.ChainID, err = positive("CHAIN_ID", 31337, math.MaxInt64)
	errs = append(errs, err)
	n, err := positive("CONFIRMATIONS", 7, math.MaxInt32)
	errs = append(errs, err)
	c.Confirmations = int(n)
	if c.Confirmations < 2 {
		errs = append(errs, errors.New("CONFIRMATIONS must be at least 2"))
	}
	c.RevertWait, err = duration("REVERT_WAIT", 10*time.Minute)
	errs = append(errs, err)
	c.TrackingWindow, err = duration("TRACKING_WINDOW", 24*time.Hour)
	errs = append(errs, err)
	blocks, err := positive("CHAIN_BLOCKS", 256, 1<<20)
	errs = append(errs, err)
	c.ChainBlocks = uint64(blocks)                //nolint:gosec // positive checks 1 <= blocks <= 2^20.
	if c.ChainBlocks <= uint64(c.Confirmations) { //nolint:gosec // Confirmations >= 1 here or reported above.
		errs = append(errs, errors.New("CHAIN_BLOCKS must be greater than CONFIRMATIONS"))
	}
	c.BarrierPoll, err = duration("BARRIER_POLL", 50*time.Millisecond)
	errs = append(errs, err)
	return c, errors.Join(errs...)
}

var usdtAddress = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// nonNegative reads an integer from 0 up to limit.
func nonNegative(name string, def, limit int64) (int64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 || n > limit {
		return def, fmt.Errorf("%s must be an integer from 0 to %d", name, limit)
	}
	return n, nil
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

// positive reads a positive integer up to limit.
func positive(name string, def, limit int64) (int64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 || n > limit {
		return def, fmt.Errorf("%s must be a positive integer up to %d", name, limit)
	}
	return n, nil
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
