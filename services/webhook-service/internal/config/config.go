// Package config reads the webhook-service configuration from the environment. Secrets are never passed in
// variables: only paths of mounted secret files are.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/m5tshift/weigate/pkg/breaker"
	"github.com/m5tshift/weigate/pkg/storage"
)

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address of health and metrics (HTTP_ADDR, default :8080).
	HTTPAddr string
	// AdminAddr is the listen address of the internal operator API, POST /internal/dlq/replay (ADMIN_ADDR, default
	// :8081). It is a separate port that is not published on the gateway.
	AdminAddr string
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// ConsumerGroup is the group prefix (KAFKA_CONSUMER_GROUP, default webhook-service): invoice.events is read by
	// the group itself, merchant.events by <group>-merchants, every retry topic by <group>-<topic>.
	ConsumerGroup string
	// Mongo is read by storage.MongoConfigFromEnv("MONGODB").
	Mongo storage.MongoConfig
	// Valkey is read by storage.ValkeyConfigFromEnv("VALKEY"). The service works without it: deliveries are not
	// deduplicated until it is reachable (SRS 4.1.7).
	Valkey storage.ValkeyConfig
	// ValkeyTimeout bounds one Valkey call (VALKEY_TIMEOUT, default 100ms).
	ValkeyTimeout time.Duration
	// DedupTTL is the lifetime of a deduplication key (DEDUP_TTL, SRS 4.1.7: 24h).
	DedupTTL time.Duration

	// OpenBaoAddr is the OpenBao base URL (OPENBAO_ADDR, http or https, required).
	OpenBaoAddr string
	// OpenBaoTokenFile is the file with the OpenBao token (OPENBAO_TOKEN_FILE, required); it is read on every request.
	OpenBaoTokenFile string
	// OpenBaoMount is the KV v2 mount of the webhook secrets (OPENBAO_KV_MOUNT, default secret).
	OpenBaoMount string

	// DeliveryTimeout bounds one POST to a merchant (DELIVERY_TIMEOUT, default 5s).
	DeliveryTimeout time.Duration
	// Breaker is the Circuit Breaker of every merchant endpoint: ENDPOINT_BREAKER_FAILURE_THRESHOLD (5),
	// ENDPOINT_BREAKER_OPEN_TIMEOUT (30s), ENDPOINT_BREAKER_HALF_OPEN_MAX (1).
	Breaker breaker.Config
	// Concurrency bounds the deliveries in flight per topic (DELIVERY_CONCURRENCY, default 64).
	Concurrency int
	// JournalTTL is how long the delivery journal is kept (JOURNAL_TTL, SRS 4.1.6: 720h).
	JournalTTL time.Duration
}

// DefaultBreaker are the endpoint breaker thresholds without ENDPOINT_BREAKER_* variables: five failures in a row
// open the breaker for 30 s (NFR-09).
var DefaultBreaker = breaker.Config{FailureThreshold: 5, OpenTimeout: 30 * time.Second, HalfOpenMax: 1}

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:         envOr("HTTP_ADDR", ":8080"),
		AdminAddr:        envOr("ADMIN_ADDR", ":8081"),
		ConsumerGroup:    envOr("KAFKA_CONSUMER_GROUP", "webhook-service"),
		KafkaBrokers:     splitList(os.Getenv("KAFKA_BROKERS")),
		OpenBaoAddr:      strings.TrimRight(os.Getenv("OPENBAO_ADDR"), "/"),
		OpenBaoTokenFile: os.Getenv("OPENBAO_TOKEN_FILE"),
		OpenBaoMount:     envOr("OPENBAO_KV_MOUNT", "secret"),
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	var err error
	c.Mongo, err = storage.MongoConfigFromEnv("MONGODB")
	errs = append(errs, err)
	c.Valkey, err = storage.ValkeyConfigFromEnv("VALKEY")
	errs = append(errs, err)
	if !httpURL(c.OpenBaoAddr) {
		errs = append(errs, errors.New("OPENBAO_ADDR is required and must be an http(s) URL"))
	}
	if c.OpenBaoTokenFile == "" {
		errs = append(errs, errors.New("OPENBAO_TOKEN_FILE is required"))
	}
	c.Breaker, err = breaker.ConfigFromEnv("ENDPOINT_BREAKER", "", DefaultBreaker)
	errs = append(errs, err)
	if c.Breaker.FailureThreshold < 1 || c.Breaker.OpenTimeout <= 0 || c.Breaker.HalfOpenMax < 1 {
		errs = append(errs, errors.New("ENDPOINT_BREAKER_* must be: failure threshold >= 1, open timeout > 0, half-open max >= 1"))
	}
	c.Concurrency, err = integer("DELIVERY_CONCURRENCY", 64, 1)
	errs = append(errs, err)
	for _, d := range []struct {
		name string
		def  time.Duration
		dst  *time.Duration
	}{
		{"VALKEY_TIMEOUT", 100 * time.Millisecond, &c.ValkeyTimeout},
		{"DEDUP_TTL", 24 * time.Hour, &c.DedupTTL},
		{"DELIVERY_TIMEOUT", 5 * time.Second, &c.DeliveryTimeout},
		{"JOURNAL_TTL", 30 * 24 * time.Hour, &c.JournalTTL},
	} {
		*d.dst, err = duration(d.name, d.def)
		errs = append(errs, err)
	}
	return c, errors.Join(errs...)
}

func httpURL(s string) bool {
	u, err := url.Parse(s)
	return s != "" && err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
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
