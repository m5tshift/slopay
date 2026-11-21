// Package config reads the reconciliation-service configuration from the environment. Secrets are never passed in
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
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// ConsumerGroup is the group of invoice.events (KAFKA_CONSUMER_GROUP, default reconciliation-service).
	ConsumerGroup string
	// Mongo is read by storage.MongoConfigFromEnv("MONGODB").
	Mongo storage.MongoConfig

	// NodeURL is the JSON-RPC endpoint of the EVM node (NODE_URL, http or https, required).
	NodeURL string
	// NodeTimeout bounds one node call or batch (NODE_TIMEOUT, default 5s).
	NodeTimeout time.Duration
	// NodeBreaker is the Circuit Breaker of the node: NODE_BREAKER_FAILURE_THRESHOLD (5),
	// NODE_BREAKER_OPEN_TIMEOUT (15s), NODE_BREAKER_HALF_OPEN_MAX (1).
	NodeBreaker breaker.Config
	// BatchSize is the most balances in one JSON-RPC batch (RPC_BATCH_SIZE, default 100).
	BatchSize int
	// ChainID is the reconciled chain (CHAIN_ID, required; 31337 for Anvil).
	ChainID uint64
	// Confirmations is N: balances are read at head - N (CONFIRMATIONS, default 7, the same N as invoice-service).
	Confirmations uint64

	// LedgerURL is the base URL of ledger-service (LEDGER_URL, http or https, required).
	LedgerURL string
	// LedgerTimeout bounds one statement request (LEDGER_TIMEOUT, default 2s).
	LedgerTimeout time.Duration

	// Interval is the period of the runs (RUN_INTERVAL, UC-08: 10m); RunTimeout bounds one run (RUN_TIMEOUT, 5m).
	Interval   time.Duration
	RunTimeout time.Duration
	// SettleDelay is waited after the block is fixed (SETTLE_DELAY, default 15s).
	SettleDelay time.Duration
	// Lookback is how far back the invoice addresses are checked (ADDRESS_LOOKBACK, UC-08: 168h).
	Lookback time.Duration
	// TrackingWindow is the address tracking window (TRACKING_WINDOW, SRS 4.1.4: 24h).
	TrackingWindow time.Duration
	// ProjectionTTL is how long an invoice is kept in the projection after its last event
	// (PROJECTION_TTL, default 192h); it must exceed ADDRESS_LOOKBACK.
	ProjectionTTL time.Duration
	// ReportTTL is how long reports are kept (REPORT_TTL, default 720h).
	ReportTTL time.Duration

	// LeaderLease is the Lease name (LEADER_LEASE_NAME); empty means this replica always leads (local runs).
	LeaderLease string
	// PodName and PodNamespace identify the replica in the Lease (POD_NAME, POD_NAMESPACE).
	PodName      string
	PodNamespace string
}

// DefaultBreaker are the node breaker thresholds without NODE_BREAKER_* variables.
var DefaultBreaker = breaker.Config{FailureThreshold: 5, OpenTimeout: 15 * time.Second, HalfOpenMax: 1}

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:      envOr("HTTP_ADDR", ":8080"),
		ConsumerGroup: envOr("KAFKA_CONSUMER_GROUP", "reconciliation-service"),
		KafkaBrokers:  splitList(os.Getenv("KAFKA_BROKERS")),
		NodeURL:       os.Getenv("NODE_URL"),
		LedgerURL:     strings.TrimRight(os.Getenv("LEDGER_URL"), "/"),
		LeaderLease:   os.Getenv("LEADER_LEASE_NAME"),
		PodName:       os.Getenv("POD_NAME"),
		PodNamespace:  os.Getenv("POD_NAMESPACE"),
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	var err error
	c.Mongo, err = storage.MongoConfigFromEnv("MONGODB")
	errs = append(errs, err)

	if !httpURL(c.NodeURL) {
		errs = append(errs, errors.New("NODE_URL is required and must be an http(s) URL"))
	}
	if !httpURL(c.LedgerURL) {
		errs = append(errs, errors.New("LEDGER_URL is required and must be an http(s) URL"))
	}
	c.NodeBreaker, err = breaker.ConfigFromEnv("NODE_BREAKER", "node", DefaultBreaker)
	errs = append(errs, err)
	if v := os.Getenv("CHAIN_ID"); v == "" {
		errs = append(errs, errors.New("CHAIN_ID is required"))
	} else if c.ChainID, err = strconv.ParseUint(v, 10, 63); err != nil || c.ChainID == 0 {
		errs = append(errs, errors.New("CHAIN_ID must be a positive integer"))
	}
	var n int
	n, err = integer("CONFIRMATIONS", 7, 1)
	errs = append(errs, err)
	c.Confirmations = uint64(n) //nolint:gosec // integer checks n >= 1.
	c.BatchSize, err = integer("RPC_BATCH_SIZE", 100, 1)
	errs = append(errs, err)

	for _, d := range []struct {
		name string
		def  time.Duration
		dst  *time.Duration
	}{
		{"NODE_TIMEOUT", 5 * time.Second, &c.NodeTimeout},
		{"LEDGER_TIMEOUT", 2 * time.Second, &c.LedgerTimeout},
		{"RUN_INTERVAL", 10 * time.Minute, &c.Interval},
		{"RUN_TIMEOUT", 5 * time.Minute, &c.RunTimeout},
		{"ADDRESS_LOOKBACK", 7 * 24 * time.Hour, &c.Lookback},
		{"TRACKING_WINDOW", 24 * time.Hour, &c.TrackingWindow},
		{"PROJECTION_TTL", 8 * 24 * time.Hour, &c.ProjectionTTL},
		{"REPORT_TTL", 30 * 24 * time.Hour, &c.ReportTTL},
	} {
		*d.dst, err = duration(d.name, d.def)
		errs = append(errs, err)
	}
	c.SettleDelay = 15 * time.Second
	if v := os.Getenv("SETTLE_DELAY"); v != "" {
		if c.SettleDelay, err = time.ParseDuration(v); err != nil || c.SettleDelay < 0 {
			errs = append(errs, errors.New("SETTLE_DELAY must be a non-negative duration, e.g. 15s"))
		}
	}
	if c.RunTimeout <= c.SettleDelay {
		errs = append(errs, errors.New("RUN_TIMEOUT must be longer than SETTLE_DELAY"))
	}
	if c.ProjectionTTL <= c.Lookback {
		errs = append(errs, errors.New("PROJECTION_TTL must be longer than ADDRESS_LOOKBACK"))
	}
	if c.LeaderLease != "" && (c.PodName == "" || c.PodNamespace == "") {
		errs = append(errs, errors.New("POD_NAME and POD_NAMESPACE are required with LEADER_LEASE_NAME"))
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
