// Package config reads the chain-watcher configuration from the environment. Secrets are never passed in
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

	"github.com/m5tshift/slopay/pkg/breaker"
	"github.com/m5tshift/slopay/pkg/storage"
)

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address of health and metrics (HTTP_ADDR, default :8080).
	HTTPAddr string
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// Mongo is read by storage.MongoConfigFromEnv("MONGODB").
	Mongo storage.MongoConfig

	// NodeURL is the JSON-RPC endpoint of the EVM node (NODE_URL, http or https, required).
	NodeURL string
	// NodeTimeout bounds one node call (NODE_TIMEOUT, default 5s).
	NodeTimeout time.Duration
	// NodeBreaker is the Circuit Breaker of the node: NODE_BREAKER_FAILURE_THRESHOLD (5),
	// NODE_BREAKER_OPEN_TIMEOUT (15s), NODE_BREAKER_HALF_OPEN_MAX (1).
	NodeBreaker breaker.Config

	// ChainID is the chain read by this deployment (CHAIN_ID, required; 31337 for Anvil).
	ChainID uint64
	// StartBlock is the first block when there is no stored cursor (CHAIN_START_BLOCK); nil means the node head.
	StartBlock *uint64
	// WindowSize is the number of processed headers kept for reorg detection (WINDOW_SIZE, SRS 4.1.6: 64).
	WindowSize int
	// MaxReorgDepth is the deepest reorg handled (REORG_MAX_DEPTH, SRS 4.1.9: 6); smaller than WINDOW_SIZE.
	MaxReorgDepth int

	// PollInterval is the pause when the cursor has reached the node head (POLL_INTERVAL, default 1s).
	PollInterval time.Duration
	// BackoffInitial and BackoffMax bound the pause after a failure (BACKOFF_INITIAL 500ms, BACKOFF_MAX 30s).
	BackoffInitial time.Duration
	BackoffMax     time.Duration

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
		HTTPAddr:     envOr("HTTP_ADDR", ":8080"),
		KafkaBrokers: splitList(os.Getenv("KAFKA_BROKERS")),
		NodeURL:      os.Getenv("NODE_URL"),
		LeaderLease:  os.Getenv("LEADER_LEASE_NAME"),
		PodName:      os.Getenv("POD_NAME"),
		PodNamespace: os.Getenv("POD_NAMESPACE"),
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	var err error
	c.Mongo, err = storage.MongoConfigFromEnv("MONGODB")
	errs = append(errs, err)

	if u, err := url.Parse(c.NodeURL); c.NodeURL == "" || err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, errors.New("NODE_URL is required and must be an http(s) URL"))
	}
	c.NodeBreaker, err = breaker.ConfigFromEnv("NODE_BREAKER", "node", DefaultBreaker)
	errs = append(errs, err)

	if v := os.Getenv("CHAIN_ID"); v == "" {
		errs = append(errs, errors.New("CHAIN_ID is required"))
	} else if c.ChainID, err = strconv.ParseUint(v, 10, 63); err != nil || c.ChainID == 0 {
		errs = append(errs, errors.New("CHAIN_ID must be a positive integer"))
	}
	if v := os.Getenv("CHAIN_START_BLOCK"); v != "" {
		n, err := strconv.ParseUint(v, 10, 63)
		if err != nil {
			errs = append(errs, errors.New("CHAIN_START_BLOCK must be a block number"))
		} else {
			c.StartBlock = &n
		}
	}
	c.WindowSize, err = integer("WINDOW_SIZE", 64, 2)
	errs = append(errs, err)
	c.MaxReorgDepth, err = integer("REORG_MAX_DEPTH", 6, 1)
	errs = append(errs, err)
	if c.MaxReorgDepth >= c.WindowSize {
		errs = append(errs, errors.New("REORG_MAX_DEPTH must be smaller than WINDOW_SIZE"))
	}

	c.NodeTimeout, err = duration("NODE_TIMEOUT", 5*time.Second)
	errs = append(errs, err)
	c.PollInterval, err = duration("POLL_INTERVAL", time.Second)
	errs = append(errs, err)
	c.BackoffInitial, err = duration("BACKOFF_INITIAL", 500*time.Millisecond)
	errs = append(errs, err)
	c.BackoffMax, err = duration("BACKOFF_MAX", 30*time.Second)
	errs = append(errs, err)
	if c.BackoffMax < c.BackoffInitial {
		errs = append(errs, errors.New("BACKOFF_MAX must not be smaller than BACKOFF_INITIAL"))
	}

	if c.LeaderLease != "" && (c.PodName == "" || c.PodNamespace == "") {
		errs = append(errs, errors.New("POD_NAME and POD_NAMESPACE are required with LEADER_LEASE_NAME"))
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
