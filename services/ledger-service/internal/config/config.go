// Package config reads the ledger-service configuration from the environment. Secrets are never passed in
// variables: only paths of mounted secret files are.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/m5tshift/slopay/pkg/storage"
)

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address of the statement API, health and metrics (HTTP_ADDR, default :8080).
	HTTPAddr string
	// KafkaBrokers are the seed brokers (KAFKA_BROKERS, comma-separated, required).
	KafkaBrokers []string
	// ConsumerGroup is the group of invoice.events (KAFKA_CONSUMER_GROUP, default ledger-service).
	ConsumerGroup string
	// Postgres is read by storage.PostgresConfigFromEnv("POSTGRES").
	Postgres storage.PostgresConfig
	// FeeBps is the platform fee in basis points (FEE_BPS, SRS 4.1.3: 50 = 0.5 %).
	FeeBps int
}

// maxBps is 100 %, the upper bound of feeBps in contracts/schemas/ledger.fee_accrued.v1.json.
const maxBps = 10000

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:      envOr("HTTP_ADDR", ":8080"),
		KafkaBrokers:  splitList(os.Getenv("KAFKA_BROKERS")),
		ConsumerGroup: envOr("KAFKA_CONSUMER_GROUP", "ledger-service"),
		FeeBps:        50,
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	pg, err := storage.PostgresConfigFromEnv("POSTGRES")
	errs = append(errs, err)
	c.Postgres = pg
	if v := os.Getenv("FEE_BPS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > maxBps {
			errs = append(errs, fmt.Errorf("FEE_BPS must be an integer 0..%d", maxBps))
		} else {
			c.FeeBps = n
		}
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
