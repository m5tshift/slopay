// Package config reads the merchant-mock configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config is the component configuration.
type Config struct {
	// HTTPAddr is the listen address (HTTP_ADDR, default :8080).
	HTTPAddr string
	// LogLevel is debug, info, warn or error (LOG_LEVEL, default info).
	LogLevel string
	// TimestampTolerance is the largest accepted difference between X-SLOPay-Timestamp and the clock
	// (TIMESTAMP_TOLERANCE, default 5m, SRS 4.1.10 and contracts/openapi/webhook.yaml).
	TimestampTolerance time.Duration
	// PreviousSecretTTL is how long the previous secret version stays valid after a rotation
	// (PREVIOUS_SECRET_TTL, default 24h, SRS 4.1.10).
	PreviousSecretTTL time.Duration
	// JournalSize is the number of journal entries kept per endpoint (JOURNAL_SIZE, default 1000).
	JournalSize int
	// SeenEvents is the number of event ids remembered per endpoint for deduplication (SEEN_EVENTS, default 100000).
	SeenEvents int
}

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	return fromLookup(os.LookupEnv)
}

func fromLookup(lookup func(string) (string, bool)) (Config, error) {
	get := func(k, def string) string {
		if v, ok := lookup(k); ok && v != "" {
			return v
		}
		return def
	}
	var errs []error
	c := Config{HTTPAddr: get("HTTP_ADDR", ":8080"), LogLevel: get("LOG_LEVEL", "info")}
	var err error
	if c.TimestampTolerance, err = time.ParseDuration(get("TIMESTAMP_TOLERANCE", "5m")); err != nil || c.TimestampTolerance <= 0 {
		errs = append(errs, errors.New("TIMESTAMP_TOLERANCE: must be a positive duration"))
	}
	if c.PreviousSecretTTL, err = time.ParseDuration(get("PREVIOUS_SECRET_TTL", "24h")); err != nil || c.PreviousSecretTTL <= 0 {
		errs = append(errs, errors.New("PREVIOUS_SECRET_TTL: must be a positive duration"))
	}
	if c.JournalSize, err = strconv.Atoi(get("JOURNAL_SIZE", "1000")); err != nil || c.JournalSize < 1 {
		errs = append(errs, fmt.Errorf("JOURNAL_SIZE: must be a positive integer"))
	}
	if c.SeenEvents, err = strconv.Atoi(get("SEEN_EVENTS", "100000")); err != nil || c.SeenEvents < 1 {
		errs = append(errs, fmt.Errorf("SEEN_EVENTS: must be a positive integer"))
	}
	return c, errors.Join(errs...)
}
