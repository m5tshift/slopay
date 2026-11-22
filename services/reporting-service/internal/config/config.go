// Package config reads the reporting-service configuration from the environment. Secrets are never passed in
// variables: only paths of mounted secret files are.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/m5tshift/weigate/services/reporting-service/internal/store"
)

// Config is the service configuration.
type Config struct {
	// HTTPAddr is the listen address (HTTP_ADDR, default :8080).
	HTTPAddr string
	// ClickHouse is read by store.ConfigFromEnv.
	ClickHouse store.Config
	// JWTIssuer is the iss claim a merchant token must carry (JWT_ISSUER, default weigate-merchant-service).
	JWTIssuer string
	// JWKSURL is the JWKS of merchant-service the token signatures are verified with (JWKS_URL, required).
	JWKSURL string
	// JWKSTimeout bounds one JWKS request (JWKS_TIMEOUT, default 2s).
	JWKSTimeout time.Duration
	// JWKSRefresh is the age after which the cached JWKS is reloaded (JWKS_REFRESH, default 5m).
	JWKSRefresh time.Duration
	// JWKSMinInterval is the shortest time between two JWKS requests (JWKS_MIN_INTERVAL, default 10s).
	JWKSMinInterval time.Duration
	// MaxDays is the longest period of a report in days (REPORT_MAX_DAYS, default 92).
	MaxDays int
	// QueryTimeout bounds one report query (QUERY_TIMEOUT, default 10s).
	QueryTimeout time.Duration
	// RetryAfter is sent with 503 when ClickHouse is unavailable (RETRY_AFTER, default 5s).
	RetryAfter time.Duration
}

// FromEnv reads and validates the configuration.
func FromEnv() (Config, error) {
	var errs []error
	c := Config{
		HTTPAddr:  envOr("HTTP_ADDR", ":8080"),
		JWTIssuer: envOr("JWT_ISSUER", "weigate-merchant-service"),
		JWKSURL:   os.Getenv("JWKS_URL"),
	}
	var err error
	c.ClickHouse, err = store.ConfigFromEnv()
	errs = append(errs, err)
	if u, perr := url.Parse(c.JWKSURL); c.JWKSURL == "" || perr != nil ||
		(u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, errors.New("JWKS_URL must be an http(s) URL, e.g. http://merchant-service/.well-known/jwks.json"))
	}
	c.MaxDays = 92
	if v := os.Getenv("REPORT_MAX_DAYS"); v != "" {
		if c.MaxDays, err = strconv.Atoi(v); err != nil || c.MaxDays < 1 {
			errs = append(errs, errors.New("REPORT_MAX_DAYS must be an integer >= 1"))
		}
	}
	for _, d := range []struct {
		name string
		def  time.Duration
		dst  *time.Duration
	}{
		{"JWKS_TIMEOUT", 2 * time.Second, &c.JWKSTimeout},
		{"JWKS_REFRESH", 5 * time.Minute, &c.JWKSRefresh},
		{"JWKS_MIN_INTERVAL", 10 * time.Second, &c.JWKSMinInterval},
		{"QUERY_TIMEOUT", 10 * time.Second, &c.QueryTimeout},
		{"RETRY_AFTER", 5 * time.Second, &c.RetryAfter},
	} {
		*d.dst, err = duration(d.name, d.def)
		errs = append(errs, err)
	}
	return c, errors.Join(errs...)
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
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
