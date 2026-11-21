// Package config reads the rate-mock configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/m5tshift/weigate/testkit/rate-mock/internal/market"
)

// maxBandBps keeps the three walks of the normal mode (coinbase ETH-USD, okx ETH-USDT, coinbase USDT-USD) within
// 1 % of each other: 3 * 33 bps < 100 bps.
const maxBandBps = 33

// Config is the component configuration.
type Config struct {
	// HTTPAddr is the listen address (HTTP_ADDR, default :8080).
	HTTPAddr string
	// LogLevel is debug, info, warn or error (LOG_LEVEL, default info).
	LogLevel string
	// ETHUSD and USDTUSD are the prices the walks circle around (BASE_PRICE_ETH_USD, BASE_PRICE_USDT_USD),
	// in millionths of a unit.
	ETHUSD, USDTUSD int64
	// TickInterval is the period of the walk (TICK_INTERVAL, default 1s).
	TickInterval time.Duration
	// StepBps is the largest change per tick (WALK_STEP_BPS, default 2).
	StepBps int
	// BandBps is how far a price may leave its centre (WALK_BAND_BPS, default 10, at most 33).
	BandBps int
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
	if c.ETHUSD, err = market.ParsePrice(get("BASE_PRICE_ETH_USD", "3700")); err != nil {
		errs = append(errs, fmt.Errorf("BASE_PRICE_ETH_USD: %w", err))
	}
	if c.USDTUSD, err = market.ParsePrice(get("BASE_PRICE_USDT_USD", "1")); err != nil {
		errs = append(errs, fmt.Errorf("BASE_PRICE_USDT_USD: %w", err))
	}
	if c.TickInterval, err = time.ParseDuration(get("TICK_INTERVAL", "1s")); err != nil || c.TickInterval <= 0 {
		errs = append(errs, errors.New("TICK_INTERVAL: must be a positive duration"))
	}
	if c.StepBps, err = strconv.Atoi(get("WALK_STEP_BPS", "2")); err != nil || c.StepBps < 0 {
		errs = append(errs, errors.New("WALK_STEP_BPS: must be a non-negative integer"))
	}
	if c.BandBps, err = strconv.Atoi(get("WALK_BAND_BPS", "10")); err != nil || c.BandBps < 0 || c.BandBps > maxBandBps {
		errs = append(errs, fmt.Errorf("WALK_BAND_BPS: must be an integer from 0 to %d", maxBandBps))
	}
	return c, errors.Join(errs...)
}
