// Package config reads the payer-sim configuration from the environment.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config is the component configuration.
type Config struct {
	// HTTPAddr is the listen address (HTTP_ADDR, default :8080).
	HTTPAddr string
	// LogLevel is debug, info, warn or error (LOG_LEVEL, default info).
	LogLevel string
	// InvoiceURL is the base URL of invoice-service (INVOICE_SERVICE_URL).
	InvoiceURL string
	// RPCURL is the Anvil JSON-RPC endpoint (ANVIL_RPC_URL).
	RPCURL string
	// KeysFile is the mounted secret file with one hex private key of a pre-funded account per line
	// (PAYER_KEYS_FILE).
	KeysFile string
	// PayPercent, UnderpayPercent and SkipPercent are the shares of invoices that are paid in full, underpaid
	// and not paid (PAY_PERCENT, UNDERPAY_PERCENT, SKIP_PERCENT, default 80/10/10, the sum is 100).
	PayPercent, UnderpayPercent, SkipPercent int
	// UnderpayBps is the part of the amount an underpaying payer sends, in basis points (UNDERPAY_BPS,
	// default 5000, from 1 to 9000: below the largest tolerance of SRS 4.1.3).
	UnderpayBps int
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
	c := Config{
		HTTPAddr:   get("HTTP_ADDR", ":8080"),
		LogLevel:   get("LOG_LEVEL", "info"),
		InvoiceURL: strings.TrimRight(get("INVOICE_SERVICE_URL", ""), "/"),
		RPCURL:     get("ANVIL_RPC_URL", ""),
		KeysFile:   get("PAYER_KEYS_FILE", ""),
	}
	for k, v := range map[string]string{"INVOICE_SERVICE_URL": c.InvoiceURL, "ANVIL_RPC_URL": c.RPCURL, "PAYER_KEYS_FILE": c.KeysFile} {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s: required", k))
		}
	}
	num := func(k, def string, lo, hi int) int {
		n, err := strconv.Atoi(get(k, def))
		if err != nil || n < lo || n > hi {
			errs = append(errs, fmt.Errorf("%s: must be an integer from %d to %d", k, lo, hi))
		}
		return n
	}
	c.PayPercent = num("PAY_PERCENT", "80", 0, 100)
	c.UnderpayPercent = num("UNDERPAY_PERCENT", "10", 0, 100)
	c.SkipPercent = num("SKIP_PERCENT", "10", 0, 100)
	if c.PayPercent+c.UnderpayPercent+c.SkipPercent != 100 {
		errs = append(errs, errors.New("PAY_PERCENT, UNDERPAY_PERCENT and SKIP_PERCENT: must sum to 100"))
	}
	c.UnderpayBps = num("UNDERPAY_BPS", "5000", 1, 9000)
	return c, errors.Join(errs...)
}
