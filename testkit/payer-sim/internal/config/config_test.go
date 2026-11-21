package config

import (
	"strings"
	"testing"
)

func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

var base = map[string]string{"INVOICE_SERVICE_URL": "http://invoice/", "ANVIL_RPC_URL": "http://anvil:8545", "PAYER_KEYS_FILE": "/k"}

func with(extra map[string]string) map[string]string {
	m := map[string]string{}
	for k, v := range base {
		m[k] = v
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func TestDefaults(t *testing.T) {
	c, err := fromLookup(lookup(base))
	if err != nil {
		t.Fatal(err)
	}
	if c.PayPercent != 80 || c.UnderpayPercent != 10 || c.SkipPercent != 10 || c.UnderpayBps != 5000 ||
		c.HTTPAddr != ":8080" || c.InvoiceURL != "http://invoice" {
		t.Errorf("%+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for name, tc := range map[string]struct {
		env  map[string]string
		want string
	}{
		"missing":     {map[string]string{}, "INVOICE_SERVICE_URL: required"},
		"sum":         {with(map[string]string{"PAY_PERCENT": "90"}), "must sum to 100"},
		"not number":  {with(map[string]string{"SKIP_PERCENT": "x"}), "SKIP_PERCENT"},
		"negative":    {with(map[string]string{"UNDERPAY_PERCENT": "-1"}), "UNDERPAY_PERCENT"},
		"bps too big": {with(map[string]string{"UNDERPAY_BPS": "9500"}), "UNDERPAY_BPS"},
		"bps zero":    {with(map[string]string{"UNDERPAY_BPS": "0"}), "UNDERPAY_BPS"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := fromLookup(lookup(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCustomShares(t *testing.T) {
	c, err := fromLookup(lookup(with(map[string]string{"PAY_PERCENT": "100", "UNDERPAY_PERCENT": "0", "SKIP_PERCENT": "0"})))
	if err != nil || c.PayPercent != 100 {
		t.Errorf("%+v %v", c, err)
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("INVOICE_SERVICE_URL", "http://i")
	t.Setenv("ANVIL_RPC_URL", "http://a")
	t.Setenv("PAYER_KEYS_FILE", "/k")
	if _, err := FromEnv(); err != nil {
		t.Fatal(err)
	}
}
