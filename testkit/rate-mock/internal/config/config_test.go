package config

import (
	"testing"
	"time"
)

func lookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestDefaults(t *testing.T) {
	c, err := fromLookup(lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.LogLevel != "info" || c.ETHUSD != 3700_000000 || c.USDTUSD != 1_000000 ||
		c.TickInterval != time.Second || c.StepBps != 2 || c.BandBps != 10 {
		t.Fatalf("%+v", c)
	}
}

func TestOverrides(t *testing.T) {
	c, err := fromLookup(lookup(map[string]string{
		"HTTP_ADDR": ":9000", "LOG_LEVEL": "debug", "BASE_PRICE_ETH_USD": "3500.5", "BASE_PRICE_USDT_USD": "0.9995",
		"TICK_INTERVAL": "500ms", "WALK_STEP_BPS": "0", "WALK_BAND_BPS": "33",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":9000" || c.LogLevel != "debug" || c.ETHUSD != 3500_500000 || c.USDTUSD != 999500 ||
		c.TickInterval != 500*time.Millisecond || c.StepBps != 0 || c.BandBps != 33 {
		t.Fatalf("%+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for k, v := range map[string]string{
		"BASE_PRICE_ETH_USD": "x", "BASE_PRICE_USDT_USD": "-1", "TICK_INTERVAL": "0s",
		"WALK_STEP_BPS": "-1", "WALK_BAND_BPS": "34",
	} {
		if _, err := fromLookup(lookup(map[string]string{k: v})); err == nil {
			t.Errorf("%s=%s: no error", k, v)
		}
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":1234")
	c, err := FromEnv()
	if err != nil || c.HTTPAddr != ":1234" {
		t.Fatalf("%+v %v", c, err)
	}
}
