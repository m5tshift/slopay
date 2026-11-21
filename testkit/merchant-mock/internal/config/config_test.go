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
	want := Config{HTTPAddr: ":8080", LogLevel: "info", TimestampTolerance: 5 * time.Minute,
		PreviousSecretTTL: 24 * time.Hour, JournalSize: 1000, SeenEvents: 100000}
	if c != want {
		t.Fatalf("%+v", c)
	}
}

func TestOverrides(t *testing.T) {
	c, err := fromLookup(lookup(map[string]string{
		"HTTP_ADDR": ":9000", "LOG_LEVEL": "debug", "TIMESTAMP_TOLERANCE": "30s", "PREVIOUS_SECRET_TTL": "1h",
		"JOURNAL_SIZE": "5", "SEEN_EVENTS": "7",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{HTTPAddr: ":9000", LogLevel: "debug", TimestampTolerance: 30 * time.Second,
		PreviousSecretTTL: time.Hour, JournalSize: 5, SeenEvents: 7}
	if c != want {
		t.Fatalf("%+v", c)
	}
}

func TestInvalid(t *testing.T) {
	for k, v := range map[string]string{
		"TIMESTAMP_TOLERANCE": "x", "PREVIOUS_SECRET_TTL": "-1h", "JOURNAL_SIZE": "0", "SEEN_EVENTS": "a",
	} {
		if _, err := fromLookup(lookup(map[string]string{k: v})); err == nil {
			t.Errorf("%s=%s accepted", k, v)
		}
	}
}
