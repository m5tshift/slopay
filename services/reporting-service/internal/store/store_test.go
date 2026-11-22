package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestConfigFromEnv(t *testing.T) {
	t.Setenv("CLICKHOUSE_ADDR", "clickhouse:9000")
	t.Setenv("CLICKHOUSE_USERNAME", "reporting")
	t.Setenv("CLICKHOUSE_PASSWORD_FILE", "/run/secrets/clickhouse/password")
	t.Setenv("CLICKHOUSE_DATABASE", "weigate")
	c, err := ConfigFromEnv()
	if err != nil || c.Addr != "clickhouse:9000" || c.Username != "reporting" || c.Database != "weigate" ||
		c.PasswordFile != "/run/secrets/clickhouse/password" {
		t.Fatalf("%+v %v", c, err)
	}
	for _, name := range []string{"CLICKHOUSE_ADDR", "CLICKHOUSE_USERNAME", "CLICKHOUSE_PASSWORD_FILE", "CLICKHOUSE_DATABASE"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "")
			if _, err := ConfigFromEnv(); err == nil {
				t.Error("want an error")
			}
		})
	}
}

func TestIsAuthError(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&clickhouse.Exception{Code: 516}, true},
		{fmt.Errorf("wrapped: %w", &clickhouse.Exception{Code: 516}), true},
		{&clickhouse.Exception{Code: 60}, false}, // UNKNOWN_TABLE
		{errors.New("connection refused"), false},
		{nil, false},
	} {
		if got := isAuthError(tc.err); got != tc.want {
			t.Errorf("isAuthError(%v) = %v", tc.err, got)
		}
	}
}

func TestNoDeadline(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), time.Hour)
	c, release := noDeadline(parent)
	defer release()
	if _, ok := c.Deadline(); ok {
		t.Error("the context must carry no deadline")
	}
	if c.Err() != nil {
		t.Fatal("canceled too early")
	}
	cancelParent()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the context must end with its parent")
	}
}

func TestUTCDay(t *testing.T) {
	loc := time.FixedZone("x", 5*3600)
	if got := utcDay(time.Date(2026, 11, 22, 0, 0, 0, 0, loc)); !got.Equal(time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("got %v", got)
	}
}
