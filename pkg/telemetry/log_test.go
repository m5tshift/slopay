package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

const testJWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.c2lnbmF0dXJl"

func logLine(t *testing.T, f func(l *slog.Logger)) (string, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	f(NewLogger(&buf, slog.LevelInfo))
	var m map[string]any
	if err := json.Unmarshal(buf.Bytes(), &m); err != nil {
		t.Fatalf("not JSON: %q: %v", buf.String(), err)
	}
	return buf.String(), m
}

func TestLoggerMasksSecrets(t *testing.T) {
	raw, m := logLine(t, func(l *slog.Logger) {
		l.With("api_key", "sk_live_123").WithGroup("req").Info("call with Bearer abc.def and "+testJWT,
			"password", "hunter2",
			"X-Signature", "deadbeef",
			"jwt", testJWT,
			"note", "token="+"s3cr3t value",
			"err", errors.New("login failed: password=hunter2"),
			"payload", testJWT,
			"user", "alice",
			"count", 3,
			slog.Group("nested", slog.String("Authorization", "Bearer xyz"), slog.String("ok", "fine")),
		)
	})
	for _, leak := range []string{"sk_live_123", "hunter2", "deadbeef", testJWT, "s3cr3t", "abc.def", "xyz"} {
		if strings.Contains(raw, leak) {
			t.Errorf("log leaks %q: %s", leak, raw)
		}
	}
	req := m["req"].(map[string]any)
	if req["user"] != "alice" || req["count"] != float64(3) {
		t.Errorf("non-secret attrs altered: %v", req)
	}
	if req["nested"].(map[string]any)["ok"] != "fine" {
		t.Errorf("nested attr altered: %v", req)
	}
	if m["level"] != "INFO" {
		t.Errorf("level = %v", m["level"])
	}
}

func TestMaskString(t *testing.T) {
	for in, want := range map[string]string{
		"plain text":              "plain text",
		"Bearer abc123":           "Bearer " + Redacted,
		"basic dXNlcjpwYXNz":      "basic " + Redacted,
		"secret: topsecret done":  "secret: " + Redacted + " done",
		"api_key=k1&x=1":          "api_key=" + Redacted + "&x=1",
		"jwt " + testJWT + " end": "jwt " + Redacted + " end",
	} {
		if got := MaskString(in); got != want {
			t.Errorf("MaskString(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLoggerTraceID(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid}))

	_, m := logLine(t, func(l *slog.Logger) { l.InfoContext(ctx, "hello") })
	if m["trace_id"] != tid.String() || m["span_id"] != sid.String() {
		t.Errorf("trace fields = %v / %v", m["trace_id"], m["span_id"])
	}
	_, m = logLine(t, func(l *slog.Logger) { l.Info("no span") })
	if _, ok := m["trace_id"]; ok {
		t.Error("trace_id present without span")
	}
}

func TestLoggerLevel(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, slog.LevelWarn).Info("dropped")
	if buf.Len() != 0 {
		t.Errorf("info logged at warn level: %s", buf.String())
	}
}

func TestParseLevel(t *testing.T) {
	if ParseLevel("debug") != slog.LevelDebug || ParseLevel("ERROR") != slog.LevelError || ParseLevel("bogus") != slog.LevelInfo {
		t.Error("ParseLevel mismatch")
	}
}
