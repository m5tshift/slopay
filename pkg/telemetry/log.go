// Package telemetry provides logging, metrics, traces and health endpoints
// shared by all Weigate services (SRS 4.5.1, 5, NFR-11).
package telemetry

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// Redacted replaces any masked value in logs.
const Redacted = "***"

// sensitiveKeys are lower-case substrings of attribute keys whose values are never logged.
var sensitiveKeys = []string{
	"password", "passwd", "secret", "token", "api_key", "apikey", "api-key",
	"authorization", "signature", "jwt", "private", "mnemonic", "credential", "cookie",
}

var (
	jwtRe    = regexp.MustCompile(`eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*`)
	bearerRe = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+`)
	// key=value or key: value inside free text, e.g. error messages.
	kvRe = regexp.MustCompile(`(?i)\b([a-z_-]*(?:password|secret|token|api[_-]?key|signature)[a-z_-]*)(\s*[=:]\s*)[^\s,;"&]+`)
)

func isSensitiveKey(key string) bool {
	k := strings.ToLower(key)
	for _, s := range sensitiveKeys {
		if strings.Contains(k, s) {
			return true
		}
	}
	return false
}

// MaskString masks JWTs, bearer credentials and key=value secrets in free text.
func MaskString(s string) string {
	s = jwtRe.ReplaceAllString(s, Redacted)
	s = bearerRe.ReplaceAllString(s, "$1 "+Redacted)
	return kvRe.ReplaceAllString(s, "${1}${2}"+Redacted)
}

func maskAttr(_ []string, a slog.Attr) slog.Attr {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		return a
	}
	if isSensitiveKey(a.Key) {
		return slog.String(a.Key, Redacted)
	}
	switch a.Value.Kind() {
	case slog.KindString:
		return slog.String(a.Key, MaskString(a.Value.String()))
	case slog.KindAny:
		if err, ok := a.Value.Any().(error); ok {
			return slog.String(a.Key, MaskString(err.Error()))
		}
	default:
	}
	return a
}

// handler adds trace_id and span_id from the context and masks the message.
type handler struct{ slog.Handler }

func (h handler) Handle(ctx context.Context, r slog.Record) error {
	r.Message = MaskString(r.Message)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return handler{h.Handler.WithAttrs(attrs)}
}

func (h handler) WithGroup(name string) slog.Handler { return handler{h.Handler.WithGroup(name)} }

// NewLogger returns a JSON slog logger that writes trace_id for contexts carrying a span
// (use the *Context logging methods) and masks secrets, API keys, JWTs and signatures.
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(handler{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, ReplaceAttr: maskAttr})})
}

// ParseLevel converts debug, info, warn or error to a level; unknown values give info.
func ParseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}
