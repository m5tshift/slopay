package storage

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/valkey-io/valkey-go"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/m5tshift/slopay/pkg/storage"

var (
	metricsOnce  sync.Once
	authFailures metric.Int64Counter
)

func authFailureCounter() metric.Int64Counter {
	metricsOnce.Do(func() {
		c, err := otel.Meter(instrumentationName).Int64Counter("storage.auth.failures",
			metric.WithDescription("Authentication failures when connecting to a store; each is retried with backoff"))
		if err == nil {
			authFailures = c
		}
	})
	return authFailures
}

// retryAuth runs op, repeating it while the store rejects the credentials. Other errors are
// returned at once. The event is counted and logged without the password.
func retryAuth(ctx context.Context, store string, b Backoff, isAuth func(error) bool, op func(ctx context.Context) error) error {
	ctx, span := otel.Tracer(instrumentationName).Start(ctx, store+" connect",
		trace.WithAttributes(attribute.String("db.system", store)))
	defer span.End()
	err := Retry(ctx, b, isAuth, func(attempt int, err error, wait time.Duration) {
		if c := authFailureCounter(); c != nil {
			c.Add(ctx, 1, metric.WithAttributes(attribute.String("store", store)))
		}
		slog.WarnContext(ctx, "storage: authentication failed, secret file will be re-read",
			"store", store, "attempt", attempt, "retry_in", wait, "error", err)
	}, op)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "connect failed")
	}
	return err
}

// IsPostgresAuthError reports whether err is a PostgreSQL authentication failure
// (invalid_password 28P01 or invalid_authorization_specification 28000).
func IsPostgresAuthError(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "28P01" || pe.Code == "28000")
}

// IsMongoAuthError reports whether err is a MongoDB authentication failure (code 18).
func IsMongoAuthError(err error) bool {
	if err == nil {
		return false
	}
	var ce mongo.CommandError
	if errors.As(err, &ce) && ce.Code == 18 {
		return true
	}
	// A failed handshake is reported inside a server selection error as text.
	s := err.Error()
	return strings.Contains(s, "AuthenticationFailed") || strings.Contains(s, "Authentication failed")
}

// IsValkeyAuthError reports whether err is a Valkey authentication failure (WRONGPASS, NOAUTH).
func IsValkeyAuthError(err error) bool {
	if err == nil {
		return false
	}
	if ve, ok := valkey.IsValkeyErr(err); ok {
		err = ve
	}
	s := err.Error()
	return strings.Contains(s, "WRONGPASS") || strings.Contains(s, "NOAUTH")
}
