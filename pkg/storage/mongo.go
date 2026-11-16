package storage

import (
	"context"
	"fmt"
	"os"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

// MongoConfig configures a MongoDB client. The password is not part of URI.
type MongoConfig struct {
	// URI is a connection string without credentials, e.g. mongodb://host:27017.
	URI      string
	Username string
	// PasswordFile is the mounted secret with the password.
	PasswordFile SecretFile
	// AuthSource is the authentication database; empty means "admin".
	AuthSource string
	// Database is the default database of the service.
	Database string
	Backoff  Backoff
}

// MongoConfigFromEnv reads <prefix>_URI, _USERNAME, _PASSWORD_FILE, _AUTH_SOURCE and _DATABASE.
func MongoConfigFromEnv(prefix string) (MongoConfig, error) {
	c := MongoConfig{
		URI: os.Getenv(prefix + "_URI"), Username: os.Getenv(prefix + "_USERNAME"),
		PasswordFile: SecretFile(os.Getenv(prefix + "_PASSWORD_FILE")),
		AuthSource:   os.Getenv(prefix + "_AUTH_SOURCE"), Database: os.Getenv(prefix + "_DATABASE"),
	}
	if c.URI == "" || c.Username == "" || c.PasswordFile == "" || c.Database == "" {
		return c, fmt.Errorf("storage: %[1]s_URI, %[1]s_USERNAME, %[1]s_PASSWORD_FILE and %[1]s_DATABASE are required", prefix)
	}
	return c, nil
}

// Mongo wraps a mongo-go-driver client. The driver reads the password only when the client is
// created, so after an authentication failure the client is rebuilt with the current file content.
type Mongo struct {
	cfg MongoConfig

	mu     sync.RWMutex
	client *mongo.Client
}

// OpenMongo connects and pings. Authentication failures are retried with backoff.
func OpenMongo(ctx context.Context, cfg MongoConfig) (*Mongo, error) {
	m := &Mongo{cfg: cfg}
	c, err := m.dial(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage: mongodb: %w", err)
	}
	m.client = c
	return m, nil
}

func (m *Mongo) dial(ctx context.Context) (*mongo.Client, error) {
	var c *mongo.Client
	err := retryAuth(ctx, "mongodb", m.cfg.Backoff, IsMongoAuthError, func(ctx context.Context) error {
		pw, err := m.cfg.PasswordFile.Read()
		if err != nil {
			return err
		}
		src := m.cfg.AuthSource
		if src == "" {
			src = "admin"
		}
		cl, err := mongo.Connect(options.Client().ApplyURI(m.cfg.URI).SetAuth(options.Credential{
			Username: m.cfg.Username, Password: pw, PasswordSet: true, AuthSource: src,
		}))
		if err != nil {
			return err
		}
		if err := cl.Ping(ctx, readpref.Primary()); err != nil {
			_ = cl.Disconnect(context.WithoutCancel(ctx))
			return err
		}
		c = cl
		return nil
	})
	return c, err
}

// Client returns the current client. Do not keep it across calls to Ready: it may be replaced.
func (m *Mongo) Client() *mongo.Client {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.client
}

// Database returns the service database.
func (m *Mongo) Database() *mongo.Database { return m.Client().Database(m.cfg.Database) }

// Ready is a readiness check for /health/ready. After an authentication failure it rebuilds the
// client with the password from the file.
func (m *Mongo) Ready(ctx context.Context) error {
	err := m.Client().Ping(ctx, readpref.Primary())
	if err == nil || !IsMongoAuthError(err) {
		return err
	}
	return m.Reconnect(ctx)
}

// Reconnect replaces the client with a new one built from the current password file.
// Operations in flight on the old client finish before it is disconnected.
func (m *Mongo) Reconnect(ctx context.Context) error {
	c, err := m.dial(ctx)
	if err != nil {
		return fmt.Errorf("storage: mongodb: %w", err)
	}
	m.mu.Lock()
	old := m.client
	m.client = c
	m.mu.Unlock()
	return old.Disconnect(context.WithoutCancel(ctx))
}

// Close disconnects the client.
func (m *Mongo) Close(ctx context.Context) error { return m.Client().Disconnect(ctx) }
