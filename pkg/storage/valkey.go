package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// ValkeyConfig configures a valkey-go client. The password is not part of Addrs.
type ValkeyConfig struct {
	Addrs    []string
	Username string
	// PasswordFile is the mounted secret with the password; empty means no authentication.
	PasswordFile SecretFile
	Backoff      Backoff
}

// ValkeyConfigFromEnv reads <prefix>_ADDRS (comma-separated), _USERNAME and _PASSWORD_FILE.
func ValkeyConfigFromEnv(prefix string) (ValkeyConfig, error) {
	c := ValkeyConfig{Username: os.Getenv(prefix + "_USERNAME"), PasswordFile: SecretFile(os.Getenv(prefix + "_PASSWORD_FILE"))}
	for a := range strings.SplitSeq(os.Getenv(prefix+"_ADDRS"), ",") {
		if a = strings.TrimSpace(a); a != "" {
			c.Addrs = append(c.Addrs, a)
		}
	}
	if len(c.Addrs) == 0 {
		return c, fmt.Errorf("storage: %s_ADDRS is required", prefix)
	}
	return c, nil
}

// Valkey is a valkey-go client. The client asks for credentials on every new connection, so the
// password file is re-read when a connection is re-established.
type Valkey struct {
	client valkey.Client
}

// OpenValkey connects and pings. Authentication failures are retried with backoff.
func OpenValkey(ctx context.Context, cfg ValkeyConfig) (*Valkey, error) {
	opt := valkey.ClientOption{InitAddress: cfg.Addrs, DisableCache: true}
	if cfg.PasswordFile != "" {
		opt.AuthCredentialsFn = func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
			pw, err := cfg.PasswordFile.Read()
			if err != nil {
				return valkey.AuthCredentials{}, err
			}
			return valkey.AuthCredentials{Username: cfg.Username, Password: pw}, nil
		}
	}
	var c valkey.Client
	err := retryAuth(ctx, "valkey", cfg.Backoff, IsValkeyAuthError, func(ctx context.Context) error {
		cl, err := valkey.NewClient(opt)
		if err != nil {
			return err
		}
		if err := cl.Do(ctx, cl.B().Ping().Build()).Error(); err != nil {
			cl.Close()
			return err
		}
		c = cl
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: valkey: %w", err)
	}
	return &Valkey{client: c}, nil
}

// Client returns the underlying client.
func (v *Valkey) Client() valkey.Client { return v.client }

// Ready is a readiness check for /health/ready.
func (v *Valkey) Ready(ctx context.Context) error {
	return v.client.Do(ctx, v.client.B().Ping().Build()).Error()
}

// Close closes the client.
func (v *Valkey) Close() { v.client.Close() }

// DefaultDedupTTL is the lifetime of a webhook deduplication key (SRS 4.1.7).
const DefaultDedupTTL = 24 * time.Hour

// ValkeyDedup is a kafka.DedupStore on Valkey: SET key NX EX ttl (SRS 4.1.7). Valkey is a cache
// without persistence, so after its loss an event may be handled twice: consumers stay idempotent.
type ValkeyDedup struct {
	client valkey.Client
	prefix string
	ttl    time.Duration
}

// NewValkeyDedup creates the store. An empty prefix becomes "dedup:", a ttl <= 0 DefaultDedupTTL.
func NewValkeyDedup(v *Valkey, prefix string, ttl time.Duration) *ValkeyDedup {
	return newValkeyDedup(v.client, prefix, ttl)
}

func newValkeyDedup(c valkey.Client, prefix string, ttl time.Duration) *ValkeyDedup {
	if prefix == "" {
		prefix = "dedup:"
	}
	if ttl <= 0 {
		ttl = DefaultDedupTTL
	}
	return &ValkeyDedup{client: c, prefix: prefix, ttl: ttl}
}

// Claim implements kafka.DedupStore.
func (d *ValkeyDedup) Claim(ctx context.Context, id string) (bool, error) {
	cmd := d.client.B().Set().Key(d.prefix + id).Value("1").Nx().ExSeconds(int64(d.ttl / time.Second)).Build()
	err := d.client.Do(ctx, cmd).Error()
	if valkey.IsValkeyNil(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("storage: valkey claim: %w", err)
	}
	return true, nil
}

// Release implements kafka.DedupStore.
func (d *ValkeyDedup) Release(ctx context.Context, id string) error {
	if err := d.client.Do(ctx, d.client.B().Del().Key(d.prefix+id).Build()).Error(); err != nil {
		return fmt.Errorf("storage: valkey release: %w", err)
	}
	return nil
}
