package merchant

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// SecretPrefix marks webhook signing secrets.
const SecretPrefix = "whsec_"

// ErrRotationUnavailable is returned when no secret store is configured.
var ErrRotationUnavailable = errors.New("merchant: webhook secret store is not configured")

// SecretStore keeps webhook signing secrets, one entry per merchant and version (OpenBao KV).
// Implementations must not put the secret into returned errors.
type SecretStore interface {
	PutWebhookSecret(ctx context.Context, merchantID uuid.UUID, version int, secret string) error
}

// GenerateSecret returns a new 256-bit webhook secret.
func GenerateSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("merchant: generate secret: %w", err)
	}
	return SecretPrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

// RotatedSecret is the new webhook secret; Secret is shown once and never stored by this service.
type RotatedSecret struct {
	Version int
	Secret  string
}

// WithSecrets sets the store of webhook secrets and returns s.
func (s *Service) WithSecrets(st SecretStore) *Service {
	s.secrets = st
	return s
}

// RotateWebhookSecret writes the next secret version to the secret store and then raises
// webhookSecretVersion, which publishes merchant.updated through the outbox (SRS 4.1.10). The merchant row is
// locked meanwhile, so concurrent rotations get consecutive versions. If the secret store fails, nothing
// changes; if the transaction fails after the write, the written version was never published and the next
// rotation overwrites it.
func (s *Service) RotateWebhookSecret(ctx context.Context, id uuid.UUID) (RotatedSecret, error) {
	if s.secrets == nil {
		return RotatedSecret{}, ErrRotationUnavailable
	}
	secret, err := GenerateSecret()
	if err != nil {
		return RotatedSecret{}, err
	}
	var version int
	_, err = s.store.Update(ctx, id, func(m *Merchant) error {
		version = m.WebhookSecretVersion + 1
		if err := s.secrets.PutWebhookSecret(ctx, id, version, secret); err != nil {
			return err
		}
		m.WebhookSecretVersion = version
		return nil
	})
	if err != nil {
		return RotatedSecret{}, err
	}
	s.log.InfoContext(ctx, "webhook secret rotated", "merchant_id", id.String(), "version", version)
	return RotatedSecret{Version: version, Secret: secret}, nil
}
