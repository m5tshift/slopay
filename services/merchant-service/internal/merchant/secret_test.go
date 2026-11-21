package merchant

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/m5tshift/weigate/services/merchant-service/internal/xpubtest"
)

type memSecrets struct {
	got map[int]string
	err error
}

func (s *memSecrets) PutWebhookSecret(_ context.Context, _ uuid.UUID, version int, secret string) error {
	if s.err != nil {
		return s.err
	}
	s.got[version] = secret
	return nil
}

func TestGenerateSecret(t *testing.T) {
	a, err := GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := GenerateSecret()
	if !strings.HasPrefix(a, SecretPrefix) || len(a) < len(SecretPrefix)+43 || a == b {
		t.Errorf("secrets: %q %q", a, b)
	}
}

func TestRotateWebhookSecretNotConfigured(t *testing.T) {
	f := newFixture(t)
	m, _ := f.register(t)
	if _, err := f.svc.RotateWebhookSecret(ctx, m.ID); !errors.Is(err, ErrRotationUnavailable) {
		t.Errorf("err = %v", err)
	}
}

func TestRotateWebhookSecret(t *testing.T) {
	f := newFixture(t)
	ss := &memSecrets{got: map[int]string{}}
	f.svc.WithSecrets(ss)
	m, _ := f.register(t)
	if _, _, err := f.svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	r1, err := f.svc.RotateWebhookSecret(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := f.svc.RotateWebhookSecret(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 2 || r2.Version != 3 || r1.Secret == r2.Secret || ss.got[2] != r1.Secret || ss.got[3] != r2.Secret {
		t.Errorf("rotations: %+v %+v %v", r1, r2, ss.got)
	}
	// Both versions stay in the store: the merchant accepts both for 24 h.
	if len(ss.got) != 2 {
		t.Errorf("stored %d versions", len(ss.got))
	}
	// xpub, rotation, rotation: the last event carries the new version and never the secret.
	if len(f.store.Events) != 3 {
		t.Fatalf("events = %d", len(f.store.Events))
	}
	last := f.store.Events[2]
	var d UpdatedData
	if err := json.Unmarshal(last.Data, &d); err != nil {
		t.Fatal(err)
	}
	if d.WebhookSecretVersion != 3 || d.Version != 4 || strings.Contains(string(last.Data), r2.Secret) {
		t.Errorf("event: %+v %s", d, last.Data)
	}
	if strings.Contains(f.logs.String(), r1.Secret) || strings.Contains(f.logs.String(), r2.Secret) {
		t.Error("secret in logs")
	}
}

func TestRotateWebhookSecretFailures(t *testing.T) {
	f := newFixture(t)
	ss := &memSecrets{got: map[int]string{}, err: errors.New("sealed")}
	f.svc.WithSecrets(ss)
	m, _ := f.register(t)
	if _, err := f.svc.RotateWebhookSecret(ctx, m.ID); err == nil {
		t.Fatal("no error")
	}
	got, _ := f.svc.Get(ctx, m.ID)
	if got.WebhookSecretVersion != 1 || got.Version != 1 {
		t.Errorf("state changed: %+v", got)
	}
	ss.err = nil
	if _, err := f.svc.RotateWebhookSecret(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
	if len(ss.got) != 0 {
		t.Errorf("stored for unknown merchant: %v", ss.got)
	}
}
