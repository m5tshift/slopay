package merchant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/hdwallet"
	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/services/merchant-service/internal/xpubtest"
)

var ctx = context.Background()

var testCfg = Config{KeyRotationGrace: 24 * time.Hour, MaxActiveKeys: 2, DefaultToleranceBps: 100, MaxToleranceBps: 500}

type fixture struct {
	svc   *Service
	store *MemStore
	logs  *bytes.Buffer
	now   time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{store: NewMemStore(codec), logs: &bytes.Buffer{}, now: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)}
	f.svc = NewService(f.store, testCfg, telemetry.NewLogger(f.logs, slog.LevelDebug))
	f.svc.now = func() time.Time { return f.now }
	return f
}

func (f *fixture) register(t *testing.T) (Merchant, IssuedKey) {
	t.Helper()
	m, k, err := f.svc.Register(ctx, "Example Shop", "owner@merchant.example.com")
	if err != nil {
		t.Fatal(err)
	}
	return m, k
}

func validationField(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve.Field
	}
	return ""
}

func TestRegister(t *testing.T) {
	f := newFixture(t)
	m, k := f.register(t)
	if m.ToleranceBps != 100 || m.WebhookSecretVersion != 1 || m.Version != 1 || m.Xpub != nil || m.WebhookURL != nil {
		t.Errorf("merchant = %+v", m)
	}
	if k.MerchantID != m.ID || k.Status() != StatusActive || !strings.HasPrefix(k.Key, KeyPrefix) {
		t.Errorf("key = %+v", k.APIKey)
	}
	// The key is stored only as a hash.
	for _, h := range f.store.StoredHashes() {
		if bytes.Contains(h, []byte(k.Key)) || !bytes.Equal(h, HashKey(k.Key)) {
			t.Error("key stored in clear")
		}
	}
	if len(f.store.Events) != 0 {
		t.Error("a merchant without xpub must not be published")
	}
	if strings.Contains(f.logs.String(), k.Key) {
		t.Error("API key logged")
	}
	if !strings.Contains(f.logs.String(), m.ID.String()) {
		t.Error("registration not logged")
	}
}

func TestRegisterValidation(t *testing.T) {
	f := newFixture(t)
	for name, tc := range map[string]struct{ name, email, field string }{
		"empty name":      {"", "a@b.example", "name"},
		"blank name":      {"   ", "a@b.example", "name"},
		"long name":       {strings.Repeat("я", 201), "a@b.example", "name"},
		"invalid utf8":    {"\xff", "a@b.example", "name"},
		"bad email":       {"Shop", "not-an-email", "email"},
		"display name":    {"Shop", "Owner <a@b.example>", "email"},
		"long email":      {"Shop", strings.Repeat("a", 250) + "@b.ex", "email"},
		"email with tail": {"Shop", "a@b.example x", "email"},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := f.svc.Register(ctx, tc.name, tc.email)
			if got := validationField(err); got != tc.field {
				t.Errorf("err = %v, want field %s", err, tc.field)
			}
		})
	}
	if _, _, err := f.svc.Register(ctx, strings.Repeat("я", 200), "a@b.example"); err != nil {
		t.Errorf("200 characters must be accepted: %v", err)
	}
}

func TestRegisterStoreError(t *testing.T) {
	f := newFixture(t)
	f.store.Err = errors.New("db down")
	if _, _, err := f.svc.Register(ctx, "Shop", "a@b.example"); err == nil {
		t.Error("store error swallowed")
	}
}

func TestAuthenticate(t *testing.T) {
	f := newFixture(t)
	m, k := f.register(t)
	id, err := f.svc.Authenticate(ctx, k.Key)
	if err != nil || id != m.ID {
		t.Fatalf("id = %v, err %v", id, err)
	}
	other, _ := GenerateKey()
	for name, key := range map[string]string{"malformed": "secret", "unknown": other, "empty": ""} {
		if _, err := f.svc.Authenticate(ctx, key); !errors.Is(err, ErrBadAPIKey) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	f.store.Err = errors.New("db down")
	if _, err := f.svc.Authenticate(ctx, k.Key); err == nil || errors.Is(err, ErrBadAPIKey) {
		t.Errorf("store error must not look like a bad key: %v", err)
	}
}

func TestSetXpub(t *testing.T) {
	f := newFixture(t)
	m, _ := f.register(t)
	got, first, err := f.svc.SetXpub(ctx, m.ID, xpubtest.Xpub)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hdwallet.Derive(xpubtest.Xpub, 0)
	if first != want || got.Xpub == nil || *got.Xpub != xpubtest.Xpub || got.Version != 2 {
		t.Errorf("first = %s (want %s), merchant %+v", first, want, got)
	}
	if len(f.store.Events) != 1 {
		t.Fatalf("events = %d", len(f.store.Events))
	}
	// Same xpub again: nothing changes, nothing is published.
	again, _, err := f.svc.SetXpub(ctx, m.ID, xpubtest.Xpub)
	if err != nil || again.Version != 2 || len(f.store.Events) != 1 {
		t.Errorf("repeat: version %d, events %d, err %v", again.Version, len(f.store.Events), err)
	}

	private := xpubtest.PrivateVersion()
	for name, in := range map[string]string{
		"private key":  private,
		"bad checksum": xpubtest.BadChecksum(),
		"garbage":      "xpubnot-base58",
		"unknown ver":  xpubtest.WithVersion(xpubtest.Xpub, [4]byte{1, 2, 3, 4}),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := f.svc.SetXpub(ctx, m.ID, in)
			if validationField(err) != "xpub" {
				t.Fatalf("err = %v", err)
			}
			if strings.Contains(err.Error(), in) {
				t.Error("the value is in the error")
			}
		})
	}
	var ve *ValidationError
	_, _, err = f.svc.SetXpub(ctx, m.ID, private)
	if errors.As(err, &ve); !strings.Contains(ve.Message, "private key") {
		t.Errorf("private key message = %q", ve.Message)
	}
	if strings.Contains(f.logs.String(), private) {
		t.Error("private key logged")
	}
	if _, _, err := f.svc.SetXpub(ctx, uuid.New(), xpubtest.Xpub); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestSetWebhook(t *testing.T) {
	f := newFixture(t)
	m, _ := f.register(t)
	tol := 250
	got, err := f.svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks/slopay", &tol)
	if err != nil {
		t.Fatal(err)
	}
	if got.WebhookURL == nil || *got.WebhookURL != "https://merchant.example.com/hooks/slopay" || got.ToleranceBps != 250 || got.Version != 2 {
		t.Errorf("merchant = %+v", got)
	}
	if len(f.store.Events) != 0 {
		t.Error("published without xpub")
	}
	// Omitted tolerance is the default.
	got, err = f.svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks/slopay", nil)
	if err != nil || got.ToleranceBps != 100 || got.Version != 3 {
		t.Errorf("default tolerance: %+v, %v", got, err)
	}
	for _, bps := range []int{0, 500} {
		if _, err := f.svc.SetWebhook(ctx, m.ID, "https://a.example", &bps); err != nil {
			t.Errorf("tolerance %d: %v", bps, err)
		}
	}
	for name, tc := range map[string]struct {
		url   string
		tol   int
		field string
	}{
		"http":         {"http://merchant.example.com/hook", 100, "webhookUrl"},
		"relative":     {"/hook", 100, "webhookUrl"},
		"no host":      {"https:///hook", 100, "webhookUrl"},
		"opaque":       {"https:merchant.example.com", 100, "webhookUrl"},
		"space":        {"https://merchant.example.com/a b", 100, "webhookUrl"},
		"newline":      {"https://merchant.example.com/a\n", 100, "webhookUrl"},
		"unparsable":   {"https://[::1", 100, "webhookUrl"},
		"too long":     {"https://a.example/" + strings.Repeat("a", 2048), 100, "webhookUrl"},
		"negative tol": {"https://a.example", -1, "toleranceBps"},
		"tol over 5 %": {"https://a.example", 501, "toleranceBps"},
	} {
		t.Run(name, func(t *testing.T) {
			tol := tc.tol
			if _, err := f.svc.SetWebhook(ctx, m.ID, tc.url, &tol); validationField(err) != tc.field {
				t.Errorf("err = %v, want field %s", err, tc.field)
			}
		})
	}
	if _, err := f.svc.SetWebhook(ctx, uuid.New(), "https://a.example", nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestPublishedEvents(t *testing.T) {
	f := newFixture(t)
	m, _ := f.register(t)
	tol := 250
	if _, err := f.svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks2", &tol); err != nil {
		t.Fatal(err)
	}
	if len(f.store.Events) != 2 {
		t.Fatalf("events = %d", len(f.store.Events))
	}
	var prev int64
	for i, ev := range f.store.Events {
		var d UpdatedData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			t.Fatal(err)
		}
		if ev.Subject != m.ID.String() || d.MerchantID != m.ID.String() || d.Xpub != xpubtest.Xpub || d.ToleranceBps != 250 {
			t.Errorf("event %d: %+v %+v", i, ev, d)
		}
		if d.Version <= prev {
			t.Errorf("version not growing: %d after %d", d.Version, prev)
		}
		prev = d.Version
	}
	if prev != 4 {
		t.Errorf("last version = %d, want 4 (register, webhook, xpub, webhook)", prev)
	}
}

func TestKeyRotation(t *testing.T) {
	f := newFixture(t)
	m, first := f.register(t)
	f.now = f.now.Add(time.Minute)

	second, err := f.svc.IssueKey(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := f.svc.ListKeys(ctx, m.ID)
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys = %v, err %v", keys, err)
	}
	if keys[0].ID != first.ID || keys[0].Status() != StatusExpiring || !keys[0].ExpiresAt.Equal(f.now.Add(24*time.Hour)) {
		t.Errorf("old key = %+v", keys[0])
	}
	if keys[1].ID != second.ID || keys[1].Status() != StatusActive {
		t.Errorf("new key = %+v", keys[1])
	}
	// Both keys work during the grace period; a third key is refused.
	for _, k := range []string{first.Key, second.Key} {
		if _, err := f.svc.Authenticate(ctx, k); err != nil {
			t.Errorf("key rejected during grace: %v", err)
		}
	}
	if _, err := f.svc.IssueKey(ctx, m.ID); !errors.Is(err, ErrTooManyKeys) {
		t.Errorf("third key: %v", err)
	}

	// After 24 h the old key stops working and a new rotation is possible.
	f.now = f.now.Add(24 * time.Hour)
	if _, err := f.svc.Authenticate(ctx, first.Key); !errors.Is(err, ErrBadAPIKey) {
		t.Errorf("expired key: %v", err)
	}
	third, err := f.svc.IssueKey(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Emergency revoke works at once; the last key cannot be revoked.
	if err := f.svc.RevokeKey(ctx, m.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Authenticate(ctx, second.Key); !errors.Is(err, ErrBadAPIKey) {
		t.Errorf("revoked key: %v", err)
	}
	if err := f.svc.RevokeKey(ctx, m.ID, third.ID); !errors.Is(err, ErrLastKey) {
		t.Errorf("last key: %v", err)
	}
	if err := f.svc.RevokeKey(ctx, m.ID, first.ID); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expired key revoke: %v", err)
	}
	if strings.Contains(f.logs.String(), third.Key) {
		t.Error("API key logged")
	}

	if _, err := f.svc.IssueKey(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown merchant issue: %v", err)
	}
	if err := f.svc.RevokeKey(ctx, uuid.New(), third.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown merchant revoke: %v", err)
	}
}

func TestRevokeNewKeyKeepsOldOne(t *testing.T) {
	f := newFixture(t)
	m, first := f.register(t)
	f.now = f.now.Add(time.Minute)
	second, err := f.svc.IssueKey(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.RevokeKey(ctx, m.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(48 * time.Hour)
	if _, err := f.svc.Authenticate(ctx, first.Key); err != nil {
		t.Errorf("remaining key expired after revoking the new one: %v", err)
	}
}

func TestStoreErrors(t *testing.T) {
	f := newFixture(t)
	m, _ := f.register(t)
	f.store.Err = errors.New("db down")
	if _, err := f.svc.Get(ctx, m.ID); err == nil {
		t.Error("get")
	}
	if _, err := f.svc.ListKeys(ctx, m.ID); err == nil {
		t.Error("list")
	}
	if _, err := f.svc.IssueKey(ctx, m.ID); err == nil {
		t.Error("issue")
	}
	if err := f.svc.RevokeKey(ctx, m.ID, uuid.New()); err == nil {
		t.Error("revoke")
	}
}

func TestNewServiceDefaultLogger(t *testing.T) {
	if NewService(NewMemStore(nil), testCfg, nil).log == nil {
		t.Error("nil logger")
	}
}
