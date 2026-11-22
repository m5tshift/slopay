//go:build integration

package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/m5tshift/weigate/pkg/kafka"
	"github.com/m5tshift/weigate/pkg/outbox"
	"github.com/m5tshift/weigate/pkg/storage"
	"github.com/m5tshift/weigate/services/merchant-service/internal/jwt"
	"github.com/m5tshift/weigate/services/merchant-service/internal/merchant"
	"github.com/m5tshift/weigate/services/merchant-service/internal/openbao"
	"github.com/m5tshift/weigate/services/merchant-service/internal/xpubtest"
	"github.com/m5tshift/weigate/services/merchant-service/migrations"
)

var (
	brokers []string
	pg      *storage.Postgres
	codec   *kafka.Codec
)

const timeout = 90 * time.Second

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	kc, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(kc) }()
	if brokers, err = kc.Brokers(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "brokers:", err)
		return 1
	}
	pc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("pw"),
		postgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pc) }()
	host, err := pc.Host(ctx)
	if err != nil {
		return 1
	}
	port, err := pc.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return 1
	}
	dir, err := os.MkdirTemp("", "merchant-store")
	if err != nil {
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()
	pwFile := filepath.Join(dir, "password")
	if err := os.WriteFile(pwFile, []byte("pw"), 0o600); err != nil {
		return 1
	}
	pg, err = storage.OpenPostgres(ctx, storage.PostgresConfig{
		URL:          fmt.Sprintf("postgres://app@%s:%s/app?sslmode=disable", host, port.Port()),
		PasswordFile: storage.SecretFile(pwFile),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open postgres:", err)
		return 1
	}
	defer pg.Close()
	if _, err := pg.Migrate(ctx, migrations.FS); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	if codec, err = kafka.NewCodec(); err != nil {
		return 1
	}
	if err := createTopic(ctx, merchant.Topic); err != nil {
		fmt.Fprintln(os.Stderr, "topic:", err)
		return 1
	}
	return m.Run()
}

func createTopic(ctx context.Context, topic string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic, rt.NumPartitions, rt.ReplicationFactor = topic, 3, 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, tr := range resp.Topics {
		if err := kerr.ErrorForCode(tr.ErrorCode); err != nil {
			return err
		}
	}
	return nil
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

var cfg = merchant.Config{KeyRotationGrace: 24 * time.Hour, MaxActiveKeys: 2, DefaultToleranceBps: 100, MaxToleranceBps: 500}

func newService() (*merchant.Service, *Postgres) {
	st := New(pg.Pool(), codec)
	return merchant.NewService(st, cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))), st
}

func TestMigrationsAreIdempotent(t *testing.T) {
	n, err := pg.Migrate(ctxT(t), migrations.FS)
	if err != nil || n != 0 {
		t.Errorf("second run applied %d, err %v", n, err)
	}
}

func TestMerchantLifecycle(t *testing.T) {
	ctx := ctxT(t)
	svc, _ := newService()
	m, key, err := svc.Register(ctx, "Example Shop", "owner@merchant.example.com")
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.Get(ctx, m.ID)
	if err != nil || got.Name != m.Name || got.Email != m.Email || got.Version != 1 || got.ToleranceBps != 100 || !got.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("get = %+v, err %v", got, err)
	}
	if id, err := svc.Authenticate(ctx, key.Key); err != nil || id != m.ID {
		t.Fatalf("authenticate: %v %v", id, err)
	}
	// The key is stored only as its hash.
	var stored []byte
	if err := pg.Pool().QueryRow(ctx, `SELECT key_hash FROM api_keys WHERE id = $1`, key.ID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) == key.Key || string(stored) != string(merchant.HashKey(key.Key)) {
		t.Error("key is not stored as its hash")
	}

	tol := 250
	if _, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
		t.Fatal(err)
	}
	if n := outboxCount(t, m.ID); n != 0 {
		t.Errorf("event without xpub: %d", n)
	}
	after, first, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub)
	if err != nil || first == "" || after.Version != 3 {
		t.Fatalf("set xpub: %+v %s %v", after, first, err)
	}
	// Unchanged settings: no new version, no new event.
	if again, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil || again.Version != 3 {
		t.Errorf("no-op update: %+v %v", again, err)
	}
	if n := outboxCount(t, m.ID); n != 1 {
		t.Errorf("outbox events = %d, want 1", n)
	}
	if _, err := svc.Get(ctx, uuid.New()); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestUpdateRollsBack(t *testing.T) {
	ctx := ctxT(t)
	svc, st := newService()
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	_, err = st.Update(ctx, m.ID, func(mm *merchant.Merchant) error {
		x := xpubtest.Xpub
		mm.Xpub = &x
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// An event that does not match the schema rolls the whole change back.
	_, err = st.Update(ctx, m.ID, func(mm *merchant.Merchant) error {
		x := "xpub-invalid"
		mm.Xpub = &x
		return nil
	})
	if !errors.Is(err, kafka.ErrInvalidEvent) {
		t.Fatalf("invalid event: %v", err)
	}
	got, err := svc.Get(ctx, m.ID)
	if err != nil || got.Xpub != nil || got.Version != 1 || outboxCount(t, m.ID) != 0 {
		t.Errorf("not rolled back: %+v %v", got, err)
	}
	if _, err := st.Update(ctx, uuid.New(), func(*merchant.Merchant) error { return nil }); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func TestConcurrentUpdatesAreSerialised(t *testing.T) {
	ctx := ctxT(t)
	svc, _ := newService()
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tol := i
			if _, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	rows, err := pg.Pool().Query(ctx, `SELECT event->'data'->>'version' FROM outbox WHERE key = $1 ORDER BY id`, m.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	// Versions in outbox order are 2, 3, ... without gaps or repeats.
	for i, v := range versions {
		if v != fmt.Sprint(i+2) {
			t.Fatalf("versions = %v", versions)
		}
	}
	if len(versions) < 2 {
		t.Errorf("versions = %v", versions)
	}
}

func TestKeyLifecycle(t *testing.T) {
	ctx := ctxT(t)
	svc, st := newService()
	m, first, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.IssueKey(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := svc.ListKeys(ctx, m.ID)
	if err != nil || len(keys) != 2 || keys[0].ID != first.ID || keys[0].ExpiresAt == nil || keys[1].ID != second.ID || keys[1].ExpiresAt != nil {
		t.Fatalf("keys = %+v, err %v", keys, err)
	}
	if _, err := svc.IssueKey(ctx, m.ID); !errors.Is(err, merchant.ErrTooManyKeys) {
		t.Errorf("third key: %v", err)
	}
	// After the grace period the old key no longer works.
	later := keys[0].ExpiresAt.Add(time.Second)
	if _, err := st.KeyOwner(ctx, merchant.HashKey(first.Key), later); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("expired key: %v", err)
	}
	if id, err := st.KeyOwner(ctx, merchant.HashKey(second.Key), later); err != nil || id != m.ID {
		t.Errorf("new key: %v %v", id, err)
	}
	// Emergency revoke of the new key leaves the old one working without an expiry.
	if err := svc.RevokeKey(ctx, m.ID, second.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Authenticate(ctx, second.Key); !errors.Is(err, merchant.ErrBadAPIKey) {
		t.Errorf("revoked key: %v", err)
	}
	if id, err := st.KeyOwner(ctx, merchant.HashKey(first.Key), later); err != nil || id != m.ID {
		t.Errorf("remaining key: %v %v", id, err)
	}
	if err := svc.RevokeKey(ctx, m.ID, first.ID); !errors.Is(err, merchant.ErrLastKey) {
		t.Errorf("last key: %v", err)
	}
	if err := svc.RevokeKey(ctx, uuid.New(), first.ID); !errors.Is(err, merchant.ErrNotFound) {
		t.Errorf("unknown merchant: %v", err)
	}
}

func outboxCount(t *testing.T, id uuid.UUID) int {
	t.Helper()
	var n int
	if err := pg.Pool().QueryRow(ctxT(t), `SELECT count(*) FROM outbox WHERE key = $1`, id.String()).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestEventsReachKafka checks the whole path: change, outbox in the same transaction, relay, merchant.events.
func TestEventsReachKafka(t *testing.T) {
	ctx := ctxT(t)
	svc, _ := newService()
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	tol := 0
	if _, err := svc.SetWebhook(ctx, m.ID, "https://merchant.example.com/hooks", &tol); err != nil {
		t.Fatal(err)
	}
	p, err := kafka.NewProducer(codec, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	r, err := outbox.NewRelay(outbox.Config{Pool: pg.Pool(), Publisher: p})
	if err != nil {
		t.Fatal(err)
	}
	for {
		n, err := r.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	c, err := kafka.NewConsumer(codec, nil, brokers, "g-"+uuid.NewString(), []string{merchant.Topic})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var got []merchant.UpdatedData
	err = c.Run(cctx, func(_ context.Context, ev kafka.Event) error {
		if ev.Subject != m.ID.String() {
			return nil
		}
		if ev.Type != merchant.EventType {
			t.Errorf("type %s", ev.Type)
		}
		var d merchant.UpdatedData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			return err
		}
		got = append(got, d)
		if len(got) == 2 {
			cancel()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Version != 2 || got[1].Version != 3 || got[1].ToleranceBps != 0 ||
		got[1].WebhookURL == nil || *got[1].WebhookURL != "https://merchant.example.com/hooks" || got[1].Xpub != xpubtest.Xpub {
		t.Errorf("events = %+v", got)
	}
}

// startOpenBao runs OpenBao in dev mode (KV v2 on secret/) and returns its address and root token.
func startOpenBao(t *testing.T) (addr, token string) {
	t.Helper()
	ctx := ctxT(t)
	const root = "dev-root-token"
	c, err := testcontainers.Run(ctx, "openbao/openbao:2.1.0",
		testcontainers.WithExposedPorts("8200/tcp"),
		testcontainers.WithCmd("server", "-dev", "-dev-root-token-id="+root, "-dev-listen-address=0.0.0.0:8200"),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/v1/sys/health").WithPort("8200/tcp")),
	)
	if err != nil {
		t.Fatalf("start openbao: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "8200/tcp")
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), root
}

func readSecret(t *testing.T, addr, token string, id uuid.UUID, version int) (secret string, v float64) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctxT(t), http.MethodGet, fmt.Sprintf("%s%s?version=%d", addr, openbao.Path("secret", id), version), nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Vault-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read version %d: %d", version, resp.StatusCode)
	}
	var out struct {
		Data struct {
			Data struct {
				Secret string `json:"secret"`
			} `json:"data"`
			Metadata struct {
				Version float64 `json:"version"`
			} `json:"metadata"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.Data.Data.Secret, out.Data.Metadata.Version
}

// TestWebhookSecretRotation checks the KV version of the new secret lands in the outbox as the secret
// version, that the old version stays readable (registration wrote version 1) and that an OpenBao failure leaves no trace in PostgreSQL.
func TestWebhookSecretRotation(t *testing.T) {
	ctx := ctxT(t)
	addr, token := startOpenBao(t)
	// The service token may only write webhook secrets (and use the JWT key), as the Kubernetes auth role does.
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(scopedToken(t, addr, token)), 0o600); err != nil {
		t.Fatal(err)
	}
	bao, err := openbao.New(openbao.Config{Addr: addr, TokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	svc, _ := newService()
	svc.WithSecrets(bao)
	m, _, err := svc.Register(ctx, "Shop", "a@b.example")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.SetXpub(ctx, m.ID, xpubtest.Xpub); err != nil {
		t.Fatal(err)
	}
	r1, err := svc.RotateWebhookSecret(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := svc.RotateWebhookSecret(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 2 || r2.Version != 3 {
		t.Fatalf("versions %d %d", r1.Version, r2.Version)
	}
	if first, v := readSecret(t, addr, token, m.ID, 1); first == "" || v != 1 {
		t.Errorf("registration secret: %q %v", first, v)
	}
	// The service token cannot read secrets back.
	if code, _ := baoCall(t, addr, scopedToken(t, addr, token), http.MethodGet, openbao.Path("secret", m.ID), ""); code != http.StatusForbidden {
		t.Errorf("read with the service token: %d", code)
	}
	for _, r := range []merchant.RotatedSecret{r1, r2} {
		if got, v := readSecret(t, addr, token, m.ID, r.Version); got != r.Secret || int(v) != r.Version {
			t.Errorf("version %d in OpenBao: %q %v", r.Version, got, v)
		}
	}
	got, err := svc.Get(ctx, m.ID)
	if err != nil || got.WebhookSecretVersion != 3 || got.Version != 4 || outboxCount(t, m.ID) != 3 {
		t.Errorf("state: %+v %v outbox=%d", got, err, outboxCount(t, m.ID))
	}
	var payload string
	if err := pg.Pool().QueryRow(ctx, `SELECT event::text FROM outbox WHERE key = $1 ORDER BY id DESC LIMIT 1`, m.ID.String()).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(payload, r2.Secret) || !strings.Contains(payload, `"webhookSecretVersion": 3`) && !strings.Contains(payload, `"webhookSecretVersion":3`) {
		t.Errorf("outbox payload: %s", payload)
	}

	// A wrong token: OpenBao rejects the write, the version does not move and no event is added.
	if err := os.WriteFile(tokenFile, []byte("wrong"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RotateWebhookSecret(ctx, m.ID); !errors.Is(err, openbao.ErrUnavailable) {
		t.Errorf("rejected token: %v", err)
	}
	got, _ = svc.Get(ctx, m.ID)
	if got.WebhookSecretVersion != 3 || got.Version != 4 || outboxCount(t, m.ID) != 3 {
		t.Errorf("state after failure: %+v outbox=%d", got, outboxCount(t, m.ID))
	}
}

// baoCall sends one request to the OpenBao API and returns the status and body.
func baoCall(t *testing.T, addr, token, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctxT(t), method, addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Vault-Token", token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func baoMust(t *testing.T, addr, token, method, path, body string) []byte {
	t.Helper()
	code, out := baoCall(t, addr, token, method, path, body)
	if code/100 != 2 {
		t.Fatalf("%s %s: %d %s", method, path, code, out)
	}
	return out
}

// scopedToken returns a token with the policy of the merchant-service role of infra/terraform/openbao:
// writes of secret/data/webhook/*, signing with transit key merchant-jwt, reading its public keys.
func scopedToken(t *testing.T, addr, root string) string {
	t.Helper()
	const policy = `path "secret/data/webhook/*" { capabilities = ["create", "update"] }
path "transit/sign/merchant-jwt" { capabilities = ["update"] }
path "transit/keys/merchant-jwt" { capabilities = ["read"] }`
	pol, _ := json.Marshal(map[string]string{"policy": policy})
	baoMust(t, addr, root, http.MethodPut, "/v1/sys/policies/acl/merchant-service", string(pol))
	out := baoMust(t, addr, root, http.MethodPost, "/v1/auth/token/create", `{"policies":["merchant-service"]}`)
	var resp struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(out, &resp); err != nil || resp.Auth.ClientToken == "" {
		t.Fatalf("token create: %v %s", err, out)
	}
	return resp.Auth.ClientToken
}

// TestTransitSigning issues JWTs through Transit with a token that cannot export the key, and follows
// a key rotation: tokens of the previous version still verify and both versions are in the JWKS.
func TestTransitSigning(t *testing.T) {
	ctx := ctxT(t)
	addr, root := startOpenBao(t)
	baoMust(t, addr, root, http.MethodPost, "/v1/sys/mounts/transit", `{"type":"transit"}`)
	baoMust(t, addr, root, http.MethodPost, "/v1/transit/keys/merchant-jwt", `{"type":"ecdsa-p256"}`)
	tokenFile := filepath.Join(t.TempDir(), "token")
	svcToken := scopedToken(t, addr, root)
	if err := os.WriteFile(tokenFile, []byte(svcToken), 0o600); err != nil {
		t.Fatal(err)
	}
	bao, err := openbao.New(openbao.Config{Addr: addr, TokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jwt.NewTransitSigner(ctx, bao.Transit("transit", "merchant-jwt"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	iss := jwt.NewIssuer(signer, "weigate-merchant-service", 15*time.Minute)
	sub := uuid.NewString()
	v1, err := iss.Issue(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := iss.Verify(ctx, v1); err != nil || c.Subject != sub {
		t.Fatalf("verify v1: %+v %v", c, err)
	}
	// The private key is not exportable, with any token.
	if code, _ := baoCall(t, addr, root, http.MethodGet, "/v1/transit/export/signing-key/merchant-jwt/latest", ""); code/100 == 2 {
		t.Errorf("signing key is exportable: %d", code)
	}

	baoMust(t, addr, root, http.MethodPost, "/v1/transit/keys/merchant-jwt/rotate", "")
	v2, err := iss.Issue(ctx, sub)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := signer.PublicKeys(ctx)
	if err != nil || len(keys) != 2 {
		t.Fatalf("keys: %d %v", len(keys), err)
	}
	for _, tok := range []string{v1, v2} {
		if _, err := iss.Verify(ctx, tok); err != nil {
			t.Errorf("verify after rotation: %v", err)
		}
	}
	if kidOf(t, v1) == kidOf(t, v2) || kidOf(t, v2) != keys[0].KID {
		t.Errorf("kids: %q %q, current %q", kidOf(t, v1), kidOf(t, v2), keys[0].KID)
	}

	// A token rejected by OpenBao: no signature, no leak.
	if err := os.WriteFile(tokenFile, []byte("wrong"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := iss.Issue(ctx, sub); !errors.Is(err, openbao.ErrUnavailable) {
		t.Errorf("rejected token: %v", err)
	}
}

func kidOf(t *testing.T, token string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[0])
	if err != nil {
		t.Fatal(err)
	}
	var h struct {
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	return h.Kid
}
