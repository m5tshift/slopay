package setup

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/m5tshift/slopay/pkg/hdwallet"
	"github.com/m5tshift/slopay/testkit/stand-setup/internal/client"
	"github.com/m5tshift/slopay/testkit/stand-setup/internal/wallet"
)

const mnemonic = "test test test test test test test test test test test junk"

type fakeAPI struct {
	merchants map[string]*client.Merchant // by api key
	registers int
	xpubSets  int
	hookSets  int
	failReg   error
	failToken error
	failMe    error
	failXpub  error
	failHook  error
	badFirst  bool
}

func newFake() *fakeAPI { return &fakeAPI{merchants: map[string]*client.Merchant{}} }

func (f *fakeAPI) Register(_ context.Context, name, email string) (client.Registered, error) {
	if f.failReg != nil {
		return client.Registered{}, f.failReg
	}
	f.registers++
	key := "slp_key_" + name
	f.merchants[key] = &client.Merchant{MerchantID: "id-" + name, Name: name, Email: email}
	r := client.Registered{MerchantID: "id-" + name}
	r.APIKey.Key = key
	return r, nil
}

func (f *fakeAPI) Token(_ context.Context, k string) (string, error) {
	if f.failToken != nil {
		return "", f.failToken
	}
	if f.merchants[k] == nil {
		return "", &client.APIError{Status: 401, Code: "unauthorized"}
	}
	return "jwt:" + k, nil
}

func (f *fakeAPI) m(jwt string) *client.Merchant { return f.merchants[strings.TrimPrefix(jwt, "jwt:")] }

func (f *fakeAPI) Me(_ context.Context, jwt string) (client.Merchant, error) {
	if f.failMe != nil {
		return client.Merchant{}, f.failMe
	}
	return *f.m(jwt), nil
}

func (f *fakeAPI) SetXpub(_ context.Context, jwt, xpub string) (string, error) {
	if f.failXpub != nil {
		return "", f.failXpub
	}
	f.xpubSets++
	f.m(jwt).Xpub = &xpub
	if f.badFirst {
		return "0x0", nil
	}
	return hdwallet.Derive(xpub, 0)
}

func (f *fakeAPI) SetWebhook(_ context.Context, jwt, url string, tol int) error {
	if f.failHook != nil {
		return f.failHook
	}
	f.hookSets++
	m := f.m(jwt)
	m.WebhookURL, m.ToleranceBps = &url, tol
	return nil
}

func cfg(t *testing.T) Config {
	return Config{Count: 3, Extra: 1, NamePrefix: "m", EmailDomain: "x.test",
		WebhookURL: "https://merchant-mock.test/hook", ToleranceBps: 100,
		KeysPath: filepath.Join(t.TempDir(), "keys.json")}
}

func run(t *testing.T, f *fakeAPI, c Config) (Result, error) {
	t.Helper()
	w, err := wallet.New(mnemonic)
	if err != nil {
		t.Fatal(err)
	}
	return Run(context.Background(), f, w, c, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestNames(t *testing.T) {
	got := Config{Count: 2, Extra: 2, NamePrefix: "p"}.Names()
	want := []string{"p-001", "p-002", "p-extra-01", "p-extra-02"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%v", got)
	}
}

func TestValidate(t *testing.T) {
	good := Config{Count: 1, NamePrefix: "p", EmailDomain: "d", WebhookURL: "u", KeysPath: "k"}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	mods := map[string]func(*Config){
		"empty":     func(c *Config) { c.Count = 0 },
		"negative":  func(c *Config) { c.Extra = -1 },
		"prefix":    func(c *Config) { c.NamePrefix = "" },
		"domain":    func(c *Config) { c.EmailDomain = "" },
		"webhook":   func(c *Config) { c.WebhookURL = "" },
		"tol high":  func(c *Config) { c.ToleranceBps = 501 },
		"tol low":   func(c *Config) { c.ToleranceBps = -1 },
		"keys path": func(c *Config) { c.KeysPath = "" },
	}
	for name, mod := range mods {
		c := good
		mod(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	if _, err := run(t, newFake(), Config{}); err == nil {
		t.Fatal("Run must validate")
	}
}

func TestFirstRun(t *testing.T) {
	f, c := newFake(), cfg(t)
	res, err := run(t, f, c)
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Registered: 4, Configured: 4}) {
		t.Fatalf("%+v", res)
	}
	keys, err := LoadKeys(c.KeysPath)
	if err != nil || len(keys.Merchants) != 4 {
		t.Fatalf("%v %v", keys, err)
	}
	st, _ := os.Stat(c.KeysPath)
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", st.Mode())
	}
	w, _ := wallet.New(mnemonic)
	seen := map[string]bool{}
	for i, e := range keys.Merchants {
		m := f.merchants[e.APIKey]
		want, _ := w.AccountXpub(uint32(i))
		if *m.Xpub != want || *m.WebhookURL != c.WebhookURL || m.ToleranceBps != 100 {
			t.Fatalf("merchant %d: %+v", i, m)
		}
		if seen[*m.Xpub] {
			t.Fatal("xpub shared between merchants")
		}
		seen[*m.Xpub] = true
	}
}

func TestIdempotentRerun(t *testing.T) {
	f, c := newFake(), cfg(t)
	if _, err := run(t, f, c); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(c.KeysPath)
	res, err := run(t, f, c)
	if err != nil {
		t.Fatal(err)
	}
	if res != (Result{Reused: 4, Unchanged: 4}) {
		t.Fatalf("%+v", res)
	}
	if f.registers != 4 || f.xpubSets != 4 || f.hookSets != 4 {
		t.Fatalf("writes on rerun: %d %d %d", f.registers, f.xpubSets, f.hookSets)
	}
	after, _ := os.ReadFile(c.KeysPath)
	if string(before) != string(after) {
		t.Fatal("keys file changed")
	}
}

func TestRerunRepairsDrift(t *testing.T) {
	f, c := newFake(), cfg(t)
	if _, err := run(t, f, c); err != nil {
		t.Fatal(err)
	}
	f.merchants["slp_key_m-002"].ToleranceBps = 5
	f.merchants["slp_key_m-003"].Xpub = nil
	res, err := run(t, f, c)
	if err != nil {
		t.Fatal(err)
	}
	if res.Configured != 2 || res.Unchanged != 2 || f.xpubSets != 5 || f.hookSets != 5 {
		t.Fatalf("%+v xpub=%d hook=%d", res, f.xpubSets, f.hookSets)
	}
}

func TestResumeAfterPartialRun(t *testing.T) {
	f, c := newFake(), cfg(t)
	c2 := c
	c2.Count, c2.Extra = 2, 0
	if _, err := run(t, f, c2); err != nil {
		t.Fatal(err)
	}
	res, err := run(t, f, c)
	if err != nil {
		t.Fatal(err)
	}
	if res.Reused != 2 || res.Registered != 2 {
		t.Fatalf("%+v", res)
	}
}

func TestErrors(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]struct {
		mod  func(*fakeAPI)
		want string
	}{
		"register": {func(f *fakeAPI) { f.failReg = boom }, "register"},
		"token":    {func(f *fakeAPI) { f.failToken = boom }, "token"},
		"me":       {func(f *fakeAPI) { f.failMe = boom }, "read settings"},
		"xpub":     {func(f *fakeAPI) { f.failXpub = boom }, "set xpub"},
		"first":    {func(f *fakeAPI) { f.badFirst = true }, "first address"},
		"hook":     {func(f *fakeAPI) { f.failHook = boom }, "set webhook"},
	}
	for name, tc := range cases {
		f := newFake()
		tc.mod(f)
		_, err := run(t, f, cfg(t))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v", name, err)
		}
	}
}

func TestStaleKeyFails(t *testing.T) {
	c := cfg(t)
	if err := SaveKeys(c.KeysPath, &KeysFile{Merchants: []Entry{{Name: "m-001", APIKey: "gone"}}}); err != nil {
		t.Fatal(err)
	}
	_, err := run(t, newFake(), c)
	if !client.IsStatus(err, 401) {
		t.Fatalf("err %v", err)
	}
}

func TestKeysSavedBeforeLaterFailure(t *testing.T) {
	f, c := newFake(), cfg(t)
	f.failHook = errors.New("boom")
	if _, err := run(t, f, c); err == nil {
		t.Fatal("want error")
	}
	keys, _ := LoadKeys(c.KeysPath)
	if len(keys.Merchants) != 1 {
		t.Fatalf("key of the registered merchant lost: %+v", keys)
	}
}

func TestNoSecretsInLogs(t *testing.T) {
	var sb strings.Builder
	w, _ := wallet.New(mnemonic)
	f := newFake()
	_, err := Run(context.Background(), f, w, cfg(t), slog.New(slog.NewTextHandler(&sb, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sb.String(), "slp_key_") || strings.Contains(sb.String(), "xpub") {
		t.Fatal("secret in log")
	}
}

func TestLoadKeys(t *testing.T) {
	dir := t.TempDir()
	if k, err := LoadKeys(filepath.Join(dir, "none")); err != nil || len(k.Merchants) != 0 {
		t.Fatal(k, err)
	}
	bad := filepath.Join(dir, "bad")
	_ = os.WriteFile(bad, []byte("{ secret"), 0o600)
	if _, err := LoadKeys(bad); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err %v", err)
	}
	if _, err := LoadKeys(dir); err == nil {
		t.Fatal("directory must fail")
	}
}

func TestSaveKeysBadDir(t *testing.T) {
	if err := SaveKeys(filepath.Join(t.TempDir(), "no", "keys.json"), &KeysFile{}); err == nil {
		t.Fatal("want error")
	}
}

func TestInsideGitRepo(t *testing.T) {
	dir := t.TempDir()
	if in, err := InsideGitRepo(filepath.Join(dir, "k.json")); err != nil || in {
		t.Fatal(in, err)
	}
	_ = os.Mkdir(filepath.Join(dir, ".git"), 0o750)
	sub := filepath.Join(dir, "a", "b")
	_ = os.MkdirAll(sub, 0o750)
	if in, err := InsideGitRepo(filepath.Join(sub, "k.json")); err != nil || !in {
		t.Fatal(in, err)
	}
}
