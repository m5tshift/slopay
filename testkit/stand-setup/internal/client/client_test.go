package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTest(t *testing.T, h http.HandlerFunc) (*Client, *[]time.Duration) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := New(srv.URL+"/", nil)
	var slept []time.Duration
	c.Sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	return c, &slept
}

func TestRegisterAndHeaders(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/merchants" || r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected %s %s auth=%q", r.Method, r.URL.Path, r.Header.Get("Authorization"))
		}
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in["name"] != "n" || in["email"] != "e@x.io" {
			t.Errorf("body %v", in)
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"merchantId":"m1","apiKey":{"id":"k","key":"slp_x"}}`))
	})
	got, err := c.Register(context.Background(), "n", "e@x.io")
	if err != nil || got.MerchantID != "m1" || got.APIKey.Key != "slp_x" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestBearerAndOperations(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/token" && r.Header.Get("Authorization") != "Bearer jwt1" {
			t.Errorf("%s: auth %q", r.URL.Path, r.Header.Get("Authorization"))
		}
		switch r.URL.Path {
		case "/api/v1/auth/token":
			_, _ = w.Write([]byte(`{"accessToken":"jwt1","tokenType":"Bearer","expiresIn":900}`))
		case "/api/v1/merchants/me":
			_, _ = w.Write([]byte(`{"merchantId":"m1","xpub":null,"webhookUrl":"https://h","toleranceBps":7}`))
		case "/api/v1/merchants/me/xpub":
			_, _ = w.Write([]byte(`{"xpub":"x","firstAddress":"0xabc"}`))
		case "/api/v1/merchants/me/webhook":
			var in map[string]any
			_ = json.NewDecoder(r.Body).Decode(&in)
			if in["webhookUrl"] != "https://h" || in["toleranceBps"] != float64(7) {
				t.Errorf("webhook body %v", in)
			}
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("path %s", r.URL.Path)
		}
	})
	ctx := context.Background()
	jwt, err := c.Token(ctx, "k")
	if err != nil || jwt != "jwt1" {
		t.Fatal(jwt, err)
	}
	me, err := c.Me(ctx, jwt)
	if err != nil || me.Xpub != nil || *me.WebhookURL != "https://h" || me.ToleranceBps != 7 {
		t.Fatalf("%+v %v", me, err)
	}
	if a, err := c.SetXpub(ctx, jwt, "x"); err != nil || a != "0xabc" {
		t.Fatal(a, err)
	}
	if err := c.SetWebhook(ctx, jwt, "https://h", 7); err != nil {
		t.Fatal(err)
	}
}

func TestRetryAfter429(t *testing.T) {
	var n atomic.Int32
	c, slept := newTest(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) < 3 {
			w.Header().Set("Retry-After", "12")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"code":"rate_limited","message":"x"}`))
			return
		}
		_, _ = w.Write([]byte(`{"accessToken":"j"}`))
	})
	if _, err := c.Token(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if len(*slept) != 2 || (*slept)[0] != 12*time.Second {
		t.Fatalf("slept %v", *slept)
	}
}

func TestRetryAfterMissingDefaultsToOneSecond(t *testing.T) {
	var n atomic.Int32
	c, slept := newTest(t, func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(429)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	if _, err := c.Token(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if (*slept)[0] != time.Second {
		t.Fatalf("slept %v", *slept)
	}
}

func TestRateLimitGivesUp(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(429)
	})
	c.MaxAttempts = 3
	_, err := c.Token(context.Background(), "k")
	if err == nil || !IsStatus(err, 429) {
		t.Fatalf("err %v", err)
	}
}

func TestSleepCancelled(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(429) })
	c.Sleep = func(context.Context, time.Duration) error { return context.Canceled }
	if _, err := c.Token(context.Background(), "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v", err)
	}
}

func TestAPIErrorHasNoSecrets(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"code":"unauthorized","message":"Invalid credentials"}`))
	})
	_, err := c.Token(context.Background(), "slp_secret")
	if !IsStatus(err, 401) || strings.Contains(err.Error(), "slp_secret") || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("err %v", err)
	}
	if IsStatus(errors.New("x"), 401) {
		t.Fatal("IsStatus on foreign error")
	}
}

func TestNonJSONErrorBody(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(502)
		_, _ = w.Write([]byte("<html>bad gateway</html>"))
	})
	_, err := c.Token(context.Background(), "k")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 502 || ae.Message != "" {
		t.Fatalf("err %v", err)
	}
}

func TestBadSuccessBody(t *testing.T) {
	c, _ := newTest(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("nope")) })
	if _, err := c.Token(context.Background(), "k"); err == nil {
		t.Fatal("want decode error")
	}
}

func TestTransportError(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	if _, err := c.Token(context.Background(), "k"); err == nil {
		t.Fatal("want error")
	}
}

func TestBadURL(t *testing.T) {
	c := New("http://bad host", nil)
	if _, err := c.Token(context.Background(), "k"); err == nil {
		t.Fatal("want error")
	}
}
