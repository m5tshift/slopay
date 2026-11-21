package api

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/m5tshift/slopay/testkit/merchant-mock/internal/receiver"
)

const contractFile = "../../../../contracts/openapi/merchant-mock.yaml"

var now = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

// do sends a request and checks the response against the OpenAPI contract.
func do(t *testing.T, mux http.Handler, method, path, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(contractFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s is not in the contract: %v", method, path, err)
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 rec.Code,
		Header:                 rec.Header(),
	}
	in.SetBodyBytes(rec.Body.Bytes())
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Fatalf("%s %s -> %d violates the contract: %v\n%s", method, path, rec.Code, err, rec.Body.String())
	}
	return rec
}

func newMux(t *testing.T) *http.ServeMux {
	t.Helper()
	r := receiver.New(receiver.Options{
		TimestampTolerance: 5 * time.Minute, PreviousSecretTTL: 24 * time.Hour, JournalSize: 10, SeenEvents: 10,
		Now:   func() time.Time { return now },
		Sleep: func(context.Context, time.Duration) error { return nil },
	})
	mux := http.NewServeMux()
	Register(mux, r, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mux
}

func event(id string, seq int) string {
	return `{"id":"` + id + `","type":"invoice.paid","data":{"invoiceId":"i1","sequence":` + strconv.Itoa(seq) + `,"status":"PAID"}}`
}

func signed(key string, version int, body string) map[string]string {
	ts := strconv.FormatInt(now.Unix(), 10)
	return map[string]string{
		"X-SLOPay-Timestamp": ts, "X-SLOPay-Signature": receiver.Sign([]byte(key), ts, []byte(body)),
		"X-SLOPay-Secret-Version": strconv.Itoa(version),
	}
}

func setSecret(t *testing.T, mux http.Handler, version int, secret string) {
	t.Helper()
	rec := do(t, mux, http.MethodPut, "/control/endpoints/m1/secret", `{"version":`+strconv.Itoa(version)+`,"secret":"`+secret+`"}`, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("set secret: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatal("the secret is returned")
	}
}

func TestWebhookFlow(t *testing.T) {
	mux := newMux(t)
	setSecret(t, mux, 1, "s1")

	b := event("e1", 2)
	rec := do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s1", 1, b))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"accepted"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s1", 1, b))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"duplicate"`) {
		t.Fatalf("duplicate: %d %s", rec.Code, rec.Body.String())
	}
	b = event("e0", 1)
	rec = do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s1", 1, b))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"stale"`) {
		t.Fatalf("stale: %d %s", rec.Code, rec.Body.String())
	}

	rec = do(t, mux, http.MethodGet, "/control/endpoints/m1", "", nil)
	for _, want := range []string{`"accepted":1`, `"duplicate":1`, `"stale":1`, `"secretVersion":1`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("state lacks %s: %s", want, rec.Body.String())
		}
	}
	rec = do(t, mux, http.MethodGet, "/control/endpoints/m1/journal?limit=2", "", nil)
	if rec.Code != 200 || strings.Count(rec.Body.String(), `"eventId"`) != 2 || !strings.Contains(rec.Body.String(), `"outcome":"stale"`) {
		t.Fatalf("journal: %d %s", rec.Code, rec.Body.String())
	}
}

func TestWebhookRejections(t *testing.T) {
	mux := newMux(t)
	setSecret(t, mux, 1, "s1")
	b := event("e1", 1)

	rec := do(t, mux, http.MethodPost, "/webhook/m1", b, signed("wrong", 1, b))
	if rec.Code != 401 || !strings.Contains(rec.Body.String(), "invalid_signature") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s1", 7, b)); rec.Code != 401 {
		t.Fatalf("version: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPost, "/webhook/m1", b, nil); rec.Code != 400 {
		t.Fatalf("no headers: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPost, "/webhook/other", b, signed("s1", 1, b)); rec.Code != 404 {
		t.Fatalf("unknown endpoint: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodPost, "/webhook/Bad_Name", b, signed("s1", 1, b)); rec.Code != 404 {
		t.Fatalf("bad name: %d", rec.Code)
	}
	over := `{"id":"x","pad":"` + strings.Repeat("a", maxWebhookBytes) + `"}`
	if rec := do(t, mux, http.MethodPost, "/webhook/m1", over, signed("s1", 1, over)); rec.Code != 400 {
		t.Fatalf("oversized: %d", rec.Code)
	}
}

func TestRotationAndFault(t *testing.T) {
	mux := newMux(t)
	setSecret(t, mux, 1, "s1")
	setSecret(t, mux, 2, "s2")
	b := event("e1", 1)
	if rec := do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s1", 1, b)); rec.Code != 200 {
		t.Fatalf("previous version: %d", rec.Code)
	}
	if rec := do(t, mux, http.MethodGet, "/control/endpoints/m1", "", nil); !strings.Contains(rec.Body.String(), `"previousSecretVersion":1`) {
		t.Fatalf("%s", rec.Body.String())
	}

	rec := do(t, mux, http.MethodPut, "/control/endpoints/m1/fault", `{"status":500,"delayMs":200}`, nil)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"fault":{"status":500,"delayMs":200}`) {
		t.Fatalf("fault: %d %s", rec.Code, rec.Body.String())
	}
	b = event("e2", 2)
	rec = do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s2", 2, b))
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "injected_error") {
		t.Fatalf("injected: %d %s", rec.Code, rec.Body.String())
	}
	do(t, mux, http.MethodPut, "/control/endpoints/m1/fault", `{}`, nil)
	if rec := do(t, mux, http.MethodPost, "/webhook/m1", b, signed("s2", 2, b)); rec.Code != 200 {
		t.Fatalf("after fault removed: %d", rec.Code)
	}
}

func TestCancelledDelayWritesNothing(t *testing.T) {
	r := receiver.New(receiver.Options{
		TimestampTolerance: time.Minute, PreviousSecretTTL: time.Hour, JournalSize: 1, SeenEvents: 1,
		Sleep: func(context.Context, time.Duration) error { return context.Canceled },
	})
	mux := http.NewServeMux()
	Register(mux, r, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := r.SetFault("m1", receiver.Fault{DelayMs: 5}); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/webhook/m1", strings.NewReader("{}")))
	if rec.Body.Len() != 0 {
		t.Fatalf("%q", rec.Body.String())
	}
}

func TestSecretInvalid(t *testing.T) {
	mux := newMux(t)
	setSecret(t, mux, 2, "s2")
	for _, body := range []string{``, `{}`, `{"version":0,"secret":"x"}`, `{"version":1,"secret":"x"}`, `{"version":3,"secret":""}`, `{"version":3,"secret":"x","y":1}`, `not json`} {
		if rec := do(t, mux, http.MethodPut, "/control/endpoints/m1/secret", body, nil); rec.Code != 400 {
			t.Errorf("%q: %d", body, rec.Code)
		}
	}
	if rec := do(t, mux, http.MethodPut, "/control/endpoints/BAD/secret", `{"version":1,"secret":"x"}`, nil); rec.Code != 400 {
		t.Errorf("bad name: %d", rec.Code)
	}
}

func TestFaultInvalid(t *testing.T) {
	mux := newMux(t)
	for _, body := range []string{``, `{"status":200}`, `{"delayMs":-1}`, `{"delayMs":60001}`, `{"status":"x"}`, `{"x":1}`} {
		if rec := do(t, mux, http.MethodPut, "/control/endpoints/m1/fault", body, nil); rec.Code != 400 {
			t.Errorf("%q: %d", body, rec.Code)
		}
	}
	if rec := do(t, mux, http.MethodPut, "/control/endpoints/m1/fault", `{"status":503}`, nil); rec.Code != 200 {
		t.Errorf("503: %d", rec.Code)
	}
	body := `{"status":500,"pad":"` + string(bytes.Repeat([]byte("a"), 2<<10)) + `"}`
	if rec := do(t, mux, http.MethodPut, "/control/endpoints/m1/fault", body, nil); rec.Code != 400 {
		t.Errorf("oversized: %d", rec.Code)
	}
}

func TestUnknownEndpointCommands(t *testing.T) {
	mux := newMux(t)
	for _, path := range []string{"/control/endpoints/nope", "/control/endpoints/nope/journal"} {
		if rec := do(t, mux, http.MethodGet, path, "", nil); rec.Code != 404 {
			t.Errorf("%s: %d", path, rec.Code)
		}
	}
}

func TestJournalLimitInvalid(t *testing.T) {
	mux := newMux(t)
	setSecret(t, mux, 1, "s1")
	for _, q := range []string{"limit=0", "limit=x", "limit=10001"} {
		if rec := do(t, mux, http.MethodGet, "/control/endpoints/m1/journal?"+q, "", nil); rec.Code != 400 {
			t.Errorf("%s: %d", q, rec.Code)
		}
	}
	if rec := do(t, mux, http.MethodGet, "/control/endpoints/m1/journal", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"entries":[]`) {
		t.Errorf("default: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(t, mux, http.MethodGet, "/control/endpoints/BAD/journal", "", nil); rec.Code != 400 {
		t.Errorf("bad name: %d", rec.Code)
	}
}

func TestReset(t *testing.T) {
	mux := newMux(t)
	setSecret(t, mux, 1, "s1")
	if rec := do(t, mux, http.MethodPost, "/control/reset", "", nil); rec.Code != http.StatusNoContent {
		t.Fatalf("%d", rec.Code)
	}
	if rec := do(t, mux, http.MethodGet, "/control/endpoints/m1", "", nil); rec.Code != 404 {
		t.Fatalf("%d", rec.Code)
	}
}
