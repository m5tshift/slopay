package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/m5tshift/weigate/services/reporting-service/internal/store"
)

type fakeReports struct {
	err       error
	turnover  []store.TurnoverDay
	conv      []store.ConversionDay
	merchant  uuid.UUID
	from, to  time.Time
	calls     int
	deadlined bool
}

func (f *fakeReports) record(ctx context.Context, m uuid.UUID, from, to time.Time) {
	f.calls++
	f.merchant, f.from, f.to = m, from, to
	_, f.deadlined = ctx.Deadline()
}

func (f *fakeReports) Turnover(ctx context.Context, m uuid.UUID, from, to time.Time) ([]store.TurnoverDay, error) {
	f.record(ctx, m, from, to)
	return f.turnover, f.err
}

func (f *fakeReports) Conversion(ctx context.Context, m uuid.UUID, from, to time.Time) ([]store.ConversionDay, error) {
	f.record(ctx, m, from, to)
	return f.conv, f.err
}

type env struct {
	h        http.Handler
	reports  *fakeReports
	logs     *bytes.Buffer
	merchant uuid.UUID
	token    string
}

func newEnv(t *testing.T, keys KeyResolver) *env {
	t.Helper()
	e := &env{reports: &fakeReports{}, logs: &bytes.Buffer{}, merchant: uuid.New()}
	mux := http.NewServeMux()
	err := Register(mux, Options{
		Reports: e.reports, JWTIssuer: issuer, Keys: keys, MaxDays: 31, RetryAfter: 7 * time.Second,
		Logger: slog.New(slog.NewTextHandler(e.logs, nil)), Now: func() time.Time { return t0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	e.h = mux
	e.token = signToken(t, testKey, testKID, map[string]any{"sub": e.merchant.String(), "iss": issuer, "exp": t0.Add(time.Minute).Unix()})
	return e
}

func (e *env) get(path, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	return w
}

// conforms validates the response against the contract.
func conforms(t *testing.T, path string, w *httptest.ResponseRecorder) {
	t.Helper()
	doc, err := GetSpec()
	if err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Fatal(err)
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 w.Code,
		Header:                 w.Header(),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true},
	}
	in.SetBodyBytes(w.Body.Bytes())
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Errorf("%s: %d does not conform to the contract: %v\n%s", path, w.Code, err, w.Body)
	}
}

func day(s string) time.Time {
	d, err := time.Parse(time.DateOnly, s)
	if err != nil {
		panic(err)
	}
	return d
}

const period = "?from=2026-11-01&to=2026-11-30"

func TestTurnover(t *testing.T) {
	e := newEnv(t, testKeys)
	e.reports.turnover = []store.TurnoverDay{
		{Day: day("2026-11-01"), PaidInvoices: 2, TurnoverUSDCents: 15300},
		{Day: day("2026-11-03"), PaidInvoices: 1, TurnoverUSDCents: 100},
	}
	path := "/api/v1/reports/turnover" + period
	w := e.get(path, e.token)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	conforms(t, path, w)
	want := `{"from":"2026-11-01","items":[{"day":"2026-11-01","paidInvoices":2,"turnoverUsdCents":15300},` +
		`{"day":"2026-11-03","paidInvoices":1,"turnoverUsdCents":100}],"to":"2026-11-30"}`
	if got := strings.TrimSpace(w.Body.String()); !equalJSON(got, want) {
		t.Errorf("body = %s", got)
	}
	r := e.reports
	if r.merchant != e.merchant || !r.from.Equal(day("2026-11-01")) || !r.to.Equal(day("2026-11-30")) || !r.deadlined {
		t.Errorf("query: merchant %v from %v to %v deadline %v", r.merchant, r.from, r.to, r.deadlined)
	}
}

func TestConversion(t *testing.T) {
	e := newEnv(t, testKeys)
	e.reports.conv = []store.ConversionDay{{Day: day("2026-11-02"), CreatedInvoices: 4, PaidInvoices: 3, Conversion: 0.75}}
	path := "/api/v1/reports/conversion" + period
	w := e.get(path, e.token)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	conforms(t, path, w)
	want := `{"from":"2026-11-01","items":[{"day":"2026-11-02","createdInvoices":4,"paidInvoices":3,"conversion":0.75}],"to":"2026-11-30"}`
	if got := strings.TrimSpace(w.Body.String()); !equalJSON(got, want) {
		t.Errorf("body = %s", got)
	}
	if e.reports.merchant != e.merchant {
		t.Errorf("merchant = %v", e.reports.merchant)
	}
}

func TestEmptyReportIsAnEmptyArray(t *testing.T) {
	e := newEnv(t, testKeys)
	for _, p := range []string{"turnover", "conversion"} {
		path := "/api/v1/reports/" + p + period
		w := e.get(path, e.token)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"items":[]`) {
			t.Errorf("%s: %d %s", p, w.Code, w.Body)
		}
		conforms(t, path, w)
	}
}

func TestUnauthorized(t *testing.T) {
	e := newEnv(t, testKeys)
	expired := signToken(t, testKey, testKID, map[string]any{"sub": uuid.NewString(), "iss": issuer, "exp": t0.Unix()})
	for name, tc := range map[string]struct{ path, token string }{
		"no token":       {"/api/v1/reports/turnover" + period, ""},
		"expired token":  {"/api/v1/reports/conversion" + period, expired},
		"garbage":        {"/api/v1/reports/turnover" + period, "abc"},
		"no params":      {"/api/v1/reports/turnover", ""},
		"unknown path":   {"/api/v1/other", ""},
		"401 before 400": {"/api/v1/reports/turnover?from=x", "abc"},
	} {
		t.Run(name, func(t *testing.T) {
			w := e.get(tc.path, tc.token)
			if w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("status %d, header %q", w.Code, w.Header().Get("WWW-Authenticate"))
			}
			if strings.HasPrefix(tc.path, "/api/v1/reports") {
				conforms(t, strings.SplitN(tc.path, "?", 2)[0]+period, w)
			}
		})
	}
	if e.reports.calls != 0 {
		t.Error("ClickHouse must not be queried without authentication")
	}
}

func TestOtherSchemeIsUnauthorized(t *testing.T) {
	e := newEnv(t, testKeys)
	r := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/v1/reports/turnover"+period, nil)
	r.Header.Set("Authorization", "Basic "+e.token)
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status %d", w.Code)
	}
}

func TestJWKSDown(t *testing.T) {
	e := newEnv(t, downKeys{})
	w := e.get("/api/v1/reports/turnover"+period, e.token)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status %d", w.Code)
	}
	if !strings.Contains(e.logs.String(), "level=WARN") || strings.Contains(e.logs.String(), e.token) {
		t.Errorf("logs: %s", e.logs)
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, testKeys)
	for name, q := range map[string]string{
		"no from":         "?to=2026-11-30",
		"no to":           "?from=2026-11-01",
		"bad date":        "?from=yesterday&to=2026-11-30",
		"not a real date": "?from=2026-02-30&to=2026-03-01",
		"from after to":   "?from=2026-11-30&to=2026-11-01",
		"too long":        "?from=2026-10-01&to=2026-11-01",
	} {
		for _, p := range []string{"turnover", "conversion"} {
			t.Run(p+" "+name, func(t *testing.T) {
				path := "/api/v1/reports/" + p + q
				w := e.get(path, e.token)
				if w.Code != http.StatusBadRequest {
					t.Fatalf("status %d: %s", w.Code, w.Body)
				}
				conforms(t, "/api/v1/reports/"+p+period, w)
			})
		}
	}
	if e.reports.calls != 0 {
		t.Error("ClickHouse must not be queried for an invalid request")
	}
}

func TestLongestPeriod(t *testing.T) {
	e := newEnv(t, testKeys)
	// 31 days: allowed; 32: not.
	if w := e.get("/api/v1/reports/turnover?from=2026-10-01&to=2026-10-31", e.token); w.Code != http.StatusOK {
		t.Errorf("31 days: %d", w.Code)
	}
	if w := e.get("/api/v1/reports/turnover?from=2026-10-01&to=2026-11-01", e.token); w.Code != http.StatusBadRequest {
		t.Errorf("32 days: %d", w.Code)
	}
	if w := e.get("/api/v1/reports/turnover?from=2026-11-05&to=2026-11-05", e.token); w.Code != http.StatusOK {
		t.Errorf("one day: %d", w.Code)
	}
}

func TestClickHouseDown(t *testing.T) {
	e := newEnv(t, testKeys)
	e.reports.err = errors.New("dial tcp: connection refused")
	for _, p := range []string{"turnover", "conversion"} {
		path := "/api/v1/reports/" + p + period
		w := e.get(path, e.token)
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "7" {
			t.Fatalf("%s: status %d, Retry-After %q", p, w.Code, w.Header().Get("Retry-After"))
		}
		conforms(t, path, w)
	}
	if strings.Contains(e.logs.String(), e.token) || !strings.Contains(e.logs.String(), "connection refused") {
		t.Errorf("logs: %s", e.logs)
	}
}

func TestRegisterRequiresKeysAndMaxDays(t *testing.T) {
	if err := Register(http.NewServeMux(), Options{MaxDays: 1}); err == nil {
		t.Error("no keys: want an error")
	}
	if err := Register(http.NewServeMux(), Options{Keys: testKeys}); err == nil {
		t.Error("no max days: want an error")
	}
}

func equalJSON(a, b string) bool {
	var x, y any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}
