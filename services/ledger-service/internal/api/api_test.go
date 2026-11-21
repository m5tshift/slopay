package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/google/uuid"

	"github.com/m5tshift/slopay/pkg/kafka"
	"github.com/m5tshift/slopay/pkg/telemetry"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledger"
	"github.com/m5tshift/slopay/services/ledger-service/internal/ledgertest"
)

const contractFile = "../../../../contracts/openapi/ledger-service.yaml"

// contract is the OpenAPI document every response is validated against, and the operations seen by tests.
var contract struct {
	once   sync.Once
	doc    *openapi3.T
	router routers.Router
	err    error
	mu     sync.Mutex
	seen   map[string]bool
}

func contractRouter(t *testing.T) (*openapi3.T, routers.Router) {
	t.Helper()
	contract.once.Do(func() {
		doc, err := openapi3.NewLoader().LoadFromFile(contractFile)
		if err != nil {
			contract.err = err
			return
		}
		if err := doc.Validate(context.Background()); err != nil {
			contract.err = err
			return
		}
		doc.Servers = openapi3.Servers{{URL: "/"}}
		contract.doc = doc
		contract.router, contract.err = legacy.NewRouter(doc)
		contract.seen = map[string]bool{}
	})
	if contract.err != nil {
		t.Fatal(contract.err)
	}
	return contract.doc, contract.router
}

var merchantID = uuid.MustParse("0b9f4e2a-7c3d-4e5f-8a1b-6c2d3e4f5a6b")

type env struct {
	mux   *http.ServeMux
	store *ledger.MemStore
	codec *kafka.Codec
	svc   *ledger.Service
	logs  *bytes.Buffer
}

func newEnv(t *testing.T) *env {
	t.Helper()
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	e := &env{mux: http.NewServeMux(), store: ledger.NewMemStore(codec), codec: codec, logs: &bytes.Buffer{}}
	log := telemetry.NewLogger(e.logs, slog.LevelDebug)
	if e.svc, err = ledger.NewService(e.store, ledger.Config{FeeBps: 50}, log); err != nil {
		t.Fatal(err)
	}
	if err := Register(e.mux, Options{Service: e.svc, Logger: log}); err != nil {
		t.Fatal(err)
	}
	return e
}

// seed posts two ETH receipts to address 1 and a token receipt to address 2.
func (e *env) seed(t *testing.T) {
	t.Helper()
	eth := ledgertest.Invoice{ID: uuid.New(), Merchant: merchantID, Address: ledgertest.Address(1), Asset: ledger.Asset{Kind: ledger.KindETH, Decimals: 18}}
	tok := ledgertest.Invoice{ID: uuid.New(), Merchant: merchantID, Address: ledgertest.Address(2), Asset: ledger.Asset{Kind: ledger.KindERC20, Contract: ledgertest.ERC20Contract, Decimals: 6}}
	for _, ev := range []kafka.Event{
		ledgertest.Event(t, e.codec, "invoice.paid", eth, 3, ledgertest.ETH(100, 1, 6_000_000), ledgertest.ETH(101, 2, 4_000_000)),
		ledgertest.Event(t, e.codec, "invoice.underpaid", tok, 3, ledgertest.ERC20(102, 3, 0, 20_000_000)),
	} {
		if err := e.svc.Apply(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
}

type resp struct {
	status int
	header http.Header
	body   []byte
}

func (r resp) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
}

// get sends a request and validates the response against the contract.
func (e *env) get(t *testing.T, path string) resp {
	t.Helper()
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
	r := resp{status: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
	validateResponse(t, path, r)
	return r
}

func validateResponse(t *testing.T, path string, r resp) {
	t.Helper()
	_, router := contractRouter(t)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil)
	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Fatalf("GET %s is not in the contract: %v", path, err)
	}
	contract.mu.Lock()
	contract.seen[route.Operation.OperationID] = true
	contract.mu.Unlock()
	err = openapi3filter.ValidateResponse(context.Background(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 r.status,
		Header:                 r.header,
		Body:                   io.NopCloser(bytes.NewReader(r.body)),
		Options:                &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
	})
	if err != nil {
		t.Errorf("GET %s -> %d does not match the contract: %v\n%s", path, r.status, err, r.body)
	}
}

func wantBadRequest(t *testing.T, r resp, field string) {
	t.Helper()
	var body Error
	r.decode(t, &body)
	if r.status != http.StatusBadRequest || body.Code != "validation_failed" || !strings.Contains(body.Message, field) {
		t.Errorf("status %d %s, want 400 about %s", r.status, r.body, field)
	}
}

func statementPath(merchant uuid.UUID, q url.Values) string {
	return "/internal/merchants/" + merchant.String() + "/statement?" + q.Encode()
}

func period(from, to time.Time) url.Values {
	return url.Values{"from": {from.Format(time.RFC3339)}, "to": {to.Format(time.RFC3339)}}
}

func TestMerchantStatement(t *testing.T) {
	e := newEnv(t)
	e.seed(t)
	now := time.Now().UTC()
	q := period(now.Add(-time.Hour), now.Add(time.Hour))
	q.Set("limit", "2")
	r := e.get(t, statementPath(merchantID, q))
	if r.status != http.StatusOK {
		t.Fatalf("%d %s", r.status, r.body)
	}
	var st MerchantStatement
	r.decode(t, &st)
	if st.MerchantId != merchantID || len(st.Items) != 2 || st.NextCursor == nil || len(st.Totals) != 2 {
		t.Fatalf("statement %s", r.body)
	}
	token, eth := st.Totals[0], st.Totals[1]
	if eth.Received != "10000000" || eth.Fee != "50000" || eth.Postings != 2 || token.Received != "20000000" || token.Fee != "100000" {
		t.Errorf("totals %+v", st.Totals)
	}
	first := st.Items[0]
	if first.DebitAccount != "address:"+ledgertest.Address(1) || first.CreditAccount != "merchant:"+merchantID.String()+":received" ||
		first.Fee != "30000" || first.Transfer.LogIndex != nil || !strings.HasPrefix(first.PostingId, "receipt:") {
		t.Errorf("posting %+v", first)
	}

	q.Set("cursor", *st.NextCursor)
	r = e.get(t, statementPath(merchantID, q))
	var last MerchantStatement
	r.decode(t, &last)
	if r.status != http.StatusOK || len(last.Items) != 1 || last.NextCursor != nil || last.Items[0].Transfer.LogIndex == nil {
		t.Errorf("last page %d %s", r.status, r.body)
	}
	if !strings.Contains(string(r.body), `"logIndex":0`) || !strings.Contains(string(r.body), `"kind":"ERC20"`) {
		t.Errorf("token posting %s", r.body)
	}
}

func TestMerchantStatementErrors(t *testing.T) {
	e := newEnv(t)
	e.seed(t)
	now := time.Now().UTC()
	q := period(now.Add(-time.Hour), now)

	if r := e.get(t, statementPath(uuid.New(), q)); r.status != http.StatusNotFound {
		t.Errorf("unknown merchant: %d %s", r.status, r.body)
	}
	wantBadRequest(t, e.get(t, statementPath(merchantID, period(now, now))), "to")
	wantBadRequest(t, e.get(t, statementPath(merchantID, url.Values{"to": {now.Format(time.RFC3339)}})), "from")
	wantBadRequest(t, e.get(t, statementPath(merchantID, url.Values{"from": {"yesterday"}, "to": {now.Format(time.RFC3339)}})), "from")
	for _, limit := range []string{"0", "1001", "x"} {
		bad := period(now.Add(-time.Hour), now)
		bad.Set("limit", limit)
		wantBadRequest(t, e.get(t, statementPath(merchantID, bad)), "limit")
	}
	bad := period(now.Add(-time.Hour), now)
	bad.Set("cursor", "not-a-cursor")
	wantBadRequest(t, e.get(t, statementPath(merchantID, bad)), "cursor")
	wantBadRequest(t, e.get(t, "/internal/merchants/42/statement?"+q.Encode()), "merchantId")
}

func addressesPath(addrs ...string) string {
	return "/internal/addresses/statement?" + url.Values{"address": addrs}.Encode()
}

func TestAddressesStatement(t *testing.T) {
	e := newEnv(t)
	e.seed(t)
	unknown := ledgertest.Address(9)
	r := e.get(t, addressesPath(ledgertest.Address(2), unknown, ledgertest.Address(1)))
	if r.status != http.StatusOK {
		t.Fatalf("%d %s", r.status, r.body)
	}
	var st AddressesStatement
	r.decode(t, &st)
	if len(st.Items) != 3 || st.Items[0].Address != ledgertest.Address(2) || st.Items[1].Address != unknown {
		t.Fatalf("items %s", r.body)
	}
	if len(st.Items[1].Totals) != 0 || !strings.Contains(string(r.body), `"totals":[]`) {
		t.Errorf("an address without postings must have empty totals: %s", r.body)
	}
	if st.Items[2].Totals[0].Received != "10000000" || st.Items[2].Totals[0].Postings != 2 {
		t.Errorf("ETH address %+v", st.Items[2])
	}

	wantBadRequest(t, e.get(t, "/internal/addresses/statement"), "address")
	wantBadRequest(t, e.get(t, addressesPath(unknown, unknown)), "address")
	wantBadRequest(t, e.get(t, addressesPath(strings.ToUpper(unknown))), "address")
	many := make([]string, 101)
	for i := range many {
		many[i] = ledgertest.Address(uint64(i))
	}
	wantBadRequest(t, e.get(t, addressesPath(many...)), "address")
	if r := e.get(t, addressesPath(many[:100]...)); r.status != http.StatusOK {
		t.Errorf("100 addresses: %d", r.status)
	}
}

func TestAddressPostings(t *testing.T) {
	e := newEnv(t)
	e.seed(t)
	path := "/internal/addresses/" + ledgertest.Address(1) + "/postings"
	r := e.get(t, path+"?limit=1")
	var page PostingPage
	r.decode(t, &page)
	if r.status != http.StatusOK || page.Address != ledgertest.Address(1) || len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatalf("page 1 %d %s", r.status, r.body)
	}
	r = e.get(t, path+"?limit=1&cursor="+url.QueryEscape(*page.NextCursor))
	var next PostingPage
	r.decode(t, &next)
	if r.status != http.StatusOK || len(next.Items) != 1 || next.NextCursor != nil || next.Items[0].PostingId == page.Items[0].PostingId {
		t.Errorf("page 2 %d %s", r.status, r.body)
	}
	r = e.get(t, "/internal/addresses/"+ledgertest.Address(9)+"/postings")
	if r.status != http.StatusOK || !strings.Contains(string(r.body), `"items":[]`) {
		t.Errorf("empty address %d %s", r.status, r.body)
	}
	wantBadRequest(t, e.get(t, "/internal/addresses/0xABC/postings"), "address")
	wantBadRequest(t, e.get(t, path+"?limit=0"), "limit")
	wantBadRequest(t, e.get(t, path+"?cursor=@@"), "cursor")
}

// brokenStore fails every call.
type brokenStore struct{ ledger.Store }

var errDown = errors.New("postgres is down")

func (brokenStore) MerchantExists(context.Context, uuid.UUID) (bool, error) { return false, errDown }

func (brokenStore) AddressTotals(context.Context, []string) (map[string][]ledger.Totals, error) {
	return nil, errDown
}

func (brokenStore) AddressPostings(context.Context, string, *ledger.Cursor, int) ([]ledger.Posting, error) {
	return nil, errDown
}

func TestInternalError(t *testing.T) {
	logs := &bytes.Buffer{}
	log := telemetry.NewLogger(logs, slog.LevelDebug)
	svc, err := ledger.NewService(brokenStore{}, ledger.Config{FeeBps: 50}, log)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err := Register(mux, Options{Service: svc, Logger: log}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for _, path := range []string{
		statementPath(merchantID, period(now.Add(-time.Hour), now)),
		addressesPath(ledgertest.Address(1)),
		"/internal/addresses/" + ledgertest.Address(1) + "/postings",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		var body Error
		if rec.Code != http.StatusInternalServerError || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body.Code != "internal" {
			t.Errorf("%s: %d %s", path, rec.Code, rec.Body)
		}
	}
	if !strings.Contains(logs.String(), errDown.Error()) {
		t.Error("internal errors are not logged")
	}
}

func TestRegisterWithoutLogger(t *testing.T) {
	codec, err := kafka.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := ledger.NewService(ledger.NewMemStore(codec), ledger.Config{FeeBps: 50}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Register(http.NewServeMux(), Options{Service: svc}); err != nil {
		t.Fatal(err)
	}
}

// TestContractCoverage checks that every operation of the contract was exercised and its responses validated.
func TestContractCoverage(t *testing.T) {
	TestMerchantStatement(t)
	TestMerchantStatementErrors(t)
	TestAddressesStatement(t)
	TestAddressPostings(t)
	doc, _ := contractRouter(t)
	contract.mu.Lock()
	defer contract.mu.Unlock()
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			if !contract.seen[op.OperationID] {
				t.Errorf("%s %s (%s) is not covered", method, path, op.OperationID)
			}
		}
	}
}

func TestErrorHelpers(t *testing.T) {
	g := &gate{log: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	rec := httptest.NewRecorder()
	g.requestError(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil), errors.New("decode"))
	var body Error
	if rec.Code != http.StatusBadRequest || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body.Message != "Request validation failed" {
		t.Errorf("request error: %d %s", rec.Code, rec.Body)
	}
	// A route of the mux that is not in the contract is a bug: 500.
	rec = httptest.NewRecorder()
	(&gate{router: mustRouter(t), log: g.log}).middleware(http.NotFoundHandler()).ServeHTTP(rec,
		httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/elsewhere", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("unknown route: %d", rec.Code)
	}
	for _, err := range []error{
		&InvalidParamFormatError{ParamName: "p"}, &RequiredParamError{ParamName: "p"}, &UnmarshalingParamError{ParamName: "p"},
		&TooManyValuesForParamError{ParamName: "p"}, &RequiredHeaderError{ParamName: "p"}, &UnescapedCookieParamError{ParamName: "p"},
	} {
		if parameterName(err) != "p" {
			t.Errorf("%T: no parameter name", err)
		}
	}
	if parameterName(errors.New("plain")) != "" {
		t.Error("plain error has a parameter")
	}
	if got := invalidParameters(errors.New("plain")); len(got) != 0 {
		t.Errorf("plain error: %v", got)
	}
}

func mustRouter(t *testing.T) routers.Router {
	t.Helper()
	_, r := contractRouter(t)
	return r
}
