package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"

	"github.com/m5tshift/slopay/testkit/payer-sim/internal/payer"
)

const (
	contractFile = "../../../../contracts/openapi/payer-sim.yaml"
	id           = "0b6ce2b8-6a8e-4c3c-9b1d-5a3c1b2f7f10"
)

type fakePayer struct {
	pay       payer.Payment
	err       error
	replayErr error
	replayID  string
	list      []payer.Payment
}

func (f *fakePayer) Pay(context.Context, string) (payer.Payment, error) { return f.pay, f.err }
func (f *fakePayer) Payments() []payer.Payment                          { return f.list }
func (f *fakePayer) Replay(_ context.Context, id string) (payer.ReplayResult, error) {
	f.replayID = id
	return payer.ReplayResult{Sent: 2, Known: 1}, f.replayErr
}

// do sends a request and checks the response against the OpenAPI contract.
func do(t *testing.T, f *fakePayer, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile(contractFile)
	if err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	Register(mux, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	req := httptest.NewRequestWithContext(context.Background(), method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	route, params, err := router.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s is not in the contract: %v", method, path, err)
	}
	in := &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route},
		Status:                 rec.Code, Header: rec.Header(),
	}
	in.SetBodyBytes(rec.Body.Bytes())
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Fatalf("%s %s -> %d violates the contract: %v\n%s", method, path, rec.Code, err, rec.Body.String())
	}
	return rec
}

var paid = payer.Payment{InvoiceID: id, Outcome: payer.Pay, Amount: big.NewInt(5412), TxHash: common.HexToHash("0x5c")}

func TestCreate(t *testing.T) {
	rec := do(t, &fakePayer{pay: paid}, "POST", "/payments", `{"invoiceId":"`+id+`"}`)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"amount":"5412"`) || !strings.Contains(rec.Body.String(), `"txHash":"0x00`) {
		t.Errorf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestCreateSkip(t *testing.T) {
	rec := do(t, &fakePayer{pay: payer.Payment{InvoiceID: id, Outcome: payer.Skip}}, "POST", "/payments", `{"invoiceId":"`+id+`"}`)
	if rec.Code != 200 || strings.Contains(rec.Body.String(), "txHash") {
		t.Errorf("%d %s", rec.Code, rec.Body.String())
	}
}

func TestCreateInvalid(t *testing.T) {
	for _, body := range []string{``, `{`, `{}`, `{"invoiceId":"nope"}`} {
		if rec := do(t, &fakePayer{}, "POST", "/payments", body); rec.Code != 400 {
			t.Errorf("%q: %d", body, rec.Code)
		}
	}
}

func TestCreateUpstream(t *testing.T) {
	for err, want := range map[error]string{payer.ErrNotETH: "not an ETH", errors.New("secret detail"): "failed"} {
		rec := do(t, &fakePayer{err: err}, "POST", "/payments", `{"invoiceId":"`+id+`"}`)
		if rec.Code != 502 || !strings.Contains(rec.Body.String(), want) || strings.Contains(rec.Body.String(), "secret") {
			t.Errorf("%d %s", rec.Code, rec.Body.String())
		}
	}
}

func TestList(t *testing.T) {
	if rec := do(t, &fakePayer{}, "GET", "/payments", ""); strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Errorf("%s", rec.Body.String())
	}
	if rec := do(t, &fakePayer{list: []payer.Payment{paid}}, "GET", "/payments", ""); !strings.Contains(rec.Body.String(), id) {
		t.Errorf("%s", rec.Body.String())
	}
}

func TestReplay(t *testing.T) {
	f := &fakePayer{}
	rec := do(t, f, "POST", "/control/replay", "")
	if rec.Code != 200 || f.replayID != "" || !strings.Contains(rec.Body.String(), `"sent":2`) {
		t.Errorf("%d %s", rec.Code, rec.Body.String())
	}
	do(t, f, "POST", "/control/replay", `{"invoiceId":"`+id+`"}`)
	if f.replayID != id {
		t.Errorf("id = %q", f.replayID)
	}
	if rec := do(t, f, "POST", "/control/replay", `{}`); rec.Code != 200 || f.replayID != "" {
		t.Errorf("%d", rec.Code)
	}
	if rec := do(t, f, "POST", "/control/replay", `{"invoiceId":"x"}`); rec.Code != 400 {
		t.Errorf("%d", rec.Code)
	}
	if rec := do(t, f, "POST", "/control/replay", `{`); rec.Code != 400 {
		t.Errorf("%d", rec.Code)
	}
	f.replayErr = errors.New("x")
	if rec := do(t, f, "POST", "/control/replay", ""); rec.Code != 502 {
		t.Errorf("%d", rec.Code)
	}
}
