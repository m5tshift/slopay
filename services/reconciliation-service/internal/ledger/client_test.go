package ledger

import (
	"context"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
)

// contractRouter loads contracts/openapi/ledger-service.yaml: the requests of the client and the answers the
// tests feed it are checked against the contract of ledger-service.
func contractRouter(t *testing.T) routers.Router {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../../contracts/openapi/ledger-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatal(err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	r, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type answer struct {
	status int
	body   string
}

// server validates every request and the answer of respond against the contract.
func server(t *testing.T, operation string, respond func(r *http.Request) answer) *httptest.Server {
	t.Helper()
	router := contractRouter(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, params, err := router.FindRoute(r)
		if err != nil {
			t.Errorf("request not in the contract: %s %s: %v", r.Method, r.URL, err)
			http.Error(w, "", http.StatusNotFound)
			return
		}
		in := &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route,
			Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}}
		if err := openapi3filter.ValidateRequest(r.Context(), in); err != nil {
			t.Errorf("request does not match the contract: %v", err)
		}
		if route.Operation.OperationID != operation {
			t.Errorf("operation %s, want %s", route.Operation.OperationID, operation)
		}
		a := respond(r)
		h := http.Header{"Content-Type": {"application/json"}}
		if err := openapi3filter.ValidateResponse(r.Context(), &openapi3filter.ResponseValidationInput{
			RequestValidationInput: in, Status: a.status, Header: h, Body: io.NopCloser(strings.NewReader(a.body)),
		}); err != nil {
			t.Errorf("fixture answer does not match the contract: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(a.status)
		_, _ = w.Write([]byte(a.body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

const (
	addrA     = "0x00000000000000000000000000000000000000aa"
	addrB     = "0x00000000000000000000000000000000000000bb"
	erc20Addr = "0x5fbdb2315678afecb367f032d93f642f64180aa3"
)

func posting(n int, amount string, asset string, block int) string {
	return `{"postingId":"receipt:0x` + strings.Repeat("1", 64) + `:0x` + strings.Repeat("2", 63) + strconv.Itoa(n) + `:",` +
		`"merchantId":"7c0b6a51-5f0e-4f57-9d35-3f7f2d2b1a10","invoiceId":"0b9f3c2e-8a1d-4c3e-9f7a-1e2d3c4b5a69",` +
		`"address":"` + addrA + `","transfer":{"blockNumber":` + itoa(block) + `,"blockHash":"0x` + strings.Repeat("1", 64) + `",` +
		`"txHash":"0x` + strings.Repeat("2", 64) + `","logIndex":null,"amount":"` + amount + `","asset":` + asset + `},` +
		`"debitAccount":"address:` + addrA + `","creditAccount":"merchant:7c0b6a51-5f0e-4f57-9d35-3f7f2d2b1a10:received",` +
		`"fee":"0","postedAt":"2026-11-20T10:00:00Z"}`
}

func itoa(n int) string { return big.NewInt(int64(n)).String() }

const ethAsset = `{"kind":"ETH","decimals":18}`

func TestAddressTotals(t *testing.T) {
	srv := server(t, "getAddressesStatement", func(r *http.Request) answer {
		if got := r.URL.Query()["address"]; len(got) != 2 || got[0] != addrA || got[1] != addrB {
			t.Errorf("addresses = %v", got)
		}
		return answer{200, `{"items":[` +
			`{"address":"` + addrA + `","totals":[{"asset":` + ethAsset + `,"received":"1000","fee":"5","postings":2},` +
			`{"asset":{"kind":"ERC20","contract":"` + erc20Addr + `","decimals":6},"received":"7","fee":"0","postings":1}]},` +
			`{"address":"` + addrB + `","totals":[]}]}`}
	})
	c, err := New(srv.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.AddressTotals(context.Background(), []string{addrA, strings.ToUpper(addrB[:2]) + addrB[2:]})
	if err != nil {
		t.Fatal(err)
	}
	if eth := got[addrA]["ETH"]; eth.Received.Cmp(big.NewInt(1000)) != 0 || eth.Postings != 2 {
		t.Errorf("eth totals = %+v", eth)
	}
	if tok := got[addrA][erc20Addr]; tok.Received.Cmp(big.NewInt(7)) != 0 || tok.Postings != 1 {
		t.Errorf("erc20Addr totals = %+v", tok)
	}
	if b, ok := got[addrB]; !ok || len(b) != 0 {
		t.Errorf("address without postings = %+v, %v", b, ok)
	}
}

func TestAddressPostingsPages(t *testing.T) {
	calls := 0
	srv := server(t, "listAddressPostings", func(r *http.Request) answer {
		calls++
		if r.URL.Query().Get("limit") != "1000" {
			t.Errorf("limit = %s", r.URL.Query().Get("limit"))
		}
		if r.URL.Query().Get("cursor") == "" {
			return answer{200, `{"address":"` + addrA + `","items":[` + posting(1, "600", ethAsset, 10) + `],"nextCursor":"p2"}`}
		}
		return answer{200, `{"address":"` + addrA + `","items":[` + posting(2, "400", ethAsset, 12) + `],"nextCursor":null}`}
	})
	c, _ := New(srv.URL, time.Second)
	got, err := c.AddressPostings(context.Background(), addrA)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(got) != 2 || got[0].Amount.Cmp(big.NewInt(600)) != 0 || got[1].BlockNumber != 12 || got[1].Asset.Key() != "ETH" {
		t.Fatalf("calls %d, postings %+v", calls, got)
	}
}

func TestErrors(t *testing.T) {
	srv := server(t, "getAddressesStatement", func(*http.Request) answer {
		return answer{400, `{"code":"validation_failed","message":"bad address"}`}
	})
	c, _ := New(srv.URL, time.Second)
	if _, err := c.AddressTotals(context.Background(), []string{addrA}); err == nil {
		t.Fatal("400: want error")
	}

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer slow.Close()
	c, _ = New(slow.URL, 50*time.Millisecond)
	if _, err := c.AddressPostings(context.Background(), addrA); err == nil {
		t.Fatal("timeout: want error")
	}

	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	c, _ = New(down.URL, time.Second)
	if _, err := c.AddressTotals(context.Background(), []string{addrA}); err == nil {
		t.Fatal("connection refused: want error")
	}
	if _, err := c.AddressTotals(context.Background(), make([]string, 101)); err == nil {
		t.Fatal("101 addresses: want error")
	}
	if _, err := New("ftp://ledger", time.Second); err == nil {
		t.Fatal("ftp url: want error")
	}
}
