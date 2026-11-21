package invoice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func serve(t *testing.T, code int, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/pay/abc" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.URL, srv.Client())
}

func TestGet(t *testing.T) {
	c := serve(t, 200, `{"address":"0x71c7656ec7ab88b098defb751b7401b5f6d8976f","amount":"5412000000000000","asset":"ETH"}`)
	p, err := c.Get(context.Background(), "abc")
	if err != nil || p.Amount.String() != "5412000000000000" || p.Asset != "ETH" || p.Address[:4] != "0x71" {
		t.Errorf("%+v %v", p, err)
	}
}

func TestGetErrors(t *testing.T) {
	for name, c := range map[string]*Client{
		"not found":  serve(t, 404, `{}`),
		"bad json":   serve(t, 200, `{`),
		"bad amount": serve(t, 200, `{"amount":"1.5"}`),
		"negative":   serve(t, 200, `{"amount":"-1"}`),
	} {
		if _, err := c.Get(context.Background(), "abc"); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := NewClient("http://127.0.0.1:1", http.DefaultClient).Get(context.Background(), "abc"); err == nil {
		t.Error("unreachable: no error")
	}
	if _, err := NewClient("http://x\x7f", http.DefaultClient).Get(context.Background(), "abc"); err == nil {
		t.Error("bad url: no error")
	}
}
