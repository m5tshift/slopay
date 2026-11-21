// Package ledger is the client of the address statement API of ledger-service (contracts/openapi/ledger-service.yaml):
// GET /internal/addresses/statement and GET /internal/addresses/{address}/postings. Resilience is the mesh's
// (ADR 0003: Istio outlier detection and timeout); the client bounds a call with its own timeout, and any failure
// postpones the reconciliation run (SRS 4.1.9).
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/m5tshift/slopay/services/reconciliation-service/internal/recon"
)

// maxBody bounds an answer read from ledger-service: a page of 1000 postings is well below it.
const maxBody = 8 << 20

// PageLimit is the page size of the postings of an address (the contract maximum).
const PageLimit = 1000

// maxPages bounds the pages of one address, so a cursor that never ends does not loop.
const maxPages = 1000

// Client calls ledger-service.
type Client struct {
	base *url.URL
	http *http.Client
}

var _ recon.Ledger = (*Client)(nil)

// New returns a client of the ledger-service at baseURL. timeout bounds one call.
func New(baseURL string, timeout time.Duration) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("ledger: invalid ledger-service URL %q", baseURL)
	}
	if timeout <= 0 {
		return nil, errors.New("ledger: timeout must be positive")
	}
	return &Client{base: u, http: &http.Client{Timeout: timeout, Transport: tracing{next: http.DefaultTransport}}}, nil
}

type assetBody struct {
	Kind     string `json:"kind"`
	Contract string `json:"contract"`
	Decimals int    `json:"decimals"`
}

func (a assetBody) asset() recon.Asset {
	return recon.Asset{Kind: a.Kind, Contract: strings.ToLower(a.Contract), Decimals: a.Decimals}
}

type statementBody struct {
	Items []struct {
		Address string `json:"address"`
		Totals  []struct {
			Asset    assetBody `json:"asset"`
			Received string    `json:"received"`
			Postings int       `json:"postings"`
		} `json:"totals"`
	} `json:"items"`
}

type pageBody struct {
	Items []struct {
		Transfer struct {
			BlockNumber uint64    `json:"blockNumber"`
			Amount      string    `json:"amount"`
			Asset       assetBody `json:"asset"`
		} `json:"transfer"`
	} `json:"items"`
	NextCursor *string `json:"nextCursor"`
}

// AddressTotals returns the totals of the addresses (at most recon.LedgerAddressBatch) by address and asset key.
func (c *Client) AddressTotals(ctx context.Context, addresses []string) (map[string]map[string]recon.LedgerTotals, error) {
	if len(addresses) == 0 {
		return map[string]map[string]recon.LedgerTotals{}, nil
	}
	if len(addresses) > recon.LedgerAddressBatch {
		return nil, fmt.Errorf("ledger: %d addresses, at most %d", len(addresses), recon.LedgerAddressBatch)
	}
	q := url.Values{}
	for _, a := range addresses {
		q.Add("address", strings.ToLower(a))
	}
	var body statementBody
	if err := c.get(ctx, "GET /internal/addresses/statement", c.base.JoinPath("internal", "addresses", "statement"), q, &body); err != nil {
		return nil, err
	}
	out := make(map[string]map[string]recon.LedgerTotals, len(body.Items))
	for _, it := range body.Items {
		byAsset := map[string]recon.LedgerTotals{}
		for _, t := range it.Totals {
			received, ok := new(big.Int).SetString(t.Received, 10)
			if !ok || received.Sign() < 0 || t.Postings < 0 {
				return nil, fmt.Errorf("ledger: statement of %s: invalid totals", it.Address)
			}
			byAsset[t.Asset.asset().Key()] = recon.LedgerTotals{Received: received, Postings: t.Postings}
		}
		out[strings.ToLower(it.Address)] = byAsset
	}
	return out, nil
}

// AddressPostings returns every posting of the address, reading all pages.
func (c *Client) AddressPostings(ctx context.Context, address string) ([]recon.Posting, error) {
	address = strings.ToLower(address)
	u := c.base.JoinPath("internal", "addresses", address, "postings")
	var out []recon.Posting
	cursor := ""
	for range maxPages {
		q := url.Values{"limit": {strconv.Itoa(PageLimit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page pageBody
		if err := c.get(ctx, "GET /internal/addresses/{address}/postings", u, q, &page); err != nil {
			return nil, err
		}
		for _, it := range page.Items {
			amount, ok := new(big.Int).SetString(it.Transfer.Amount, 10)
			if !ok || amount.Sign() < 0 {
				return nil, fmt.Errorf("ledger: postings of %s: invalid amount", address)
			}
			out = append(out, recon.Posting{Asset: it.Transfer.Asset.asset(), BlockNumber: it.Transfer.BlockNumber, Amount: amount})
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			return out, nil
		}
		cursor = *page.NextCursor
	}
	return nil, fmt.Errorf("ledger: postings of %s: more than %d pages", address, maxPages)
}

func (c *Client) get(ctx context.Context, route string, u *url.URL, q url.Values, out any) error {
	u = &url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawQuery: q.Encode()}
	req, err := http.NewRequestWithContext(withRoute(ctx, route), http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("ledger: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("ledger: %s: %w", route, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return fmt.Errorf("ledger: %s: %w", route, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ledger: %s answered %d", route, resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("ledger: %s: decode: %w", route, err)
	}
	return nil
}

type routeKey struct{}

func withRoute(ctx context.Context, route string) context.Context {
	return context.WithValue(ctx, routeKey{}, route)
}

// tracing starts a client span for a call and passes its context in the W3C headers (SRS 4.5.1).
type tracing struct{ next http.RoundTripper }

func (t tracing) RoundTrip(req *http.Request) (*http.Response, error) {
	name, _ := req.Context().Value(routeKey{}).(string)
	if name == "" {
		name = req.Method
	}
	ctx, span := otel.Tracer("github.com/m5tshift/slopay/services/reconciliation-service/internal/ledger").Start(
		req.Context(), name, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("http.request.method", req.Method), attribute.String("server.address", req.URL.Host)))
	defer span.End()
	req = req.Clone(ctx)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := t.next.RoundTrip(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
	if resp.StatusCode >= 500 {
		span.SetStatus(codes.Error, http.StatusText(resp.StatusCode))
	}
	return resp, nil
}
