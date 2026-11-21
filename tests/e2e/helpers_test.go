//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/m5tshift/slopay/pkg/hdwallet"
	"github.com/m5tshift/slopay/pkg/kafka"
)

// waitTimeout bounds every wait for an asynchronous result of the slice.
const waitTimeout = 90 * time.Second

// rootXpub is the public root of BIP-32 test vector 1; every merchant gets its own non-hardened child.
const rootXpub = "xpub661MyMwAqRbcFtXgS5sYJABqqG9YLmC4Q1Rdap9gSE8NqtwybGhePY2gZ29ESFjqJoCu1Rupje8YtGqsefD265TMg7usUDFdp6W1EGMcet8"

var httpClient = &http.Client{Timeout: 10 * time.Second}

// call sends a JSON request and decodes a JSON response into out; it returns the status code.
func call(ctx context.Context, method, url, token string, headers map[string]string, body, out any) (int, []byte, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, r)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, raw, fmt.Errorf("decode %s %s: %w", method, url, err)
		}
	}
	return resp.StatusCode, raw, nil
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// merchant is a registered merchant with an access token and its own xpub.
type merchant struct {
	id           string
	token        string
	xpub         string
	firstAddress string
}

// registerMerchant registers a merchant, exchanges its API key for a JWT and uploads a fresh xpub.
func registerMerchant(ctx context.Context, t *testing.T, name string) merchant {
	t.Helper()
	var reg struct {
		MerchantID string `json:"merchantId"`
		APIKey     struct {
			Key string `json:"key"`
		} `json:"apiKey"`
	}
	code, raw, err := call(ctx, http.MethodPost, env.merchantURL+"/api/v1/merchants", "", nil,
		map[string]string{"name": name, "email": "e2e@merchant.example.com"}, &reg)
	if err != nil || code != http.StatusCreated {
		t.Fatalf("register merchant: %d %v %s", code, err, raw)
	}
	var tok struct {
		AccessToken string `json:"accessToken"`
	}
	code, raw, err = call(ctx, http.MethodPost, env.merchantURL+"/api/v1/auth/token", "", nil,
		map[string]string{"apiKey": reg.APIKey.Key}, &tok)
	if err != nil || code != http.StatusOK || tok.AccessToken == "" {
		t.Fatalf("issue token: %d %v %s", code, err, raw)
	}
	xpub := childXpub(t)
	var xr struct {
		Xpub         string `json:"xpub"`
		FirstAddress string `json:"firstAddress"`
	}
	code, raw, err = call(ctx, http.MethodPut, env.merchantURL+"/api/v1/merchants/me/xpub", tok.AccessToken, nil,
		map[string]string{"xpub": xpub}, &xr)
	if err != nil || code != http.StatusOK {
		t.Fatalf("set xpub: %d %v %s", code, err, raw)
	}
	if xr.Xpub != xpub || !common.IsHexAddress(xr.FirstAddress) {
		t.Fatalf("set xpub: unexpected response %s", raw)
	}
	return merchant{id: reg.MerchantID, token: tok.AccessToken, xpub: xpub, firstAddress: xr.FirstAddress}
}

// childXpub returns a random non-hardened child of the test vector root: a distinct xpub per merchant.
func childXpub(t *testing.T) string {
	t.Helper()
	root, err := hdwallet.Parse(rootXpub)
	if err != nil {
		t.Fatal(err)
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	child, err := root.Child(binary.BigEndian.Uint32(b[:]) &^ (1 << 31))
	if err != nil {
		t.Fatal(err)
	}
	return child.String()
}

// invoice is the Invoice of contracts/openapi/invoice-service.yaml.
type invoice struct {
	InvoiceID      string `json:"invoiceId"`
	MerchantID     string `json:"merchantId"`
	Status         string `json:"status"`
	Address        string `json:"address"`
	Amount         string `json:"amount"`
	AmountUSDCents int64  `json:"amountUsdCents"`
	Rate           string `json:"rate"`
	Confirmations  int    `json:"confirmations"`
	ReceivedAmount string `json:"receivedAmount"`
	Overpaid       bool   `json:"overpaid"`
	Sequence       int    `json:"sequence"`
	OrderID        string `json:"orderId"`
	Asset          struct {
		Kind     string `json:"kind"`
		Decimals int    `json:"decimals"`
	} `json:"asset"`
}

// createInvoice creates an invoice; it retries while the merchant projection or the rate are not there yet
// (409 xpub_not_set, 503). It returns the invoice and the status code of the successful response.
func createInvoice(ctx context.Context, t *testing.T, m merchant, key string, cents int64) (invoice, int) {
	t.Helper()
	body := map[string]any{"amountUsdCents": cents, "orderId": "order-" + key}
	headers := map[string]string{"Idempotency-Key": key}
	deadline := time.Now().Add(waitTimeout)
	for {
		var inv invoice
		code, raw, err := call(ctx, http.MethodPost, env.invoiceURL+"/api/v1/invoices", m.token, headers, body, &inv)
		switch {
		case err == nil && (code == http.StatusCreated || code == http.StatusOK):
			return inv, code
		case time.Now().After(deadline):
			t.Fatalf("create invoice: %d %v %s", code, err, raw)
		case err == nil && code != http.StatusConflict && code != http.StatusServiceUnavailable:
			t.Fatalf("create invoice: unexpected %d %s", code, raw)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func getInvoice(ctx context.Context, t *testing.T, m merchant, id string) invoice {
	t.Helper()
	var inv invoice
	code, raw, err := call(ctx, http.MethodGet, env.invoiceURL+"/api/v1/invoices/"+id, m.token, nil, nil, &inv)
	if err != nil || code != http.StatusOK {
		t.Fatalf("get invoice: %d %v %s", code, err, raw)
	}
	return inv
}

// waitStatus polls the invoice until it reaches status; with mine set it mines a block per poll so that
// confirmations grow.
func waitStatus(ctx context.Context, t *testing.T, m merchant, id, status string, mine bool) invoice {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var inv invoice
	for time.Now().Before(deadline) {
		inv = getInvoice(ctx, t, m, id)
		if inv.Status == status {
			return inv
		}
		if mine {
			mineBlocks(ctx, t, 1)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("invoice %s: status %s, want %s (confirmations %d, received %s)", id, inv.Status, status,
		inv.Confirmations, inv.ReceivedAmount)
	return inv
}

// waitConfirmations polls the invoice without mining until it has at least n confirmations.
func waitConfirmations(ctx context.Context, t *testing.T, m merchant, id string, n int) invoice {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var inv invoice
	for time.Now().Before(deadline) {
		if inv = getInvoice(ctx, t, m, id); inv.Confirmations >= n {
			return inv
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("invoice %s: %d confirmations, want %d (status %s)", id, inv.Confirmations, n, inv.Status)
	return inv
}

// waitTracked waits until chain-watcher tracks the address and has searched the earlier blocks for it, mining
// empty blocks so that the rescan runs. After that a payment to the address is detected in its own block.
func waitTracked(ctx context.Context, t *testing.T, address string) {
	t.Helper()
	coll := env.mongo.Database().Collection("tracked_addresses")
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		var doc struct {
			Rescanned bool `bson:"rescanned"`
		}
		err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: strings.ToLower(address)}}).Decode(&doc)
		if err == nil && doc.Rescanned {
			return
		}
		mineBlocks(ctx, t, 1)
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("chain-watcher does not track %s", address)
}

// payment is the response of POST /payments of payer-sim.
type payment struct {
	InvoiceID string `json:"invoiceId"`
	Outcome   string `json:"outcome"`
	TxHash    string `json:"txHash"`
	Amount    string `json:"amount"`
}

// pay asks payer-sim to pay the invoice and waits for the receipt of the transaction.
func pay(ctx context.Context, t *testing.T, payer, invoiceID string) (payment, *types.Receipt) {
	t.Helper()
	var p payment
	code, raw, err := call(ctx, http.MethodPost, env.payerURLs[payer]+"/payments", "", nil,
		map[string]string{"invoiceId": invoiceID}, &p)
	if err != nil || code != http.StatusOK || p.TxHash == "" {
		t.Fatalf("payer-sim %s: %d %v %s", payer, code, err, raw)
	}
	return p, waitReceipt(ctx, t, common.HexToHash(p.TxHash))
}

// replay asks payer-sim to send the signed transactions of the invoice again, as after a reorg.
func replay(ctx context.Context, t *testing.T, payer, invoiceID string) int {
	t.Helper()
	var r struct {
		Sent  int `json:"sent"`
		Known int `json:"known"`
	}
	code, raw, err := call(ctx, http.MethodPost, env.payerURLs[payer]+"/control/replay", "", nil,
		map[string]string{"invoiceId": invoiceID}, &r)
	if err != nil || code != http.StatusOK {
		t.Fatalf("payer-sim replay: %d %v %s", code, err, raw)
	}
	return r.Sent
}

func ethClient(ctx context.Context, t *testing.T) *ethclient.Client {
	t.Helper()
	c, err := ethclient.DialContext(ctx, env.anvilURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func rpcCall(ctx context.Context, t *testing.T, result any, method string, args ...any) {
	t.Helper()
	c, err := rpc.DialContext(ctx, env.anvilURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CallContext(ctx, result, method, args...); err != nil {
		t.Fatalf("%s: %v", method, err)
	}
}

func mineBlocks(ctx context.Context, t *testing.T, n uint64) {
	t.Helper()
	rpcCall(ctx, t, nil, "anvil_mine", hexutil.EncodeUint64(n))
}

func headNumber(ctx context.Context, t *testing.T) uint64 {
	t.Helper()
	n, err := ethClient(ctx, t).BlockNumber(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// waitReceipt polls for the receipt: automine includes the transaction asynchronously.
func waitReceipt(ctx context.Context, t *testing.T, hash common.Hash) *types.Receipt {
	t.Helper()
	c := ethClient(ctx, t)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		r, err := c.TransactionReceipt(ctx, hash)
		if err == nil {
			return r
		}
		if !errors.Is(err, ethereum.NotFound) {
			t.Fatalf("receipt %s: %v", hash, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no receipt for %s", hash)
	return nil
}

func bigInt(t *testing.T, s string) *big.Int {
	t.Helper()
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("not an integer: %q", s)
	}
	return v
}

// collector reads invoice.events from the start, for the order of the events of an invoice.
type collector struct {
	client *kgo.Client
	codec  *kafka.Codec
	cancel context.CancelFunc
	done   chan struct{}

	mu     sync.Mutex
	events map[string][]kafka.Event
	errs   []error
}

func newCollector(brokers []string) (*collector, error) {
	codec, err := kafka.NewCodec()
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("invoice.events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{client: cl, codec: codec, cancel: cancel, done: make(chan struct{}), events: map[string][]kafka.Event{}}
	go c.run(ctx)
	return c, nil
}

func (c *collector) run(ctx context.Context) {
	defer close(c.done)
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			ev, err := c.codec.Decode(r.Value)
			c.mu.Lock()
			defer c.mu.Unlock()
			if err != nil {
				c.errs = append(c.errs, err)
				return
			}
			c.events[ev.Subject] = append(c.events[ev.Subject], ev)
		})
	}
}

func (c *collector) close() {
	c.cancel()
	<-c.done
	c.client.Close()
}

// of returns the events of an invoice in the order of its partition.
func (c *collector) of(id string) []kafka.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]kafka.Event(nil), c.events[id]...)
}

// waitEvents waits until the events of the invoice contain want as a subsequence of types; it fails on an event
// that does not match its schema.
func waitEvents(t *testing.T, id string, want ...string) []kafka.Event {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	var got []kafka.Event
	for time.Now().Before(deadline) {
		env.events.mu.Lock()
		errs := errors.Join(env.events.errs...)
		env.events.mu.Unlock()
		if errs != nil {
			t.Fatalf("invoice.events: %v", errs)
		}
		got = env.events.of(id)
		if subsequence(eventTypes(got), want) {
			return got
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("invoice %s: events %v, want in order %v", id, eventTypes(got), want)
	return nil
}

func eventTypes(evs []kafka.Event) []string {
	out := make([]string, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

func subsequence(got, want []string) bool {
	i := 0
	for _, g := range got {
		if i < len(want) && g == want[i] {
			i++
		}
	}
	return i == len(want)
}

// eventData decodes the data of the last event of type typ.
func eventData(t *testing.T, evs []kafka.Event, typ string, out any) {
	t.Helper()
	for i := len(evs) - 1; i >= 0; i-- {
		if evs[i].Type == typ {
			if err := json.Unmarshal(evs[i].Data, out); err != nil {
				t.Fatalf("%s data: %v", typ, err)
			}
			return
		}
	}
	t.Fatalf("no %s event", typ)
}
