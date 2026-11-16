package contracts

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

// TestOpenAPIDocuments lints the OpenAPI documents: OpenAPI 3.0.3, valid against the specification,
// every $ref resolves.
func TestOpenAPIDocuments(t *testing.T) {
	files, err := filepath.Glob("openapi/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("no openapi/*.yaml: %v", err)
	}
	for _, file := range files {
		t.Run(service(file), func(t *testing.T) {
			loader := openapi3.NewLoader()
			doc, err := loader.LoadFromFile(file)
			if err != nil {
				t.Fatal(err)
			}
			if doc.OpenAPI != "3.0.3" {
				t.Errorf("openapi = %q, want 3.0.3", doc.OpenAPI)
			}
			if err := doc.Validate(context.Background()); err != nil {
				t.Errorf("not a valid OpenAPI document: %v", err)
			}
		})
	}
}

// TestMerchantServiceAPI checks the merchant-service contract against SRS 4.1.10 and UC-06, UC-07.
func TestMerchantServiceAPI(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("openapi/merchant-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct{ method, path string }{
		{"POST", "/api/v1/merchants"},
		{"POST", "/api/v1/auth/token"},
		{"PUT", "/api/v1/merchants/me/xpub"},
		{"PUT", "/api/v1/merchants/me/webhook"},
		{"POST", "/api/v1/merchants/me/api-keys"},
		{"DELETE", "/api/v1/merchants/me/api-keys/{keyId}"},
		{"POST", "/api/v1/merchants/me/webhook-secret/rotate"},
		{"GET", "/.well-known/jwks.json"},
	} {
		item := doc.Paths.Find(p.path)
		if item == nil || item.GetOperation(p.method) == nil {
			t.Errorf("missing %s %s", p.method, p.path)
		}
	}

	// Only token, registration and JWKS are open; everything else needs a JWT.
	open := map[string]bool{"/api/v1/merchants": true, "/api/v1/auth/token": true, "/.well-known/jwks.json": true}
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			isOpen := op.Security != nil && len(*op.Security) == 0
			if isOpen != open[path] {
				t.Errorf("%s %s: open = %v, want %v", method, path, isOpen, open[path])
			}
			if strings.HasPrefix(path, "/api/v1/") && op.Responses.Value("429") == nil {
				t.Errorf("%s %s: no 429 response", method, path)
			}
		}
	}

	tok := doc.Paths.Find("/api/v1/auth/token").Post
	if tok.Responses.Value("401") == nil {
		t.Error("token: no 401")
	}
	ra := doc.Components.Responses["TooManyRequests"].Value.Headers["Retry-After"]
	if ra == nil || !ra.Value.Required {
		t.Error("429 must carry a required Retry-After header")
	}
	tol := doc.Components.Schemas["ToleranceBps"].Value
	if tol.Min == nil || *tol.Min != 0 || tol.Max == nil || *tol.Max != 500 {
		t.Error("tolerance must be 0..500 bps")
	}
}

// TestInvoiceServiceAPI checks the invoice-service contract against SRS 4.1.4, 4.1.8, UC-01
// (FR-01, FR-02, FR-03, FR-08).
func TestInvoiceServiceAPI(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("openapi/invoice-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []struct{ method, path string }{
		{"POST", "/api/v1/invoices"},
		{"GET", "/api/v1/invoices"},
		{"GET", "/api/v1/invoices/{id}"},
		{"GET", "/pay/{id}"},
		{"GET", "/pay/{id}/status"},
	} {
		item := doc.Paths.Find(p.path)
		if item == nil || item.GetOperation(p.method) == nil {
			t.Errorf("missing %s %s", p.method, p.path)
		}
	}

	// /pay/* is public; /api/v1/* needs a JWT. Every operation answers 429 with a required Retry-After.
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			isOpen := op.Security != nil && len(*op.Security) == 0
			if isOpen != strings.HasPrefix(path, "/pay/") {
				t.Errorf("%s %s: open = %v", method, path, isOpen)
			}
			if op.Responses.Value("429") == nil {
				t.Errorf("%s %s: no 429 response", method, path)
			}
		}
	}
	ra := doc.Components.Responses["TooManyRequests"].Value.Headers["Retry-After"]
	if ra == nil || !ra.Value.Required {
		t.Error("429 must carry a required Retry-After header")
	}

	create := doc.Paths.Find("/api/v1/invoices").Post
	key := false
	for _, p := range create.Parameters {
		if p.Value.In == "header" && p.Value.Name == "Idempotency-Key" && p.Value.Required {
			key = true
		}
	}
	if !key {
		t.Error("create: Idempotency-Key header is not required")
	}
	for _, code := range []string{"201", "200", "400", "401", "422", "503"} {
		if create.Responses.Value(code) == nil {
			t.Errorf("create: no %s response", code)
		}
	}
	if _, ok := doc.Components.Responses["RateUnavailable"].Value.Headers["Retry-After"]; !ok {
		t.Error("503 must carry Retry-After")
	}

	req := doc.Components.Schemas["CreateInvoiceRequest"].Value
	for _, f := range []string{"amountUsdCents", "orderId"} {
		if !slices.Contains(req.Required, f) {
			t.Errorf("create request: %s is not required", f)
		}
	}
	if _, ok := req.Properties["webhookUrl"]; ok {
		t.Error("webhook URL must not be in the create request (SRS 4.1.10)")
	}
	if cents := req.Properties["amountUsdCents"].Value; cents.Type == nil || !cents.Type.Is("integer") {
		t.Error("amountUsdCents must be an integer")
	}

	// List: filters and pagination.
	params := map[string]bool{}
	for _, p := range doc.Paths.Find("/api/v1/invoices").Get.Parameters {
		params[p.Value.Name] = true
	}
	for _, n := range []string{"status", "orderId", "createdFrom", "createdTo", "limit", "cursor"} {
		if !params[n] {
			t.Errorf("list: no %q parameter", n)
		}
	}
	if _, ok := doc.Components.Schemas["InvoicePage"].Value.Properties["nextCursor"]; !ok {
		t.Error("list page has no nextCursor")
	}

	// Public page: address, amount, asset, expiresAt, status.
	page := doc.Components.Schemas["PaymentPage"].Value
	for _, f := range []string{"address", "amount", "asset", "expiresAt", "status"} {
		if !slices.Contains(page.Required, f) {
			t.Errorf("payment page: %s is not required", f)
		}
	}
	if n := len(doc.Components.Schemas["InvoiceStatus"].Value.Enum); n != 7 {
		t.Errorf("status enum has %d values, want 7 (SRS 4.1.4)", n)
	}
}

func hasOp(t *testing.T, doc *openapi3.T, method, path string) *openapi3.Operation {
	t.Helper()
	item := doc.Paths.Find(path)
	if item == nil || item.GetOperation(method) == nil {
		t.Errorf("missing %s %s", method, path)
		return nil
	}
	return item.GetOperation(method)
}

// TestInternalAPIsAreNotPublic checks that the internal APIs carry no gateway authentication: they are
// protected by mesh mTLS and not published (SRS 4.1.2, FR-09).
func TestInternalAPIsAreNotPublic(t *testing.T) {
	for _, name := range []string{"rate-service", "ledger-service", "webhook-service"} {
		doc, err := openapi3.NewLoader().LoadFromFile("openapi/" + name + ".yaml")
		if err != nil {
			t.Fatal(err)
		}
		if doc.Components != nil && len(doc.Components.SecuritySchemes) != 0 {
			t.Errorf("%s: internal API must not define security schemes", name)
		}
		if len(doc.Security) != 0 {
			t.Errorf("%s: internal API must not require security", name)
		}
	}
}

// TestRateServiceAPI checks the rate-service contract against SRS 4.1.2, 4.1.9 (FR-12).
func TestRateServiceAPI(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("openapi/rate-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	get := hasOp(t, doc, "GET", "/rates/{pair}")
	if get == nil {
		return
	}
	pair := doc.Components.Schemas["Pair"].Value
	var got []string
	for _, v := range pair.Enum {
		got = append(got, v.(string))
	}
	if !slices.Equal(got, []string{"ETH-USD", "USDT-USD"}) {
		t.Errorf("pair enum = %v", got)
	}
	for _, code := range []string{"200", "404", "503"} {
		if get.Responses.Value(code) == nil {
			t.Errorf("no %s response", code)
		}
	}
	if ra := get.Responses.Value("503").Value.Headers["Retry-After"]; ra == nil || !ra.Value.Required {
		t.Error("503 must carry a required Retry-After header")
	}
	rate := doc.Components.Schemas["Rate"].Value
	for _, f := range []string{"price", "quotedAt", "ageSeconds"} {
		if !slices.Contains(rate.Required, f) {
			t.Errorf("rate: %s is not required", f)
		}
	}
	if typ := rate.Properties["price"].Value.Type; typ == nil || !typ.Is("string") {
		t.Error("price must be a decimal string, not a number")
	}
	if age := rate.Properties["ageSeconds"].Value; age.Max == nil || *age.Max != 60 {
		t.Error("a served rate is at most 60 s old")
	}
}

// TestLedgerServiceAPI checks the ledger-service contract against SRS 4.1.2, 4.1.3 (FR-12).
func TestLedgerServiceAPI(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("openapi/ledger-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	merchant := hasOp(t, doc, "GET", "/internal/merchants/{merchantId}/statement")
	addrs := hasOp(t, doc, "GET", "/internal/addresses/statement")
	hasOp(t, doc, "GET", "/internal/addresses/{address}/postings")
	if merchant == nil || addrs == nil {
		return
	}
	req := map[string]bool{}
	for _, p := range merchant.Parameters {
		req[p.Value.Name] = p.Value.Required
	}
	if !req["from"] || !req["to"] {
		t.Error("merchant statement: from and to must be required")
	}
	if len(addrs.Parameters) != 1 || addrs.Parameters[0].Value.Name != "address" || !addrs.Parameters[0].Value.Required {
		t.Error("addresses statement: required address parameter expected")
	}
	for _, name := range []string{"Amount", "Posting", "MerchantStatement", "AddressesStatement"} {
		if doc.Components.Schemas[name] == nil {
			t.Fatalf("no schema %s", name)
		}
	}
	if typ := doc.Components.Schemas["Amount"].Value.Type; typ == nil || !typ.Is("string") {
		t.Error("amounts must be decimal strings")
	}
	for _, f := range []string{"merchantId", "invoiceId", "address", "transfer", "fee", "postedAt"} {
		if !slices.Contains(doc.Components.Schemas["Posting"].Value.Required, f) {
			t.Errorf("posting: %s is not required", f)
		}
	}
	if _, ok := doc.Components.Schemas["MerchantStatement"].Value.Properties["nextCursor"]; !ok {
		t.Error("merchant statement has no nextCursor")
	}
}

// TestWebhookServiceAPI checks the DLQ replay endpoint against SRS 4.1.3, FR-09.
func TestWebhookServiceAPI(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("openapi/webhook-service.yaml")
	if err != nil {
		t.Fatal(err)
	}
	replay := hasOp(t, doc, "POST", "/internal/dlq/replay")
	if replay == nil {
		return
	}
	if len(doc.Paths.Map()) != 1 {
		t.Error("webhook-service exposes only /internal/dlq/replay")
	}
	for _, code := range []string{"200", "400"} {
		if replay.Responses.Value(code) == nil {
			t.Errorf("no %s response", code)
		}
	}
	props := doc.Components.Schemas["ReplayRequest"].Value.Properties
	for _, f := range []string{"invoiceId", "merchantId", "limit"} {
		if _, ok := props[f]; !ok {
			t.Errorf("replay request: no %s", f)
		}
	}
}

// TestMerchantWebhook checks the webhook contract against SRS 4.1.10 (FR-07): the body carries sequence,
// the headers carry the timestamp, the HMAC-SHA256 signature and the secret version, and the string to sign
// is spelled out.
func TestMerchantWebhook(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile("openapi/webhook.yaml")
	if err != nil {
		t.Fatal(err)
	}
	post := hasOp(t, doc, "POST", "/")
	if post == nil {
		return
	}
	hdr := map[string]*openapi3.Parameter{}
	for _, p := range post.Parameters {
		if p.Value.In == "header" {
			hdr[p.Value.Name] = p.Value
		}
	}
	for _, n := range []string{"X-SLOPay-Timestamp", "X-SLOPay-Signature", "X-SLOPay-Secret-Version"} {
		if hdr[n] == nil || !hdr[n].Required {
			t.Errorf("header %s is not required", n)
		}
	}
	if h := hdr["X-SLOPay-Signature"]; h != nil {
		// v1= plus the 64 hex characters of HMAC-SHA256.
		if !strings.Contains(h.Schema.Value.Pattern, "{64}") {
			t.Errorf("signature pattern %q must fix 64 hex characters", h.Schema.Value.Pattern)
		}
	}
	for _, want := range []string{"HMAC-SHA256", "<timestamp>", "raw", "Secret-Version", "`sequence`"} {
		if !strings.Contains(doc.Info.Description+post.Description+hdr["X-SLOPay-Signature"].Description, want) {
			t.Errorf("description does not mention %q", want)
		}
	}
	if !strings.Contains(doc.Info.Description, "`timestamp` + `.`") {
		t.Error("the string to sign must be spelled out: timestamp + '.' + raw body")
	}

	body := doc.Components.Schemas["InvoiceWebhook"].Value
	data := body.Properties["data"].Value
	for _, f := range []string{"invoiceId", "sequence", "status"} {
		if !slices.Contains(data.Required, f) {
			t.Errorf("webhook data: %s is not required", f)
		}
	}
	if len(body.Properties["type"].Value.Enum) != 9 {
		t.Errorf("want 9 invoice event types (SRS 4.1.5), got %d", len(body.Properties["type"].Value.Enum))
	}
	if post.Responses.Value("2XX") == nil {
		t.Error("no 2XX response")
	}
}
