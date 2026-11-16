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
