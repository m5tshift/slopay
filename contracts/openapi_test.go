package contracts

import (
	"context"
	"path/filepath"
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
