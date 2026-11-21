// Package api is the HTTP layer of ledger-service: the server generated from
// contracts/openapi/ledger-service.yaml, request validation and the statement handlers.
package api

import (
	_ "embed"

	"github.com/getkin/kin-openapi/openapi3"
)

// The contract is copied next to the code so that it can be embedded; make generate-check keeps the copy
// identical to contracts/openapi/ledger-service.yaml.
//go:generate go tool oapi-codegen -config oapi-codegen.yaml ../../../../contracts/openapi/ledger-service.yaml
//go:generate cp ../../../../contracts/openapi/ledger-service.yaml openapi.yaml

//go:embed openapi.yaml
var spec []byte

// GetSpec returns the parsed contract the requests are validated against.
func GetSpec() (*openapi3.T, error) {
	return openapi3.NewLoader().LoadFromData(spec)
}
