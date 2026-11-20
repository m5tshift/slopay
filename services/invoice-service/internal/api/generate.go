// Package api is the HTTP layer of invoice-service: the server generated from
// contracts/openapi/invoice-service.yaml, request validation, merchant identification and the handlers.
package api

//go:generate go tool oapi-codegen -config oapi-codegen.yaml ../../../../contracts/openapi/invoice-service.yaml
