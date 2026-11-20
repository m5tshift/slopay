// Package api is the HTTP layer of merchant-service: the server generated from
// contracts/openapi/merchant-service.yaml, request validation, JWT authentication and the handlers.
package api

//go:generate go tool oapi-codegen -config oapi-codegen.yaml ../../../../contracts/openapi/merchant-service.yaml
