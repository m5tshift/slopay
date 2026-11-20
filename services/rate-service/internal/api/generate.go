// Package api is the HTTP layer of rate-service: the server generated from contracts/openapi/rate-service.yaml
// and the handlers of the latest rate and of the fault injection flag.
package api

//go:generate go tool oapi-codegen -config oapi-codegen.yaml ../../../../contracts/openapi/rate-service.yaml
