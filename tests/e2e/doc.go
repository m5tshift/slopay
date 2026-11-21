// Package e2e is the end-to-end test of the vertical slice up to PAID (SRS 7.4, 7.8, UC-01, UC-02, UC-04;
// FR-01, FR-02, FR-04, FR-05).
//
// The test is behind the build tag e2e and needs Docker:
//
//	go test -tags e2e ./tests/e2e/...
//
// It starts PostgreSQL, Kafka, MongoDB, Valkey and Anvil in Testcontainers, builds merchant-service,
// invoice-service, chain-watcher, rate-service, rate-mock and payer-sim and runs them as local processes
// configured through their environment variables, as in their charts. The payer accounts and every password
// are generated for the run; nothing secret is kept in the repository.
package e2e
