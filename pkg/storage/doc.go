// Package storage opens PostgreSQL (pgx), MongoDB (mongo-go-driver v2) and Valkey (valkey-go)
// connections from configuration and mounted secret files (SRS 4.1.6, 4.1.7, INF-2.4).
//
// Passwords are never taken from the environment: each store reads its password from a file
// mounted by External Secrets. The file is read again for every new connection, so a password
// rotated by OpenBao reaches the service without a restart. Connections that are already open
// keep working. When authentication fails (the file is not yet synced), the attempt is repeated
// with exponential backoff for up to two minutes (Backoff).
//
// Readiness checks have the signature of telemetry.CheckFunc:
//
//	h.AddReadiness("postgres", pg.Ready)
//	h.AddReadiness("mongodb", mg.Ready)
//	h.AddReadiness("valkey", vk.Ready)
//
// Migrations of a service live in services/<service>/migrations and run with Migrate.
package storage
