// Package migrations holds the goose migrations of invoice-service.
package migrations

import "embed"

// FS contains the migrations, applied at startup.
//
//go:embed *.sql
var FS embed.FS
