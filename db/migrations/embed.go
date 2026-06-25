package migrations

import "embed"

// FS contains versioned SQL migration files for golang-migrate.
//
//go:embed *.sql
var FS embed.FS
