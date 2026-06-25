package admin

import "embed"

// FS contains the admin dashboard static files.
//
//go:embed index.html
var FS embed.FS
