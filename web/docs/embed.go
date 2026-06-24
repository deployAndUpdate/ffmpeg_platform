package docs

import "embed"

// FS contains the project documentation static files.
//
//go:embed index.html
var FS embed.FS
