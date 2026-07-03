package static

import "embed"

// FS serves admin UI assets under /admin/static/.
//
//go:embed css/*
//go:embed js/*
//go:embed vendor/*
var FS embed.FS
