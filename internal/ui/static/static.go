package static

import (
	"embed"
	"net/http"
)

// FS is the embedded static files.
//
//go:embed css/*.css js/*.js
var FS embed.FS

// Handler returns an http.Handler that serves the static files.
func Handler() http.Handler {
	return http.FileServer(http.FS(FS))
}
