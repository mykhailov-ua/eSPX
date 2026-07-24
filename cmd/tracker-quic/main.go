// Command tracker-quic is an HTTP/3 evaluation sidecar (M5-D1).
// Terminates QUIC/H3 and proxies /track to the gnet tracker over loopback HTTP/1.1.
// Not on the production hot path — use edge nginx http3 for prod ingress.
package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/quic-go/quic-go/http3"
)

func main() {
	var (
		listenAddr = flag.String("listen", ":8443", "HTTP/3 listen address")
		trackerURL = flag.String("tracker", "http://127.0.0.1:8181", "upstream gnet tracker base URL")
		certFile   = flag.String("cert", "deploy/nginx/certs/edge-dev.crt", "TLS certificate")
		keyFile    = flag.String("key", "deploy/nginx/certs/edge-dev.key", "TLS private key")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	transport := &http.Transport{
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   5 * time.Second,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/track", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		up, err := http.NewRequest(http.MethodPost, *trackerURL+"/track", bytes.NewReader(body))
		if err != nil {
			http.Error(w, "proxy error", http.StatusInternalServerError)
			return
		}
		up.Header.Set("Content-Type", r.Header.Get("Content-Type"))
		up.Header.Set("X-Forwarded-For", r.Header.Get("X-Forwarded-For"))
		up.Header.Set("X-Real-IP", r.Header.Get("X-Real-IP"))
		up.Header.Set("User-Agent", r.Header.Get("User-Agent"))
		up.Header.Set("X-Original-Method", r.Method)
		up.Header.Set("X-Original-Path", r.URL.RequestURI())
		if h := r.Header.Get("X-TLS-Hash"); h != "" {
			up.Header.Set("X-TLS-Hash", h)
		}

		resp, err := client.Do(up)
		if err != nil {
			http.Error(w, "tracker unreachable", http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vals := range resp.Header {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, io.LimitReader(resp.Body, 1<<20))
	})

	cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		slog.Error("load tls cert", "error", err)
		os.Exit(1)
	}

	srv := &http3.Server{
		Addr:    *listenAddr,
		Handler: mux,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{http3.NextProtoH3},
			MinVersion:   tls.VersionTLS13,
		},
	}

	go func() {
		slog.Info("tracker-quic sidecar listening", "addr", *listenAddr, "upstream", *trackerURL)
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("http3 server failed", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	_ = srv.Close()
	slog.Info("tracker-quic shutdown complete")
}
