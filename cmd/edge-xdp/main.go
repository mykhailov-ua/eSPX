// Command edge-xdp attaches the ingress XDP program and pins BPF maps consumed by edge-bpf-sync.
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"

	"espx/internal/edge"
	"espx/internal/edge/bpf"
	"espx/pkg/lifecycle"

	"github.com/cilium/ebpf/rlimit"
)

func main() {
	iface := flag.String("iface", os.Getenv("INGRESS_INTERFACE"), "network interface for XDP attach")
	pinDir := flag.String("pin-dir", edge.EnvOr("BPF_PIN_DIR", "/sys/fs/bpf/espx"), "directory for pinned BPF maps")
	mode := flag.String("mode", edge.EnvOr("XDP_MODE", "generic"), "XDP attach mode: generic|native|offload")
	flag.Parse()

	if *iface == "" {
		slog.Error("INGRESS_INTERFACE or -iface is required")
		os.Exit(1)
	}

	if err := rlimit.RemoveMemlock(); err != nil {
		slog.Error("rlimit remove memlock", "error", err)
		os.Exit(1)
	}

	if _, err := net.InterfaceByName(*iface); err != nil {
		slog.Error("interface lookup failed", "iface", *iface, "error", err)
		os.Exit(1)
	}

	objs := bpf.EdgeObjects{}
	if err := bpf.LoadEdgeObjects(&objs, nil); err != nil {
		slog.Error("load bpf objects", "error", err)
		os.Exit(1)
	}
	defer objs.Close()

	if err := bpf.InitConfigFromEnv(objs.Config); err != nil {
		slog.Error("init bpf config", "error", err)
		os.Exit(1)
	}
	if err := wireProgArray(&objs); err != nil {
		slog.Error("wire prog array", "error", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(*pinDir, 0o755); err != nil {
		slog.Error("mkdir pin dir", "path", *pinDir, "error", err)
		os.Exit(1)
	}
	if err := pinMaps(&objs, *pinDir); err != nil {
		slog.Error("pin maps", "error", err)
		os.Exit(1)
	}

	xdpLink, err := attachXDP(*iface, objs.XdpEdgeFilter, *mode)
	if err != nil {
		slog.Error("attach xdp", "iface", *iface, "mode", *mode, "error", err)
		os.Exit(1)
	}
	defer xdpLink.Close()

	slog.Info("edge xdp attached", "iface", *iface, "mode", *mode, "pin_dir", *pinDir, "syn_cookie", bpf.SynCookieEnabled())

	sig := lifecycle.WaitSignal()
	slog.Info("received shutdown signal", "signal", sig.String(), "iface", *iface)
}
