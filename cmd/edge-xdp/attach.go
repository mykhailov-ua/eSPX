package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	"espx/internal/edge/bpf"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
)

func pinMaps(objs *bpf.EdgeObjects, pinDir string) error {
	pins := map[string]*ebpf.Map{
		"blocklist_v4":            objs.BlocklistV4,
		"allow_v4":                objs.AllowV4,
		"syn_ratelimit_v4":        objs.SynRatelimitV4,
		"syn_subnet_ratelimit_v4": objs.SynSubnetRatelimitV4,
		"ratelimit_v4":            objs.RatelimitV4,
		"rst_ratelimit_v4":        objs.RstRatelimitV4,
		"global_syn":              objs.GlobalSyn,
		"stats":                   objs.Stats,
		"config":                  objs.Config,
		"violations":              objs.Violations,
		"fingerprints":            objs.Fingerprints,
		"prog_array":              objs.ProgArray,
	}
	for name, m := range pins {
		if m == nil {
			return fmt.Errorf("map %s not loaded", name)
		}
		path := filepath.Join(pinDir, name)
		_ = os.Remove(path)
		if err := m.Pin(path); err != nil {
			return fmt.Errorf("pin %s: %w", name, err)
		}
	}
	return nil
}

func attachXDP(iface string, prog *ebpf.Program, mode string) (link.Link, error) {
	ifaceObj, err := net.InterfaceByName(iface)
	if err != nil {
		return nil, err
	}
	switch mode {
	case "generic":
		return link.AttachXDP(link.XDPOptions{
			Program:   prog,
			Interface: ifaceObj.Index,
			Flags:     link.XDPGenericMode,
		})
	case "native":
		return link.AttachXDP(link.XDPOptions{
			Program:   prog,
			Interface: ifaceObj.Index,
			Flags:     link.XDPDriverMode,
		})
	case "offload":
		return link.AttachXDP(link.XDPOptions{
			Program:   prog,
			Interface: ifaceObj.Index,
			Flags:     link.XDPOffloadMode,
		})
	default:
		return nil, fmt.Errorf("unknown XDP_MODE %q", mode)
	}
}
