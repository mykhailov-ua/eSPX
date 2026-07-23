package main

import (
	"fmt"

	"espx/internal/edge/bpf"

	"github.com/cilium/ebpf"
)

func wireProgArray(objs *bpf.EdgeObjects) error {
	if objs.ProgArray == nil || objs.XdpSynCookie == nil {
		return fmt.Errorf("prog_array or xdp_syn_cookie not loaded")
	}
	key := uint32(0)
	return objs.ProgArray.Update(&key, objs.XdpSynCookie, ebpf.UpdateAny)
}
