package bpf

import (
	"fmt"

	"github.com/cilium/ebpf"
)

// LoadEdgeObjectsForTest loads edge BPF objects. It prefers the full collection
// (including xdp_syn_cookie) and falls back to the main filter only when the
// kernel rejects syncookie helpers (common in BPF_PROG_TEST_RUN sandboxes).
func LoadEdgeObjectsForTest(objs *EdgeObjects, opts *ebpf.CollectionOptions) error {
	if err := LoadEdgeObjects(objs, opts); err == nil {
		return nil
	}

	spec, err := LoadEdge()
	if err != nil {
		return err
	}
	delete(spec.Programs, EdgeProgXdpSynCookie)

	var collOpts ebpf.CollectionOptions
	if opts != nil {
		collOpts = *opts
	}
	coll, err := ebpf.NewCollectionWithOptions(spec, collOpts)
	if err != nil {
		return err
	}

	prog := coll.Programs[EdgeProgXdpEdgeFilter]
	if prog == nil {
		coll.Close()
		return fmt.Errorf("missing program %s", EdgeProgXdpEdgeFilter)
	}
	objs.XdpEdgeFilter = prog

	if m := coll.Maps[EdgeMapAllowV4]; m != nil {
		objs.AllowV4 = m
	}
	if m := coll.Maps[EdgeMapBlocklistV4]; m != nil {
		objs.BlocklistV4 = m
	}
	if m := coll.Maps[EdgeMapConfig]; m != nil {
		objs.Config = m
	}
	if m := coll.Maps[EdgeMapGlobalSyn]; m != nil {
		objs.GlobalSyn = m
	}
	if m := coll.Maps[EdgeMapProgArray]; m != nil {
		objs.ProgArray = m
	}
	if m := coll.Maps[EdgeMapRatelimitV4]; m != nil {
		objs.RatelimitV4 = m
	}
	if m := coll.Maps[EdgeMapRstRatelimitV4]; m != nil {
		objs.RstRatelimitV4 = m
	}
	if m := coll.Maps[EdgeMapStats]; m != nil {
		objs.Stats = m
	}
	if m := coll.Maps[EdgeMapSynRatelimitV4]; m != nil {
		objs.SynRatelimitV4 = m
	}
	if m := coll.Maps[EdgeMapSynSubnetRatelimitV4]; m != nil {
		objs.SynSubnetRatelimitV4 = m
	}
	if m := coll.Maps[EdgeMapViolations]; m != nil {
		objs.Violations = m
	}
	if m := coll.Maps[EdgeMapFingerprints]; m != nil {
		objs.Fingerprints = m
	}

	for name := range coll.Programs {
		delete(coll.Programs, name)
	}
	for name := range coll.Maps {
		delete(coll.Maps, name)
	}
	coll.Close()
	return nil
}
