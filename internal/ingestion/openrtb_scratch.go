package ingestion

import (
	"sync"
	"unsafe"

	"espx/internal/campaignmodel"
)

const openRTBScratchMagic = 0x4f525442335f01 // "ORTB3_" — distinguishes from fraud accumulator Scratch.

type openRTBScratchSlot struct {
	magic  uintptr
	parsed OpenRTB3Parsed
}

var openRTBScratchPool = sync.Pool{
	New: func() any {
		return &openRTBScratchSlot{magic: openRTBScratchMagic}
	},
}

func acquireOpenRTBScratchSlot() *openRTBScratchSlot {
	slot := openRTBScratchPool.Get().(*openRTBScratchSlot)
	slot.magic = openRTBScratchMagic
	return slot
}

func parseOpenRTB3FSMInto(out *OpenRTB3Parsed, payload []byte) bool {
	n := len(payload)
	if n < 2 {
		*out = OpenRTB3Parsed{}
		return false
	}
	_ = payload[n-1]

	out.MinBid = 0
	out.DeviceType = 1
	out.CategoryMask = 1
	out.DealIDOff = 0
	out.DealIDLen = 0
	out.ItemIDOff = 0
	out.ItemIDLen = 0
	out.RequestIDOff = 0
	out.RequestIDLen = 0
	out.TagIDOff = 0
	out.TagIDLen = 0
	out.IsOpenRTB = false
	out.OK = false

	i := skipJSONWS(payload, 0, n)
	if i >= n || payload[i] != '{' {
		return false
	}

	var stack [ortbMaxDepth]ortbFrame
	depth := 0
	stack[0] = ortbFrame{parent: ortbKeyUnknown, itemIdx: -1}

	i, ok := parseOrtbObject(payload, i, n, out, &stack, &depth)
	_ = i
	if !ok && !out.IsOpenRTB {
		*out = OpenRTB3Parsed{}
		return false
	}
	if out.IsOpenRTB {
		out.OK = true
	}
	return out.OK
}

func attachOpenRTB3Scratch(evt *campaignmodel.Event, slot *openRTBScratchSlot) {
	if evt == nil || slot == nil {
		return
	}
	slot.magic = openRTBScratchMagic
	evt.Scratch = unsafe.Pointer(slot)
}

func openRTB3ParsedFromScratch(evt *campaignmodel.Event) (*OpenRTB3Parsed, bool) {
	if evt == nil || evt.Scratch == nil {
		return nil, false
	}
	slot := (*openRTBScratchSlot)(evt.Scratch)
	if slot.magic != openRTBScratchMagic {
		return nil, false
	}
	if !slot.parsed.OK || !slot.parsed.IsOpenRTB {
		return nil, false
	}
	return &slot.parsed, true
}

func releaseOpenRTB3Scratch(evt *campaignmodel.Event) {
	if evt == nil || evt.Scratch == nil {
		return
	}
	slot := (*openRTBScratchSlot)(evt.Scratch)
	if slot.magic != openRTBScratchMagic {
		return
	}
	slot.parsed = OpenRTB3Parsed{}
	slot.magic = openRTBScratchMagic
	openRTBScratchPool.Put(slot)
	evt.Scratch = nil
}

func releaseOpenRTBScratchSlot(slot *openRTBScratchSlot) {
	if slot == nil {
		return
	}
	slot.parsed = OpenRTB3Parsed{}
	slot.magic = openRTBScratchMagic
	openRTBScratchPool.Put(slot)
}
