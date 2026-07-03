package bpf

import (
	"os"
	"testing"

	"github.com/cilium/ebpf/rlimit"
)

func TestMain(m *testing.M) {
	_ = rlimit.RemoveMemlock()
	os.Exit(m.Run())
}
