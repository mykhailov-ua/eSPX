// Command patch_vtproto_hotpath patches vtproto UnmarshalVT for repeated bytes fields.
// Run after buf generate; see scripts/codegen/gen.sh.
package main

import (
	"fmt"
	"os"
	"strings"
)

const defaultPath = "internal/ads/pb/events_vtproto.pb.go"

var patches = []struct {
	from string
	to   string
}{
	{
		from: "m.ExtraKeys = append(m.ExtraKeys, make([]byte, postIndex-iNdEx))\n\t\t\tcopy(m.ExtraKeys[len(m.ExtraKeys)-1], dAtA[iNdEx:postIndex])",
		to:   "m.ExtraKeys = appendReuseBytes(m.ExtraKeys, dAtA[iNdEx:postIndex])",
	},
	{
		from: "m.ExtraValues = append(m.ExtraValues, make([]byte, postIndex-iNdEx))\n\t\t\tcopy(m.ExtraValues[len(m.ExtraValues)-1], dAtA[iNdEx:postIndex])",
		to:   "m.ExtraValues = appendReuseBytes(m.ExtraValues, dAtA[iNdEx:postIndex])",
	},
}

func main() {
	path := defaultPath
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "patch_vtproto_hotpath: read %s: %v\n", path, err)
		os.Exit(1)
	}
	text := string(data)

	if strings.Contains(text, "appendReuseBytes(m.ExtraKeys") {
		return
	}

	for _, p := range patches {
		if !strings.Contains(text, p.from) {
			fmt.Fprintf(os.Stderr, "patch_vtproto_hotpath: pattern missing in %s (buf plugin output changed?)\n", path)
			os.Exit(1)
		}
		text = strings.Replace(text, p.from, p.to, 1)
	}

	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "patch_vtproto_hotpath: write %s: %v\n", path, err)
		os.Exit(1)
	}
}
