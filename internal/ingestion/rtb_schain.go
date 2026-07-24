package ingestion

const (
	schainNodeMax     = 8
	schainASIMax      = 64
	schainSIDMax      = 64
	schainAllowlistAS = 32
)

// SchainNode is one hop in an OpenRTB source.ext.schain.nodes[] array.
type SchainNode struct {
	ASI    [schainASIMax]byte
	ASILen uint8
	SID    [schainSIDMax]byte
	SIDLen uint8
}

// SchainNodes holds a stack-fixed schain hop list parsed on the hot path.
type SchainNodes struct {
	Count uint8
	Nodes [schainNodeMax]SchainNode
}

// SupplyChainAllowlistSnapshot is an immutable asi|sid allowlist for hot schain validation.
type SupplyChainAllowlistSnapshot struct {
	Allowed map[string]struct{}
}

func schainAllowKey(asi, sid []byte) string {
	if len(asi) == 0 || len(sid) == 0 {
		return ""
	}
	buf := make([]byte, 0, len(asi)+1+len(sid))
	buf = append(buf, asi...)
	buf = append(buf, '|')
	buf = append(buf, sid...)
	return string(buf)
}

// ValidateSchainNodes checks each hop against the allowlist snapshot (R18).
func ValidateSchainNodes(nodes SchainNodes, allow *SupplyChainAllowlistSnapshot) bool {
	if nodes.Count == 0 {
		return true
	}
	if allow == nil || len(allow.Allowed) == 0 {
		return true
	}
	for i := uint8(0); i < nodes.Count; i++ {
		n := nodes.Nodes[i]
		if n.ASILen == 0 || n.SIDLen == 0 {
			continue
		}
		key := schainAllowKey(n.ASI[:n.ASILen], n.SID[:n.SIDLen])
		if _, ok := allow.Allowed[key]; !ok {
			return false
		}
	}
	return true
}

// BuildSupplyChainAllowlistFromSellers materializes asi|sid keys from seller rows.
func BuildSupplyChainAllowlistFromSellers(domains []string, sellerIDs []string) *SupplyChainAllowlistSnapshot {
	if len(domains) == 0 || len(domains) != len(sellerIDs) {
		return &SupplyChainAllowlistSnapshot{Allowed: make(map[string]struct{})}
	}
	allowed := make(map[string]struct{}, len(domains))
	for i := range domains {
		key := schainAllowKey([]byte(domains[i]), []byte(sellerIDs[i]))
		if key != "" {
			allowed[key] = struct{}{}
		}
	}
	return &SupplyChainAllowlistSnapshot{Allowed: allowed}
}
