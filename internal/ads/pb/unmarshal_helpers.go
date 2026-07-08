package pb

// appendReuseBytes appends wire bytes into a reused [][]byte slot on the track hot path.
// vtproto emits alloc-heavy make+copy for repeated bytes; gen.sh patches events_vtproto.pb.go
// to call this helper after each buf generate (scripts/codegen/patch_vtproto_hotpath).
func appendReuseBytes(dst [][]byte, src []byte) [][]byte {
	idx := len(dst)
	if idx < cap(dst) {
		dst = dst[:idx+1]
		dst[idx] = append(dst[idx][:0], src...)
		return dst
	}
	return append(dst, append([]byte(nil), src...))
}
