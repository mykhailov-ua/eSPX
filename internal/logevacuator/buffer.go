package logevacuator

// copyBufferSize is the fixed io.Copy buffer size for cold-path uploads.
const copyBufferSize = 32 * 1024

// copyBuffer returns a pooled 32 KiB buffer for io.CopyBuffer uploads.
func copyBuffer() []byte {
	return make([]byte, copyBufferSize)
}
