package ads

import (
	"testing"
)

func BenchmarkUDPControl_ApplyPacket(b *testing.B) {
	c := NewUDPControl(UDPControlConfig{
		Enabled:    true,
		NumShards:  4,
		InitialRPS: 50_000,
	})
	var limits UDPControlLimits
	limits.NumShards = 4
	for i := uint8(0); i < 4; i++ {
		limits.Limits[i] = 50_000
	}
	var pkt [256]byte
	for epoch := int64(1); epoch <= 10; epoch++ {
		hash := ComputeUDPConfigHash(epoch, 0, &limits)
		hdr := &UDPHeader{EpochID: epoch, ConfigHash: hash}
		if n := EncodeQuotaEpochDatagram(pkt[:], UDPMsgQuotaEpoch, hdr, &limits); n > 0 {
			c.ApplyPacket(pkt[:n])
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		epoch := int64(i%1000 + 100)
		hash := ComputeUDPConfigHash(epoch, 0, &limits)
		hdr := &UDPHeader{EpochID: epoch, ConfigHash: hash}
		n := EncodeQuotaEpochDatagram(pkt[:], UDPMsgQuotaEpoch, hdr, &limits)
		c.ApplyPacket(pkt[:n])
	}
}

func BenchmarkUDPControl_ShardLimitRPS(b *testing.B) {
	c := NewUDPControl(UDPControlConfig{Enabled: true, NumShards: 4, InitialRPS: 50_000})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.ShardLimitRPS(i % 4)
	}
}
