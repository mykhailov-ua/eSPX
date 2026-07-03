package partition

import (
	"testing"

	"github.com/google/uuid"
)

func TestIndexMatchesStaticSlot(t *testing.T) {
	const n = 6
	id := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	want := int(Slot(id[:])) % n
	if got := int(Index(id[:], n)); got != want {
		t.Fatalf("Index=%d want %d", got, want)
	}
}

func TestIndexSinglePartition(t *testing.T) {
	id := uuid.New()
	if Index(id[:], 1) != 0 {
		t.Fatal("expected partition 0 for single partition")
	}
}
