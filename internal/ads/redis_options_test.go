package ads

import (
	"testing"
	"time"
)

func TestFilterRedisOptions_alignsWithFilterDeadline(t *testing.T) {
	cases := []struct {
		name     string
		filterMs int
		addrs    []string
		poolSize int
		password string
	}{
		{name: "tracker_default", filterMs: 75, addrs: []string{"127.0.0.1:6379"}, poolSize: 64},
		{name: "short_deadline", filterMs: 50, addrs: []string{"127.0.0.1:6379"}, poolSize: 8},
		{name: "local_secret", filterMs: 150, addrs: []string{"localhost:6379"}, poolSize: 32, password: "secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := FilterRedisOptions(tc.addrs, tc.password, tc.poolSize, tc.filterMs)
			want := time.Duration(tc.filterMs) * time.Millisecond
			if opts.ReadTimeout != want {
				t.Fatalf("ReadTimeout = %v, want %v", opts.ReadTimeout, want)
			}
			if opts.WriteTimeout != want {
				t.Fatalf("WriteTimeout = %v, want %v", opts.WriteTimeout, want)
			}
		})
	}

	t.Run("zero_deadline_omits_timeout", func(t *testing.T) {
		opts := FilterRedisOptions([]string{"localhost:6379"}, "", 8, 0)
		if opts.ReadTimeout != 0 {
			t.Fatalf("zero filter timeout should omit ReadTimeout, got %v", opts.ReadTimeout)
		}
	})
}

func TestFilterRedisReadTimeoutMs_matchesFilterEngineDeadline(t *testing.T) {
	const filterMs = 120
	if got := FilterRedisReadTimeoutMs(filterMs); got != filterMs {
		t.Fatalf("FilterRedisReadTimeoutMs = %d, want %d", got, filterMs)
	}
	if got := FilterRedisReadTimeoutMs(0); got != 0 {
		t.Fatalf("zero filter timeout should return 0, got %d", got)
	}
}
