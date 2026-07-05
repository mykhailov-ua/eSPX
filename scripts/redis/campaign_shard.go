//go:build ignore

// Command campaign_shard prints the StaticSlot shard index for a campaign UUID.
package main

import (
	"fmt"
	"os"
	"strconv"

	"espx/internal/ads"

	"github.com/google/uuid"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: campaign_shard <campaign_uuid> [shard_count]")
		os.Exit(2)
	}
	id, err := uuid.Parse(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid campaign uuid:", err)
		os.Exit(1)
	}
	n := 4
	if len(os.Args) >= 3 {
		n, err = strconv.Atoi(os.Args[2])
		if err != nil || n <= 0 {
			fmt.Fprintln(os.Stderr, "invalid shard_count:", os.Args[2])
			os.Exit(1)
		}
	}
	sharder := ads.NewStaticSlotSharder(n)
	fmt.Println(sharder.GetShard(id))
}
