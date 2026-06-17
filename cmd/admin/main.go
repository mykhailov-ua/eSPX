package main

import (
	"fmt"
	"os"

	"espx/cmd/admin/cmd"
)

// main delegates to the admin Cobra CLI so operators get a single entry point for dev tooling.
func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
