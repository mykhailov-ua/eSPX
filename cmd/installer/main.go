package main

import (
	"fmt"
	"os"

	"espx/internal/installer"
)

func main() {
	if err := installer.NewCLI().Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
