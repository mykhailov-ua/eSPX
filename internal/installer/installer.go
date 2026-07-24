package installer

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type CLI struct {
	Args []string
}

func NewCLI() *CLI {
	return &CLI{Args: os.Args}
}

func (c *CLI) Run() error {
	if len(c.Args) < 2 {
		c.PrintUsage()
		return nil
	}

	cmd := c.Args[1]
	switch cmd {
	case "preflight":
		strict := false
		asJSON := false
		for _, arg := range c.Args[2:] {
			if arg == "--strict" {
				strict = true
			}
			if arg == "--json" {
				asJSON = true
			}
		}
		_, err := RunPreflight(strict, asJSON)
		return err

	case "provision":
		yes := false
		for _, arg := range c.Args[2:] {
			if arg == "--yes" {
				yes = true
			}
		}
		return RunProvision(yes)

	case "configure":
		interactive := false
		for _, arg := range c.Args[2:] {
			if arg == "--interactive" {
				interactive = true
			}
		}
		return RunConfigure(interactive)

	case "apply":
		dryRun := false
		for _, arg := range c.Args[2:] {
			if arg == "--dry-run" {
				dryRun = true
			}
		}
		return c.RunApply(dryRun)

	case "rollback":
		if len(c.Args) < 3 {
			return fmt.Errorf("usage: espx-install rollback <tracker|processor>")
		}
		return RunRollbackCLI(c.Args[2])

	case "doctor":
		asJSON := false
		for _, arg := range c.Args[2:] {
			if arg == "--json" {
				asJSON = true
			}
		}
		return RunDoctor(asJSON)

	case "license":
		if len(c.Args) < 3 {
			fmt.Println("Usage: espx-install license <install|activate|status>")
			return nil
		}
		return RunLicense(c.Args[2])

	default:
		c.PrintUsage()
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func (c *CLI) PrintUsage() {
	fmt.Println("Usage: espx-install <command> [options]")
	fmt.Println("Commands:")
	fmt.Println("  preflight [--strict] [--json]")
	fmt.Println("  provision [--yes]")
	fmt.Println("  configure [--interactive]")
	fmt.Println("  apply     [--dry-run]")
	fmt.Println("  rollback  <tracker|processor>")
	fmt.Println("  doctor    [--json]")
	fmt.Println("  license   <install|activate|status>")
}

func (c *CLI) RunApply(dryRun bool) error {
	data, err := os.ReadFile("install.yaml")
	if err != nil {
		return fmt.Errorf("failed to read install.yaml: %w", err)
	}

	var profile InstallProfile
	if err := yaml.Unmarshal(data, &profile); err != nil {
		return fmt.Errorf("failed to parse install.yaml: %w", err)
	}

	if err := profile.Validate(); err != nil {
		return err
	}

	return renderTemplates(&profile, dryRun)
}
