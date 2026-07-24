package installer

import "fmt"

var serviceTargets = map[string]string{
	"tracker":   "/usr/local/bin/tracker",
	"processor": "/usr/local/bin/processor",
}

// RunRollbackCLI restores the last backup for a hot-path service.
func RunRollbackCLI(service string) error {
	target, ok := serviceTargets[service]
	if !ok {
		return fmt.Errorf("unknown service %q (want tracker or processor)", service)
	}
	if err := RollbackService(service, target); err != nil {
		return err
	}
	fmt.Printf("Rolled back %s to previous backup\n", service)
	return nil
}
