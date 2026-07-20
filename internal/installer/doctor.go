package installer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

func RunDoctor(asJSON bool) error {
	script := checkDepsScript()
	cmd := exec.Command("bash", script)
	cmd.Dir = repoRoot()
	cmd.Env = append(os.Environ(), "ROOT="+repoRoot())

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	results := map[string]string{
		"dependencies": "OK",
		"topology":     "OK",
	}
	if err != nil {
		results["dependencies"] = "FAIL"
		results["topology"] = stderr.String()
		if results["topology"] == "" {
			results["topology"] = stdout.String()
		}
	}

	if asJSON {
		data, marshalErr := json.MarshalIndent(results, "", "  ")
		if marshalErr != nil {
			return marshalErr
		}
		fmt.Println(string(data))
		if err != nil {
			return fmt.Errorf("doctor: check_deps failed")
		}
		return nil
	}

	fmt.Println("Running doctor health checks...")
	for k, v := range results {
		fmt.Printf("%s: %s\n", k, v)
	}
	if err != nil {
		return fmt.Errorf("doctor: check_deps failed")
	}
	return nil
}
