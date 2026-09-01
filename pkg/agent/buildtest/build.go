// Package buildtest provides a test helper that builds the mutagen-agent
// binary for security-focused tests. Tests that exercise the agent's root
// enforcement contract must fail (never skip) when the binary cannot be
// built.
package buildtest

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

var (
	buildOnce  sync.Once
	binaryPath string
	buildErr   error
)

// Agent builds the mutagen-agent binary (with its required build tag) into a
// shared temporary directory and returns its path. It fails the test (rather
// than skipping it) if the Go toolchain or the agent package is unavailable,
// because the agent root-enforcement tests are security tests.
func Agent(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		directory, err := os.MkdirTemp("", "mutagen-agent-buildtest")
		if err != nil {
			buildErr = err
			return
		}
		path := filepath.Join(directory, "mutagen-agent"+exeSuffix())
		command := exec.Command("go", "build", "-tags", "mutagenagent", "-o", path, "./cmd/mutagen-agent")
		_, source, _, ok := runtime.Caller(0)
		if !ok {
			buildErr = os.ErrInvalid
			return
		}
		command.Dir = filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..", ".."))
		output, err := command.CombinedOutput()
		if err != nil {
			buildErr = err
			t.Logf("agent build failed: %v\n%s", err, output)
			return
		}
		binaryPath = path
	})
	if buildErr != nil {
		t.Fatalf("unable to build mutagen-agent binary for root enforcement tests: %v", buildErr)
	}
	return binaryPath
}

func exeSuffix() string {
	if os.PathSeparator == '\\' {
		return ".exe"
	}
	return ""
}
