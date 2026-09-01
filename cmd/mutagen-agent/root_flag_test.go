package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mutagen-io/mutagen/pkg/agent/buildtest"
)

// runSynchronizer runs the packaged agent binary in synchronizer mode with the
// specified arguments and returns the combined stderr output.
func runSynchronizer(t *testing.T, arguments ...string) string {
	t.Helper()
	command := exec.Command(buildtest.Agent(t), append([]string{"synchronizer"}, arguments...)...)
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	command.Stdin = nil
	if err := command.Run(); err == nil {
		t.Fatal("synchronizer invocation unexpectedly succeeded")
	}
	return stderr.String()
}

func TestExternalSynchronizerBinaryRequiresRootFlag(t *testing.T) {
	// The external mode used by Happier must refuse to serve without a root.
	stderr := runSynchronizer(t, "--external")
	if !strings.Contains(stderr, "root") {
		t.Fatalf("missing-root failure did not mention the root flag: %q", stderr)
	}
}

func TestSynchronizerBinaryRejectsEmptyRootFlag(t *testing.T) {
	// An explicitly empty --root must fail through the runtime guard, not the
	// flag-presence guard.
	stderr := runSynchronizer(t, "--external", "--root=")
	if !strings.Contains(stderr, "missing required --root") {
		t.Fatalf("empty-root failure did not come from the runtime root guard: %q", stderr)
	}
}

func TestSynchronizerBinaryRejectsFileRoot(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal("unable to create root candidate file:", err)
	}
	stderr := runSynchronizer(t, "--external", "--root", file)
	if strings.Contains(stderr, "missing required --root") {
		t.Fatalf("file root rejected by the wrong guard: %q", stderr)
	}
}

func TestStandardSynchronizerBinaryDoesNotRequireRootFlag(t *testing.T) {
	stderr := runSynchronizer(t)
	if strings.Contains(stderr, "required flag") || strings.Contains(stderr, "missing required --root") {
		t.Fatalf("standard SSH/Docker synchronizer invocation was rejected by the external root guard: %q", stderr)
	}
}
