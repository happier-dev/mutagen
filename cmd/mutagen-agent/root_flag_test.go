package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mutagen-io/mutagen/pkg/agent"
	"github.com/mutagen-io/mutagen/pkg/agent/buildtest"
	"github.com/mutagen-io/mutagen/pkg/mutagen"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/endpoint/remote"
)

type rootedAgentCommandStream struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	cmd    *exec.Cmd
}

func (s *rootedAgentCommandStream) Read(buffer []byte) (int, error) {
	return s.stdout.Read(buffer)
}

func (s *rootedAgentCommandStream) Write(buffer []byte) (int, error) {
	return s.stdin.Write(buffer)
}

func (s *rootedAgentCommandStream) Close() error {
	s.stdin.Close()
	s.stdout.Close()
	if s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}
	return s.cmd.Wait()
}

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

func TestExternalSynchronizerRetainsAuthorizedRootAcrossRealHandshakes(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal("unable to create authorized root:", err)
	}
	if err := os.WriteFile(filepath.Join(root, "authorized"), []byte("original"), 0o600); err != nil {
		t.Fatal("unable to seed authorized root:", err)
	}

	command := exec.Command(buildtest.Agent(t), "synchronizer", "--external", "--root", root)
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal("unable to create agent input:", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		stdin.Close()
		t.Fatal("unable to create agent output:", err)
	}
	if err := command.Start(); err != nil {
		stdin.Close()
		stdout.Close()
		t.Fatal("unable to start rooted agent:", err)
	}
	stream := &rootedAgentCommandStream{stdin: stdin, stdout: stdout, cmd: command}
	t.Cleanup(func() { stream.Close() })

	if err := agent.ClientHandshake(stream); err != nil {
		t.Fatalf("agent command handshake failed: %v (stderr: %s)", err, stderr.String())
	}
	if err := mutagen.ClientVersionHandshake(stream); err != nil {
		t.Fatalf("agent version/build-identity handshake failed: %v (stderr: %s)", err, stderr.String())
	}

	original := filepath.Join(parent, "original")
	if err := os.Rename(root, original); err != nil {
		// Windows retains directory handles with sharing that can prevent the
		// replacement itself. That is the other accepted enforcement outcome.
		if runtime.GOOS == "windows" {
			if _, statErr := os.Stat(filepath.Join(root, "authorized")); statErr != nil {
				t.Fatal("held root handle blocked replacement without preserving the authorized root:", statErr)
			}
			return
		}
		t.Fatal("unable to move authorized root during handshake:", err)
	}
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal("unable to create substituted root:", err)
	}
	if err := os.WriteFile(filepath.Join(root, "substituted"), []byte("replacement"), 0o600); err != nil {
		t.Fatal("unable to seed substituted root:", err)
	}

	endpoint, err := remote.NewEndpoint(
		nil,
		stream,
		"",
		"root-substitution-session",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	)
	if err == nil {
		endpoint.Shutdown()
		t.Fatal("rooted agent accepted a directory substituted after its real command handshakes")
	}
}

func TestStandardSynchronizerBinaryDoesNotRequireRootFlag(t *testing.T) {
	stderr := runSynchronizer(t)
	if strings.Contains(stderr, "required flag") || strings.Contains(stderr, "missing required --root") {
		t.Fatalf("standard SSH/Docker synchronizer invocation was rejected by the external root guard: %q", stderr)
	}
}
