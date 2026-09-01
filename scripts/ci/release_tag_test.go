package main

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestValidateReleaseTag(t *testing.T) {
	script := filepath.Join("validate_release_tag.sh")
	for _, tag := range []string{"mutagen-v0.18.1", "mutagen-v0.18.1-happier.2"} {
		if output, err := exec.Command("bash", script, tag).CombinedOutput(); err != nil {
			t.Fatalf("valid release tag %q rejected: %v: %s", tag, err, output)
		}
	}
	for _, tag := range []string{"", "mutagen-v", "mutagen-vnext", "mutagen-v0.18", "mutagen-v0.18.1+mutable", "other-v0.18.1"} {
		if err := exec.Command("bash", script, tag).Run(); err == nil {
			t.Fatalf("invalid release tag %q accepted", tag)
		}
	}
}
