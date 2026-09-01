package main

import (
	"strings"
	"testing"
)

func TestExternalSynchronizerRequiresRoot(t *testing.T) {
	previous := synchronizerConfiguration.root
	previousExternal := synchronizerConfiguration.external
	synchronizerConfiguration.root = ""
	synchronizerConfiguration.external = true
	t.Cleanup(func() {
		synchronizerConfiguration.root = previous
		synchronizerConfiguration.external = previousExternal
	})

	if err := synchronizerMain(synchronizerCommand, nil); err == nil || !strings.Contains(err.Error(), "missing required --root") {
		t.Fatalf("external synchronizer accepted invocation without mandatory --root: %v", err)
	}
}

func TestStandardSynchronizerRetainsControllerSelectedRoot(t *testing.T) {
	previous := synchronizerConfiguration.root
	previousExternal := synchronizerConfiguration.external
	synchronizerConfiguration.root = ""
	synchronizerConfiguration.external = false
	t.Cleanup(func() {
		synchronizerConfiguration.root = previous
		synchronizerConfiguration.external = previousExternal
	})

	if err := validateSynchronizerRootMode(); err != nil {
		t.Fatalf("standard SSH/Docker synchronizer invocation unexpectedly required a rooted external mode: %v", err)
	}
}
