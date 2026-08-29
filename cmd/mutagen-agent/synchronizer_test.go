package main

import "testing"

func TestSynchronizerRequiresRoot(t *testing.T) {
	previous := synchronizerConfiguration.root
	synchronizerConfiguration.root = ""
	t.Cleanup(func() { synchronizerConfiguration.root = previous })

	if err := synchronizerMain(synchronizerCommand, nil); err == nil {
		t.Fatal("synchronizer accepted invocation without mandatory --root")
	}
}
