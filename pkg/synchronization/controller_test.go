package synchronization

import (
	"testing"

	"github.com/mutagen-io/mutagen/pkg/synchronization/gitworktree"
	"google.golang.org/protobuf/proto"
)

func TestSelectionErrorCodePreservesMidScanGitSelectorFailure(t *testing.T) {
	_, err := gitworktree.CompilePatterns(t.TempDir(), nil, nil)
	if err == nil {
		t.Fatal("non-Git selector fixture unexpectedly succeeded")
	}
	if code := selectionErrorCode(err); code != "git_selection_unavailable" {
		t.Fatalf("mid-scan Git selector failure lost its typed code: %q (%v)", code, err)
	}
}

func TestStateClonePreservesTypedFailure(t *testing.T) {
	state := &State{LastError: "selection failed", LastErrorCode: "git_selection_unavailable"}
	cloned := proto.Clone(state).(*State)
	if cloned.LastErrorCode != state.LastErrorCode {
		t.Fatal("canonical state clone lost typed failure metadata")
	}
}
