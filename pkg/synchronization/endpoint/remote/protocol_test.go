package remote

import (
	"testing"

	"github.com/mutagen-io/mutagen/pkg/synchronization/gitworktree"
)

func TestRemoteErrorDecodingDoesNotClassifyDiagnosticText(t *testing.T) {
	for _, code := range []string{"", "future_error"} {
		err := decodeEndpointError(code, "git_selection_unavailable")
		if err == nil || gitworktree.IsSelectionUnavailable(err) {
			t.Fatalf("uncoded or unknown error acquired typed authority: %q: %v", code, err)
		}
	}
}
