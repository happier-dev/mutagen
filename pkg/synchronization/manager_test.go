package synchronization

import "testing"

func TestMaximumListConflictsMatchesWorkspaceSyncAPI(t *testing.T) {
	if maximumListConflicts != 100 {
		t.Fatalf("workspace sync exposes a 100-conflict bounded projection, manager retains %d", maximumListConflicts)
	}
}
