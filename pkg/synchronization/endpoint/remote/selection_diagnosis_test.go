package remote

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
	"github.com/mutagen-io/mutagen/pkg/synchronization/gitworktree"
)

func TestRootedRemoteSelectionDiagnosisUsesEndpointPolicy(t *testing.T) {
	root := t.TempDir()
	if output, err := exec.Command("git", "-C", root, "init", "--quiet").CombinedOutput(); err != nil {
		t.Fatalf("unable to initialize test worktree: %v: %s", err, output)
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tracked.log"), []byte("tracked"), 0o600); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", root, "add", "-f", "tracked.log").CombinedOutput(); err != nil {
		t.Fatalf("unable to track test file: %v: %s", err, output)
	}
	connection, _ := startRootedServer(t, root)
	endpoint, err := NewEndpoint(nil, connection, "", "selection-diagnosis", synchronization.DefaultVersion,
		&synchronization.Configuration{IgnoreSyntax: ignore.Syntax_SyntaxGitWorktree, WatchMode: synchronization.WatchMode_WatchModeNoWatch}, false)
	if err != nil {
		t.Fatal("unable to connect rooted endpoint:", err)
	}
	defer endpoint.Shutdown()
	diagnoser, ok := endpoint.(interface {
		DiagnoseSelection(context.Context, string) (gitworktree.SelectionVerdict, error)
	})
	if !ok {
		t.Fatal("remote endpoint has no selection diagnosis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	verdict, err := diagnoser.DiagnoseSelection(ctx, "tracked.log")
	if err != nil || verdict.Excluded {
		t.Fatalf("remote endpoint did not preserve tracked override: %+v, %v", verdict, err)
	}
	verdict, err = diagnoser.DiagnoseSelection(ctx, ".git/config")
	if err != nil || !verdict.Excluded || verdict.Reason != gitworktree.SelectionExcludedRepositoryMetadata {
		t.Fatalf("remote endpoint did not classify repository metadata: %+v, %v", verdict, err)
	}
}
