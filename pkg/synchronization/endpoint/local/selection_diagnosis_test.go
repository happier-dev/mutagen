package local

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
	"github.com/mutagen-io/mutagen/pkg/synchronization/gitworktree"
)

func TestEndpointSelectionDiagnosisUsesCurrentScannerPolicy(t *testing.T) {
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
	endpoint, err := NewEndpoint(nil, root, "selection-diagnosis", synchronization.DefaultVersion,
		&synchronization.Configuration{IgnoreSyntax: ignore.Syntax_SyntaxGitWorktree, WatchMode: synchronization.WatchMode_WatchModeNoWatch}, true)
	if err != nil {
		t.Fatal("unable to create live endpoint:", err)
	}
	t.Cleanup(func() { endpoint.Shutdown() })
	diagnoser, ok := endpoint.(interface {
		DiagnoseSelection(context.Context, string) (gitworktree.SelectionVerdict, error)
	})
	if !ok {
		t.Fatal("live endpoint has no selection diagnosis")
	}
	verdict, err := diagnoser.DiagnoseSelection(context.Background(), "tracked.log")
	if err != nil || verdict.Excluded {
		t.Fatalf("tracked file did not override Git ignore through the live endpoint: %+v, %v", verdict, err)
	}
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.log\nnew.cache\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := diagnoser.DiagnoseSelection(context.Background(), "new.cache"); !gitworktree.IsSelectionUnavailable(err) {
		t.Fatalf("changed policy generation was reported as current: %v", err)
	}
}

func TestEndpointSelectionDiagnosisUsesAllFilesScannerIgnorer(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{".gitfoo", ".git"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "nested", ".gitfoo"), 0o700); err != nil {
		t.Fatal(err)
	}
	endpoint, err := NewEndpoint(nil, root, "all-files-selection-diagnosis", synchronization.DefaultVersion,
		&synchronization.Configuration{Ignores: []string{"build/", ".git/"}, WatchMode: synchronization.WatchMode_WatchModeNoWatch}, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { endpoint.Shutdown() })
	diagnoser := endpoint.(synchronization.SelectionDiagnosingEndpoint)
	for _, test := range []struct {
		path     string
		excluded bool
		reason   gitworktree.SelectionExclusionReason
	}{
		{".gitfoo/file", false, ""},
		{"nested/.gitfoo/file", false, ""},
		{".git/config", true, gitworktree.SelectionExcludedRepositoryMetadata},
		{"build/output/file", true, gitworktree.SelectionExcludedConfiguredRule},
	} {
		verdict, err := diagnoser.DiagnoseSelection(context.Background(), test.path)
		if err != nil || verdict.Excluded != test.excluded || verdict.Reason != test.reason {
			t.Errorf("%s: unexpected verdict %+v, %v", test.path, verdict, err)
		}
	}
}

func TestEndpointSelectionDiagnosisTreatsLinkedWorktreeDotGitFileAsRepositoryMetadata(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: /private/common/worktrees/linked\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	endpoint, err := NewEndpoint(nil, root, "all-files-linked-worktree-selection-diagnosis", synchronization.DefaultVersion,
		&synchronization.Configuration{Ignores: []string{".git"}, WatchMode: synchronization.WatchMode_WatchModeNoWatch}, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { endpoint.Shutdown() })
	verdict, err := endpoint.(synchronization.SelectionDiagnosingEndpoint).DiagnoseSelection(context.Background(), ".git")
	if err != nil || !verdict.Excluded || verdict.Reason != gitworktree.SelectionExcludedRepositoryMetadata {
		t.Fatalf("linked-worktree .git file was not diagnosed as repository metadata: %+v, %v", verdict, err)
	}
}
