package gitworktree

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
)

func runGit(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v: %s", arguments, err, output)
	}
	return string(output)
}

func writeFixtureFile(t *testing.T, root, path string) {
	t.Helper()
	absolute := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(absolute), 0o700); err != nil {
		t.Fatal("unable to create fixture directory:", err)
	}
	if err := os.WriteFile(absolute, []byte(path), 0o600); err != nil {
		t.Fatal("unable to write fixture file:", err)
	}
}

func gitSelectsPath(t *testing.T, root, path string) bool {
	t.Helper()
	tracked := exec.Command("git", "-C", root, "ls-files", "--error-unmatch", "--", path)
	if err := tracked.Run(); err == nil {
		return true
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("git ls-files failed for %q: %v", path, err)
	}

	ignored := exec.Command("git", "-C", root, "check-ignore", "--quiet", "--no-index", "--", path)
	if err := ignored.Run(); err == nil {
		return false
	} else if exit := new(exec.ExitError); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("git check-ignore failed for %q: %v", path, err)
	}
	return true
}

func assertMatchesGitSelection(t *testing.T, root string, paths ...string) {
	t.Helper()
	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	for _, path := range paths {
		selected := selectorSelectsPath(ignorer, path)
		if expected := gitSelectsPath(t, root, path); selected != expected {
			t.Errorf("selection mismatch for %q: selector selected=%t, Git selected=%t", path, selected, expected)
		}
	}
}

func TestIgnorerDelegatesUntranslatablePatternsToGit(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte(
		"literal\\[bracket].tmp\n"+
			"recursive/**/value.tmp\n"+
			"[ab].class\n"+
			"space\\ file.tmp\n",
	), 0o600); err != nil {
		t.Fatal(err)
	}

	paths := []string{
		"literal[bracket].tmp",
		"literala.tmp",
		"recursive/value.tmp",
		"recursive/deep/value.tmp",
		"a.class",
		"c.class",
		"space file.tmp",
	}
	for _, path := range paths {
		writeFixtureFile(t, root, path)
	}
	assertMatchesGitSelection(t, root, paths...)
}

func TestIgnorerDoesNotDescendIntoSubmoduleWorktrees(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	runGit(t, root, "update-index", "--add", "--cacheinfo", "160000,1111111111111111111111111111111111111111,dependency")
	writeFixtureFile(t, root, "dependency/working-tree-file.txt")

	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if selected := selectorSelectsPath(ignorer, "dependency"); !selected {
		t.Fatal("selector excluded the tracked gitlink itself")
	}
	if selected := selectorSelectsPath(ignorer, "dependency/working-tree-file.txt"); selected {
		t.Fatal("selector included submodule worktree contents that Git does not select")
	}
}

func TestIgnorerExplicitPolicyOverridesGitAndTrackedSelection(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("generated/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, root, "generated/keep.txt")
	writeFixtureFile(t, root, "generated/deep/keep.txt")
	writeFixtureFile(t, root, "tracked.txt")
	runGit(t, root, "add", "tracked.txt")

	ignorer, err := NewIgnorer(root, []string{"/tracked.txt"}, []string{"/generated/keep.txt", "/generated/**/keep.txt", "/.git/config"})
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if selectorSelectsPath(ignorer, "tracked.txt") {
		t.Fatal("explicit ignore did not override tracked-file selection")
	}
	if !selectorSelectsPath(ignorer, "generated/keep.txt") {
		t.Fatal("explicit include did not override Git's ignored-directory selection")
	}
	if !selectorSelectsPath(ignorer, "generated/deep/keep.txt") {
		t.Fatal("wildcard explicit include did not retain traversal below an ignored directory")
	}
	if selectorSelectsPath(ignorer, ".git/config") {
		t.Fatal("explicit include selected Git administration data")
	}
}

func TestIgnorerPreservesNULTerminatedPathBytes(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		" leading tracked.txt",
		"trailing tracked.txt ",
		"line\nbreak.secret",
		"line\nbreak.txt",
	}
	for _, path := range paths {
		writeFixtureFile(t, root, path)
	}
	runGit(t, root, "add", " leading tracked.txt", "trailing tracked.txt ")
	assertMatchesGitSelection(t, root, paths...)
}

func TestIgnorerEvaluatesLargeTraversalThroughOneOracle(t *testing.T) {
	if testing.Short() {
		t.Skip("large selector traversal")
	}
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.ignored\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	for index := 0; index < 10_000; index++ {
		path := fmt.Sprintf("directory-%05d/value", index)
		if index%2 == 0 {
			path += ".ignored"
		} else {
			path += ".txt"
		}
		selected := selectorSelectsPath(ignorer, path)
		if selected != (index%2 != 0) {
			t.Fatalf("unexpected selection for %q: %t", path, selected)
		}
	}
}

func TestIgnorerFailsClosedWhenGitOracleBecomesUnavailable(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if err := ignorer.oracle.command.Process.Kill(); err != nil {
		t.Fatal("unable to stop Git oracle:", err)
	}
	_ = ignorer.oracle.command.Wait()

	status, continueTraversal := ignorer.Ignore("untracked.txt", false)
	if status != ignore.IgnoreStatusIgnored || continueTraversal {
		t.Fatalf("unavailable oracle did not fail closed: status=%v continue=%t", status, continueTraversal)
	}
	if err := ignorer.SelectionError(); err == nil || err.Error() != "git_selection_unavailable" {
		t.Fatalf("unavailable oracle did not surface git_selection_unavailable: %v", err)
	}
}

func BenchmarkIgnorerGitOracle(b *testing.B) {
	root := b.TempDir()
	runGitBenchmark(b, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.ignored\n"), 0o600); err != nil {
		b.Fatal(err)
	}
	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		b.Fatal("unable to construct Git selector:", err)
	}
	b.Cleanup(func() { ignorer.Close() })
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, _ := ignorer.Ignore(fmt.Sprintf("entry-%09d.ignored", index), false)
		if status != ignore.IgnoreStatusIgnored {
			b.Fatalf("unexpected selection status: %v", status)
		}
	}
}

func runGitBenchmark(b *testing.B, root string, arguments ...string) {
	b.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	if output, err := command.CombinedOutput(); err != nil {
		b.Fatalf("git %v failed: %v: %s", arguments, err, output)
	}
}

func selectorSelectsPath(ignorer *Ignorer, path string) bool {
	components := strings.Split(path, "/")
	masked := false
	for index := range components {
		candidate := strings.Join(components[:index+1], "/")
		directory := index < len(components)-1
		status, continueTraversal := ignorer.Ignore(candidate, directory)
		switch status {
		case ignore.IgnoreStatusIgnored:
			masked = true
			if directory && !continueTraversal {
				return false
			}
		case ignore.IgnoreStatusUnignored:
			masked = false
		}
	}
	return !masked
}

func TestIgnorerMatchesGitSelectionSemantics(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")

	globalExcludes := filepath.Join(t.TempDir(), "global-ignore")
	if err := os.WriteFile(globalExcludes, []byte("global.tmp\nglobal-ignored.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "config", "core.excludesFile", globalExcludes)

	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte(
		"cache/\n"+
			"/root-only.tmp\n"+
			"*.log\n"+
			"!keep.log\n"+
			"!global.tmp\n"+
			"\\#literal.txt\n"+
			"\\!literal.txt\n"+
			"\\ leading.txt\n"+
			"trailing\\ \n"+
			"trimmed   \n",
	), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "src", ".gitignore"), []byte("build/\n/anchored.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", "info", "exclude"), []byte("info.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	paths := []string{
		"cache/value.txt",
		"cache/tracked.txt",
		"cache/untracked.txt",
		"deep/cache/value.txt",
		"root-only.tmp",
		"deep/root-only.tmp",
		"ordinary.log",
		"keep.log",
		"#literal.txt",
		"!literal.txt",
		" leading.txt",
		"trailing ",
		"trimmed",
		"src/build/value.txt",
		"src/deep/build/value.txt",
		"other/build/value.txt",
		"src/anchored.tmp",
		"src/deep/anchored.tmp",
		"info.tmp",
		"global.tmp",
		"global-ignored.tmp",
		"tracked.log",
		" tracked.log",
		"ordinary.txt",
	}
	for _, path := range paths {
		writeFixtureFile(t, root, path)
	}
	runGit(t, root, "add", "-f", "tracked.log", " tracked.log", "cache/tracked.txt")

	assertMatchesGitSelection(t, root, paths...)
}

func TestIgnorerUsesLinkedWorktreeCommonGitMetadata(t *testing.T) {
	mainRoot := filepath.Join(t.TempDir(), "main")
	linkedRoot := filepath.Join(filepath.Dir(mainRoot), "linked")
	if err := os.MkdirAll(mainRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, mainRoot, "init", "--quiet")
	runGit(t, mainRoot, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "--allow-empty", "-m", "fixture")
	runGit(t, mainRoot, "worktree", "add", "--quiet", "--detach", linkedRoot, "HEAD")

	commonDirectory := strings.TrimSpace(runGit(t, mainRoot, "rev-parse", "--path-format=absolute", "--git-common-dir"))
	if err := os.WriteFile(filepath.Join(commonDirectory, "info", "exclude"), []byte("common.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	worktreeExcludes := filepath.Join(t.TempDir(), "worktree-ignore")
	if err := os.WriteFile(worktreeExcludes, []byte("configured.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, mainRoot, "config", "extensions.worktreeConfig", "true")
	runGit(t, linkedRoot, "config", "--worktree", "core.excludesFile", worktreeExcludes)

	paths := []string{"common.tmp", "configured.tmp", "ordinary.txt"}
	for _, path := range paths {
		writeFixtureFile(t, linkedRoot, path)
	}
	assertMatchesGitSelection(t, linkedRoot, paths...)
}

func TestIgnorerRefreshesChangedPolicy(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	writeFixtureFile(t, root, "fresh.cache")
	ignorePath := filepath.Join(root, ".gitignore")
	if err := os.WriteFile(ignorePath, []byte("*.log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if !selectorSelectsPath(ignorer, "fresh.cache") {
		t.Fatal("selector ignored a path before its policy rule was added")
	}
	if err := os.WriteFile(ignorePath, []byte("*.log\nfresh.cache\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ignorer.Refresh(); err != nil {
		t.Fatal("unable to refresh changed Git policy:", err)
	}
	if selectorSelectsPath(ignorer, "fresh.cache") || gitSelectsPath(t, root, "fresh.cache") {
		t.Fatal("changed Git policy was not applied at the refresh boundary")
	}
}

func TestCompilePatternsFailsClosedWhenGlobalExcludesLookupFails(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", t.TempDir())

	if _, err := CompilePatterns(root, nil, nil); err == nil || err.Error() != "git_selection_unavailable" {
		t.Fatalf("global excludes lookup failure did not fail closed with git_selection_unavailable: %v", err)
	}
}

func TestCompilePatternsFailsClosedOutsideGitWorktree(t *testing.T) {
	if _, err := CompilePatterns(t.TempDir(), nil, nil); err == nil || err.Error() != "git_selection_unavailable" {
		t.Fatalf("non-Git root did not fail closed with git_selection_unavailable: %v", err)
	}
}
