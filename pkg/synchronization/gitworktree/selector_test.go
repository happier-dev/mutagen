package gitworktree

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	// Windows rejects LF in file names. Use a legal Unicode line separator,
	// along with leading and embedded spaces, to retain the cross-platform
	// assertion that Git's NUL-delimited oracle preserves exact path bytes.
	paths := []string{
		" leading tracked.txt",
		"tracked name with spaces.txt",
		"line\u2028separator.secret",
		"line\u2028separator.txt",
	}
	for _, path := range paths {
		writeFixtureFile(t, root, path)
	}
	runGit(t, root, "add", " leading tracked.txt", "tracked name with spaces.txt")
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

// BenchmarkIgnorerSelectionCycle is the reproducible selection measurement for
// the one Git oracle. One iteration is one complete scanner cycle over a
// materialized worktree of the named candidate-path count: Refresh accepts the
// current Git policy and index generation, the persistent oracle decides every
// candidate path, and SelectionError re-verifies that generation. The corpus
// mixes tracked, untracked-selected and Git-ignored paths and places nested
// .gitignore files inside an ignored directory, so the policy inventory that
// has to see ignored policy files is measured rather than assumed.
//
// Select a tier with -bench and hold the iteration count at one so the reported
// ns/op is the elapsed time of a single cycle:
//
//	go test -run '^$' -benchtime 1x -bench 'BenchmarkIgnorerSelectionCycle/candidates=10000' ./pkg/synchronization/gitworktree
//	go test -run '^$' -benchtime 1x -bench 'BenchmarkIgnorerSelectionCycle/candidates=100000' ./pkg/synchronization/gitworktree
//	go test -run '^$' -benchtime 1x -bench 'BenchmarkIgnorerSelectionCycle/candidates=1000000' ./pkg/synchronization/gitworktree
//
// Reported metrics are elapsed ns/op plus go_heap_sys_bytes, the Go heap the
// selector itself reserved. Peak memory of the whole selection — this process
// plus the Git children it owns — is captured by wrapping that command in the
// platform's resource reporter, `/usr/bin/time -v` on Linux or `/usr/bin/time
// -l` on macOS, because Go cannot observe a child process's peak resident set
// portably.
func BenchmarkIgnorerSelectionCycle(b *testing.B) {
	for _, candidatePaths := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("candidates=%d", candidatePaths), func(b *testing.B) {
			root := b.TempDir()
			candidates := materializeSelectionCorpus(b, root, candidatePaths)
			ignorer, err := NewIgnorer(root, nil, nil)
			if err != nil {
				b.Fatal("unable to construct Git selector:", err)
			}
			b.Cleanup(func() { ignorer.Close() })

			runtime.GC()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := ignorer.Refresh(); err != nil {
					b.Fatal("unable to refresh Git selection policy:", err)
				}
				for _, candidate := range candidates {
					status, _ := ignorer.Ignore(candidate.path, candidate.directory)
					if selected := status != ignore.IgnoreStatusIgnored; selected != candidate.selected {
						b.Fatalf("unexpected selection for %q: selected=%t", candidate.path, selected)
					}
				}
				if err := ignorer.SelectionError(); err != nil {
					b.Fatal("stable Git selection policy reported a failure:", err)
				}
			}
			b.StopTimer()
			var statistics runtime.MemStats
			runtime.ReadMemStats(&statistics)
			b.ReportMetric(float64(statistics.HeapSys), "go_heap_sys_bytes")
		})
	}
}

// selectionCandidate is one path the benchmark asks the selector to decide,
// with the decision Git itself makes for it.
type selectionCandidate struct {
	path      string
	directory bool
	selected  bool
}

// materializeSelectionCorpus builds a real worktree of candidatePaths files and
// returns the candidate list in traversal order. Every fourth file is tracked,
// every fourth is Git-ignored by the repository policy, and one subtree is an
// ignored directory that carries its own .gitignore.
func materializeSelectionCorpus(b *testing.B, root string, candidatePaths int) []selectionCandidate {
	b.Helper()
	runGitBenchmark(b, root, "init", "--quiet")
	writeBenchmarkFile(b, filepath.Join(root, ".gitignore"), "*.ignored\nignored-subtree/\n")

	const filesPerDirectory = 1_000
	candidates := make([]selectionCandidate, 0, candidatePaths+candidatePaths/filesPerDirectory+2)
	tracked := make([]string, 0, candidatePaths/4)
	for index := 0; index < candidatePaths; index++ {
		if index%filesPerDirectory == 0 {
			directory := fmt.Sprintf("directory-%06d", index/filesPerDirectory)
			if err := os.Mkdir(filepath.Join(root, directory), 0o700); err != nil {
				b.Fatal("unable to create corpus directory:", err)
			}
			candidates = append(candidates, selectionCandidate{path: directory, directory: true, selected: true})
		}
		path := fmt.Sprintf("directory-%06d/entry-%09d", index/filesPerDirectory, index)
		if index%4 == 0 {
			path += ".ignored"
		} else {
			path += ".txt"
		}
		writeBenchmarkFile(b, filepath.Join(root, filepath.FromSlash(path)), "")
		if index%4 == 1 {
			tracked = append(tracked, path)
		}
		candidates = append(candidates, selectionCandidate{path: path, selected: index%4 != 0})
	}

	// A nested policy file inside a directory the repository ignores still
	// decides selection below it, so the corpus carries one.
	if err := os.Mkdir(filepath.Join(root, "ignored-subtree"), 0o700); err != nil {
		b.Fatal("unable to create corpus directory:", err)
	}
	writeBenchmarkFile(b, filepath.Join(root, "ignored-subtree", ".gitignore"), "*.nested\n")
	writeBenchmarkFile(b, filepath.Join(root, "ignored-subtree", "entry.nested"), "")
	candidates = append(candidates,
		selectionCandidate{path: "ignored-subtree", directory: true},
		selectionCandidate{path: "ignored-subtree/entry.nested"},
	)

	// Track a quarter of the corpus by writing the index directly. Every corpus
	// file is empty, so they all share Git's empty-blob object and `git add`
	// would spend its time creating a quarter of a million identical loose
	// objects; update-index produces the same index without them.
	const emptyBlobObject = "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
	var indexInfo strings.Builder
	for _, path := range tracked {
		fmt.Fprintf(&indexInfo, "100644 %s\t%s\x00", emptyBlobObject, path)
	}
	runGitBenchmarkWithInput(b, root, indexInfo.String(), "update-index", "-z", "--index-info")
	return candidates
}

func writeBenchmarkFile(b *testing.B, path, contents string) {
	b.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		b.Fatal("unable to write corpus file:", err)
	}
}

func runGitBenchmark(b *testing.B, root string, arguments ...string) {
	b.Helper()
	runGitBenchmarkWithInput(b, root, "", arguments...)
}

func runGitBenchmarkWithInput(b *testing.B, root, input string, arguments ...string) {
	b.Helper()
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	if input != "" {
		command.Stdin = strings.NewReader(input)
	}
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

// TestIgnorerReportsPolicyChangedDuringTraversal covers the generation contract
// for the scan boundary: the snapshot that a traversal produced is only valid
// for the Git policy generation that Refresh accepted, so a policy edit landing
// between Refresh and the post-scan check has to surface as the typed
// unavailable result rather than as a successful, partially stale snapshot.
func TestIgnorerReportsPolicyChangedDuringTraversal(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	ignorePath := filepath.Join(root, ".gitignore")
	if err := os.WriteFile(ignorePath, []byte("*.log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, root, "value.cache")

	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if !selectorSelectsPath(ignorer, "value.cache") {
		t.Fatal("selector ignored a path before its policy rule was added")
	}
	if err := ignorer.SelectionError(); err != nil {
		t.Fatal("unchanged Git policy reported a selection failure:", err)
	}

	// The traversal is already in flight at this point, so this edit cannot be
	// applied to the snapshot being produced.
	if err := os.WriteFile(ignorePath, []byte("*.log\nvalue.cache\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ignorer.SelectionError(); err == nil || err.Error() != "git_selection_unavailable" {
		t.Fatalf("Git policy changed during traversal did not fail closed: %v", err)
	}
	if err := ignorer.Refresh(); err != nil {
		t.Fatal("unable to refresh changed Git policy:", err)
	}
	if err := ignorer.SelectionError(); err != nil {
		t.Fatal("refreshed Git policy still reported a selection failure:", err)
	}
	if selectorSelectsPath(ignorer, "value.cache") || gitSelectsPath(t, root, "value.cache") {
		t.Fatal("changed Git policy was not applied at the refresh boundary")
	}
}

// TestIgnorerReportsNestedPolicyIgnoredByParentChangedDuringTraversal pins the
// policy inventory itself: a nested .gitignore that the repository ignores
// still decides selection for its descendants, so it has to participate in the
// generation fingerprint. Applying repository excludes to the inventory hides
// exactly this file and silently accepts a stale snapshot.
func TestIgnorerReportsNestedPolicyIgnoredByParentChangedDuringTraversal(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("nested/.gitignore\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nestedIgnorePath := filepath.Join(root, "nested", ".gitignore")
	writeFixtureFile(t, root, "nested/.gitignore")
	if err := os.WriteFile(nestedIgnorePath, []byte("*.other\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, root, "nested/value.secret")

	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if !selectorSelectsPath(ignorer, "nested/value.secret") {
		t.Fatal("selector ignored a path before its nested policy rule was added")
	}
	if err := ignorer.SelectionError(); err != nil {
		t.Fatal("unchanged nested Git policy reported a selection failure:", err)
	}

	if err := os.WriteFile(nestedIgnorePath, []byte("*.secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ignorer.SelectionError(); err == nil || err.Error() != "git_selection_unavailable" {
		t.Fatalf("ignored nested Git policy changed during traversal did not fail closed: %v", err)
	}
	if err := ignorer.Refresh(); err != nil {
		t.Fatal("unable to refresh changed nested Git policy:", err)
	}
	if selectorSelectsPath(ignorer, "nested/value.secret") || gitSelectsPath(t, root, "nested/value.secret") {
		t.Fatal("changed nested Git policy was not applied at the refresh boundary")
	}
}

// TestIgnorerReportsIndexChangedDuringTraversal covers the other selection
// input that the scan boundary reads once: the tracked set loaded at Refresh
// decides whether Git-ignored paths are force-selected.
func TestIgnorerReportsIndexChangedDuringTraversal(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFixtureFile(t, root, "tracked.log")

	ignorer, err := NewIgnorer(root, nil, nil)
	if err != nil {
		t.Fatal("unable to construct Git selector:", err)
	}
	t.Cleanup(func() { ignorer.Close() })
	if selectorSelectsPath(ignorer, "tracked.log") {
		t.Fatal("selector selected an ignored, untracked path")
	}
	if err := ignorer.SelectionError(); err != nil {
		t.Fatal("unchanged Git index reported a selection failure:", err)
	}

	runGit(t, root, "add", "-f", "tracked.log")
	if err := ignorer.SelectionError(); err == nil || err.Error() != "git_selection_unavailable" {
		t.Fatalf("Git index changed during traversal did not fail closed: %v", err)
	}
	if err := ignorer.Refresh(); err != nil {
		t.Fatal("unable to refresh changed Git index:", err)
	}
	if !selectorSelectsPath(ignorer, "tracked.log") {
		t.Fatal("newly tracked path was not selected at the refresh boundary")
	}
}

// TestCheckIgnoreOracleFramingMatchesGit characterizes the exact tuple that
// `git check-ignore --stdin -z --verbose --non-matching --no-index` emits, so
// the strict parser below cannot drift away from the framing it validates.
func TestCheckIgnoreOracleFramingMatchesGit(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "--quiet")
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.secret\n!keep.secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", "info", "exclude"), []byte("info.tmp\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command("git", "-C", root, "check-ignore", "--stdin", "-z", "--verbose", "--non-matching", "--no-index")
	command.Stdin = strings.NewReader("value.secret\x00keep.secret\x00info.tmp\x00ordinary.txt\x00")
	output, err := command.Output()
	if err != nil {
		t.Fatal("unable to run Git ignore oracle:", err)
	}
	fields := strings.Split(string(output), "\x00")
	if len(fields) == 0 || fields[len(fields)-1] != "" {
		t.Fatalf("Git ignore oracle output was not NUL terminated: %q", output)
	}
	fields = fields[:len(fields)-1]
	if len(fields) != 16 {
		t.Fatalf("Git ignore oracle did not emit one four-field tuple per path: %q", fields)
	}
	for index, expected := range [][4]string{
		{".gitignore", "1", "*.secret", "value.secret"},
		{".gitignore", "2", "!keep.secret", "keep.secret"},
		{filepath.Join(".git", "info", "exclude"), "1", "info.tmp", "info.tmp"},
		{"", "", "", "ordinary.txt"},
	} {
		tuple := fields[index*4 : index*4+4]
		source := filepath.FromSlash(tuple[0])
		if source != expected[0] || tuple[1] != expected[1] || tuple[2] != expected[2] || tuple[3] != expected[3] {
			t.Fatalf("unexpected Git ignore oracle tuple %d: got %q, want %q", index, tuple, expected)
		}
	}
}

// TestCheckIgnoreOracleRejectsMalformedTuples proves that every field of the
// tuple is validated. Only the fully populated matching shape and the fully
// empty non-matching shape are selection decisions; anything else means the
// oracle is not the process this selector believes it is talking to and must
// fail closed instead of being interpreted.
func TestCheckIgnoreOracleRejectsMalformedTuples(t *testing.T) {
	for _, test := range []struct {
		name     string
		response string
		ignored  bool
		rejected bool
	}{
		{name: "matching", response: ".gitignore\x001\x00*.secret\x00value.secret\x00", ignored: true},
		{name: "negated", response: ".gitignore\x002\x00!value.secret\x00value.secret\x00"},
		{name: "nonMatching", response: "\x00\x00\x00value.secret\x00"},
		{name: "pathMismatch", response: "\x00\x00\x00other.secret\x00", rejected: true},
		{name: "sourceWithoutLineNumber", response: ".gitignore\x00\x00*.secret\x00value.secret\x00", rejected: true},
		{name: "sourceWithoutPattern", response: ".gitignore\x001\x00\x00value.secret\x00", rejected: true},
		{name: "patternWithoutSource", response: "\x001\x00*.secret\x00value.secret\x00", rejected: true},
		{name: "nonNumericLineNumber", response: ".gitignore\x00one\x00*.secret\x00value.secret\x00", rejected: true},
		{name: "zeroLineNumber", response: ".gitignore\x000\x00*.secret\x00value.secret\x00", rejected: true},
		{name: "negativeLineNumber", response: ".gitignore\x00-1\x00*.secret\x00value.secret\x00", rejected: true},
		{name: "truncatedTuple", response: ".gitignore\x001\x00*.secret\x00", rejected: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			oracle := &checkIgnoreOracle{
				writer: bufio.NewWriter(io.Discard),
				reader: bufio.NewReader(strings.NewReader(test.response)),
			}
			ignored, err := oracle.ignored("value.secret")
			if test.rejected {
				if err == nil {
					t.Fatalf("malformed Git ignore oracle response was accepted: ignored=%t", ignored)
				}
				if !oracle.failed {
					t.Fatal("malformed Git ignore oracle response did not mark the oracle failed")
				}
				return
			}
			if err != nil {
				t.Fatal("well-formed Git ignore oracle response was rejected:", err)
			}
			if oracle.failed {
				t.Fatal("well-formed Git ignore oracle response marked the oracle failed")
			}
			if ignored != test.ignored {
				t.Fatalf("unexpected selection decision: got %t, want %t", ignored, test.ignored)
			}
		})
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
