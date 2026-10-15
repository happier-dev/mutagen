package local

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
	"github.com/mutagen-io/mutagen/pkg/synchronization/gitworktree"
)

// DiagnoseSelection reads the live endpoint's scanner ignorer while holding
// its existing scan lock. A changed Git policy generation is unavailable until
// the next scanner Refresh; a diagnostic never manufactures a fresh policy.
func (e *endpoint) DiagnoseSelection(ctx context.Context, relativePath string) (gitworktree.SelectionVerdict, error) {
	if relativePath == "" || path.IsAbs(relativePath) || filepath.IsAbs(relativePath) || strings.ContainsRune(relativePath, '\\') || strings.ContainsRune(relativePath, 0) {
		return gitworktree.SelectionVerdict{}, errors.New("invalid selection diagnosis path")
	}
	components := strings.Split(relativePath, "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return gitworktree.SelectionVerdict{}, errors.New("invalid selection diagnosis path")
		}
	}
	if !e.lockScanLock(ctx) {
		return gitworktree.SelectionVerdict{}, ctx.Err()
	}
	defer e.unlockScanLock()

	verdict := gitworktree.SelectionVerdict{}
	for index := range components {
		candidate := strings.Join(components[:index+1], "/")
		last := index == len(components)-1
		information, err := os.Lstat(filepath.Join(e.root, filepath.FromSlash(candidate)))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return gitworktree.SelectionVerdict{}, gitworktree.SelectionUnavailableError("unable to observe entry kind")
		}
		directory := !last || (err == nil && information.IsDir())
		if !last && err == nil && !information.IsDir() {
			return gitworktree.SelectionVerdict{}, gitworktree.SelectionUnavailableError("path ancestor is not a directory")
		}
		if e.gitWorktreeIgnorer != nil {
			// The Git owner evaluates ancestor traversal and entry kind itself.
			// Only the final path is passed to avoid a second traversal decision.
			if last {
				verdict, err = e.gitWorktreeIgnorer.Diagnose(relativePath, directory)
				if err != nil {
					return gitworktree.SelectionVerdict{}, err
				}
			}
			continue
		}
		status, traversal := e.ignorer.Ignore(candidate, directory)
		switch status {
		case ignore.IgnoreStatusIgnored:
			reason := gitworktree.SelectionExcludedConfiguredRule
			if components[index] == ".git" {
				reason = gitworktree.SelectionExcludedRepositoryMetadata
			}
			verdict = gitworktree.SelectionVerdict{Excluded: true, Reason: reason}
			if last || !traversal {
				return verdict, nil
			}
		case ignore.IgnoreStatusUnignored:
			verdict = gitworktree.SelectionVerdict{}
		}
	}
	if e.gitWorktreeIgnorer != nil {
		if err := e.gitWorktreeIgnorer.SelectionError(); err != nil {
			return gitworktree.SelectionVerdict{}, err
		}
	}
	return verdict, nil
}
