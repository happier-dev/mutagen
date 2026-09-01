// Package gitworktree applies endpoint-local Git worktree selection policy at
// Mutagen's scanner boundary. Git remains the authority for Git ignore syntax;
// Happier never supplies a precomputed workspace entry list.
package gitworktree

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
	mutagenignore "github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore/mutagen"
)

var errSelectionUnavailable = errors.New("git_selection_unavailable")

type Ignorer struct {
	root          string
	extraIgnores  []string
	extraIncludes []string
	overlay       ignore.Ignorer
	tracked       map[string]struct{}
	trackedPrefix map[string]struct{}
	submodules    map[string]struct{}
	oracle        *checkIgnoreOracle
	fingerprint   string
}

func NewIgnorer(root string, extraIgnores, extraIncludes []string) (*Ignorer, error) {
	result := &Ignorer{
		root:          root,
		extraIgnores:  append([]string(nil), extraIgnores...),
		extraIncludes: append([]string(nil), extraIncludes...),
	}
	if err := result.Refresh(); err != nil {
		return nil, err
	}
	return result, nil
}

// Refresh atomically reloads Git policy and index state at a scanner-cycle
// boundary. Only Git policy files and the index are inventoried; workspace
// contents are evaluated lazily as Mutagen traverses them.
func (i *Ignorer) Refresh() error {
	fingerprintBefore, err := policyFingerprint(i.root)
	if err != nil {
		return errSelectionUnavailable
	}
	if i.oracle != nil && !i.oracle.failed && fingerprintBefore == i.fingerprint {
		return nil
	}

	patterns, err := CompilePatterns(i.root, i.extraIgnores, i.extraIncludes)
	if err != nil {
		return err
	}
	overlay, err := mutagenignore.NewIgnorer(patterns)
	if err != nil {
		return errSelectionUnavailable
	}
	tracked, trackedPrefixes, submodules, err := loadIndex(i.root)
	if err != nil {
		return errSelectionUnavailable
	}
	oracle, err := newCheckIgnoreOracle(i.root)
	if err != nil {
		return errSelectionUnavailable
	}
	if _, err := oracle.ignored(".gitignore"); err != nil {
		oracle.close()
		return errSelectionUnavailable
	}
	fingerprintAfter, err := policyFingerprint(i.root)
	if err != nil || fingerprintAfter != fingerprintBefore {
		oracle.close()
		return errSelectionUnavailable
	}

	previous := i.oracle
	i.overlay = overlay
	i.tracked = tracked
	i.trackedPrefix = trackedPrefixes
	i.submodules = submodules
	i.oracle = oracle
	i.fingerprint = fingerprintAfter
	if previous != nil {
		previous.close()
	}
	return nil
}

// Close releases the endpoint-local Git oracle process.
func (i *Ignorer) Close() {
	if i.oracle != nil {
		i.oracle.close()
		i.oracle = nil
	}
}

// SelectionError reports a Git oracle failure observed during traversal. The
// scanner checks this after traversal so an unavailable oracle cannot turn a
// fail-closed partial snapshot into a successful relationship cycle.
func (i *Ignorer) SelectionError() error {
	if i.oracle != nil && i.oracle.failed {
		return errSelectionUnavailable
	}
	return nil
}

func (i *Ignorer) Ignore(path string, directory bool) (ignore.IgnoreStatus, bool) {
	path = filepath.ToSlash(path)
	if path == ".git" || strings.HasPrefix(path, ".git/") || strings.Contains(path, "/.git/") || strings.HasSuffix(path, "/.git") {
		return ignore.IgnoreStatusIgnored, false
	}

	overlayStatus, overlayTraversal := i.overlay.Ignore(path, directory)
	if overlayStatus != ignore.IgnoreStatusNominal {
		return overlayStatus, overlayTraversal
	}
	if i.withinSubmodule(path) {
		return ignore.IgnoreStatusIgnored, false
	}
	if _, ok := i.tracked[path]; ok {
		return ignore.IgnoreStatusUnignored, false
	}

	ignoredByGit, err := i.oracle.ignored(path)
	if err != nil {
		// Ignore cannot return an error. Exclusion is the only safe result until
		// the next Refresh reports the typed selection failure.
		return ignore.IgnoreStatusIgnored, false
	}
	if ignoredByGit {
		if directory {
			if _, containsTrackedPath := i.trackedPrefix[path]; containsTrackedPath {
				return ignore.IgnoreStatusIgnored, true
			}
			if i.includeCouldMatchDescendant(path) {
				return ignore.IgnoreStatusIgnored, true
			}
		}
		return ignore.IgnoreStatusIgnored, overlayTraversal
	}
	return ignore.IgnoreStatusNominal, overlayTraversal
}

func (i *Ignorer) includeCouldMatchDescendant(directory string) bool {
	for _, pattern := range i.extraIncludes {
		pattern = strings.TrimPrefix(filepath.ToSlash(strings.TrimPrefix(pattern, "!")), "/")
		firstSlash := strings.IndexByte(pattern, '/')
		if firstSlash < 0 {
			return true
		}
		literalEnd := len(pattern)
		hasWildcard := false
		for index, character := range pattern {
			if strings.ContainsRune("*?[{", character) {
				literalEnd = index
				hasWildcard = true
				break
			}
		}
		literalPrefix := strings.TrimSuffix(pattern[:literalEnd], "/")
		if literalPrefix == "" || strings.HasPrefix(literalPrefix, directory+"/") || (hasWildcard && (directory == literalPrefix || strings.HasPrefix(directory, literalPrefix+"/"))) {
			return true
		}
	}
	return false
}

func (i *Ignorer) withinSubmodule(path string) bool {
	for parent := filepath.ToSlash(filepath.Dir(path)); parent != "." && parent != ""; parent = filepath.ToSlash(filepath.Dir(parent)) {
		if _, ok := i.submodules[parent]; ok {
			return true
		}
	}
	return false
}

// CompilePatterns validates and compiles only the explicit policy overlay.
// Repository ignore syntax is evaluated by Git and is never translated.
func CompilePatterns(root string, extraIgnores, extraIncludes []string) ([]string, error) {
	if _, _, _, err := resolveGitPolicyPaths(root); err != nil {
		return nil, errSelectionUnavailable
	}
	patterns := make([]string, 0, len(extraIgnores)+len(extraIncludes)+1)
	for _, pattern := range extraIgnores {
		if pattern == "" || len(pattern) > 4096 || strings.ContainsRune(pattern, '\x00') {
			return nil, errSelectionUnavailable
		}
		patterns = append(patterns, pattern)
	}
	for _, pattern := range extraIncludes {
		pattern = strings.TrimPrefix(pattern, "!")
		if pattern == "" || len(pattern) > 4096 || strings.ContainsRune(pattern, '\x00') {
			return nil, errSelectionUnavailable
		}
		patterns = append(patterns, "!"+pattern)
	}
	// Keep this final so an explicit include cannot select repository metadata.
	patterns = append(patterns, "/**/.git")
	return patterns, nil
}

func loadIndex(root string) (map[string]struct{}, map[string]struct{}, map[string]struct{}, error) {
	output, err := gitOutput(root, "ls-files", "--stage", "-z", "--cached")
	if err != nil {
		return nil, nil, nil, err
	}
	tracked := make(map[string]struct{})
	prefixes := make(map[string]struct{})
	submodules := make(map[string]struct{})
	for _, record := range splitNUL(output) {
		tab := bytes.IndexByte(record, '\t')
		if tab < 0 {
			return nil, nil, nil, errors.New("malformed Git index output")
		}
		metadata, pathBytes := record[:tab], record[tab+1:]
		fields := bytes.Fields(metadata)
		if len(fields) != 3 || len(pathBytes) == 0 {
			return nil, nil, nil, errors.New("malformed Git index output")
		}
		path := filepath.ToSlash(string(pathBytes))
		if !validRelativePath(path) {
			return nil, nil, nil, errors.New("invalid Git index path")
		}
		tracked[path] = struct{}{}
		if string(fields[0]) == "160000" {
			submodules[path] = struct{}{}
		}
		for parent := filepath.ToSlash(filepath.Dir(path)); parent != "." && parent != ""; parent = filepath.ToSlash(filepath.Dir(parent)) {
			prefixes[parent] = struct{}{}
		}
	}
	return tracked, prefixes, submodules, nil
}

func validRelativePath(path string) bool {
	return path != "" && path != "." && !filepath.IsAbs(path) && path != ".." && !strings.HasPrefix(path, "../") && !strings.ContainsRune(path, '\x00')
}

type checkIgnoreOracle struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	writer  *bufio.Writer
	reader  *bufio.Reader
	closed  bool
	failed  bool
}

func newCheckIgnoreOracle(root string) (*checkIgnoreOracle, error) {
	command := exec.Command("git", "-C", root, "check-ignore", "--stdin", "-z", "--verbose", "--non-matching", "--no-index")
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, err
	}
	command.Stderr = new(boundedBuffer)
	if err := command.Start(); err != nil {
		stdin.Close()
		return nil, err
	}
	return &checkIgnoreOracle{
		command: command,
		stdin:   stdin,
		writer:  bufio.NewWriter(stdin),
		reader:  bufio.NewReader(stdout),
	}, nil
}

func (o *checkIgnoreOracle) ignored(path string) (bool, error) {
	if o == nil || o.closed || !validRelativePath(path) {
		return false, errors.New("Git ignore oracle unavailable")
	}
	if _, err := o.writer.WriteString(path); err != nil {
		o.failed = true
		return false, err
	}
	if err := o.writer.WriteByte(0); err != nil {
		o.failed = true
		return false, err
	}
	if err := o.writer.Flush(); err != nil {
		o.failed = true
		return false, err
	}
	fields := make([][]byte, 4)
	for index := range fields {
		field, err := o.reader.ReadBytes(0)
		if err != nil {
			o.failed = true
			return false, err
		}
		fields[index] = field[:len(field)-1]
	}
	if !bytes.Equal(fields[3], []byte(path)) {
		o.failed = true
		return false, errors.New("Git ignore oracle response mismatch")
	}
	if len(fields[0]) == 0 {
		return false, nil
	}
	return !bytes.HasPrefix(fields[2], []byte("!")), nil
}

func (o *checkIgnoreOracle) close() {
	if o == nil || o.closed {
		return
	}
	o.closed = true
	o.writer.Flush()
	o.stdin.Close()
	o.command.Wait()
}

func gitOutput(root string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", append([]string{"-C", root}, arguments...)...)
	var output, diagnostics boundedBuffer
	command.Stdout = &output
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("git command failed: %w", err)
	}
	return append([]byte(nil), output.Bytes()...), nil
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(data []byte) (int, error) {
	const maximumGitOutput = 256 << 20
	if b.Len()+len(data) > maximumGitOutput {
		return 0, errors.New("git output exceeds selection limit")
	}
	return b.Buffer.Write(data)
}

func splitNUL(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}
	parts := bytes.Split(data, []byte{0})
	if len(parts[len(parts)-1]) == 0 {
		parts = parts[:len(parts)-1]
	}
	return parts
}

func policyFingerprint(root string) (string, error) {
	infoExclude, globalExclude, index, err := resolveGitPolicyPaths(root)
	if err != nil {
		return "", err
	}
	policyOutput, err := gitOutput(root, "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", ".gitignore", "**/.gitignore")
	if err != nil {
		return "", err
	}
	paths := []string{index, infoExclude}
	if globalExclude != "" {
		paths = append(paths, globalExclude)
	}
	for _, relative := range splitNUL(policyOutput) {
		path := filepath.ToSlash(string(relative))
		if !validRelativePath(path) {
			return "", errors.New("invalid Git policy path")
		}
		paths = append(paths, filepath.Join(root, filepath.FromSlash(path)))
	}
	hasher := sha256.New()
	for _, path := range paths {
		fmt.Fprintf(hasher, "%d:", len(path))
		hasher.Write([]byte(path))
		contents, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			hasher.Write([]byte("|missing\n"))
			continue
		}
		if err != nil {
			return "", err
		}
		fmt.Fprintf(hasher, "|%d:", len(contents))
		hasher.Write(contents)
		hasher.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func resolveGitPolicyPaths(root string) (string, string, string, error) {
	prefix, err := gitOutput(root, "rev-parse", "--show-prefix")
	if err != nil || len(bytes.TrimRight(prefix, "\r\n")) != 0 {
		return "", "", "", errors.New("root is not a Git worktree")
	}
	infoExclude, err := resolveGitPath(root, "info/exclude")
	if err != nil {
		return "", "", "", err
	}
	index, err := resolveGitPath(root, "index")
	if err != nil {
		return "", "", "", err
	}
	globalExclude, err := resolveGlobalExcludePath(root)
	if err != nil {
		return "", "", "", err
	}
	return infoExclude, globalExclude, index, nil
}

func resolveGitPath(root, path string) (string, error) {
	value, err := gitOutput(root, "rev-parse", "--path-format=absolute", "--git-path", path)
	if err != nil {
		return "", err
	}
	value = bytes.TrimRight(value, "\r\n")
	if len(value) == 0 || !filepath.IsAbs(string(value)) {
		return "", errors.New("invalid Git path")
	}
	return filepath.Clean(string(value)), nil
}

func resolveGlobalExcludePath(root string) (string, error) {
	command := exec.Command("git", "-C", root, "config", "--path", "--get", "core.excludesFile")
	var output, diagnostics boundedBuffer
	command.Stdout = &output
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) || exitError.ExitCode() != 1 || diagnostics.Len() != 0 {
			return "", err
		}
		if configHome := os.Getenv("XDG_CONFIG_HOME"); configHome != "" {
			if !filepath.IsAbs(configHome) {
				return "", errors.New("relative XDG_CONFIG_HOME")
			}
			return filepath.Join(configHome, "git", "ignore"), nil
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".config", "git", "ignore"), nil
	}
	path := string(bytes.TrimRight(output.Bytes(), "\r\n"))
	if path == "" {
		return "", nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	return filepath.Clean(path), nil
}

var _ ignore.Ignorer = (*Ignorer)(nil)
