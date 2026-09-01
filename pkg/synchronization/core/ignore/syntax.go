package ignore

import (
	"fmt"
)

// IsDefault indicates whether or not the ignore syntax is Syntax_SyntaxDefault.
func (s Syntax) IsDefault() bool {
	return s == Syntax_SyntaxDefault
}

// MarshalText implements encoding.TextMarshaler.MarshalText.
func (s Syntax) MarshalText() ([]byte, error) {
	var result string
	switch s {
	case Syntax_SyntaxDefault:
	case Syntax_SyntaxMutagen:
		result = "mutagen"
	case Syntax_SyntaxDocker:
		result = "docker"
	case Syntax_SyntaxGitWorktree:
		result = "git-worktree"
	default:
		result = "unknown"
	}
	return []byte(result), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.UnmarshalText.
func (s *Syntax) UnmarshalText(textBytes []byte) error {
	// Convert the bytes to a string.
	text := string(textBytes)

	// Convert to an ignore syntax.
	switch text {
	case "mutagen":
		*s = Syntax_SyntaxMutagen
	case "docker":
		*s = Syntax_SyntaxDocker
	case "git-worktree":
		*s = Syntax_SyntaxGitWorktree
	default:
		return fmt.Errorf("unknown ignore syntax specification: %s", text)
	}

	// Success.
	return nil
}

// Supported indicates whether or not a particular ignore syntax is a valid,
// non-default value.
func (s Syntax) Supported() bool {
	switch s {
	case Syntax_SyntaxMutagen:
		return true
	case Syntax_SyntaxDocker:
		return true
	case Syntax_SyntaxGitWorktree:
		return true
	default:
		return false
	}
}

// Description returns a human-readable description of an ignore syntax.
func (s Syntax) Description() string {
	switch s {
	case Syntax_SyntaxDefault:
		return "Default"
	case Syntax_SyntaxMutagen:
		return "Mutagen"
	case Syntax_SyntaxDocker:
		return "Docker"
	case Syntax_SyntaxGitWorktree:
		return "Git worktree"
	default:
		return "Unknown"
	}
}
