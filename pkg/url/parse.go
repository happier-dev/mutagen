package url

import (
	"errors"
)

// Parse parses a raw URL string into a URL type. It accepts information about
// the URL kind (e.g. synchronization vs. forwarding) and position (i.e. the URL
// is considered an alpha/source URL if first is true and a beta/destination URL
// otherwise).
func Parse(raw string, kind Kind, first bool) (*URL, error) {
	// Ensure that the kind is supported.
	if !kind.Supported() {
		panic("unsupported URL kind")
	}

	// Don't allow empty raw URLs.
	if raw == "" {
		return nil, errors.New("empty URL")
	}

	// Dispatch URL parsing based on type. We have to be careful about the
	// ordering here because URLs may be classified as multiple types (e.g. a
	// Docker URL would also be classified as an SCP-style SSH URL), but we only
	// want them to be parsed according to the better and more specific match.
	// If we don't match anything, we assume the URL is a local path.
	if isExternalURL(raw) {
		return parseExternal(raw, kind)
	} else if isDockerURL(raw) {
		return parseDocker(raw, kind, first)
	} else if kind == Kind_Synchronization && hasHierarchicalScheme(raw) {
		// Synchronization URLs with an explicit hierarchical scheme that isn't
		// one of the supported schemes are rejected outright. Without this
		// check, an unknown scheme:// URL would silently fall through to
		// SCP-style SSH parsing and be misinterpreted with the scheme name as
		// the hostname.
		return nil, errors.New("unknown URL scheme")
	} else if isSCPSSHURL(raw, kind) {
		return parseSCPSSH(raw, kind)
	} else {
		return parseLocal(raw, kind)
	}
}

// hasHierarchicalScheme determines whether or not a raw URL begins with a
// syntactically valid scheme followed by an hierarchical authority marker
// (i.e. "scheme://"). It is used to reject unknown schemes instead of
// misinterpreting them as SCP-style SSH URLs or local paths.
func hasHierarchicalScheme(raw string) bool {
	// Scan the leading scheme characters.
	index := 0
	for index < len(raw) {
		character := raw[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' {
			// Any position is fine for a letter.
		} else if character >= '0' && character <= '9' || character == '+' || character == '-' || character == '.' {
			// Digits and scheme symbols are only valid after the first
			// character.
			if index == 0 {
				return false
			}
		} else {
			break
		}
		index++
	}

	// There must be at least one scheme character and the scheme must be
	// followed by the hierarchical authority marker.
	return index > 0 && index <= len(raw)-3 && raw[index] == ':' && raw[index+1:index+3] == "//"
}
