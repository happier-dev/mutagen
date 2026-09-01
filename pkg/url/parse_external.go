package url

import (
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
)

const externalURLPrefix = "external://"
const externalSchemePrefix = "external:"

func isExternalURL(raw string) bool {
	// Dispatch case-insensitively so malformed casing reaches the external
	// parser and is rejected, rather than being misinterpreted as a local path.
	// Matching the bare scheme prefix ensures that scheme-matched but
	// non-hierarchical forms (e.g. "external:id") also reach the external
	// parser and fail its strict validation instead of being classified as
	// another protocol.
	return strings.HasPrefix(strings.ToLower(raw), externalSchemePrefix)
}

// validateExternalEndpointIdentifier enforces the generic opaque endpoint
// identifier contract. Endpoint identifiers are opaque: they carry no path,
// grant, bearer, or routing material of any kind. They are bounded in length
// and restricted to a conservative lowercase character set so that persisted
// endpoint URLs have exactly one canonical serialized form.
func validateExternalEndpointIdentifier(identifier string) error {
	if identifier == "" {
		return errors.New("External URL with empty endpoint identifier")
	} else if len(identifier) > MaxExternalEndpointIdentifierBytes {
		return errors.New("External URL with oversized endpoint identifier")
	}
	for index, character := range identifier {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case character == '.' || character == '_' || character == '-':
			if index == 0 {
				return errors.New("External URL with endpoint identifier leading separator")
			}
		default:
			return errors.New("External URL with disallowed endpoint identifier character")
		}
	}
	return nil
}

func parseExternal(raw string, kind Kind) (*URL, error) {
	if kind != Kind_Synchronization {
		return nil, errors.New("External URLs only support synchronization endpoints")
	}
	if !strings.HasPrefix(raw, externalURLPrefix) {
		return nil, errors.New("invalid External URL scheme")
	}

	parsed, err := neturl.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("unable to parse External URL: %w", err)
	} else if parsed.Scheme != "external" {
		return nil, errors.New("invalid External URL scheme")
	} else if parsed.User != nil {
		return nil, errors.New("External URL with user information")
	} else if parsed.Port() != "" {
		return nil, errors.New("External URL with port")
	} else if parsed.Path != "" {
		return nil, errors.New("External URL with path")
	} else if parsed.RawQuery != "" || parsed.ForceQuery {
		return nil, errors.New("External URL with query parameters")
	} else if parsed.Fragment != "" || parsed.RawFragment != "" {
		return nil, errors.New("External URL with fragment")
	}

	// The endpoint identifier is the entire URL authority. It is opaque: the
	// external transport resolves it to an authorized root and live operation
	// on the serving side, and no such material may be persisted in the URL.
	if err := validateExternalEndpointIdentifier(parsed.Host); err != nil {
		return nil, err
	}

	result := &URL{
		Kind:     kind,
		Protocol: Protocol_External,
		Host:     parsed.Host,
	}
	if err := result.EnsureValid(); err != nil {
		return nil, fmt.Errorf("invalid External URL: %w", err)
	}

	return result, nil
}
