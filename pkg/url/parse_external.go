package url

import (
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
)

const externalURLPrefix = "external://"

const externalPathQueryParameter = "path"

func isExternalURL(raw string) bool {
	// Dispatch case-insensitively so malformed casing reaches the external
	// parser and is rejected, rather than being misinterpreted as a local path.
	return strings.HasPrefix(strings.ToLower(raw), externalURLPrefix)
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
	} else if parsed.Hostname() == "" {
		return nil, errors.New("External URL with empty machine identifier")
	} else if parsed.Port() != "" {
		return nil, errors.New("External URL with port")
	} else if parsed.Path != "" {
		return nil, errors.New("External URL with path outside query")
	} else if parsed.Fragment != "" {
		return nil, errors.New("External URL with fragment")
	}

	query, err := neturl.ParseQuery(parsed.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("External URL with malformed query: %w", err)
	}
	if len(query) != 2 || len(query[externalPathQueryParameter]) != 1 || len(query[ExternalRootGrantIdentifierParameter]) != 1 {
		return nil, errors.New("External URL with invalid query parameters")
	}

	result := &URL{
		Kind:     kind,
		Protocol: Protocol_External,
		Host:     parsed.Hostname(),
		Path:     query.Get(externalPathQueryParameter),
		Parameters: map[string]string{
			ExternalRootGrantIdentifierParameter: query.Get(ExternalRootGrantIdentifierParameter),
		},
	}
	if err := result.EnsureValid(); err != nil {
		return nil, fmt.Errorf("invalid External URL: %w", err)
	}

	return result, nil
}
