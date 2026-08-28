package url

import (
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
)

const happierURLPrefix = "happier://"

const happierPathQueryParameter = "path"

func isHappierURL(raw string) bool {
	return strings.HasPrefix(strings.ToLower(raw), happierURLPrefix)
}

func parseHappier(raw string, kind Kind) (*URL, error) {
	if kind != Kind_Synchronization {
		return nil, errors.New("Happier URLs only support synchronization endpoints")
	}

	parsed, err := neturl.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("unable to parse Happier URL: %w", err)
	} else if strings.ToLower(parsed.Scheme) != "happier" {
		return nil, errors.New("invalid Happier URL scheme")
	} else if parsed.User != nil {
		return nil, errors.New("Happier URL with user information")
	} else if parsed.Hostname() == "" {
		return nil, errors.New("Happier URL with empty machine identifier")
	} else if parsed.Port() != "" {
		return nil, errors.New("Happier URL with port")
	} else if parsed.Path != "" {
		return nil, errors.New("Happier URL with path outside query")
	} else if parsed.Fragment != "" {
		return nil, errors.New("Happier URL with fragment")
	}

	query := parsed.Query()
	if len(query) != 2 || len(query[happierPathQueryParameter]) != 1 || len(query[HappierRootGrantIdentifierParameter]) != 1 {
		return nil, errors.New("Happier URL with invalid query parameters")
	}

	result := &URL{
		Kind:     kind,
		Protocol: Protocol_Happier,
		Host:     parsed.Hostname(),
		Path:     query.Get(happierPathQueryParameter),
		Parameters: map[string]string{
			HappierRootGrantIdentifierParameter: query.Get(HappierRootGrantIdentifierParameter),
		},
	}
	if err := result.EnsureValid(); err != nil {
		return nil, fmt.Errorf("invalid Happier URL: %w", err)
	}

	return result, nil
}
