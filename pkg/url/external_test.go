package url

import (
	"strings"
	"testing"
)

func TestExternalProtocolTextEncoding(t *testing.T) {
	encoded, err := Protocol_External.MarshalText()
	if err != nil {
		t.Fatal("unable to marshal External protocol:", err)
	} else if string(encoded) != "external" {
		t.Fatal("unexpected External protocol text encoding:", string(encoded))
	}

	var decoded Protocol
	if err := decoded.UnmarshalText([]byte("external")); err != nil {
		t.Fatal("unable to unmarshal External protocol:", err)
	} else if decoded != Protocol_External {
		t.Fatal("unexpected decoded protocol:", decoded)
	}
}

func TestExternalURLRoundTrip(t *testing.T) {
	original := &URL{
		Kind:     Kind_Synchronization,
		Protocol: Protocol_External,
		Host:     "ws1_mzxw42tbnrfwqy3cmfvgge",
	}

	if err := original.EnsureValid(); err != nil {
		t.Fatal("valid External URL rejected:", err)
	}

	formatted := original.Format("")
	if formatted != "external://ws1_mzxw42tbnrfwqy3cmfvgge" {
		t.Fatal("formatted External URL is not the bare opaque endpoint form:", formatted)
	}
	parsed, err := Parse(formatted, Kind_Synchronization, false)
	if err != nil {
		t.Fatal("unable to parse formatted External URL:", err)
	} else if !original.Equal(parsed) {
		t.Fatalf("External URL round-trip mismatch:\noriginal: %#v\nparsed:   %#v", original, parsed)
	}
}

func TestParseExternalURLIsStrict(t *testing.T) {
	valid := "external://ws1_mzxw42tbnrfwqy3cmfvgge"
	if parsed, err := Parse(valid, Kind_Synchronization, false); err != nil {
		t.Fatalf("valid external URL rejected: %v", err)
	} else if parsed.Protocol != Protocol_External || parsed.Host != "ws1_mzxw42tbnrfwqy3cmfvgge" {
		t.Fatalf("unexpected parsed external URL: %#v", parsed)
	} else if parsed.Path != "" {
		t.Fatal("opaque external URL parsed with a path:", parsed.Path)
	} else if len(parsed.Parameters) != 0 {
		t.Fatal("opaque external URL parsed with parameters:", parsed.Parameters)
	}

	for _, raw := range []string{
		// Unknown or malformed schemes.
		"happier://ws1_mzxw42tbnrfwqy3cmfvgge",
		"EXTERNAL://ws1_mzxw42tbnrfwqy3cmfvgge",
		"external:ws1_mzxw42tbnrfwqy3cmfvgge",
		// Path-bearing forms.
		"external://machine-01/workspace",
		"external://machine-01/",
		// Query-bearing forms, including the retired path/root grant parameters.
		"external://machine-01?path=%2Fworkspace",
		"external://machine-01?path=%2Fworkspace&rootGrantId=grant-01",
		"external://machine-01?rootGrantId=grant-01",
		"external://machine-01?route=direct",
		"external://machine-01?bearer=secret",
		// Fragment-bearing forms.
		"external://machine-01#fragment",
		// Userinfo- and port-bearing forms.
		"external://user:secret@machine-01",
		"external://machine-01:443",
		// Empty endpoint identifiers.
		"external://",
		// Oversized endpoint identifiers.
		"external://" + strings.Repeat("a", MaxExternalEndpointIdentifierBytes+1),
		// Endpoint identifiers with forbidden characters or separators.
		"external://Machine-01",
		"external://machine 01",
		"external://machine/01",
		"external://-machine-01",
		"external://machine-01?",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := Parse(raw, Kind_Synchronization, false); err == nil {
				t.Fatalf("malformed external URL accepted: %q", raw)
			}
		})
	}
}

func TestParseExternalURLAcceptsMaximumLengthIdentifier(t *testing.T) {
	identifier := "ws1_" + strings.Repeat("a", MaxExternalEndpointIdentifierBytes-4)
	parsed, err := Parse("external://"+identifier, Kind_Synchronization, false)
	if err != nil {
		t.Fatalf("maximum-length endpoint identifier rejected: %v", err)
	} else if parsed.Host != identifier {
		t.Fatal("maximum-length endpoint identifier mismatch:", parsed.Host)
	}
}

func TestExternalURLEnsureValidRejectsMissingAuthority(t *testing.T) {
	if (&URL{Protocol: Protocol_External, Kind: Kind_Synchronization}).EnsureValid() == nil {
		t.Fatal("External URL without an endpoint identifier accepted")
	}
}

func TestExternalURLEnsureValidRejectsUnsupportedFields(t *testing.T) {
	base := func() *URL {
		return &URL{
			Kind:     Kind_Synchronization,
			Protocol: Protocol_External,
			Host:     "ws1_mzxw42tbnrfwqy3cmfvgge",
		}
	}

	testCases := map[string]func(*URL){
		"path":        func(candidate *URL) { candidate.Path = "/workspace" },
		"user":        func(candidate *URL) { candidate.User = "alice" },
		"port":        func(candidate *URL) { candidate.Port = 443 },
		"environment": func(candidate *URL) { candidate.Environment = map[string]string{"KEY": "value"} },
		"parameter":   func(candidate *URL) { candidate.Parameters = map[string]string{"rootGrantId": "grant-01"} },
		"kind":        func(candidate *URL) { candidate.Kind = Kind_Forwarding },
	}

	for name, mutate := range testCases {
		t.Run(name, func(t *testing.T) {
			candidate := base()
			mutate(candidate)
			if candidate.EnsureValid() == nil {
				t.Fatal("invalid External URL accepted")
			}
		})
	}
}
