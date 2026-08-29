package url

import "testing"

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
		Host:     "machine-01",
		Path:     `C:\\Users\\alice\\work tree`,
		Parameters: map[string]string{
			ExternalRootGrantIdentifierParameter: "grant-01",
		},
	}

	if err := original.EnsureValid(); err != nil {
		t.Fatal("valid External URL rejected:", err)
	}

	formatted := original.Format("")
	parsed, err := Parse(formatted, Kind_Synchronization, false)
	if err != nil {
		t.Fatal("unable to parse formatted External URL:", err)
	} else if !original.Equal(parsed) {
		t.Fatalf("External URL round-trip mismatch:\noriginal: %#v\nparsed:   %#v", original, parsed)
	}
}

func TestParseExternalURLIsStrict(t *testing.T) {
	valid := "external://machine-01?path=%2Fworkspace&rootGrantId=grant-01"
	if parsed, err := Parse(valid, Kind_Synchronization, false); err != nil {
		t.Fatalf("valid external URL rejected: %v", err)
	} else if parsed.Protocol != Protocol_External || parsed.Host != "machine-01" || parsed.Path != "/workspace" {
		t.Fatalf("unexpected parsed external URL: %#v", parsed)
	}

	for _, raw := range []string{
		"EXTERNAL://machine-01?path=%2Fworkspace&rootGrantId=grant-01",
		"external://machine-01/workspace?rootGrantId=grant-01&path=%2Fworkspace",
		"external://machine-01?path=%2Fworkspace&path=%2Fother&rootGrantId=grant-01",
		"external://machine-01?path=%2Fworkspace&rootGrantId=grant-01&extra=x",
		"external://machine-01?path=%2Fworkspace;rootGrantId=grant-01",
		"external://user:secret@machine-01?path=%2Fworkspace&rootGrantId=grant-01",
		"external://machine-01:443?path=%2Fworkspace&rootGrantId=grant-01",
		"external://machine-01?path=%2Fworkspace&rootGrantId=grant-01#fragment",
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := Parse(raw, Kind_Synchronization, false); err == nil {
				t.Fatalf("malformed external URL accepted: %q", raw)
			}
		})
	}
}

func TestExternalURLEnsureValidRejectsMissingAuthority(t *testing.T) {
	testCases := map[string]*URL{
		"machine": {
			Protocol: Protocol_External,
			Path:     "/workspace",
			Parameters: map[string]string{
				ExternalRootGrantIdentifierParameter: "grant-01",
			},
		},
		"root grant": {
			Protocol: Protocol_External,
			Host:     "machine-01",
			Path:     "/workspace",
		},
	}

	for name, candidate := range testCases {
		t.Run(name, func(t *testing.T) {
			if candidate.EnsureValid() == nil {
				t.Fatal("invalid External URL accepted")
			}
		})
	}
}

func TestExternalURLEnsureValidRejectsUnsupportedFields(t *testing.T) {
	base := func() *URL {
		return &URL{
			Protocol: Protocol_External,
			Host:     "machine-01",
			Path:     "/workspace",
			Parameters: map[string]string{
				ExternalRootGrantIdentifierParameter: "grant-01",
			},
		}
	}

	testCases := map[string]func(*URL){
		"user":        func(candidate *URL) { candidate.User = "alice" },
		"port":        func(candidate *URL) { candidate.Port = 443 },
		"environment": func(candidate *URL) { candidate.Environment = map[string]string{"KEY": "value"} },
		"parameter":   func(candidate *URL) { candidate.Parameters["route"] = "direct" },
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
