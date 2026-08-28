package url

import "testing"

func TestHappierProtocolTextEncoding(t *testing.T) {
	encoded, err := Protocol_Happier.MarshalText()
	if err != nil {
		t.Fatal("unable to marshal Happier protocol:", err)
	} else if string(encoded) != "happier" {
		t.Fatal("unexpected Happier protocol text encoding:", string(encoded))
	}

	var decoded Protocol
	if err := decoded.UnmarshalText([]byte("happier")); err != nil {
		t.Fatal("unable to unmarshal Happier protocol:", err)
	} else if decoded != Protocol_Happier {
		t.Fatal("unexpected decoded protocol:", decoded)
	}
}

func TestHappierURLRoundTrip(t *testing.T) {
	original := &URL{
		Kind:     Kind_Synchronization,
		Protocol: Protocol_Happier,
		Host:     "machine-01",
		Path:     `C:\\Users\\alice\\work tree`,
		Parameters: map[string]string{
			HappierRootGrantIdentifierParameter: "grant-01",
		},
	}

	if err := original.EnsureValid(); err != nil {
		t.Fatal("valid Happier URL rejected:", err)
	}

	formatted := original.Format("")
	parsed, err := Parse(formatted, Kind_Synchronization, false)
	if err != nil {
		t.Fatal("unable to parse formatted Happier URL:", err)
	} else if !original.Equal(parsed) {
		t.Fatalf("Happier URL round-trip mismatch:\noriginal: %#v\nparsed:   %#v", original, parsed)
	}
}

func TestHappierURLEnsureValidRejectsMissingAuthority(t *testing.T) {
	testCases := map[string]*URL{
		"machine": {
			Protocol: Protocol_Happier,
			Path:     "/workspace",
			Parameters: map[string]string{
				HappierRootGrantIdentifierParameter: "grant-01",
			},
		},
		"root grant": {
			Protocol: Protocol_Happier,
			Host:     "machine-01",
			Path:     "/workspace",
		},
	}

	for name, candidate := range testCases {
		t.Run(name, func(t *testing.T) {
			if candidate.EnsureValid() == nil {
				t.Fatal("invalid Happier URL accepted")
			}
		})
	}
}

func TestHappierURLEnsureValidRejectsUnsupportedFields(t *testing.T) {
	base := func() *URL {
		return &URL{
			Protocol: Protocol_Happier,
			Host:     "machine-01",
			Path:     "/workspace",
			Parameters: map[string]string{
				HappierRootGrantIdentifierParameter: "grant-01",
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
				t.Fatal("invalid Happier URL accepted")
			}
		})
	}
}
