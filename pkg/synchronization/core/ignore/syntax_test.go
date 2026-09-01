package ignore

import "testing"

func TestGitWorktreeSyntaxIsExplicitAndSupported(t *testing.T) {
	if !Syntax_SyntaxGitWorktree.Supported() {
		t.Fatal("Git worktree selection syntax is not supported")
	}
	encoded, err := Syntax_SyntaxGitWorktree.MarshalText()
	if err != nil {
		t.Fatal("unable to marshal Git worktree syntax:", err)
	}
	if string(encoded) != "git-worktree" {
		t.Fatalf("unexpected Git worktree syntax encoding: %q", encoded)
	}
	var decoded Syntax
	if err := decoded.UnmarshalText(encoded); err != nil {
		t.Fatal("unable to unmarshal Git worktree syntax:", err)
	}
	if decoded != Syntax_SyntaxGitWorktree {
		t.Fatalf("Git worktree syntax round trip mismatch: %v", decoded)
	}
}

// TestSyntaxIsDefault tests Syntax.IsDefault.
func TestSyntaxIsDefault(t *testing.T) {
	// Define test cases.
	tests := []struct {
		value    Syntax
		expected bool
	}{
		{Syntax_SyntaxDefault - 1, false},
		{Syntax_SyntaxDefault, true},
		{Syntax_SyntaxMutagen, false},
		{Syntax_SyntaxDocker, false},
		{Syntax_SyntaxGitWorktree, false},
		{Syntax_SyntaxGitWorktree + 1, false},
	}

	// Process test cases.
	for i, test := range tests {
		if result := test.value.IsDefault(); result && !test.expected {
			t.Errorf("test index %d: value was unexpectedly classified as default", i)
		} else if !result && test.expected {
			t.Errorf("test index %d: value was unexpectedly classified as non-default", i)
		}
	}
}

// TestSyntaxUnmarshalText tests Syntax.UnmarshalText.
func TestSyntaxUnmarshalText(t *testing.T) {
	// Define test cases.
	tests := []struct {
		text          string
		expectedMode  Syntax
		expectFailure bool
	}{
		{"", Syntax_SyntaxDefault, true},
		{"asdf", Syntax_SyntaxDefault, true},
		{"mutagen", Syntax_SyntaxMutagen, false},
		{"docker", Syntax_SyntaxDocker, false},
		{"git-worktree", Syntax_SyntaxGitWorktree, false},
	}

	// Process test cases.
	for _, test := range tests {
		var mode Syntax
		if err := mode.UnmarshalText([]byte(test.text)); err != nil {
			if !test.expectFailure {
				t.Errorf("unable to unmarshal text (%s): %s", test.text, err)
			}
		} else if test.expectFailure {
			t.Error("unmarshaling succeeded unexpectedly for text:", test.text)
		} else if mode != test.expectedMode {
			t.Errorf(
				"unmarshaled mode (%s) does not match expected (%s)",
				mode,
				test.expectedMode,
			)
		}
	}
}

// TestSyntaxSupported tests Syntax.Supported.
func TestSyntaxSupported(t *testing.T) {
	// Set up test cases.
	testCases := []struct {
		mode            Syntax
		expectSupported bool
	}{
		{Syntax_SyntaxDefault, false},
		{Syntax_SyntaxMutagen, true},
		{Syntax_SyntaxDocker, true},
		{Syntax_SyntaxGitWorktree, true},
		{(Syntax_SyntaxGitWorktree + 1), false},
	}

	// Process test cases.
	for _, testCase := range testCases {
		if supported := testCase.mode.Supported(); supported != testCase.expectSupported {
			t.Errorf(
				"mode support status (%t) does not match expected (%t)",
				supported,
				testCase.expectSupported,
			)
		}
	}
}

// TestSyntaxDescription tests Syntax.Description.
func TestSyntaxDescription(t *testing.T) {
	// Set up test cases.
	testCases := []struct {
		mode                Syntax
		expectedDescription string
	}{
		{Syntax_SyntaxDefault, "Default"},
		{Syntax_SyntaxMutagen, "Mutagen"},
		{Syntax_SyntaxDocker, "Docker"},
		{Syntax_SyntaxGitWorktree, "Git worktree"},
		{(Syntax_SyntaxGitWorktree + 1), "Unknown"},
	}

	// Process test cases.
	for _, testCase := range testCases {
		if description := testCase.mode.Description(); description != testCase.expectedDescription {
			t.Errorf(
				"mode description (%s) does not match expected (%s)",
				description,
				testCase.expectedDescription,
			)
		}
	}
}
