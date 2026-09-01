package synchronization

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/mutagen-io/mutagen/pkg/selection"
	"github.com/mutagen-io/mutagen/pkg/url"
)

func TestManagerReloadsPersistedExternalEndpoint(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	alpha := &url.URL{Path: filepath.Join(t.TempDir(), "alpha")}
	beta := &url.URL{
		Protocol: url.Protocol_External,
		Host:     "endpoint-01",
	}

	manager, err := NewManager(nil)
	if err != nil {
		t.Fatal("unable to create synchronization manager:", err)
	}
	sessionIdentifier, err := manager.Create(
		context.Background(),
		alpha,
		beta,
		&Configuration{},
		&Configuration{},
		&Configuration{},
		"persisted-external-session",
		nil,
		true,
		"",
	)
	if err != nil {
		manager.Shutdown()
		t.Fatal("unable to create paused External session:", err)
	}
	manager.Shutdown()

	reloaded, err := NewManager(nil)
	if err != nil {
		t.Fatal("unable to reload synchronization manager:", err)
	}
	defer reloaded.Shutdown()
	selection := &selection.Selection{Specifications: []string{sessionIdentifier}}
	_, states, err := reloaded.List(context.Background(), selection, 0)
	if err != nil {
		t.Fatal("unable to load persisted External session:", err)
	} else if len(states) != 1 {
		t.Fatal("unexpected persisted session count:", len(states))
	} else if !states[0].Session.Beta.Equal(beta) {
		t.Fatalf("persisted External endpoint mismatch:\nexpected: %#v\nactual:   %#v", beta, states[0].Session.Beta)
	}

	if err := reloaded.Terminate(context.Background(), selection, ""); err != nil {
		t.Fatal("unable to terminate persisted External session:", err)
	}
}
