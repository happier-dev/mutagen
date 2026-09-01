package remote

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/mutagen-io/mutagen/pkg/synchronization"
)

// startRootedServer serves an endpoint at the specified enforced root on one
// end of a buffered in-memory socket pair. A loopback TCP pair is used instead
// of net.Pipe because the synchronization protocol exchanges concurrent final
// writes when initialization fails, which real (buffered) transports absorb
// but net.Pipe's synchronous semantics turn into a mutual-write deadlock. It
// returns the client end and a channel that receives the server's termination
// error.
func startRootedServer(t *testing.T, root string) (net.Conn, chan error) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("unable to create loopback listener:", err)
	}
	t.Cleanup(func() { listener.Close() })
	clientConnection, err := net.Dial(listener.Addr().Network(), listener.Addr().String())
	if err != nil {
		t.Fatal("unable to dial loopback listener:", err)
	}
	serverConnection, err := listener.Accept()
	if err != nil {
		t.Fatal("unable to accept loopback connection:", err)
	}
	termination := make(chan error, 1)
	go func() {
		termination <- ServeEndpointAtRoot(nil, serverConnection, root)
	}()
	t.Cleanup(func() {
		clientConnection.Close()
		<-termination
	})
	return clientConnection, termination
}

// writeMarkerFile creates a uniquely named file with known contents in the
// specified directory and returns its base name.
func writeMarkerFile(t *testing.T, directory string) string {
	t.Helper()
	path := filepath.Join(directory, "marker.txt")
	if err := os.WriteFile(path, []byte("enforced-root-marker"), 0o600); err != nil {
		t.Fatal("unable to write marker file:", err)
	}
	return "marker.txt"
}

func TestServeEndpointAtRootRejectsEmptyRoot(t *testing.T) {
	clientConnection, serverConnection := net.Pipe()
	defer clientConnection.Close()
	defer serverConnection.Close()

	if err := ServeEndpointAtRoot(nil, serverConnection, ""); err == nil {
		t.Fatal("unrooted endpoint serving accepted")
	}
}

func TestServeEndpointRejectsRootMismatch(t *testing.T) {
	enforcedRoot := t.TempDir()
	writeMarkerFile(t, enforcedRoot)
	otherRoot := t.TempDir()

	clientConnection, _ := startRootedServer(t, enforcedRoot)
	if _, err := NewEndpoint(
		nil,
		clientConnection,
		otherRoot,
		"root-enforcement-session",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	); err == nil {
		t.Fatal("initialize request for a root other than the enforced root accepted")
	}
}

func TestServeEndpointServesEnforcedRootForEmptyClientRoot(t *testing.T) {
	// The external transport persists no root in the endpoint URL, so the
	// endpoint client transmits no root. The serving side must use its own
	// explicitly enforced root (from the mandatory --root flag) and must never
	// infer or accept a caller-specified root.
	enforcedRoot := t.TempDir()
	marker := writeMarkerFile(t, enforcedRoot)

	clientConnection, _ := startRootedServer(t, enforcedRoot)
	endpoint, err := NewEndpoint(
		nil,
		clientConnection,
		"",
		"root-enforcement-session",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	)
	if err != nil {
		t.Fatal("initialize request without a client-specified root rejected:", err)
	}

	snapshot, err, tryAgain := endpoint.Scan(context.Background(), nil, true)
	if err != nil {
		t.Fatal("scan of enforced root failed:", err)
	} else if tryAgain {
		t.Fatal("successful scan requested retry")
	} else if snapshot == nil || snapshot.Content == nil {
		t.Fatal("scan returned no content")
	}
	if snapshot == nil || snapshot.Content == nil || snapshot.Content.Contents == nil {
		t.Fatal("scan returned no content")
	}
	if _, ok := snapshot.Content.Contents[marker]; !ok {
		t.Fatal("scan did not observe the enforced root; served root was inferred from elsewhere")
	} else if len(snapshot.Content.Contents) != 1 {
		t.Fatalf("scan observed unexpected content outside the enforced root: %v", snapshot.Content.Contents)
	}

	if err := endpoint.Shutdown(); err != nil && !errors.Is(err, os.ErrClosed) {
		t.Fatal("unable to shut down endpoint:", err)
	}
}
