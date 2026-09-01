package netpipe

import (
	"context"
	"fmt"
	"net"

	"github.com/mutagen-io/mutagen/pkg/logging"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/endpoint/remote"
	urlpkg "github.com/mutagen-io/mutagen/pkg/url"
)

// synchronizationProtocolHandler implements the synchronization.ProtocolHandler
// interface for connecting to "remote" endpoints that actually exist in memory
// via an in-memory pipe.
type synchronizationProtocolHandler struct{}

// waitingSynchronizationEndpoint wraps and implements synchronization.Endpoint,
// but adds a waiting function that's invoked after invoking Shutdown on the
// underlying endpoint. It is necessary to ensure full endpoint shutdown in
// tests, where open file descriptors or handles can prevent temporary directory
// removal.
type waitingSynchronizationEndpoint struct {
	// Endpoint is the underlying endpoint.
	synchronization.Endpoint
	// wait is an arbitrary waiting function.
	wait func()
}

// Shutdown implements synchronization.Endpoint.Shutdown.
func (w *waitingSynchronizationEndpoint) Shutdown() error {
	// Shutdown on the underlying endpoint.
	result := w.Endpoint.Shutdown()

	// Perform the wait operation.
	w.wait()

	// Done.
	return result
}

// Dial starts an endpoint server in a background Goroutine and creates an
// endpoint client connected to the server via an in-memory connection.
func (h *synchronizationProtocolHandler) Connect(
	_ context.Context,
	logger *logging.Logger,
	url *urlpkg.URL,
	prompter string,
	session string,
	version synchronization.Version,
	configuration *synchronization.Configuration,
	alpha bool,
) (synchronization.Endpoint, error) {
	// Verify that the URL is of the correct kind and protocol.
	if url.Kind != urlpkg.Kind_Synchronization {
		panic("non-synchronization URL dispatched to synchronization protocol handler")
	} else if url.Protocol != Protocol_Netpipe {
		panic("non-netpipe URL dispatched to netpipe protocol handler")
	}

	// The in-memory transport is a test-only stand-in for a target agent, but
	// it must obey the same target-owned root contract. Authorize the exact
	// existing directory before opening the stream instead of retaining an
	// unrooted endpoint-server entry point for the harness.
	authorizedRoot, err := remote.ValidateEndpointRoot(url.Path)
	if err != nil {
		return nil, fmt.Errorf("unable to authorize in-memory endpoint root: %w", err)
	}

	// Create an in-memory network connection.
	clientConnection, serverConnection := net.Pipe()

	// Serve the endpoint in a background Goroutine. This will terminate once
	// the client connection is closed. We monitor for its termination so that
	// we can block on it in our endpoint wrapper.
	remoteEndpointDone := make(chan struct{})
	go func() {
		remote.ServeEndpointAtRoot(logger.Sublogger("remote"), serverConnection, authorizedRoot)
		close(remoteEndpointDone)
	}()

	// Create a client for this endpoint. Use the same canonical path that the
	// test agent authorized so platform aliases (for example /var and
	// /private/var on macOS) don't manufacture a root mismatch.
	endpoint, err := remote.NewEndpoint(
		logger,
		clientConnection,
		authorizedRoot,
		session,
		version,
		configuration,
		alpha,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create in-memory endpoint client: %w", err)
	}

	// Wrap the client so that it blocks on the full shutdown of the remote
	// endpoint after closing the connection. This is necessary for testing,
	// where we need to ensure that all file descriptors or handles point to
	// temporary test directories are closed before attempting to remove those
	// directories. This is not necessary for other remote protocols in normal
	// usage (because we don't have the same constraints) or in testing (because
	// the underlying connection closure waits for agent process termination).
	endpoint = &waitingSynchronizationEndpoint{endpoint, func() { <-remoteEndpointDone }}

	// Success.
	return endpoint, nil
}

func init() {
	// Register the netpipe protocol handler with the synchronization package.
	synchronization.ProtocolHandlers[Protocol_Netpipe] = &synchronizationProtocolHandler{}
}
