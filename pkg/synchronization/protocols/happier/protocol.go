// Package happier connects Mutagen synchronization endpoints through streams
// established and authorized by the Happier daemon.
package happier

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mutagen-io/mutagen/pkg/agent"
	"github.com/mutagen-io/mutagen/pkg/logging"
	"github.com/mutagen-io/mutagen/pkg/mutagen"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/endpoint/remote"
	urlpkg "github.com/mutagen-io/mutagen/pkg/url"
)

// DialRequest contains the authority and endpoint metadata that Happier needs
// to establish a stream. Carrier selection deliberately does not appear here:
// the Happier daemon owns direct-versus-relay routing behind this contract.
type DialRequest struct {
	MachineIdentifier   string
	RootGrantIdentifier string
	Root                string
	SessionIdentifier   string
	Alpha               bool
}

// StreamDialer establishes an authenticated, full-duplex stream to a Happier
// machine. Closing the stream must unblock pending reads and writes.
type StreamDialer interface {
	Dial(context.Context, DialRequest) (io.ReadWriteCloser, error)
}

type protocolHandler struct {
	dialer StreamDialer
}

// NewProtocolHandler creates a synchronization protocol handler backed by the
// specified Happier stream dialer.
func NewProtocolHandler(dialer StreamDialer) synchronization.ProtocolHandler {
	if dialer == nil {
		panic("nil Happier stream dialer")
	}
	return &protocolHandler{dialer: dialer}
}

func (h *protocolHandler) Connect(
	ctx context.Context,
	logger *logging.Logger,
	url *urlpkg.URL,
	_ string,
	session string,
	version synchronization.Version,
	configuration *synchronization.Configuration,
	alpha bool,
) (synchronization.Endpoint, error) {
	if url.Kind != urlpkg.Kind_Synchronization {
		panic("non-synchronization URL dispatched to Happier protocol handler")
	} else if url.Protocol != urlpkg.Protocol_Happier {
		panic("non-Happier URL dispatched to Happier protocol handler")
	} else if err := url.EnsureValid(); err != nil {
		return nil, fmt.Errorf("invalid Happier endpoint URL: %w", err)
	}

	stream, err := h.dialer.Dial(ctx, DialRequest{
		MachineIdentifier:   url.Host,
		RootGrantIdentifier: url.Parameters[urlpkg.HappierRootGrantIdentifierParameter],
		Root:                url.Path,
		SessionIdentifier:   session,
		Alpha:               alpha,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("unable to dial Happier synchronization stream: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() { stream.Close() })
	defer stopCancellation()
	if err := agent.ClientHandshake(stream); err != nil {
		stream.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("unable to handshake with Happier synchronization agent: %w", err)
	} else if err := mutagen.ClientVersionHandshake(stream); err != nil {
		stream.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("Happier synchronization agent version handshake failed: %w", err)
	}

	endpoint, err := remote.NewEndpoint(logger, stream, url.Path, session, version, configuration, alpha)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	} else if ctx.Err() != nil {
		endpoint.Shutdown()
		return nil, ctx.Err()
	}
	return endpoint, nil
}
