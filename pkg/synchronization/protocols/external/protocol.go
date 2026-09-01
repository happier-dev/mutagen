// Package external connects Mutagen synchronization endpoints through streams
// established and authorized by an external transport.
package external

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

// DialRequest contains the only datum that the generic external transport may
// receive. The identifier is opaque: authorization, route selection, and root
// resolution are owned by the embedding transport at connection time.
type DialRequest struct {
	EndpointIdentifier string
}

// StreamDialer establishes an authenticated, full-duplex stream to an external
// machine. Closing the stream must unblock pending reads and writes.
type StreamDialer interface {
	Dial(context.Context, DialRequest) (io.ReadWriteCloser, error)
}

type protocolHandler struct {
	dialer StreamDialer
}

// NewProtocolHandler creates a synchronization protocol handler backed by the
// specified external stream dialer.
func NewProtocolHandler(dialer StreamDialer) synchronization.ProtocolHandler {
	if dialer == nil {
		panic("nil External stream dialer")
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
		panic("non-synchronization URL dispatched to External protocol handler")
	} else if url.Protocol != urlpkg.Protocol_External {
		panic("non-External URL dispatched to External protocol handler")
	} else if err := url.EnsureValid(); err != nil {
		return nil, fmt.Errorf("invalid External endpoint URL: %w", err)
	}

	stream, err := h.dialer.Dial(ctx, DialRequest{
		EndpointIdentifier: url.Host,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, context.Canceled
		}
		return nil, fmt.Errorf("unable to dial External synchronization stream: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() { stream.Close() })
	defer stopCancellation()
	if err := agent.ClientHandshake(stream); err != nil {
		stream.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("unable to handshake with External synchronization agent: %w", err)
	} else if err := mutagen.ClientVersionHandshake(stream); err != nil {
		stream.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("External synchronization agent version handshake failed: %w", err)
	}

	// External endpoint URLs intentionally contain no root. The target-owned
	// agent substitutes and revalidates its explicit --root before serving.
	endpoint, err := remote.NewEndpoint(logger, stream, "", session, version, configuration, alpha)
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
