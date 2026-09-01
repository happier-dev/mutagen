//go:build !windows

package externalbroker

import (
	"context"
	"net"

	"github.com/mutagen-io/mutagen/pkg/ipc"
)

func dialBrokerEndpoint(ctx context.Context, endpoint string) (net.Conn, error) {
	return ipc.DialContext(ctx, endpoint)
}
