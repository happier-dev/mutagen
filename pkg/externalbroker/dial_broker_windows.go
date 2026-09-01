//go:build windows

package externalbroker

import (
	"context"
	"net"

	winio "github.com/Microsoft/go-winio"
)

// The Happier broker descriptor already contains the raw secured pipe name.
// Mutagen's generic ipc package consumes locator files on Windows, so the
// external-broker transport owns this one direct raw-pipe dial without
// changing generic IPC or any other Mutagen transport.
func dialBrokerEndpoint(ctx context.Context, endpoint string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, endpoint)
}
