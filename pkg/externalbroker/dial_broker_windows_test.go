//go:build windows

package externalbroker

import (
	"context"
	"fmt"
	"io"
	"os/user"
	"testing"
	"time"

	winio "github.com/Microsoft/go-winio"
	"github.com/google/uuid"
)

func TestDialBrokerEndpointConsumesRawNamedPipePath(t *testing.T) {
	current, err := user.Current()
	if err != nil || current.Uid == "" {
		t.Fatalf("resolve current user SID: %v", err)
	}
	pipeName := fmt.Sprintf(`\\.\pipe\happier-workspace-sync-test-%s`, uuid.NewString())
	listener, err := winio.ListenPipe(pipeName, &winio.PipeConfig{
		SecurityDescriptor: fmt.Sprintf("D:P(A;;GA;;;%s)", current.Uid),
		MessageMode:        true,
	})
	if err != nil {
		t.Fatalf("listen on raw pipe path: %v", err)
	}
	defer listener.Close()
	accepted := make(chan acceptedConnection, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		accepted <- acceptedConnection{connection: connection, err: acceptErr}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := dialBrokerEndpoint(ctx, pipeName)
	if err != nil {
		t.Fatalf("dial raw pipe path: %v", err)
	}
	defer client.Close()
	server := <-accepted
	if server.err != nil {
		t.Fatalf("accept raw pipe connection: %v", server.err)
	}
	defer server.connection.Close()
	payload := []byte("raw-workspace-pipe")
	if _, err := client.Write(payload); err != nil {
		t.Fatalf("write raw pipe payload: %v", err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(server.connection, received); err != nil {
		t.Fatalf("read raw pipe payload: %v", err)
	}
	if string(received) != string(payload) {
		t.Fatalf("unexpected raw pipe payload %q", received)
	}
}

type acceptedConnection struct {
	connection io.ReadWriteCloser
	err        error
}
