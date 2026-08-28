package happier

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/mutagen-io/mutagen/pkg/agent"
	"github.com/mutagen-io/mutagen/pkg/mutagen"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/endpoint/remote"
	urlpkg "github.com/mutagen-io/mutagen/pkg/url"
)

type endpointDialer struct {
	request  DialRequest
	serveErr chan error
}

type processStream struct {
	stdin  io.WriteCloser
	stdout io.ReadCloser
	cmd    *exec.Cmd
}

func (s *processStream) Read(buffer []byte) (int, error) {
	return s.stdout.Read(buffer)
}

func (s *processStream) Write(buffer []byte) (int, error) {
	return s.stdin.Write(buffer)
}

func (s *processStream) Close() error {
	s.stdin.Close()
	s.stdout.Close()
	if s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}
	s.cmd.Wait()
	return nil
}

type agentProcessDialer struct {
	path         string
	rootOverride string
	stderr       bytes.Buffer
}

func (d *agentProcessDialer) Dial(_ context.Context, request DialRequest) (io.ReadWriteCloser, error) {
	root := request.Root
	if d.rootOverride != "" {
		root = d.rootOverride
	}
	command := exec.Command(d.path, "synchronizer", "--root", root)
	command.Stderr = &d.stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		stdin.Close()
		stdout.Close()
		return nil, err
	}
	return &processStream{stdin: stdin, stdout: stdout, cmd: command}, nil
}

func TestProtocolHandlerRejectsPackagedAgentRootMismatch(t *testing.T) {
	agentPath := os.Getenv("HAPPIER_MUTAGEN_AGENT_TEST_BINARY")
	if agentPath == "" {
		t.Skip("packaged agent test binary not provided")
	}

	dialer := &agentProcessDialer{path: agentPath, rootOverride: t.TempDir()}
	_, err := NewProtocolHandler(dialer).Connect(
		context.Background(),
		nil,
		&urlpkg.URL{
			Kind:     urlpkg.Kind_Synchronization,
			Protocol: urlpkg.Protocol_Happier,
			Host:     "machine-01",
			Path:     t.TempDir(),
			Parameters: map[string]string{
				urlpkg.HappierRootGrantIdentifierParameter: "grant-01",
			},
		},
		"",
		"sync_session-01",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	)
	if err == nil {
		t.Fatal("packaged agent accepted a root outside its target-owned grant")
	}
}

func TestProtocolHandlerConnectsPackagedAgentProcess(t *testing.T) {
	agentPath := os.Getenv("HAPPIER_MUTAGEN_AGENT_TEST_BINARY")
	if agentPath == "" {
		t.Skip("packaged agent test binary not provided")
	}

	dialer := &agentProcessDialer{path: agentPath}
	root := t.TempDir()
	endpoint, err := NewProtocolHandler(dialer).Connect(
		context.Background(),
		nil,
		&urlpkg.URL{
			Kind:     urlpkg.Kind_Synchronization,
			Protocol: urlpkg.Protocol_Happier,
			Host:     "machine-01",
			Path:     root,
			Parameters: map[string]string{
				urlpkg.HappierRootGrantIdentifierParameter: "grant-01",
			},
		},
		"",
		"sync_session-01",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	)
	if err != nil {
		t.Fatalf("unable to connect to packaged agent process: %v (agent stderr: %s)", err, dialer.stderr.String())
	}
	defer endpoint.Shutdown()

	snapshot, err, tryAgain := endpoint.Scan(context.Background(), nil, true)
	if err != nil {
		t.Fatal("packaged agent scan failed:", err)
	} else if tryAgain {
		t.Fatal("successful packaged agent scan requested retry")
	} else if snapshot == nil || snapshot.Content == nil {
		t.Fatal("packaged agent scan returned no content")
	}
}

func (d *endpointDialer) Dial(_ context.Context, request DialRequest) (io.ReadWriteCloser, error) {
	d.request = request
	client, server := net.Pipe()
	d.serveErr = make(chan error, 1)
	go func() {
		if err := agent.ServerHandshake(server); err != nil {
			d.serveErr <- err
			return
		} else if err := mutagen.ServerVersionHandshake(server); err != nil {
			d.serveErr <- err
			return
		}
		d.serveErr <- remote.ServeEndpoint(nil, server)
	}()
	return client, nil
}

func TestProtocolHandlerConnectsRemoteEndpointThroughDialer(t *testing.T) {
	dialer := &endpointDialer{}
	handler := NewProtocolHandler(dialer)
	root := t.TempDir()
	endpoint, err := handler.Connect(
		context.Background(),
		nil,
		&urlpkg.URL{
			Kind:     urlpkg.Kind_Synchronization,
			Protocol: urlpkg.Protocol_Happier,
			Host:     "machine-01",
			Path:     root,
			Parameters: map[string]string{
				urlpkg.HappierRootGrantIdentifierParameter: "grant-01",
			},
		},
		"",
		"sync_session-01",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	)
	if err != nil {
		t.Fatal("unable to connect through Happier dialer:", err)
	}

	if dialer.request.MachineIdentifier != "machine-01" {
		t.Fatal("machine identifier not forwarded to dialer:", dialer.request.MachineIdentifier)
	} else if dialer.request.RootGrantIdentifier != "grant-01" {
		t.Fatal("root grant identifier not forwarded to dialer:", dialer.request.RootGrantIdentifier)
	} else if dialer.request.Root != root {
		t.Fatal("root not forwarded to dialer:", dialer.request.Root)
	} else if dialer.request.SessionIdentifier != "sync_session-01" {
		t.Fatal("session identifier not forwarded to dialer:", dialer.request.SessionIdentifier)
	} else if dialer.request.Alpha {
		t.Fatal("endpoint side not forwarded to dialer")
	}

	snapshot, err, tryAgain := endpoint.Scan(context.Background(), nil, true)
	if err != nil {
		t.Fatal("remote scan failed:", err)
	} else if tryAgain {
		t.Fatal("successful scan requested retry")
	} else if snapshot == nil || snapshot.Content == nil {
		t.Fatal("remote scan returned no content")
	}

	if err := endpoint.Shutdown(); err != nil {
		t.Fatal("unable to shut down remote endpoint:", err)
	}
	select {
	case <-dialer.serveErr:
	case <-time.After(5 * time.Second):
		t.Fatal("remote endpoint did not stop after shutdown")
	}
}

type cancellingDialer struct{}

func (cancellingDialer) Dial(ctx context.Context, _ DialRequest) (io.ReadWriteCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type stalledHandshakeDialer struct {
	server io.Closer
}

func (d *stalledHandshakeDialer) Dial(_ context.Context, _ DialRequest) (io.ReadWriteCloser, error) {
	client, server := net.Pipe()
	d.server = server
	return client, nil
}

func TestProtocolHandlerCancelsStalledAgentHandshake(t *testing.T) {
	dialer := &stalledHandshakeDialer{}
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan error, 1)
	go func() {
		_, err := NewProtocolHandler(dialer).Connect(
			ctx,
			nil,
			&urlpkg.URL{
				Kind:     urlpkg.Kind_Synchronization,
				Protocol: urlpkg.Protocol_Happier,
				Host:     "machine-01",
				Path:     "/workspace",
				Parameters: map[string]string{
					urlpkg.HappierRootGrantIdentifierParameter: "grant-01",
				},
			},
			"",
			"sync_session-01",
			synchronization.DefaultVersion,
			&synchronization.Configuration{},
			false,
		)
		results <- err
	}()

	cancel()
	select {
	case err := <-results:
		if err != context.Canceled {
			t.Fatal("stalled handshake cancellation not preserved:", err)
		}
	case <-time.After(250 * time.Millisecond):
		if dialer.server != nil {
			dialer.server.Close()
		}
		t.Fatal("stalled agent handshake ignored context cancellation")
	}
}

func TestProtocolHandlerPropagatesDialCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewProtocolHandler(cancellingDialer{}).Connect(
		ctx,
		nil,
		&urlpkg.URL{
			Kind:     urlpkg.Kind_Synchronization,
			Protocol: urlpkg.Protocol_Happier,
			Host:     "machine-01",
			Path:     "/workspace",
			Parameters: map[string]string{
				urlpkg.HappierRootGrantIdentifierParameter: "grant-01",
			},
		},
		"",
		"sync_session-01",
		synchronization.DefaultVersion,
		&synchronization.Configuration{},
		false,
	)
	if err != context.Canceled {
		t.Fatal("dial cancellation not preserved:", err)
	}
}
