package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mutagen-io/mutagen/pkg/agent"
	"github.com/mutagen-io/mutagen/pkg/mutagen"
	"github.com/mutagen-io/mutagen/pkg/selection"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/compression"
	"github.com/mutagen-io/mutagen/pkg/synchronization/core"
	"github.com/mutagen-io/mutagen/pkg/synchronization/endpoint/remote"
	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
	"github.com/mutagen-io/mutagen/pkg/url"
)

type countingStream struct {
	io.ReadWriteCloser
	bytesRead    *atomic.Uint64
	bytesWritten *atomic.Uint64
}

func (s *countingStream) Read(buffer []byte) (int, error) {
	count, err := s.ReadWriteCloser.Read(buffer)
	s.bytesRead.Add(uint64(count))
	return count, err
}

func (s *countingStream) Write(buffer []byte) (int, error) {
	count, err := s.ReadWriteCloser.Write(buffer)
	s.bytesWritten.Add(uint64(count))
	return count, err
}

type authorizedEndpointDialer struct {
	roots        map[string]string
	bytesRead    atomic.Uint64
	bytesWritten atomic.Uint64
	dialCount    atomic.Uint64
	lock         sync.Mutex
	active       []io.Closer
}

func (d *authorizedEndpointDialer) Dial(
	_ context.Context,
	request externalprotocol.DialRequest,
) (io.ReadWriteCloser, error) {
	root, ok := d.roots[request.EndpointIdentifier]
	if !ok {
		return nil, errors.New("unknown opaque endpoint")
	}

	client, server := net.Pipe()
	d.dialCount.Add(1)
	d.lock.Lock()
	d.active = append(d.active, client)
	d.lock.Unlock()
	go func() {
		if err := agent.ServerHandshake(server); err != nil {
			server.Close()
			return
		} else if err := mutagen.ServerVersionHandshake(server); err != nil {
			server.Close()
			return
		}
		remote.ServeEndpointAtRoot(nil, server, root)
	}()

	return &countingStream{
		ReadWriteCloser: client,
		bytesRead:       &d.bytesRead,
		bytesWritten:    &d.bytesWritten,
	}, nil
}

func (d *authorizedEndpointDialer) transferredBytes() uint64 {
	return d.bytesRead.Load() + d.bytesWritten.Load()
}

func (d *authorizedEndpointDialer) disconnectActiveStreams() {
	d.lock.Lock()
	active := d.active
	d.active = nil
	d.lock.Unlock()
	for _, stream := range active {
		stream.Close()
	}
}

func terminateSession(t *testing.T, sessionIdentifier string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := synchronizationManager.Terminate(ctx, &selection.Selection{
		Specifications: []string{sessionIdentifier},
	}, ""); err != nil {
		t.Error("unable to terminate synchronization session:", err)
	}
}

func flushSession(t *testing.T, sessionIdentifier string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := synchronizationManager.Flush(ctx, &selection.Selection{
		Specifications: []string{sessionIdentifier},
	}, "", false); err != nil {
		t.Fatal("unable to flush synchronization session:", err)
	}
}

func waitForConnectedSession(
	t *testing.T,
	sessionIdentifier string,
	dialer *authorizedEndpointDialer,
	minimumDialCount uint64,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	selection := &selection.Selection{Specifications: []string{sessionIdentifier}}
	var previousStateIndex uint64
	for {
		stateIndex, states, err := synchronizationManager.List(ctx, selection, previousStateIndex)
		if err != nil {
			t.Fatal("unable to wait for session reconnection:", err)
		} else if len(states) != 1 {
			t.Fatal("unexpected session count while waiting for reconnection:", len(states))
		} else if dialer.dialCount.Load() >= minimumDialCount && states[0].AlphaState.Connected && states[0].BetaState.Connected && states[0].Status >= synchronization.Status_Watching {
			return
		}
		previousStateIndex = stateIndex
	}
}

func createExternalSession(
	t *testing.T,
	alphaRoot, betaRoot, endpointIdentifier string,
	mode core.SynchronizationMode,
) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sessionIdentifier, err := synchronizationManager.Create(
		ctx,
		&url.URL{Path: alphaRoot},
		&url.URL{
			Protocol: url.Protocol_External,
			Host:     endpointIdentifier,
		},
		&synchronization.Configuration{
			SynchronizationMode:  mode,
			CompressionAlgorithm: compression.Algorithm_AlgorithmNone,
		},
		&synchronization.Configuration{},
		&synchronization.Configuration{},
		"external-transport-spike",
		nil,
		false,
		"",
	)
	if err != nil {
		t.Fatal("unable to create synchronization session:", err)
	}
	t.Cleanup(func() { terminateSession(t, sessionIdentifier) })
	if err := waitForSuccessfulSynchronizationCycle(ctx, sessionIdentifier, false, false, false); err != nil {
		t.Fatal("initial synchronization cycle failed:", err)
	}
	return sessionIdentifier
}

func requireFileContents(t *testing.T, path string, expected []byte) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("unable to read synchronized file:", err)
	} else if !bytes.Equal(contents, expected) {
		t.Fatal("synchronized file contents mismatch")
	}
}

func TestSynchronizationThroughExternalTransport(t *testing.T) {
	oneWayAlpha := filepath.Join(t.TempDir(), "alpha")
	oneWayBeta := filepath.Join(t.TempDir(), "beta")
	twoWayAlpha := filepath.Join(t.TempDir(), "alpha")
	twoWayBeta := filepath.Join(t.TempDir(), "beta")
	for _, root := range []string{oneWayAlpha, oneWayBeta, twoWayAlpha, twoWayBeta} {
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal("unable to create synchronization root:", err)
		}
	}

	dialer := &authorizedEndpointDialer{
		roots: map[string]string{
			"one-way-root": oneWayBeta,
			"two-way-root": twoWayBeta,
		},
	}
	synchronization.ProtocolHandlers[url.Protocol_External] = externalprotocol.NewProtocolHandler(dialer)

	largeContents := make([]byte, 4*1024*1024)
	for index := range largeContents {
		largeContents[index] = byte((index*31 + index/251) % 251)
	}
	if err := os.WriteFile(filepath.Join(oneWayAlpha, "large.bin"), largeContents, 0o600); err != nil {
		t.Fatal("unable to write one-way source file:", err)
	}

	oneWaySession := createExternalSession(
		t,
		oneWayAlpha,
		oneWayBeta,
		"one-way-root",
		core.SynchronizationMode_SynchronizationModeOneWaySafe,
	)
	flushSession(t, oneWaySession)
	requireFileContents(t, filepath.Join(oneWayBeta, "large.bin"), largeContents)

	beforeDelta := dialer.transferredBytes()
	changedContents := append([]byte(nil), largeContents...)
	copy(changedContents[2*1024*1024:2*1024*1024+4096], bytes.Repeat([]byte{0xE7}, 4096))
	if err := os.WriteFile(filepath.Join(oneWayAlpha, "large.bin"), changedContents, 0o600); err != nil {
		t.Fatal("unable to modify one-way source file:", err)
	}
	flushSession(t, oneWaySession)
	requireFileContents(t, filepath.Join(oneWayBeta, "large.bin"), changedContents)
	deltaBytes := dialer.transferredBytes() - beforeDelta
	if deltaBytes >= uint64(len(changedContents))/2 {
		t.Fatalf("small edit transferred too many bytes: %d", deltaBytes)
	}

	dialsBeforeDisconnect := dialer.dialCount.Load()
	dialer.disconnectActiveStreams()
	waitForConnectedSession(t, oneWaySession, dialer, dialsBeforeDisconnect+1)
	reconnectContents := []byte("reconnected through a fresh External stream")
	if err := os.WriteFile(filepath.Join(oneWayAlpha, "after-reconnect.txt"), reconnectContents, 0o600); err != nil {
		t.Fatal("unable to write reconnect source file:", err)
	}
	flushSession(t, oneWaySession)
	requireFileContents(t, filepath.Join(oneWayBeta, "after-reconnect.txt"), reconnectContents)
	if count := dialer.dialCount.Load(); count <= dialsBeforeDisconnect {
		t.Fatalf("transport loss did not trigger a fresh dial: %d", count)
	}

	twoWaySession := createExternalSession(
		t,
		twoWayAlpha,
		twoWayBeta,
		"two-way-root",
		core.SynchronizationMode_SynchronizationModeTwoWaySafe,
	)
	alphaContents := []byte("created on alpha")
	if err := os.WriteFile(filepath.Join(twoWayAlpha, "from-alpha.txt"), alphaContents, 0o600); err != nil {
		t.Fatal("unable to write alpha file:", err)
	}
	flushSession(t, twoWaySession)
	requireFileContents(t, filepath.Join(twoWayBeta, "from-alpha.txt"), alphaContents)

	betaContents := []byte("created on beta")
	if err := os.WriteFile(filepath.Join(twoWayBeta, "from-beta.txt"), betaContents, 0o600); err != nil {
		t.Fatal("unable to write beta file:", err)
	}
	flushSession(t, twoWaySession)
	requireFileContents(t, filepath.Join(twoWayAlpha, "from-beta.txt"), betaContents)

	conflictPathAlpha := filepath.Join(twoWayAlpha, "conflict.txt")
	conflictPathBeta := filepath.Join(twoWayBeta, "conflict.txt")
	if err := os.WriteFile(conflictPathAlpha, []byte("common ancestor"), 0o600); err != nil {
		t.Fatal("unable to write conflict ancestor:", err)
	}
	flushSession(t, twoWaySession)

	selection := &selection.Selection{Specifications: []string{twoWaySession}}
	if err := synchronizationManager.Pause(context.Background(), selection, ""); err != nil {
		t.Fatal("unable to pause two-way session:", err)
	}
	if err := os.WriteFile(conflictPathAlpha, []byte("alpha edit"), 0o600); err != nil {
		t.Fatal("unable to write alpha conflict:", err)
	} else if err := os.WriteFile(conflictPathBeta, []byte("beta edit"), 0o600); err != nil {
		t.Fatal("unable to write beta conflict:", err)
	}
	dialsBeforeResume := dialer.dialCount.Load()
	if err := synchronizationManager.Resume(context.Background(), selection, ""); err != nil {
		t.Fatal("unable to resume two-way session:", err)
	}
	waitForConnectedSession(t, twoWaySession, dialer, dialsBeforeResume+1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := waitForSuccessfulSynchronizationCycle(ctx, twoWaySession, false, true, false); err != nil {
		t.Fatal("two-way conflict cycle failed:", err)
	}
	_, states, err := synchronizationManager.List(ctx, selection, 0)
	if err != nil {
		t.Fatal("unable to list two-way session:", err)
	} else if len(states) != 1 {
		t.Fatal("unexpected two-way session count:", len(states))
	} else if len(states[0].Conflicts) == 0 {
		t.Fatal("divergent edits did not produce a two-way-safe conflict")
	}

	t.Logf(
		"External transport spike transferred %d bytes across %d stream dials; 4 MiB small-edit delta used %d bytes",
		dialer.transferredBytes(),
		dialer.dialCount.Load(),
		deltaBytes,
	)
}
