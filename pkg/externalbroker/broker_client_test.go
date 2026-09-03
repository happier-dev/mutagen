//go:build !windows

package externalbroker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
)

const brokerTestOperationTimeout = 5 * time.Second

func shortUnixSocketPath(t *testing.T, name string) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "mutagen-broker-")
	if err != nil {
		t.Fatal("unable to create short broker socket directory:", err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	return filepath.Join(directory, name)
}

func TestReadBrokerDescriptorRejectsTrailingJSON(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	raw := `{"protocol":1,"brokerEndpoint":"/tmp/broker.sock","brokerInstanceId":"broker-01","launchNonce":"nonce-01","secret":"` + secret + `"}{}`
	if _, err := ReadBrokerDescriptor(bytes.NewBufferString(raw)); err == nil {
		t.Fatal("broker descriptor with a trailing JSON value accepted")
	}
}

func TestBrokerDescriptorBootstrapConnectsIPCAndReturnsTypedError(t *testing.T) {
	secret := bytes.Repeat([]byte{7}, 32)
	endpoint := shortUnixSocketPath(t, "control.sock")
	descriptor := BrokerDescriptor{
		Protocol: BrokerProtocolVersion, BrokerEndpoint: endpoint,
		BrokerInstanceID: "broker-01", LaunchNonce: "nonce-01",
		Secret: base64.RawURLEncoding.EncodeToString(secret),
	}
	var bootstrap bytes.Buffer
	if err := json.NewEncoder(&bootstrap).Encode(descriptor); err != nil {
		t.Fatal(err)
	}
	parsed, err := ReadBrokerDescriptor(&bootstrap)
	if err != nil {
		t.Fatal("unable to read inherited descriptor:", err)
	}
	listener, err := net.Listen("unix", endpoint)
	if err != nil {
		t.Fatal("unable to listen on broker endpoint:", err)
	}
	defer listener.Close()
	deadline := time.Now().Add(brokerTestOperationTimeout)
	if err := listener.(*net.UnixListener).SetDeadline(deadline); err != nil {
		t.Fatal("unable to bound broker accept:", err)
	}
	brokerFailure := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			brokerFailure <- err
			return
		}
		defer connection.Close()
		if err := connection.SetDeadline(deadline); err != nil {
			brokerFailure <- err
			return
		}
		reader := bufio.NewReader(connection)
		raw, err := readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var greeting hello
		if err := decodeStrict(raw, &greeting); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(connection, helloOK{
			T: "hello_ok", Protocol: 1, BrokerInstanceID: descriptor.BrokerInstanceID,
			Proof: brokerProof(secret, "workspace-sync-broker-ok-v1", descriptor, greeting.SidecarPID),
		}); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(connection, map[string]any{
			"t": "command", "requestId": "request-01",
			"command": map[string]any{"t": "list", "requestId": "request-01"},
		}); err != nil {
			brokerFailure <- err
			return
		}
		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var response struct{ T, RequestID, Code, Message string }
		if err := decodeStrict(raw, &response); err != nil || response.T != "error" || response.RequestID != "request-01" || response.Code != "engine_unavailable" || response.Message == "" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		brokerFailure <- nil
	}()
	clientContext, cancelClient := context.WithTimeout(context.Background(), brokerTestOperationTimeout)
	defer cancelClient()
	client, err := NewBrokerClient(clientContext, parsed)
	if err != nil {
		t.Fatal("unable to connect authenticated IPC client:", err)
	}
	defer client.Close()
	command := <-client.Commands()
	if command.RequestID != "request-01" {
		t.Fatal("wrong command request identifier:", command.RequestID)
	}
	if err := client.SendResult(command.RequestID, nil, io.ErrUnexpectedEOF); err != nil {
		t.Fatal("unable to send typed command error:", err)
	}
	if err := <-brokerFailure; err != nil {
		t.Fatal("broker simulation failed:", err)
	}
}

func TestBrokerClientCancelsOneCommandWithoutClosingControl(t *testing.T) {
	secret := bytes.Repeat([]byte{9}, 32)
	descriptor := BrokerDescriptor{
		Protocol: BrokerProtocolVersion, BrokerEndpoint: shortUnixSocketPath(t, "control.sock"),
		BrokerInstanceID: "broker-01", LaunchNonce: "nonce-01",
		Secret: base64.RawURLEncoding.EncodeToString(secret),
	}
	clientControl, brokerControl := net.Pipe()
	allowCancel := make(chan struct{})
	brokerFailure := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(brokerControl)
		raw, err := readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var greeting hello
		if err := decodeStrict(raw, &greeting); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, helloOK{
			T: "hello_ok", Protocol: 1, BrokerInstanceID: descriptor.BrokerInstanceID,
			Proof: brokerProof(secret, "workspace-sync-broker-ok-v1", descriptor, greeting.SidecarPID),
		}); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, map[string]any{
			"t": "command", "requestId": "request-01",
			"command": map[string]any{"t": "flush", "requestId": "request-01", "sessionIdentifier": "session-1"},
		}); err != nil {
			brokerFailure <- err
			return
		}
		<-allowCancel
		if err := writeTestFrame(brokerControl, map[string]any{"t": "cancel", "requestId": "request-01"}); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, map[string]any{
			"t": "command", "requestId": "request-02",
			"command": map[string]any{"t": "list", "requestId": "request-02"},
		}); err != nil {
			brokerFailure <- err
			return
		}
		brokerFailure <- nil
	}()

	client, err := newBrokerClientWithConnection(context.Background(), clientControl, descriptor)
	if err != nil {
		t.Fatal("unable to authenticate broker client:", err)
	}
	defer client.Close()
	command := <-client.Commands()
	close(allowCancel)
	select {
	case <-command.Context.Done():
	case <-time.After(time.Second):
		t.Fatal("broker cancel did not cancel the selected command")
	}
	select {
	case <-client.Done():
		t.Fatal("command cancellation closed the authenticated control connection")
	default:
	}
	client.FinishCommand(command.RequestID)
	select {
	case next := <-client.Commands():
		if next.RequestID != "request-02" {
			t.Fatal("wrong command after cancellation:", next.RequestID)
		}
		select {
		case <-next.Context.Done():
			t.Fatal("next command inherited the previous command cancellation")
		default:
		}
		client.FinishCommand(next.RequestID)
	case <-time.After(time.Second):
		t.Fatal("authenticated control connection did not deliver the next command")
	}
	if err := <-brokerFailure; err != nil {
		t.Fatal("broker simulation failed:", err)
	}
}

func TestBrokerClientDrainsLateResponseAfterCancelledPendingRequest(t *testing.T) {
	secret := bytes.Repeat([]byte{11}, 32)
	descriptor := BrokerDescriptor{
		Protocol: BrokerProtocolVersion, BrokerEndpoint: shortUnixSocketPath(t, "control.sock"),
		BrokerInstanceID: "broker-01", LaunchNonce: "nonce-01",
		Secret: base64.RawURLEncoding.EncodeToString(secret),
	}
	clientControl, brokerControl := net.Pipe()
	brokerFailure := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(brokerControl)
		raw, err := readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var greeting hello
		if err := decodeStrict(raw, &greeting); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, helloOK{
			T: "hello_ok", Protocol: 1, BrokerInstanceID: descriptor.BrokerInstanceID,
			Proof: brokerProof(secret, "workspace-sync-broker-ok-v1", descriptor, greeting.SidecarPID),
		}); err != nil {
			brokerFailure <- err
			return
		}

		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var first brokerTag
		if err := json.Unmarshal(raw, &first); err != nil {
			brokerFailure <- err
			return
		}
		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var cancelled cancelCommand
		if err := decodeStrict(raw, &cancelled); err != nil || cancelled.T != "cancel" || cancelled.RequestID != first.RequestID {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		if err := writeTestFrame(brokerControl, map[string]any{
			"t": "error", "requestId": first.RequestID, "code": "cancelled", "message": "late terminal response",
		}); err != nil {
			brokerFailure <- err
			return
		}

		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var second brokerTag
		if err := json.Unmarshal(raw, &second); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, map[string]any{
			"t": "result", "requestId": second.RequestID, "result": map[string]bool{"ok": true},
		}); err != nil {
			brokerFailure <- err
			return
		}
		brokerFailure <- nil
	}()

	client, err := newBrokerClientWithConnection(context.Background(), clientControl, descriptor)
	if err != nil {
		t.Fatal("unable to authenticate broker client:", err)
	}
	defer client.Close()
	cancelledContext, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, requestErr := client.request(cancelledContext, "cancelled-request", map[string]any{"t": "open_data", "requestId": "cancelled-request"})
		firstDone <- requestErr
	}()
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request returned the wrong error: %v", err)
	}
	response, err := client.request(context.Background(), "next-request", map[string]any{"t": "list", "requestId": "next-request"})
	if err != nil {
		t.Fatal("late cancelled response tore down authenticated control:", err)
	}
	var result struct{ T, RequestID string }
	if err := json.Unmarshal(response, &result); err != nil || result.T != "result" || result.RequestID != "next-request" {
		t.Fatalf("unexpected next response: %s (%v)", response, err)
	}
	if err := <-brokerFailure; err != nil {
		t.Fatal("broker simulation failed:", err)
	}
}

func TestBrokerClientOpensSeparateRawDataStream(t *testing.T) {
	secret := make([]byte, 32)
	for index := range secret {
		secret[index] = byte(index + 1)
	}
	descriptor := BrokerDescriptor{
		Protocol:         BrokerProtocolVersion,
		BrokerEndpoint:   shortUnixSocketPath(t, "control.sock"),
		BrokerInstanceID: "broker-01",
		LaunchNonce:      "nonce-01",
		Secret:           base64.RawURLEncoding.EncodeToString(secret),
	}
	clientControl, brokerControl := net.Pipe()
	deadline := time.Now().Add(brokerTestOperationTimeout)
	if err := clientControl.SetDeadline(deadline); err != nil {
		t.Fatal("unable to bound client control operations:", err)
	}
	if err := brokerControl.SetDeadline(deadline); err != nil {
		t.Fatal("unable to bound broker control operations:", err)
	}
	dataEndpoint := shortUnixSocketPath(t, "data.sock")
	brokerFailure := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(brokerControl)
		raw, err := readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var greeting hello
		if err := decodeStrict(raw, &greeting); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, helloOK{
			T: "hello_ok", Protocol: 1, BrokerInstanceID: descriptor.BrokerInstanceID,
			Proof: brokerProof(secret, "workspace-sync-broker-ok-v1", descriptor, greeting.SidecarPID),
		}); err != nil {
			brokerFailure <- err
			return
		}
		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var open struct {
			T, RequestID, EndpointID string
			ExpiresAtMS              int64
		}
		if err := json.Unmarshal(raw, &open); err != nil {
			brokerFailure <- err
			return
		}
		if open.T != "open_data" || open.EndpointID != "endpoint-01" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		listener, err := net.Listen("unix", dataEndpoint)
		if err != nil {
			brokerFailure <- err
			return
		}
		defer listener.Close()
		if err := listener.(*net.UnixListener).SetDeadline(deadline); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, dataReady{
			T: "data_ready", RequestID: open.RequestID, StreamID: "stream-01",
			DataEndpoint: dataEndpoint, AttachNonce: "attach-01", ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
		}); err != nil {
			brokerFailure <- err
			return
		}
		data, err := listener.Accept()
		if err != nil {
			brokerFailure <- err
			return
		}
		defer data.Close()
		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var attach struct{ T, StreamID, AttachNonce string }
		if err := json.Unmarshal(raw, &attach); err != nil {
			brokerFailure <- err
			return
		}
		if attach.T != "attach_data" || attach.StreamID != "stream-01" || attach.AttachNonce != "attach-01" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		if err := writeTestFrame(brokerControl, dataOK{T: "data_ok", StreamID: "stream-01"}); err != nil {
			brokerFailure <- err
			return
		}
		_, err = data.Write([]byte("raw-data"))
		brokerFailure <- err
	}()

	client, err := newBrokerClientWithConnection(context.Background(), clientControl, descriptor)
	if err != nil {
		t.Fatal("unable to authenticate broker client:", err)
	}
	defer client.Close()
	dialContext, cancelDial := context.WithTimeout(context.Background(), brokerTestOperationTimeout)
	defer cancelDial()
	stream, err := client.Dial(dialContext, externalprotocol.DialRequest{EndpointIdentifier: "endpoint-01"})
	if err != nil {
		t.Fatal("unable to open external data stream:", err)
	}
	defer stream.Close()
	payload := make([]byte, len("raw-data"))
	if _, err := io.ReadFull(stream, payload); err != nil {
		t.Fatal("unable to read raw data:", err)
	}
	if string(payload) != "raw-data" {
		t.Fatal("raw data was framed or changed:", string(payload))
	}
	if err := <-brokerFailure; err != nil {
		t.Fatal("broker simulation failed:", err)
	}
}

func TestBrokerClientKeepsControlAfterAttachFailureUsesOriginalRequestID(t *testing.T) {
	secret := bytes.Repeat([]byte{13}, 32)
	descriptor := BrokerDescriptor{
		Protocol: BrokerProtocolVersion, BrokerEndpoint: shortUnixSocketPath(t, "control.sock"),
		BrokerInstanceID: "broker-01", LaunchNonce: "nonce-01",
		Secret: base64.RawURLEncoding.EncodeToString(secret),
	}
	clientControl, brokerControl := net.Pipe()
	deadline := time.Now().Add(brokerTestOperationTimeout)
	if err := clientControl.SetDeadline(deadline); err != nil {
		t.Fatal("unable to bound client control operations:", err)
	}
	if err := brokerControl.SetDeadline(deadline); err != nil {
		t.Fatal("unable to bound broker control operations:", err)
	}
	dataEndpoint := shortUnixSocketPath(t, "data.sock")
	brokerFailure := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(brokerControl)
		raw, err := readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var greeting hello
		if err := decodeStrict(raw, &greeting); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, helloOK{
			T: "hello_ok", Protocol: 1, BrokerInstanceID: descriptor.BrokerInstanceID,
			Proof: brokerProof(secret, "workspace-sync-broker-ok-v1", descriptor, greeting.SidecarPID),
		}); err != nil {
			brokerFailure <- err
			return
		}

		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var open struct{ T, RequestID string }
		if err := json.Unmarshal(raw, &open); err != nil || open.T != "open_data" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		listener, err := net.Listen("unix", dataEndpoint)
		if err != nil {
			brokerFailure <- err
			return
		}
		defer listener.Close()
		if err := listener.(*net.UnixListener).SetDeadline(deadline); err != nil {
			brokerFailure <- err
			return
		}
		if err := writeTestFrame(brokerControl, dataReady{
			T: "data_ready", RequestID: open.RequestID, StreamID: "stream-01",
			DataEndpoint: dataEndpoint, AttachNonce: "attach-01", ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
		}); err != nil {
			brokerFailure <- err
			return
		}
		data, err := listener.Accept()
		if err != nil {
			brokerFailure <- err
			return
		}
		data.Close()
		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var attach struct{ T, StreamID string }
		if err := json.Unmarshal(raw, &attach); err != nil || attach.T != "attach_data" || attach.StreamID != "stream-01" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		if err := writeTestFrame(brokerControl, map[string]any{
			"t": "error", "requestId": open.RequestID, "code": "data_attach_failed", "message": "data peer rejected",
		}); err != nil {
			brokerFailure <- err
			return
		}

		raw, err = readControlFrame(reader)
		if err != nil {
			brokerFailure <- err
			return
		}
		var next brokerTag
		if err := json.Unmarshal(raw, &next); err != nil || next.T != "list" || next.RequestID != "next-request" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		brokerFailure <- writeTestFrame(brokerControl, map[string]any{
			"t": "result", "requestId": next.RequestID, "result": map[string]bool{"ok": true},
		})
	}()

	client, err := newBrokerClientWithConnection(context.Background(), clientControl, descriptor)
	if err != nil {
		t.Fatal("unable to authenticate broker client:", err)
	}
	defer client.Close()
	dialContext, cancelDial := context.WithTimeout(context.Background(), brokerTestOperationTimeout)
	defer cancelDial()
	if _, err := client.Dial(dialContext, externalprotocol.DialRequest{EndpointIdentifier: "endpoint-01"}); err == nil {
		t.Fatal("data attach failure was reported as success")
	}
	response, err := client.request(context.Background(), "next-request", map[string]any{
		"t": "list", "requestId": "next-request",
	})
	if err != nil {
		t.Fatal("attach failure tore down authenticated control:", err)
	}
	var result struct{ T, RequestID string }
	if err := json.Unmarshal(response, &result); err != nil || result.T != "result" || result.RequestID != "next-request" {
		t.Fatalf("unexpected next response: %s (%v)", response, err)
	}
	if err := <-brokerFailure; err != nil {
		t.Fatal("broker simulation failed:", err)
	}
}

func writeTestFrame(writer io.Writer, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	_, err = writer.Write(payload)
	return err
}

func TestBrokerClientAcceptsDerivedLargeRequestFrameWithoutRaisingResponseLimit(t *testing.T) {
	clientConnection, brokerConnection := net.Pipe()
	defer brokerConnection.Close()
	client := &BrokerClient{
		connection: clientConnection, reader: bufio.NewReader(clientConnection),
		commandRoot: context.Background(), commandStops: make(map[string]context.CancelFunc),
		pending: make(map[string]chan json.RawMessage), cancelled: make(map[string]time.Time),
		commands: make(chan BrokerCommand, 1), done: make(chan struct{}),
	}
	go client.readLoop()
	defer client.Close()

	patterns := make([]string, 128)
	for index := range patterns {
		patterns[index] = string(bytes.Repeat([]byte{byte('a' + index%26)}, 1024))
	}
	raw, err := json.Marshal(map[string]any{
		"t": "command", "requestId": "large-create", "command": map[string]any{
			"t": "create", "requestId": "large-create", "session": map[string]any{
				"alpha": "external://alpha", "beta": "external://beta", "mode": "one-way-safe",
				"contentPolicy": map[string]any{"selection": "all_files", "extraIgnorePatterns": patterns, "extraIncludePatterns": patterns},
				"name":          "large-create", "labels": map[string]string{},
			},
		},
	})
	if err != nil || len(raw) <= MaximumControlFrameBytes {
		t.Fatalf("test command did not exceed one frame: %d (%v)", len(raw), err)
	}
	if len(raw) > MaximumControlRequestFrameBytes {
		t.Fatalf("valid maximum policy exceeded the derived request bound: %d", len(raw))
	}
	if err := writeTestRawFrame(brokerConnection, raw); err != nil {
		t.Fatal("unable to write large request frame:", err)
	}
	select {
	case command := <-client.Commands():
		defer client.FinishCommand(command.RequestID)
		if command.RequestID != "large-create" || !bytes.Equal(command.Raw, raw) {
			t.Fatal("large request command was not delivered exactly")
		}
	case <-time.After(time.Second):
		t.Fatal("large request command was not dispatched")
	}
}

func writeTestRawFrame(writer io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	_, err := writer.Write(payload)
	return err
}

func TestSendResultReturnsTypedProtocolErrorForOversizeWithoutClosingConnection(t *testing.T) {
	clientConnection, brokerConnection := net.Pipe()
	defer clientConnection.Close()
	defer brokerConnection.Close()
	client := &BrokerClient{connection: clientConnection}
	result := make(chan error, 1)
	go func() {
		result <- client.SendResult("oversize", map[string]string{"body": strings.Repeat("x", MaximumControlFrameBytes)}, nil)
		if err := client.SendResult("next", map[string]bool{"ok": true}, nil); err != nil {
			result <- err
		}
	}()
	reader := bufio.NewReader(brokerConnection)
	raw, err := readControlFrame(reader)
	if err != nil {
		t.Fatal("unable to read typed oversize response:", err)
	}
	var response brokerError
	if err := decodeStrict(raw, &response); err != nil || response.T != "error" || response.RequestID != "oversize" || response.Code != "protocol_error" {
		t.Fatalf("oversize response was not converted to a typed protocol error: %s (%v)", raw, err)
	}
	raw, err = readControlFrame(reader)
	if err != nil {
		t.Fatal("connection did not remain usable after oversize response:", err)
	}
	var next brokerTag
	if err := json.Unmarshal(raw, &next); err != nil || next.T != "result" || next.RequestID != "next" {
		t.Fatalf("unexpected follow-up response: %s (%v)", raw, err)
	}
	if err := <-result; err != nil {
		t.Fatal("typed oversize response failed:", err)
	}
}
