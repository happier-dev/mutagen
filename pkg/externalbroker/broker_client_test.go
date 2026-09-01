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
	"path/filepath"
	"testing"
	"time"

	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
)

func TestReadBrokerDescriptorRejectsTrailingJSON(t *testing.T) {
	secret := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	raw := `{"protocol":1,"brokerEndpoint":"/tmp/broker.sock","brokerInstanceId":"broker-01","launchNonce":"nonce-01","secret":"` + secret + `"}{}`
	if _, err := ReadBrokerDescriptor(bytes.NewBufferString(raw)); err == nil {
		t.Fatal("broker descriptor with a trailing JSON value accepted")
	}
}

func TestBrokerDescriptorBootstrapConnectsIPCAndReturnsTypedError(t *testing.T) {
	secret := bytes.Repeat([]byte{7}, 32)
	endpoint := filepath.Join(t.TempDir(), "control.sock")
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
	brokerFailure := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			brokerFailure <- err
			return
		}
		defer connection.Close()
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
	client, err := NewBrokerClient(context.Background(), parsed)
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
		Protocol: BrokerProtocolVersion, BrokerEndpoint: filepath.Join(t.TempDir(), "control.sock"),
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
		Protocol: BrokerProtocolVersion, BrokerEndpoint: filepath.Join(t.TempDir(), "control.sock"),
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
		BrokerEndpoint:   filepath.Join(t.TempDir(), "control.sock"),
		BrokerInstanceID: "broker-01",
		LaunchNonce:      "nonce-01",
		Secret:           base64.RawURLEncoding.EncodeToString(secret),
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
		endpoint := filepath.Join(t.TempDir(), "data.sock")
		listener, err := net.Listen("unix", endpoint)
		if err != nil {
			brokerFailure <- err
			return
		}
		defer listener.Close()
		if err := writeTestFrame(brokerControl, dataReady{
			T: "data_ready", RequestID: open.RequestID, StreamID: "stream-01",
			DataEndpoint: endpoint, AttachNonce: "attach-01", ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
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
	stream, err := client.Dial(context.Background(), externalprotocol.DialRequest{EndpointIdentifier: "endpoint-01"})
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
		Protocol: BrokerProtocolVersion, BrokerEndpoint: filepath.Join(t.TempDir(), "control.sock"),
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
		var open struct{ T, RequestID string }
		if err := json.Unmarshal(raw, &open); err != nil || open.T != "open_data" {
			brokerFailure <- io.ErrUnexpectedEOF
			return
		}
		dataEndpoint := filepath.Join(t.TempDir(), "data.sock")
		listener, err := net.Listen("unix", dataEndpoint)
		if err != nil {
			brokerFailure <- err
			return
		}
		defer listener.Close()
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
	if _, err := client.Dial(context.Background(), externalprotocol.DialRequest{EndpointIdentifier: "endpoint-01"}); err == nil {
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
