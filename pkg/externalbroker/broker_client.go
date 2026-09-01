package externalbroker

import (
	"bufio"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
)

const (
	BrokerProtocolVersion    = 1
	MaximumControlFrameBytes = 65_536
	MaximumIdentifierBytes   = 256
	openDataLifetime         = 15 * time.Second
	cancelledRequestLifetime = time.Minute
	maximumCancelledRequests = 256
)

// BrokerDescriptor is delivered through the inherited descriptor, never argv
// or the environment. Secret is standard base64 for exactly 32 random bytes.
type BrokerDescriptor struct {
	Protocol         int    `json:"protocol"`
	BrokerEndpoint   string `json:"brokerEndpoint"`
	BrokerInstanceID string `json:"brokerInstanceId"`
	LaunchNonce      string `json:"launchNonce"`
	Secret           string `json:"secret"`
}

// BrokerCommand is the strict outer envelope for the bounded manager command
// union. The raw command is decoded by the engine owner, not by the stream
// provider.
type BrokerCommand struct {
	RequestID string
	Raw       json.RawMessage
	Context   context.Context
}

type BrokerClient struct {
	connection   io.ReadWriteCloser
	reader       *bufio.Reader
	writeLock    sync.Mutex
	pendingLock  sync.Mutex
	pending      map[string]chan json.RawMessage
	cancelled    map[string]time.Time
	cancelOrder  []string
	commandRoot  context.Context
	commandLock  sync.Mutex
	commandStops map[string]context.CancelFunc
	commands     chan BrokerCommand
	done         chan struct{}
	errLock      sync.Mutex
	err          error
}

type brokerTag struct {
	T         string `json:"t"`
	RequestID string `json:"requestId,omitempty"`
	StreamID  string `json:"streamId,omitempty"`
}

type cancelCommand struct {
	T         string `json:"t"`
	RequestID string `json:"requestId"`
}

type hello struct {
	T                string `json:"t"`
	Protocol         int    `json:"protocol"`
	BrokerInstanceID string `json:"brokerInstanceId"`
	LaunchNonce      string `json:"launchNonce"`
	SidecarPID       int    `json:"sidecarPid"`
	Proof            string `json:"proof"`
}

type helloOK struct {
	T                string `json:"t"`
	Protocol         int    `json:"protocol"`
	BrokerInstanceID string `json:"brokerInstanceId"`
	Proof            string `json:"proof"`
}

type dataReady struct {
	T            string `json:"t"`
	RequestID    string `json:"requestId"`
	StreamID     string `json:"streamId"`
	DataEndpoint string `json:"dataEndpoint"`
	AttachNonce  string `json:"attachNonce"`
	ExpiresAtMS  int64  `json:"expiresAtMs"`
}

type dataOK struct {
	T        string `json:"t"`
	StreamID string `json:"streamId"`
}

type brokerError struct {
	T         string `json:"t"`
	RequestID string `json:"requestId,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

func brokerResponseError(raw json.RawMessage) error {
	var response brokerError
	if err := decodeStrict(raw, &response); err == nil && response.T == "error" && response.Code != "" && response.Message != "" {
		return fmt.Errorf("broker %s: %s", response.Code, response.Message)
	}
	return nil
}

func ReadBrokerDescriptor(reader io.Reader) (BrokerDescriptor, error) {
	var descriptor BrokerDescriptor
	raw, err := io.ReadAll(io.LimitReader(reader, MaximumControlFrameBytes+1))
	if err != nil {
		return descriptor, fmt.Errorf("invalid broker descriptor: %w", err)
	}
	if len(raw) == 0 || len(raw) > MaximumControlFrameBytes {
		return descriptor, errors.New("invalid broker descriptor length")
	}
	if err := decodeStrict(raw, &descriptor); err != nil {
		return descriptor, fmt.Errorf("invalid broker descriptor: %w", err)
	}
	if descriptor.Protocol != BrokerProtocolVersion || descriptor.BrokerEndpoint == "" || len(descriptor.BrokerEndpoint) > 4096 || !validIdentifier(descriptor.BrokerInstanceID) || !validIdentifier(descriptor.LaunchNonce) {
		return descriptor, errors.New("invalid broker descriptor identity")
	}
	secret, err := base64.RawURLEncoding.DecodeString(descriptor.Secret)
	if err != nil || len(secret) != 32 {
		return descriptor, errors.New("invalid broker descriptor secret")
	}
	return descriptor, nil
}

func NewBrokerClient(ctx context.Context, descriptor BrokerDescriptor) (*BrokerClient, error) {
	connection, err := dialBrokerEndpoint(ctx, descriptor.BrokerEndpoint)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to broker control endpoint: %w", err)
	}
	client, err := newBrokerClientWithConnection(ctx, connection, descriptor)
	if err != nil {
		connection.Close()
		return nil, err
	}
	return client, nil
}

func newBrokerClientWithConnection(ctx context.Context, connection io.ReadWriteCloser, descriptor BrokerDescriptor) (*BrokerClient, error) {
	secret, err := base64.RawURLEncoding.DecodeString(descriptor.Secret)
	if err != nil || len(secret) != 32 {
		return nil, errors.New("invalid broker secret")
	}
	client := &BrokerClient{
		connection:   connection,
		reader:       bufio.NewReader(connection),
		pending:      make(map[string]chan json.RawMessage),
		cancelled:    make(map[string]time.Time),
		commandRoot:  ctx,
		commandStops: make(map[string]context.CancelFunc),
		commands:     make(chan BrokerCommand, 16),
		done:         make(chan struct{}),
	}
	pid := os.Getpid()
	request := hello{
		T: "hello", Protocol: BrokerProtocolVersion,
		BrokerInstanceID: descriptor.BrokerInstanceID,
		LaunchNonce:      descriptor.LaunchNonce,
		SidecarPID:       pid,
		Proof:            brokerProof(secret, "workspace-sync-broker-hello-v1", descriptor, pid),
	}
	if err := client.write(request); err != nil {
		return nil, fmt.Errorf("unable to write broker hello: %w", err)
	}
	raw, err := readControlFrame(client.reader)
	if err != nil {
		return nil, fmt.Errorf("unable to read broker hello: %w", err)
	}
	var response helloOK
	if err := decodeStrict(raw, &response); err != nil || response.T != "hello_ok" || response.Protocol != BrokerProtocolVersion || response.BrokerInstanceID != descriptor.BrokerInstanceID || !hmac.Equal([]byte(response.Proof), []byte(brokerProof(secret, "workspace-sync-broker-ok-v1", descriptor, pid))) {
		return nil, errors.New("broker hello authentication failed")
	}
	go client.readLoop()
	return client, nil
}

func (c *BrokerClient) Commands() <-chan BrokerCommand { return c.commands }
func (c *BrokerClient) Done() <-chan struct{}          { return c.done }

// FinishCommand releases the cancellable context associated with a command.
// It must be called exactly once after a command leaves the engine.
func (c *BrokerClient) FinishCommand(requestID string) {
	c.commandLock.Lock()
	stop := c.commandStops[requestID]
	delete(c.commandStops, requestID)
	c.commandLock.Unlock()
	if stop != nil {
		stop()
	}
}

func (c *BrokerClient) Err() error {
	c.errLock.Lock()
	defer c.errLock.Unlock()
	return c.err
}

func (c *BrokerClient) Close() error { return c.connection.Close() }

// Dial implements the generic external stream provider. Only the opaque
// endpoint identifier crosses this boundary.
func (c *BrokerClient) Dial(ctx context.Context, request externalprotocol.DialRequest) (io.ReadWriteCloser, error) {
	if !validIdentifier(request.EndpointIdentifier) {
		return nil, errors.New("invalid opaque endpoint identifier")
	}
	requestID := uuid.NewString()
	response, err := c.request(ctx, requestID, map[string]any{
		"t": "open_data", "requestId": requestID,
		"endpointId":  request.EndpointIdentifier,
		"expiresAtMs": time.Now().Add(openDataLifetime).UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	if err := brokerResponseError(response); err != nil {
		return nil, err
	}
	var ready dataReady
	if err := decodeStrict(response, &ready); err != nil || ready.T != "data_ready" || ready.RequestID != requestID || !validIdentifier(ready.StreamID) || !validIdentifier(ready.AttachNonce) || ready.DataEndpoint == "" || time.Now().UnixMilli() >= ready.ExpiresAtMS {
		c.cancelLogicalRequest(requestID)
		return nil, errors.New("invalid data_ready response")
	}
	data, err := dialBrokerEndpoint(ctx, ready.DataEndpoint)
	if err != nil {
		c.cancelLogicalRequest(requestID)
		return nil, fmt.Errorf("unable to attach broker data stream: %w", err)
	}
	response, err = c.requestWithKeys(ctx, []string{requestID, ready.StreamID}, requestID, map[string]any{
		"t": "attach_data", "streamId": ready.StreamID, "attachNonce": ready.AttachNonce,
	})
	if err != nil {
		data.Close()
		return nil, err
	}
	if err := brokerResponseError(response); err != nil {
		data.Close()
		return nil, err
	}
	var attached dataOK
	if err := decodeStrict(response, &attached); err != nil || attached.T != "data_ok" || attached.StreamID != ready.StreamID {
		c.cancelLogicalRequest(requestID)
		data.Close()
		return nil, errors.New("invalid data_ok response")
	}
	return data, nil
}

func (c *BrokerClient) SendResult(requestID string, result any, commandError error) error {
	if commandError != nil {
		return c.write(map[string]any{
			"t": "error", "requestId": requestID,
			"code": "engine_unavailable", "message": commandError.Error(),
		})
	}
	return c.write(map[string]any{"t": "result", "requestId": requestID, "result": result})
}

func (c *BrokerClient) request(ctx context.Context, key string, message any) (json.RawMessage, error) {
	return c.requestWithKeys(ctx, []string{key}, key, message)
}

func (c *BrokerClient) requestWithKeys(ctx context.Context, keys []string, requestID string, message any) (json.RawMessage, error) {
	responses := make(chan json.RawMessage, 1)
	c.pendingLock.Lock()
	for _, key := range keys {
		if _, exists := c.pending[key]; exists {
			c.pendingLock.Unlock()
			return nil, errors.New("duplicate broker request identifier")
		}
	}
	for _, key := range keys {
		c.pending[key] = responses
	}
	c.pendingLock.Unlock()
	if err := c.write(message); err != nil {
		c.pendingLock.Lock()
		for _, key := range keys {
			delete(c.pending, key)
		}
		c.pendingLock.Unlock()
		return nil, err
	}
	select {
	case response := <-responses:
		c.pendingLock.Lock()
		for _, key := range keys {
			delete(c.pending, key)
		}
		c.pendingLock.Unlock()
		return response, nil
	case <-ctx.Done():
		c.pendingLock.Lock()
		now := time.Now()
		for _, key := range keys {
			delete(c.pending, key)
			c.rememberCancelledRequestLocked(key, now)
		}
		c.pendingLock.Unlock()
		_ = c.write(cancelCommand{T: "cancel", RequestID: requestID})
		return nil, ctx.Err()
	case <-c.done:
		c.pendingLock.Lock()
		for _, key := range keys {
			delete(c.pending, key)
		}
		c.pendingLock.Unlock()
		return nil, c.Err()
	}
}

func (c *BrokerClient) cancelLogicalRequest(requestID string) {
	c.pendingLock.Lock()
	c.rememberCancelledRequestLocked(requestID, time.Now())
	c.pendingLock.Unlock()
	_ = c.write(cancelCommand{T: "cancel", RequestID: requestID})
}

func (c *BrokerClient) rememberCancelledRequestLocked(key string, now time.Time) {
	cutoff := now.Add(-cancelledRequestLifetime)
	for len(c.cancelOrder) > 0 {
		oldest := c.cancelOrder[0]
		when := c.cancelled[oldest]
		if len(c.cancelOrder) < maximumCancelledRequests && when.After(cutoff) {
			break
		}
		delete(c.cancelled, oldest)
		c.cancelOrder = c.cancelOrder[1:]
	}
	if _, exists := c.cancelled[key]; !exists {
		c.cancelOrder = append(c.cancelOrder, key)
	}
	c.cancelled[key] = now
}

func (c *BrokerClient) readLoop() {
	defer close(c.done)
	defer close(c.commands)
	defer c.cancelCommands()
	for {
		raw, err := readControlFrame(c.reader)
		if err != nil {
			c.setErr(err)
			return
		}
		var tag brokerTag
		if err := json.Unmarshal(raw, &tag); err != nil {
			c.setErr(err)
			return
		}
		if tag.T == "command" {
			if !validIdentifier(tag.RequestID) {
				c.setErr(errors.New("invalid command request identifier"))
				return
			}
			commandContext, stop := context.WithCancel(c.commandRoot)
			c.commandLock.Lock()
			if _, exists := c.commandStops[tag.RequestID]; exists {
				c.commandLock.Unlock()
				stop()
				c.setErr(errors.New("duplicate command request identifier"))
				return
			}
			c.commandStops[tag.RequestID] = stop
			c.commandLock.Unlock()
			select {
			case c.commands <- BrokerCommand{RequestID: tag.RequestID, Raw: raw, Context: commandContext}:
			case <-c.done:
				c.FinishCommand(tag.RequestID)
				return
			}
			continue
		}
		if tag.T == "cancel" {
			var cancel cancelCommand
			if err := decodeStrict(raw, &cancel); err != nil || cancel.T != "cancel" || !validIdentifier(cancel.RequestID) {
				c.setErr(errors.New("invalid command cancellation"))
				return
			}
			c.commandLock.Lock()
			stop := c.commandStops[cancel.RequestID]
			c.commandLock.Unlock()
			if stop != nil {
				stop()
			}
			continue
		}
		key := tag.RequestID
		if key == "" {
			key = tag.StreamID
		}
		c.pendingLock.Lock()
		responses := c.pending[key]
		_, cancelled := c.cancelled[key]
		if cancelled {
			delete(c.cancelled, key)
		}
		c.pendingLock.Unlock()
		if responses == nil {
			if cancelled {
				continue
			}
			c.setErr(errors.New("unexpected broker response"))
			return
		}
		responses <- raw
	}
}

func (c *BrokerClient) cancelCommands() {
	c.commandLock.Lock()
	stops := c.commandStops
	c.commandStops = make(map[string]context.CancelFunc)
	c.commandLock.Unlock()
	for _, stop := range stops {
		stop()
	}
}

func (c *BrokerClient) setErr(err error) {
	c.errLock.Lock()
	if c.err == nil {
		c.err = err
	}
	c.errLock.Unlock()
}

func (c *BrokerClient) write(value any) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) == 0 || len(payload) > MaximumControlFrameBytes {
		return errors.New("control frame exceeds limit")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := c.connection.Write(header[:]); err != nil {
		return err
	}
	_, err = c.connection.Write(payload)
	return err
}

func readControlFrame(reader io.Reader) (json.RawMessage, error) {
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > MaximumControlFrameBytes {
		return nil, errors.New("invalid control frame length")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func decodeStrict(raw []byte, destination any) error {
	decoder := json.NewDecoder(io.LimitReader(bytesReader(raw), MaximumControlFrameBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values in control frame")
	}
	return nil
}

type byteReader struct {
	data   []byte
	offset int
}

func bytesReader(data []byte) *byteReader { return &byteReader{data: data} }
func (r *byteReader) Read(p []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.offset:])
	r.offset += n
	return n, nil
}

func brokerProof(secret []byte, domain string, descriptor BrokerDescriptor, pid int) string {
	mac := hmac.New(sha256.New, secret)
	for _, value := range []string{domain, fmt.Sprint(BrokerProtocolVersion), descriptor.BrokerInstanceID, descriptor.LaunchNonce, fmt.Sprint(pid)} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		mac.Write(size[:])
		mac.Write([]byte(value))
	}
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > MaximumIdentifierBytes {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.') {
			return false
		}
	}
	return true
}

var _ externalprotocol.StreamDialer = (*BrokerClient)(nil)
