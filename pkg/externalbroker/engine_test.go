package externalbroker

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mutagen-io/mutagen/pkg/agent"
	synchronizationapi "github.com/mutagen-io/mutagen/pkg/api/models/synchronization"
	"github.com/mutagen-io/mutagen/pkg/mutagen"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/core"
	"github.com/mutagen-io/mutagen/pkg/synchronization/endpoint/remote"
	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
	"github.com/mutagen-io/mutagen/pkg/url"
)

func TestCommandDecoderRejectsProductOwnedWorkspaceCommands(t *testing.T) {
	tests := []struct {
		name    string
		command map[string]any
		error   string
	}{
		{
			name: "relationship DTO",
			command: map[string]any{
				"t": "create", "requestId": "request-01",
				"relationship": map[string]any{"v": 1, "relationshipId": "relationship-1"},
				"sessionName":  "relationship-1",
			},
			error: "unknown field",
		},
		{
			name: "copy once operation",
			command: map[string]any{
				"t": "copy_once", "requestId": "request-01",
				"operation": map[string]any{"operationId": "copy-01"},
			},
			error: "unknown manager command",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, _ := json.Marshal(map[string]any{"t": "command", "requestId": "request-01", "command": test.command})
			if _, _, err := executeCommand(context.Background(), nil, raw); err == nil || !strings.Contains(err.Error(), test.error) {
				t.Fatalf("product-owned workspace command was not rejected: %v", err)
			}
		})
	}
}

func TestCommandDecoderRejectsProductOwnedConflictMutation(t *testing.T) {
	command := map[string]any{
		"t": "delete_conflict_loser", "requestId": "request-01", "relationshipId": "rel-01",
		"path": "src/file.go", "keep": "alpha", "expectedDigest": "abc", "expectedKind": "file",
	}
	raw, _ := json.Marshal(map[string]any{"t": "command", "requestId": "request-01", "command": command})
	if _, _, err := executeCommand(context.Background(), nil, raw); err == nil || !strings.Contains(err.Error(), "unknown manager command") {
		t.Fatalf("product-owned delete-loser command was not rejected: %v", err)
	}
}

func TestConflictProjectionReportsHonestBoundedCounts(t *testing.T) {
	projection := projectConflicts([]synchronizationapi.Conflict{{}, {}, {}}, 2, 2)
	if projection.TotalCount != 5 || projection.ShownCount != 2 || projection.TruncatedCount != 3 || len(projection.Conflicts) != 2 {
		t.Fatalf("dishonest bounded conflict projection: %+v", projection)
	}
}

func TestCreateRequiresCallerSuppliedOpaqueExternalEndpoints(t *testing.T) {
	definition := sessionDefinition{
		Alpha: "/tmp/source", Beta: "external://opaque-beta", Mode: core.SynchronizationMode_SynchronizationModeOneWaySafe,
		Name: "session-1", ContentPolicy: contentPolicy{Selection: "all_files"},
	}
	if _, err := createSession(context.Background(), nil, definition); err == nil || !strings.Contains(err.Error(), "external endpoints") {
		t.Fatalf("non-external endpoint was not rejected before manager dispatch: %v", err)
	}
}

func TestCreateRejectsGitDirectoryInclusion(t *testing.T) {
	definition := sessionDefinition{
		Alpha: "external://opaque-alpha", Beta: "external://opaque-beta",
		Mode: core.SynchronizationMode_SynchronizationModeOneWaySafe, Name: "session-1",
		ContentPolicy: contentPolicy{Selection: "git_worktree", IncludeGitDirectory: true},
	}
	if _, err := createSession(context.Background(), nil, definition); err == nil || !strings.Contains(err.Error(), "includeGitDirectory") {
		t.Fatalf("inert Git directory inclusion policy did not fail closed: %v", err)
	}
}

func TestServeEngineRunsIndependentSessionsConcurrentlyAndSerializesEachSession(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	manager, err := synchronization.NewManager(nil)
	if err != nil {
		t.Fatal("unable to create manager:", err)
	}

	clientConnection, testConnection := net.Pipe()
	broker := &BrokerClient{
		connection:   clientConnection,
		commandStops: make(map[string]context.CancelFunc),
		commands:     make(chan BrokerCommand, 4),
		done:         make(chan struct{}),
	}
	defer testConnection.Close()

	responses := make(chan string, 4)
	responseFailure := make(chan error, 1)
	go func() {
		reader := bufio.NewReader(testConnection)
		for index := 0; index < 4; index++ {
			raw, err := readControlFrame(reader)
			if err != nil {
				responseFailure <- err
				return
			}
			var response struct {
				T         string          `json:"t"`
				RequestID string          `json:"requestId"`
				Result    json.RawMessage `json:"result"`
			}
			if err := decodeStrict(raw, &response); err != nil || response.T != "result" {
				if err == nil {
					err = io.ErrUnexpectedEOF
				}
				responseFailure <- err
				return
			}
			responses <- response.RequestID
		}
		responseFailure <- nil
	}()

	slowStarted := make(chan struct{})
	independentStarted := make(chan struct{})
	sameSessionStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseSlow) })
	execute := func(ctx context.Context, _ *synchronization.Manager, raw json.RawMessage) (any, bool, error) {
		var envelope commandEnvelope
		if err := decodeStrict(raw, &envelope); err != nil {
			return nil, false, err
		}
		switch envelope.RequestID {
		case "slow-a":
			close(slowStarted)
			select {
			case <-releaseSlow:
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		case "next-a":
			close(sameSessionStarted)
		case "fast-b":
			close(independentStarted)
		}
		return map[string]string{"requestId": envelope.RequestID}, false, nil
	}

	engineContext, cancelEngine := context.WithCancel(context.Background())
	defer cancelEngine()
	engineResult := make(chan error, 1)
	go func() { engineResult <- serveEngine(engineContext, broker, manager, execute) }()

	for _, command := range []struct {
		requestID         string
		sessionIdentifier string
	}{
		{requestID: "slow-a", sessionIdentifier: "session-a"},
		{requestID: "next-a", sessionIdentifier: "session-a"},
		{requestID: "fast-b", sessionIdentifier: "session-b"},
	} {
		broker.commands <- BrokerCommand{
			RequestID: command.requestID,
			Raw: commandFrame(t, command.requestID, map[string]any{
				"t": "flush", "requestId": command.requestID,
				"sessionIdentifier": command.sessionIdentifier,
			}),
			Context: context.Background(),
		}
	}
	broker.commands <- BrokerCommand{
		RequestID: "shutdown-1",
		Raw: commandFrame(t, "shutdown-1", map[string]any{
			"t": "shutdown", "requestId": "shutdown-1",
		}),
		Context: context.Background(),
	}

	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow command was not dispatched")
	}
	select {
	case <-independentStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("independent session command was blocked behind a slow command")
	}
	select {
	case <-sameSessionStarted:
		t.Fatal("same-session mutation was reordered ahead of the slow command")
	default:
	}

	releaseOnce.Do(func() { close(releaseSlow) })
	select {
	case <-sameSessionStarted:
	case <-time.After(time.Second):
		t.Fatal("same-session mutation did not run after the preceding command completed")
	}

	seen := make(map[string]int)
	for index := 0; index < 4; index++ {
		select {
		case requestID := <-responses:
			seen[requestID]++
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for command response")
		}
	}
	if seen["slow-a"] != 1 || seen["next-a"] != 1 || seen["fast-b"] != 1 || seen["shutdown-1"] != 1 || len(seen) != 4 {
		t.Fatalf("commands did not receive exactly one response each: %#v", seen)
	}
	if err := <-responseFailure; err != nil {
		t.Fatal("unable to read command responses:", err)
	}

	if err := <-engineResult; err != nil {
		t.Fatalf("unexpected engine shutdown result: %v", err)
	}
}

func TestServeEngineAcknowledgesShutdownOnlyAfterManagerShutdownCompletes(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	manager, err := synchronization.NewManager(nil)
	if err != nil {
		t.Fatal("unable to create manager:", err)
	}
	clientConnection, testConnection := net.Pipe()
	broker := &BrokerClient{
		connection: clientConnection, commandStops: make(map[string]context.CancelFunc),
		commands: make(chan BrokerCommand, 1), done: make(chan struct{}),
	}
	defer testConnection.Close()
	shutdownStarted := make(chan struct{})
	releaseShutdown := make(chan struct{})
	engineResult := make(chan error, 1)
	go func() {
		engineResult <- serveEngineWithShutdown(context.Background(), broker, manager, executeCommand, func() {
			close(shutdownStarted)
			<-releaseShutdown
			manager.Shutdown()
		})
	}()
	broker.commands <- BrokerCommand{
		RequestID: "shutdown-1",
		Raw:       commandFrame(t, "shutdown-1", map[string]any{"t": "shutdown", "requestId": "shutdown-1"}),
		Context:   context.Background(),
	}
	select {
	case <-shutdownStarted:
	case <-time.After(time.Second):
		t.Fatal("manager shutdown did not start")
	}
	response := make(chan json.RawMessage, 1)
	responseError := make(chan error, 1)
	go func() {
		raw, readErr := readControlFrame(bufio.NewReader(testConnection))
		if readErr != nil {
			responseError <- readErr
			return
		}
		response <- raw
	}()
	select {
	case raw := <-response:
		t.Fatalf("shutdown was acknowledged before manager shutdown completed: %s", raw)
	case err := <-responseError:
		t.Fatal("unable to await shutdown response:", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseShutdown)
	select {
	case raw := <-response:
		var result struct{ T, RequestID string }
		if err := json.Unmarshal(raw, &result); err != nil || result.T != "result" || result.RequestID != "shutdown-1" {
			t.Fatalf("invalid shutdown acknowledgement: %s (%v)", raw, err)
		}
	case err := <-responseError:
		t.Fatal("unable to read shutdown response:", err)
	case <-time.After(time.Second):
		t.Fatal("shutdown was not acknowledged after manager shutdown completed")
	}
	if err := <-engineResult; err != nil {
		t.Fatalf("unexpected engine shutdown result: %v", err)
	}
}

type commandTestDialer struct{ alpha, beta string }

func (d commandTestDialer) Dial(_ context.Context, request externalprotocol.DialRequest) (io.ReadWriteCloser, error) {
	client, server := net.Pipe()
	root := d.alpha
	if request.EndpointIdentifier == "opaque-beta" {
		root = d.beta
	}
	go func() {
		if err := agent.ServerHandshake(server); err != nil {
			server.Close()
			return
		}
		if err := mutagen.ServerVersionHandshake(server); err != nil {
			server.Close()
			return
		}
		_ = remote.ServeEndpointAtRoot(nil, server, root)
	}()
	return client, nil
}

func TestManagerCommandResponsesUseExportedSessionDTOs(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	manager, err := synchronization.NewManager(nil)
	if err != nil {
		t.Fatal("unable to create manager:", err)
	}
	defer manager.Shutdown()
	previous := synchronization.ProtocolHandlers[url.Protocol_External]
	synchronization.ProtocolHandlers[url.Protocol_External] = externalprotocol.NewProtocolHandler(commandTestDialer{alpha: t.TempDir(), beta: t.TempDir()})
	defer func() { synchronization.ProtocolHandlers[url.Protocol_External] = previous }()

	policy := map[string]any{"selection": "all_files", "extraIgnorePatterns": []string{}, "extraIncludePatterns": []string{}, "includeGitDirectory": false}
	session := map[string]any{
		"alpha": "external://opaque-alpha", "beta": "external://opaque-beta",
		"mode": "one-way-safe", "contentPolicy": policy, "name": "session-1",
		"labels": map[string]string{"caller.owner": "adapter", "caller.operation": "opaque-1"},
	}
	create := map[string]any{"t": "create", "requestId": "create-1", "session": session}
	result, _, err := executeCommand(context.Background(), manager, commandFrame(t, "create-1", create))
	if err != nil {
		t.Fatal("create command failed:", err)
	}
	created, ok := result.(synchronizationapi.Session)
	if !ok || created.Name != "session-1" || created.Mode != core.SynchronizationMode_SynchronizationModeOneWaySafe || created.Labels["caller.owner"] != "adapter" || created.Labels["caller.operation"] != "opaque-1" || len(created.Labels) != 2 || created.Alpha.Protocol != url.Protocol_External || created.Alpha.Host != "opaque-alpha" || created.Beta.Protocol != url.Protocol_External || created.Beta.Host != "opaque-beta" || created.Alpha.Path != "" || created.Beta.Path != "" {
		t.Fatalf("create did not return the canonical exported session DTO: %#v", result)
	}

	for _, tag := range []string{"get", "resume", "flush", "pause"} {
		if tag == "flush" {
			deadline := time.Now().Add(10 * time.Second)
			for {
				status, statusErr := getSession(context.Background(), manager, created.Identifier)
				if statusErr == nil && status.Status == synchronization.Status_Watching {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("session did not become watchable before flush: status=%v err=%v", status.Status, statusErr)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		command := map[string]any{"t": tag, "requestId": tag + "-1", "sessionIdentifier": created.Identifier}
		result, _, err = executeCommand(context.Background(), manager, commandFrame(t, tag+"-1", command))
		if err != nil {
			t.Fatalf("%s command failed: %v", tag, err)
		}
		if _, ok := result.(synchronizationapi.Session); !ok {
			t.Fatalf("%s did not return an exported session DTO: %#v", tag, result)
		}
	}
	list := map[string]any{"t": "list", "requestId": "list-1"}
	result, _, err = executeCommand(context.Background(), manager, commandFrame(t, "list-1", list))
	if err != nil {
		t.Fatal("list command failed:", err)
	}
	if sessions, ok := result.([]synchronizationapi.Session); !ok || len(sessions) != 1 {
		t.Fatalf("list did not return exported session DTOs: %#v", result)
	}
	terminate := map[string]any{"t": "terminate", "requestId": "terminate-1", "sessionIdentifier": created.Identifier}
	if result, _, err = executeCommand(context.Background(), manager, commandFrame(t, "terminate-1", terminate)); err != nil {
		t.Fatal("terminate command failed:", err)
	} else if acknowledged, ok := result.(map[string]bool); !ok || !acknowledged["ok"] {
		t.Fatalf("terminate was not acknowledged: %#v", result)
	}
}

func commandFrame(t *testing.T, requestID string, command any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"t": "command", "requestId": requestID, "command": command})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
