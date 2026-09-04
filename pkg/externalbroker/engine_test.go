package externalbroker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
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
	projection, err := projectConflictPage("request-01", []synchronizationapi.Conflict{{Root: "c"}, {Root: "a"}, {Root: "b"}}, 2, "", 2)
	if err != nil {
		t.Fatal("unable to project conflicts:", err)
	}
	if projection.TotalCount != 5 || projection.ShownCount != 2 || projection.TruncatedCount != 3 || len(projection.Conflicts) != 2 {
		t.Fatalf("dishonest bounded conflict projection: %+v", projection)
	}
	if projection.Conflicts[0].Root != "a" || projection.Conflicts[1].Root != "b" || projection.NextCursor == nil {
		t.Fatalf("conflicts were not stably ordered and paginated: %+v", projection)
	}
}

func TestConflictProjectionFitsControlFrameAndPreservesCounts(t *testing.T) {
	conflicts := make([]synchronizationapi.Conflict, 100)
	for index := range conflicts {
		conflicts[index].Root = fmt.Sprintf("%03d-%s", index, strings.Repeat("x", 4096))
	}
	projection, err := projectConflictPage("request-01", conflicts, 7, "", 100)
	if err != nil {
		t.Fatal("unable to project conflicts:", err)
	}
	if projection.TotalCount != 107 || projection.ShownCount != len(projection.Conflicts) || projection.TruncatedCount != 107-projection.ShownCount {
		t.Fatalf("byte-bounded projection reported dishonest counts: %+v", projection)
	}
	if projection.ShownCount == 0 || projection.ShownCount >= len(conflicts) {
		t.Fatalf("test did not exercise byte-budget truncation: %+v", projection)
	}
	if !resultFitsControlFrame("request-01", projection) {
		t.Fatal("projected conflicts exceed the broker control frame")
	}
}

func TestConflictPaginationExhaustsStablePagesAndRejectsAChangedView(t *testing.T) {
	conflicts := []synchronizationapi.Conflict{{Root: "c"}, {Root: "a"}, {Root: "b"}}
	first, err := projectConflictPage("request-01", conflicts, 0, "", 2)
	if err != nil || first.NextCursor == nil || first.ShownCount != 2 || first.TruncatedCount != 1 {
		t.Fatalf("invalid first page: %+v (%v)", first, err)
	}
	second, err := projectConflictPage("request-02", conflicts, 0, *first.NextCursor, 2)
	if err != nil || second.NextCursor != nil || second.ShownCount != 1 || second.TruncatedCount != 0 || second.Conflicts[0].Root != "c" {
		t.Fatalf("invalid second page: %+v (%v)", second, err)
	}
	changed := append(conflicts, synchronizationapi.Conflict{Root: "d"})
	if _, err := projectConflictPage("request-03", changed, 0, *first.NextCursor, 2); err == nil {
		t.Fatalf("changed conflict view did not invalidate the cursor: %v", err)
	} else if coded, ok := err.(interface{ BrokerCode() string }); !ok || coded.BrokerCode() != "cursor_invalidated" {
		t.Fatalf("changed conflict view returned the wrong typed error: %v", err)
	}
}

func TestConflictPaginationClearsCursorWhenExactFitFinalPageHasMultipleItems(t *testing.T) {
	conflicts := []synchronizationapi.Conflict{{Root: "b"}, {Root: "a"}}
	page, err := projectConflictPage("request-01", conflicts, 0, "", 2)
	if err != nil {
		t.Fatal("unable to project exact-fit conflict page:", err)
	}
	if page.ShownCount != 2 || page.TruncatedCount != 0 || page.NextCursor != nil {
		t.Fatalf("exact-fit final conflict page retained a continuation cursor: %+v", page)
	}
}

func compactSessionState(identifier string) *synchronization.State {
	return &synchronization.State{
		Session: &synchronization.Session{
			Identifier:    identifier,
			Name:          identifier,
			Labels:        map[string]string{"external.owner": "happier-workspace-sync"},
			Alpha:         &url.URL{Protocol: url.Protocol_External, Host: "alpha-" + identifier},
			Beta:          &url.URL{Protocol: url.Protocol_External, Host: "beta-" + identifier},
			Configuration: &synchronization.Configuration{SynchronizationMode: core.SynchronizationMode_SynchronizationModeOneWaySafe},
		},
		AlphaState: &synchronization.EndpointState{},
		BetaState:  &synchronization.EndpointState{},
	}
}

func TestSessionListPaginationUsesStableIdentifierCursor(t *testing.T) {
	states := []*synchronization.State{compactSessionState("session-c"), compactSessionState("session-a"), compactSessionState("session-b")}
	first, err := projectSessionPage("request-01", states, "", 2)
	if err != nil {
		t.Fatal("unable to project first page:", err)
	}
	if len(first.Sessions) != 2 || first.Sessions[0].Identifier != "session-a" || first.Sessions[1].Identifier != "session-b" || first.NextCursor == nil || !strings.HasPrefix(*first.NextCursor, "s1_") {
		t.Fatalf("unexpected first page: %+v", first)
	}
	second, err := projectSessionPage("request-02", states, *first.NextCursor, 2)
	if err != nil {
		t.Fatal("unable to project second page:", err)
	}
	if len(second.Sessions) != 1 || second.Sessions[0].Identifier != "session-c" || second.NextCursor != nil {
		t.Fatalf("unexpected second page: %+v", second)
	}
	changed := append([]*synchronization.State{compactSessionState("session-0")}, states...)
	if _, err := projectSessionPage("request-03", changed, *first.NextCursor, 2); err == nil {
		t.Fatal("changed session view accepted a stale continuation cursor")
	} else if coded, ok := err.(interface{ BrokerCode() string }); !ok || coded.BrokerCode() != "cursor_invalidated" {
		t.Fatalf("changed session view returned the wrong error: %v", err)
	}
}

func TestCompactSessionSummaryOmitsPolicyBodiesAndFitsOneControlFrame(t *testing.T) {
	state := compactSessionState("session-large-policy")
	state.Session.Configuration.Ignores = make([]string, 256)
	for index := range state.Session.Configuration.Ignores {
		state.Session.Configuration.Ignores[index] = fmt.Sprintf("%03d-%s", index, strings.Repeat("x", 1024))
	}
	summary := summarizeSession(state)
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal("unable to encode summary:", err)
	}
	if strings.Contains(string(encoded), "ignorePaths") || !resultFitsControlFrame("request-01", summary) {
		t.Fatalf("summary leaked policy bodies or exceeded one frame: %s", encoded)
	}
}

func TestPolicyBodiesAreAvailableOnlyThroughBoundedPages(t *testing.T) {
	state := compactSessionState("session-policy")
	state.Session.Configuration.DefaultIgnores = []string{"engine-owned-default"}
	state.Session.Configuration.Ignores = make([]string, 128)
	for index := range state.Session.Configuration.Ignores {
		state.Session.Configuration.Ignores[index] = fmt.Sprintf("%03d-%s", index, strings.Repeat("x", 1020))
	}
	first, _, err := projectPolicyPage("request-01", state, "", 100)
	if err != nil || first.Selection != "all_files" || len(first.Patterns) == 0 || first.Patterns[0] == "engine-owned-default" || len(first.Patterns) >= len(state.Session.Configuration.Ignores) || first.NextCursor == nil || !resultFitsControlFrame("request-01", first) {
		t.Fatalf("invalid byte-bounded policy page: %+v (%v)", first, err)
	}
	second, _, err := projectPolicyPage("request-02", state, *first.NextCursor, 100)
	if err != nil || second.Selection != first.Selection || len(second.Patterns) == 0 {
		t.Fatalf("policy continuation was not readable: %+v (%v)", second, err)
	}
	state.Session.Configuration.Ignores[0] = "changed-policy"
	if _, _, err := projectPolicyPage("request-03", state, *first.NextCursor, 100); err == nil {
		t.Fatal("changed policy view accepted a stale continuation cursor")
	} else if coded, ok := err.(interface{ BrokerCode() string }); !ok || coded.BrokerCode() != "cursor_invalidated" {
		t.Fatalf("changed policy view returned the wrong error: %v", err)
	}
}

func TestPolicyPageProjectsGitWorktreeSelectionOutOfBand(t *testing.T) {
	state := compactSessionState("session-git-policy")
	state.Session.Configuration.IgnoreSyntax = ignore.Syntax_SyntaxGitWorktree
	state.Session.Configuration.Ignores = []string{"build/", "!build/keep.txt", ".git/"}
	page, _, err := projectPolicyPage("request-01", state, "", 100)
	if err != nil || page.Selection != "git_worktree" || len(page.Patterns) != 3 || page.Patterns[0] != "build/" {
		t.Fatalf("Git selector was not projected independently of ignore patterns: %+v (%v)", page, err)
	}
}

func TestPolicyPaginationClearsCursorWhenExactFitFinalPageHasMultipleItems(t *testing.T) {
	state := compactSessionState("session-git-policy-exact-fit")
	state.Session.Configuration.IgnoreSyntax = ignore.Syntax_SyntaxGitWorktree
	state.Session.Configuration.Ignores = []string{"build/", ".git/"}
	page, _, err := projectPolicyPage("request-01", state, "", 2)
	if err != nil {
		t.Fatal("unable to project exact-fit policy page:", err)
	}
	if len(page.Patterns) != 2 || page.NextCursor != nil {
		t.Fatalf("exact-fit final policy page retained a continuation cursor: %+v", page)
	}
}

func TestExecuteCommandDecodesMaximumPolicyAboveResponseFrameLimit(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	manager, err := synchronization.NewManager(nil)
	if err != nil {
		t.Fatal("unable to create manager:", err)
	}
	defer manager.Shutdown()
	previous := synchronization.ProtocolHandlers[url.Protocol_External]
	synchronization.ProtocolHandlers[url.Protocol_External] = externalprotocol.NewProtocolHandler(commandTestDialer{alpha: t.TempDir(), beta: t.TempDir()})
	defer func() { synchronization.ProtocolHandlers[url.Protocol_External] = previous }()

	patterns := make([]string, 128)
	for index := range patterns {
		patterns[index] = fmt.Sprintf("%03d-%s", index, strings.Repeat("x", 1020))
	}
	command := map[string]any{
		"t": "create", "requestId": "large-create",
		"session": map[string]any{
			"alpha": "external://opaque-alpha", "beta": "external://opaque-beta", "mode": "one-way-safe",
			"name": "large-session", "labels": map[string]string{},
			"contentPolicy": map[string]any{"selection": "all_files", "extraIgnorePatterns": patterns, "extraIncludePatterns": patterns},
		},
	}
	raw := commandFrame(t, "large-create", command)
	if len(raw) <= MaximumControlFrameBytes {
		t.Fatalf("test command did not cross response frame boundary: %d", len(raw))
	}
	if serialKey, shutdown := commandScheduling(raw); serialKey != "session:large-session" || shutdown {
		t.Fatalf("large command did not pass engine scheduling decode: key=%q shutdown=%v", serialKey, shutdown)
	}
	result, _, err := executeCommand(context.Background(), manager, raw)
	if err != nil {
		t.Fatal("maximum public policy command failed strict engine decoding:", err)
	}
	if summary, ok := result.(sessionSummary); !ok || summary.Name != "large-session" {
		t.Fatalf("unexpected large create result: %#v", result)
	}
}

func TestWorkspaceContentPolicyV1WireLimitsMatchPublicProtocol(t *testing.T) {
	valid := contentPolicy{
		ExtraIgnorePatterns:  make([]string, 128),
		ExtraIncludePatterns: make([]string, 128),
	}
	for index := range valid.ExtraIgnorePatterns {
		valid.ExtraIgnorePatterns[index] = strings.Repeat("x", 1024)
		valid.ExtraIncludePatterns[index] = strings.Repeat("y", 1024)
	}
	if err := validateContentPolicy(valid); err != nil {
		t.Fatalf("public maximum policy was rejected: %v", err)
	}
	tooMany := valid
	tooMany.ExtraIgnorePatterns = append(append([]string(nil), valid.ExtraIgnorePatterns...), "x")
	if err := validateContentPolicy(tooMany); err == nil {
		t.Fatal("129th policy pattern was accepted")
	}
	tooLarge := valid
	tooLarge.ExtraIncludePatterns = append([]string(nil), valid.ExtraIncludePatterns...)
	tooLarge.ExtraIncludePatterns[0] = strings.Repeat("x", 1025)
	if err := validateContentPolicy(tooLarge); err == nil {
		t.Fatal("1025-byte policy pattern was accepted")
	}
	negatedInclude := valid
	negatedInclude.ExtraIncludePatterns = append([]string(nil), valid.ExtraIncludePatterns...)
	negatedInclude.ExtraIncludePatterns[0] = "!src/generated.ts"
	if err := validateContentPolicy(negatedInclude); err == nil {
		t.Fatal("negated positive include pattern was accepted")
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

func TestCreateRejectsRemovedGitDirectoryInclusionField(t *testing.T) {
	command := map[string]any{
		"t": "create", "requestId": "request-01",
		"session": map[string]any{
			"alpha": "external://opaque-alpha", "beta": "external://opaque-beta",
			"mode": "one-way-safe", "name": "session-1", "labels": map[string]string{},
			"contentPolicy": map[string]any{
				"selection": "git_worktree", "extraIgnorePatterns": []string{},
				"extraIncludePatterns": []string{}, "includeGitDirectory": true,
			},
		},
	}
	if _, _, err := executeCommand(context.Background(), nil, commandFrame(t, "request-01", command)); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("removed Git directory inclusion field did not fail closed: %v", err)
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

func TestManagerCommandResponsesUseCompactSessionSummariesAndPagedList(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	manager, err := synchronization.NewManager(nil)
	if err != nil {
		t.Fatal("unable to create manager:", err)
	}
	defer manager.Shutdown()
	previous := synchronization.ProtocolHandlers[url.Protocol_External]
	synchronization.ProtocolHandlers[url.Protocol_External] = externalprotocol.NewProtocolHandler(commandTestDialer{alpha: t.TempDir(), beta: t.TempDir()})
	defer func() { synchronization.ProtocolHandlers[url.Protocol_External] = previous }()

	policy := map[string]any{"selection": "all_files", "extraIgnorePatterns": []string{}, "extraIncludePatterns": []string{}}
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
	created, ok := result.(sessionSummary)
	if !ok || created.Name != "session-1" || created.Mode != core.SynchronizationMode_SynchronizationModeOneWaySafe || created.Labels["caller.owner"] != "adapter" || created.Labels["caller.operation"] != "opaque-1" || len(created.Labels) != 2 || created.Alpha.Protocol != url.Protocol_External || created.Alpha.Host != "opaque-alpha" || created.Beta.Protocol != url.Protocol_External || created.Beta.Host != "opaque-beta" || created.Alpha.Path != "" || created.Beta.Path != "" {
		t.Fatalf("create did not return the compact session summary: %#v", result)
	}
	encoded, err := json.Marshal(created)
	if err != nil {
		t.Fatal("unable to encode compact summary:", err)
	}
	for _, forbidden := range []string{"configuration", "creationTime", "creatingVersion", "conflicts", "scanProblems", "transitionProblems", "ignorePaths"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("compact summary leaked verbose field %q: %s", forbidden, encoded)
		}
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
		if _, ok := result.(sessionSummary); !ok {
			t.Fatalf("%s did not return a compact session summary: %#v", tag, result)
		}
	}
	list := map[string]any{"t": "list", "requestId": "list-1", "limit": 100}
	result, _, err = executeCommand(context.Background(), manager, commandFrame(t, "list-1", list))
	if err != nil {
		t.Fatal("list command failed:", err)
	}
	if page, ok := result.(sessionListPage); !ok || len(page.Sessions) != 1 || page.NextCursor != nil {
		t.Fatalf("list did not return a compact terminal page: %#v", result)
	}
	terminate := map[string]any{"t": "terminate", "requestId": "terminate-1", "sessionIdentifier": created.Identifier}
	if result, _, err = executeCommand(context.Background(), manager, commandFrame(t, "terminate-1", terminate)); err != nil {
		t.Fatal("terminate command failed:", err)
	} else if acknowledged, ok := result.(map[string]bool); !ok || !acknowledged["ok"] {
		t.Fatalf("terminate was not acknowledged: %#v", result)
	}
}

// TestEngineManagerReconnectsPersistedExternalSessionsOnRestart covers engine
// restart, which is the only path where the manager owns sessions it did not
// just create. NewManager starts a synchronization loop for every persisted
// unpaused session before it returns, and those loops resolve their endpoints
// through the shared protocol handler registry. Registering the External
// handler after manager construction therefore both writes that registry while
// the reloaded loops are reading it and lets their first connect attempt miss
// the handler entirely, which parks each session on the reconnect interval.
func TestEngineManagerReconnectsPersistedExternalSessionsOnRestart(t *testing.T) {
	t.Setenv("MUTAGEN_DATA_DIRECTORY", t.TempDir())
	dialer := commandTestDialer{alpha: t.TempDir(), beta: t.TempDir()}
	previous := synchronization.ProtocolHandlers[url.Protocol_External]
	defer func() { synchronization.ProtocolHandlers[url.Protocol_External] = previous }()

	// Sessions are created paused, so each one is resumed and observed running
	// before shutdown; that is what leaves an unpaused session on disk for the
	// restarted manager to reload.
	seed, err := NewEngineManager(nil, dialer)
	if err != nil {
		t.Fatal("unable to create seed engine manager:", err)
	}
	const sessionCount = 4
	identifiers := make([]string, 0, sessionCount)
	for index := 0; index < sessionCount; index++ {
		name := fmt.Sprintf("session-%d", index)
		create := map[string]any{
			"t": "create", "requestId": "create-" + name,
			"session": map[string]any{
				"alpha": "external://opaque-alpha", "beta": "external://opaque-beta",
				"mode": "one-way-safe", "name": name,
				"contentPolicy": map[string]any{
					"selection":            "all_files",
					"extraIgnorePatterns":  []string{},
					"extraIncludePatterns": []string{},
				},
			},
		}
		result, _, err := executeCommand(context.Background(), seed, commandFrame(t, "create-"+name, create))
		if err != nil {
			t.Fatal("create command failed:", err)
		}
		identifier := result.(sessionSummary).Identifier
		resume := map[string]any{"t": "resume", "requestId": "resume-" + name, "sessionIdentifier": identifier}
		if _, _, err := executeCommand(context.Background(), seed, commandFrame(t, "resume-"+name, resume)); err != nil {
			t.Fatal("resume command failed:", err)
		}
		identifiers = append(identifiers, identifier)
	}
	for _, identifier := range identifiers {
		awaitExternalSessionConnected(t, seed, identifier, 30*time.Second, "before engine restart")
	}
	seed.Shutdown()

	// Restart against a registry that has no External handler yet, exactly as a
	// freshly launched sidecar process would.
	delete(synchronization.ProtocolHandlers, url.Protocol_External)
	manager, err := NewEngineManager(nil, dialer)
	if err != nil {
		t.Fatal("unable to restart engine manager:", err)
	}
	defer manager.Shutdown()

	// The reconnect interval is the failure signal: a reloaded session whose
	// first connect attempt missed the handler waits it out before retrying, so
	// every persisted session has to connect well inside it.
	for _, identifier := range identifiers {
		awaitExternalSessionConnected(t, manager, identifier, 10*time.Second, "after engine restart")
	}
}

// awaitExternalSessionConnected waits for a session to leave the disconnected
// and connecting statuses, which is the observable difference between a session
// whose endpoints resolved and one parked on the reconnect interval.
func awaitExternalSessionConnected(t *testing.T, manager *synchronization.Manager, identifier string, within time.Duration, stage string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		session, err := getSession(context.Background(), manager, identifier)
		if err != nil {
			t.Fatalf("unable to inspect session %s %s: %v", identifier, stage, err)
		}
		if session.Paused {
			t.Fatalf("session %s was paused %s", identifier, stage)
		}
		if session.Status >= synchronization.Status_Watching {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("External session %s did not connect %s within %s: status=%v", identifier, stage, within, session.Status)
		}
		time.Sleep(10 * time.Millisecond)
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
