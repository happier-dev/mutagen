package externalbroker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	synchronizationapi "github.com/mutagen-io/mutagen/pkg/api/models/synchronization"
	"github.com/mutagen-io/mutagen/pkg/logging"
	"github.com/mutagen-io/mutagen/pkg/selection"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	"github.com/mutagen-io/mutagen/pkg/synchronization/core"
	"github.com/mutagen-io/mutagen/pkg/synchronization/core/ignore"
	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
	"github.com/mutagen-io/mutagen/pkg/url"
)

// These mirror the public WorkspaceContentPolicyV1 boundary; the boundary test
// pins the exact accepted and rejected edges on the Go side of the wire.
const (
	maximumContentPolicyPatterns     = 128
	maximumContentPolicyPatternBytes = 1024
)

type commandEnvelope struct {
	T         string          `json:"t"`
	RequestID string          `json:"requestId"`
	Command   json.RawMessage `json:"command"`
}

type commandTag struct {
	T         string `json:"t"`
	RequestID string `json:"requestId"`
}

type sessionDefinition struct {
	Alpha         string                   `json:"alpha"`
	Beta          string                   `json:"beta"`
	Mode          core.SynchronizationMode `json:"mode"`
	ContentPolicy contentPolicy            `json:"contentPolicy"`
	Name          string                   `json:"name"`
	Labels        map[string]string        `json:"labels"`
}

type contentPolicy struct {
	Selection            string   `json:"selection"`
	ExtraIgnorePatterns  []string `json:"extraIgnorePatterns"`
	ExtraIncludePatterns []string `json:"extraIncludePatterns"`
}

type createCommand struct {
	T         string            `json:"t"`
	RequestID string            `json:"requestId"`
	Session   sessionDefinition `json:"session"`
}

type selectedCommand struct {
	T                 string `json:"t"`
	RequestID         string `json:"requestId"`
	SessionIdentifier string `json:"sessionIdentifier"`
	Limit             int    `json:"limit,omitempty"`
	Cursor            string `json:"cursor,omitempty"`
}

type listCommand struct {
	T         string `json:"t"`
	RequestID string `json:"requestId"`
	Cursor    string `json:"cursor,omitempty"`
	Limit     int    `json:"limit,omitempty"`
}

type sessionEndpointSummary struct {
	Protocol  url.Protocol `json:"protocol"`
	Host      string       `json:"host"`
	Path      string       `json:"path"`
	Connected bool         `json:"connected"`
	Scanned   bool         `json:"scanned"`
}

type sessionSummary struct {
	Identifier       string                   `json:"identifier"`
	Name             string                   `json:"name"`
	Labels           map[string]string        `json:"labels"`
	Alpha            sessionEndpointSummary   `json:"alpha"`
	Beta             sessionEndpointSummary   `json:"beta"`
	Mode             core.SynchronizationMode `json:"mode"`
	Paused           bool                     `json:"paused"`
	Status           synchronization.Status   `json:"status"`
	SuccessfulCycles uint64                   `json:"successfulCycles"`
	ConflictCount    uint64                   `json:"conflictCount"`
	LastError        string                   `json:"lastError,omitempty"`
}

type sessionListPage struct {
	Sessions   []sessionSummary `json:"sessions"`
	NextCursor *string          `json:"nextCursor"`
}

type conflictProjection struct {
	TotalCount     int                           `json:"totalCount"`
	ShownCount     int                           `json:"shownCount"`
	TruncatedCount int                           `json:"truncatedCount"`
	Conflicts      []synchronizationapi.Conflict `json:"conflicts"`
	NextCursor     *string                       `json:"nextCursor"`
}

type policyPage struct {
	Selection  string   `json:"selection"`
	Patterns   []string `json:"patterns"`
	NextCursor *string  `json:"nextCursor"`
}

// NewEngineManager bootstraps the private managed engine's synchronization
// manager against the supplied external stream dialer. Handler registration has
// to precede manager construction: NewManager starts a synchronization loop for
// every persisted unpaused session before it returns, and those loops read the
// shared protocol handler registry as they connect.
func NewEngineManager(logger *logging.Logger, dialer externalprotocol.StreamDialer) (*synchronization.Manager, error) {
	if dialer == nil {
		return nil, errors.New("nil external stream dialer")
	}
	synchronization.ProtocolHandlers[url.Protocol_External] = externalprotocol.NewProtocolHandler(dialer)
	return synchronization.NewManager(logger)
}

// ServeEngine maps the closed, generic command union to the existing Mutagen
// manager. Product relationship and operation semantics remain in the caller.
func ServeEngine(ctx context.Context, broker *BrokerClient, manager *synchronization.Manager) error {
	return serveEngine(ctx, broker, manager, executeCommand)
}

type engineCommandExecutor func(context.Context, *synchronization.Manager, json.RawMessage) (any, bool, error)

func serveEngine(ctx context.Context, broker *BrokerClient, manager *synchronization.Manager, execute engineCommandExecutor) error {
	return serveEngineWithShutdown(ctx, broker, manager, execute, manager.Shutdown)
}

func serveEngineWithShutdown(ctx context.Context, broker *BrokerClient, manager *synchronization.Manager, execute engineCommandExecutor, shutdown func()) error {
	if broker == nil || manager == nil {
		return errors.New("nil broker or synchronization manager")
	}
	if shutdown == nil {
		return errors.New("nil synchronization manager shutdown")
	}
	engineContext, stopEngine := context.WithCancel(ctx)
	var workers sync.WaitGroup
	var shutdownOnce sync.Once
	defer func() {
		stopEngine()
		workers.Wait()
		shutdownOnce.Do(shutdown)
	}()

	var orderLock sync.Mutex
	orderedTails := make(map[string]chan struct{})
	workerFailure := make(chan error, 1)
	dispatch := func(incoming BrokerCommand, serialKey string) {
		var previous <-chan struct{}
		var complete chan struct{}
		if serialKey != "" {
			complete = make(chan struct{})
			orderLock.Lock()
			previous = orderedTails[serialKey]
			orderedTails[serialKey] = complete
			orderLock.Unlock()
		}

		workers.Add(1)
		go func() {
			defer workers.Done()
			if complete != nil {
				defer func() {
					close(complete)
					orderLock.Lock()
					if orderedTails[serialKey] == complete {
						delete(orderedTails, serialKey)
					}
					orderLock.Unlock()
				}()
			}

			commandContext, stopCommand := context.WithCancel(incoming.Context)
			stopEnginePropagation := context.AfterFunc(engineContext, stopCommand)
			defer stopEnginePropagation()
			defer stopCommand()
			defer broker.FinishCommand(incoming.RequestID)

			if previous != nil {
				select {
				case <-previous:
				case <-commandContext.Done():
					return
				}
			}
			if commandContext.Err() != nil {
				return
			}

			result, _, err := execute(commandContext, manager, incoming.Raw)
			if commandContext.Err() != nil {
				return
			}
			if sendErr := broker.SendResult(incoming.RequestID, result, err); sendErr != nil {
				select {
				case workerFailure <- sendErr:
				default:
				}
			}
		}()
	}

	for {
		select {
		case <-engineContext.Done():
			return engineContext.Err()
		case <-broker.Done():
			return broker.Err()
		case err := <-workerFailure:
			return err
		case incoming, ok := <-broker.Commands():
			if !ok {
				return broker.Err()
			}
			serialKey, shutdownCommand := commandScheduling(incoming.Raw)
			if shutdownCommand {
				drained := make(chan struct{})
				go func() {
					workers.Wait()
					close(drained)
				}()
				select {
				case <-drained:
				case <-engineContext.Done():
					broker.FinishCommand(incoming.RequestID)
					return engineContext.Err()
				case <-broker.Done():
					broker.FinishCommand(incoming.RequestID)
					return broker.Err()
				case err := <-workerFailure:
					broker.FinishCommand(incoming.RequestID)
					return err
				}
				result, _, err := execute(incoming.Context, manager, incoming.Raw)
				cancelled := incoming.Context.Err() != nil
				broker.FinishCommand(incoming.RequestID)
				if cancelled {
					return nil
				}
				shutdownOnce.Do(shutdown)
				if sendErr := broker.SendResult(incoming.RequestID, result, err); sendErr != nil {
					return sendErr
				}
				return nil
			}
			dispatch(incoming, serialKey)
		}
	}
}

// commandScheduling identifies only commands whose effects must retain broker
// receipt order. Read-only commands are intentionally concurrent, and malformed
// commands are left to the strict executor so that scheduling never becomes a
// second protocol validator.
func commandScheduling(raw json.RawMessage) (string, bool) {
	var envelope commandEnvelope
	if err := decodeCommandStrict(raw, &envelope); err != nil || envelope.T != "command" {
		return "", false
	}
	var tag commandTag
	if err := json.Unmarshal(envelope.Command, &tag); err != nil || tag.RequestID != envelope.RequestID {
		return "", false
	}
	switch tag.T {
	case "create":
		var command createCommand
		if err := decodeCommandStrict(envelope.Command, &command); err == nil && command.Session.Name != "" {
			return "session:" + command.Session.Name, false
		}
	case "flush", "pause", "resume", "terminate":
		var command selectedCommand
		if err := decodeCommandStrict(envelope.Command, &command); err == nil && validIdentifier(command.SessionIdentifier) {
			return "session:" + command.SessionIdentifier, false
		}
	case "shutdown":
		var command listCommand
		if err := decodeCommandStrict(envelope.Command, &command); err == nil {
			return "", true
		}
	}
	return "", false
}

func executeCommand(ctx context.Context, manager *synchronization.Manager, raw json.RawMessage) (any, bool, error) {
	var envelope commandEnvelope
	if err := decodeCommandStrict(raw, &envelope); err != nil || envelope.T != "command" || !validIdentifier(envelope.RequestID) {
		return nil, false, errors.New("malformed command envelope")
	}
	var tag commandTag
	if err := json.Unmarshal(envelope.Command, &tag); err != nil || tag.RequestID != envelope.RequestID {
		return nil, false, errors.New("malformed command")
	}
	switch tag.T {
	case "create":
		var command createCommand
		if err := decodeCommandStrict(envelope.Command, &command); err != nil {
			return nil, false, err
		}
		identifier, err := createSession(ctx, manager, command.Session)
		if err != nil {
			return nil, false, err
		}
		status, err := getSession(ctx, manager, identifier)
		return status, false, err
	case "get", "flush", "pause", "resume", "terminate", "get_policy", "list_conflicts":
		var command selectedCommand
		if err := decodeCommandStrict(envelope.Command, &command); err != nil || !validIdentifier(command.SessionIdentifier) {
			return nil, false, errors.New("invalid selected command")
		}
		selected := &selection.Selection{Specifications: []string{command.SessionIdentifier}}
		switch tag.T {
		case "get":
			status, err := getSession(ctx, manager, command.SessionIdentifier)
			return status, false, err
		case "flush":
			if err := manager.Flush(ctx, selected, "", false); err != nil {
				return nil, false, err
			}
			status, err := getSession(ctx, manager, command.SessionIdentifier)
			return status, false, err
		case "pause":
			if err := manager.Pause(ctx, selected, ""); err != nil {
				return nil, false, err
			}
			status, err := getSession(ctx, manager, command.SessionIdentifier)
			return status, false, err
		case "resume":
			if err := manager.Resume(ctx, selected, ""); err != nil {
				return nil, false, err
			}
			status, err := getSession(ctx, manager, command.SessionIdentifier)
			return status, false, err
		case "terminate":
			if err := manager.Terminate(ctx, selected, ""); err != nil {
				return nil, false, err
			}
			return map[string]bool{"ok": true}, false, nil
		case "get_policy":
			if command.Limit < 1 || command.Limit > 100 || (command.Cursor != "" && !validIdentifier(command.Cursor)) {
				return nil, false, errors.New("invalid policy page")
			}
			_, states, err := manager.List(ctx, selected, 0)
			if err != nil {
				return nil, false, err
			}
			if len(states) != 1 {
				return nil, false, errors.New("unexpected selected session count")
			}
			return projectPolicyPage(envelope.RequestID, states[0], command.Cursor, command.Limit)
		case "list_conflicts":
			if command.Limit < 1 || command.Limit > 100 || (command.Cursor != "" && !validIdentifier(command.Cursor)) {
				return nil, false, errors.New("conflict limit must be between 1 and 100")
			}
			_, states, err := manager.List(ctx, selected, 0)
			if err != nil {
				return nil, false, err
			}
			if len(states) != 1 {
				return nil, false, errors.New("unexpected selected session count")
			}
			exported := synchronizationapi.ExportSessions(states)
			var conflicts []synchronizationapi.Conflict
			if exported[0].SessionState != nil {
				conflicts = exported[0].Conflicts
			}
			page, err := projectConflictPage(envelope.RequestID, conflicts, states[0].ExcludedConflicts, command.Cursor, command.Limit)
			return page, false, err
		}
		return nil, false, errors.New("invalid selected command")
	case "list":
		var command listCommand
		if err := decodeCommandStrict(envelope.Command, &command); err != nil || command.Limit < 1 || command.Limit > 100 || (command.Cursor != "" && !validIdentifier(command.Cursor)) {
			return nil, false, errors.New("invalid list command")
		}
		_, states, err := manager.List(ctx, &selection.Selection{All: true}, 0)
		if err != nil {
			return nil, false, err
		}
		page, err := projectSessionPage(envelope.RequestID, states, command.Cursor, command.Limit)
		return page, false, err
	case "shutdown":
		var command listCommand
		if err := decodeCommandStrict(envelope.Command, &command); err != nil {
			return nil, false, err
		}
		return map[string]bool{"ok": true}, true, nil
	default:
		return nil, false, fmt.Errorf("unknown manager command: %s", tag.T)
	}
}

func createSession(ctx context.Context, manager *synchronization.Manager, definition sessionDefinition) (string, error) {
	if definition.Name == "" || len(definition.Name) > MaximumIdentifierBytes || definition.Alpha == definition.Beta {
		return "", errors.New("invalid session definition")
	}
	alpha, err := url.Parse(definition.Alpha, url.Kind_Synchronization, true)
	if err != nil {
		return "", fmt.Errorf("invalid alpha URL: %w", err)
	}
	beta, err := url.Parse(definition.Beta, url.Kind_Synchronization, false)
	if err != nil {
		return "", fmt.Errorf("invalid beta URL: %w", err)
	}
	if alpha.Protocol != url.Protocol_External || beta.Protocol != url.Protocol_External {
		return "", errors.New("session requires opaque external endpoints")
	}
	if !definition.Mode.Supported() {
		return "", errors.New("unsupported synchronization mode")
	}
	configuration := &synchronization.Configuration{SynchronizationMode: definition.Mode}
	policy := definition.ContentPolicy
	if err := validateContentPolicy(policy); err != nil {
		return "", err
	}
	if len(definition.Labels) > 16 {
		return "", errors.New("invalid session labels")
	}
	for key, value := range definition.Labels {
		if !validIdentifier(key) || value == "" || len(value) > MaximumIdentifierBytes {
			return "", errors.New("invalid session labels")
		}
	}
	switch policy.Selection {
	case "git_worktree":
		configuration.IgnoreSyntax = ignore.Syntax_SyntaxGitWorktree
		configuration.Ignores = append(configuration.Ignores, policy.ExtraIgnorePatterns...)
		for _, pattern := range policy.ExtraIncludePatterns {
			configuration.Ignores = append(configuration.Ignores, "!"+strings.TrimPrefix(pattern, "!"))
		}
	case "all_files":
		configuration.Ignores = append(configuration.Ignores, policy.ExtraIgnorePatterns...)
		for _, pattern := range policy.ExtraIncludePatterns {
			configuration.Ignores = append(configuration.Ignores, "!"+strings.TrimPrefix(pattern, "!"))
		}
	default:
		return "", errors.New("git_selection_unavailable")
	}
	configuration.Ignores = append(configuration.Ignores, ".git/")
	return manager.Create(ctx, alpha, beta, configuration, &synchronization.Configuration{}, &synchronization.Configuration{}, definition.Name, definition.Labels, true, "")
}

func validateContentPolicy(policy contentPolicy) error {
	if len(policy.ExtraIgnorePatterns) > maximumContentPolicyPatterns || len(policy.ExtraIncludePatterns) > maximumContentPolicyPatterns {
		return errors.New("invalid content policy")
	}
	for _, patterns := range [][]string{policy.ExtraIgnorePatterns, policy.ExtraIncludePatterns} {
		for _, pattern := range patterns {
			if len(pattern) == 0 || len(pattern) > maximumContentPolicyPatternBytes {
				return errors.New("invalid content policy pattern")
			}
		}
	}
	for _, pattern := range policy.ExtraIncludePatterns {
		if strings.HasPrefix(pattern, "!") {
			return errors.New("invalid positive content policy include pattern")
		}
	}
	return nil
}

func conflictViewFingerprint(conflicts []synchronizationapi.Conflict, excluded uint64) string {
	payload, _ := json.Marshal(struct {
		Excluded  uint64                        `json:"excluded"`
		Conflicts []synchronizationapi.Conflict `json:"conflicts"`
	}{excluded, conflicts})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:8])
}

func parseConflictCursor(cursor, fingerprint string, maximum int) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	parts := strings.Split(cursor, "_")
	if len(parts) != 3 || parts[0] != "c1" || parts[1] != fingerprint {
		return 0, newBrokerResponseError("cursor_invalidated", "conflict view changed; refresh required")
	}
	offset, err := strconv.Atoi(parts[2])
	if err != nil || offset < 0 || offset > maximum {
		return 0, newBrokerResponseError("cursor_invalidated", "conflict view changed; refresh required")
	}
	return offset, nil
}

func projectConflictPage(requestID string, conflicts []synchronizationapi.Conflict, excluded uint64, cursor string, limit int) (conflictProjection, error) {
	sorted := append([]synchronizationapi.Conflict(nil), conflicts...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Root != sorted[j].Root {
			return sorted[i].Root < sorted[j].Root
		}
		left, _ := json.Marshal(sorted[i])
		right, _ := json.Marshal(sorted[j])
		return string(left) < string(right)
	})
	fingerprint := conflictViewFingerprint(sorted, excluded)
	offset, err := parseConflictCursor(cursor, fingerprint, len(sorted))
	if err != nil {
		return conflictProjection{}, err
	}
	total := len(conflicts) + int(excluded)
	result := conflictProjection{
		TotalCount: total,
		Conflicts:  make([]synchronizationapi.Conflict, 0, min(len(sorted)-offset, limit)),
	}
	for _, conflict := range sorted[offset:] {
		if len(result.Conflicts) == limit {
			break
		}
		candidate := result
		candidate.Conflicts = append(append([]synchronizationapi.Conflict(nil), result.Conflicts...), conflict)
		candidate.ShownCount = len(candidate.Conflicts)
		candidate.TruncatedCount = total - offset - candidate.ShownCount
		if offset+candidate.ShownCount < len(sorted) {
			next := fmt.Sprintf("c1_%s_%d", fingerprint, offset+candidate.ShownCount)
			candidate.NextCursor = &next
		}
		if !resultFitsControlFrame(requestID, candidate) {
			break
		}
		result = candidate
	}
	result.ShownCount = len(result.Conflicts)
	result.TruncatedCount = total - offset - result.ShownCount
	if offset+result.ShownCount < len(sorted) {
		next := fmt.Sprintf("c1_%s_%d", fingerprint, offset+result.ShownCount)
		result.NextCursor = &next
	}
	if len(sorted) > offset && result.ShownCount == 0 {
		return conflictProjection{}, newBrokerResponseError("protocol_error", "one conflict cannot fit the response frame")
	}
	return result, nil
}

func summarizeSession(state *synchronization.State) sessionSummary {
	endpoint := func(endpointURL *url.URL, endpointState *synchronization.EndpointState) sessionEndpointSummary {
		result := sessionEndpointSummary{Protocol: endpointURL.Protocol, Host: endpointURL.Host, Path: endpointURL.Path}
		if endpointState != nil {
			result.Connected = endpointState.Connected
			result.Scanned = endpointState.Scanned
		}
		return result
	}
	configuration := state.Session.Configuration
	labels := state.Session.Labels
	if labels == nil {
		labels = make(map[string]string)
	}
	return sessionSummary{
		Identifier:       state.Session.Identifier,
		Name:             state.Session.Name,
		Labels:           labels,
		Alpha:            endpoint(state.Session.Alpha, state.AlphaState),
		Beta:             endpoint(state.Session.Beta, state.BetaState),
		Mode:             configuration.SynchronizationMode,
		Paused:           state.Session.Paused,
		Status:           state.Status,
		SuccessfulCycles: state.SuccessfulCycles,
		ConflictCount:    uint64(len(state.Conflicts)) + state.ExcludedConflicts,
		LastError:        boundedResponseString(state.LastError, 4096),
	}
}

func projectPolicyPage(requestID string, state *synchronization.State, cursor string, limit int) (policyPage, bool, error) {
	// Configuration.Ignores is the exact caller-supplied policy materialized by
	// createSession. DefaultIgnores is engine-owned and must not enter recovery.
	patterns := append([]string(nil), state.Session.Configuration.Ignores...)
	selection := "all_files"
	switch state.Session.Configuration.IgnoreSyntax {
	case ignore.Syntax_SyntaxDefault, ignore.Syntax_SyntaxMutagen:
	case ignore.Syntax_SyntaxGitWorktree:
		selection = "git_worktree"
	default:
		return policyPage{}, false, newBrokerResponseError("protocol_error", "unsupported persisted policy selection")
	}
	fingerprint := policyViewFingerprint(selection, patterns)
	offset, err := parsePolicyCursor(cursor, fingerprint, len(patterns))
	if err != nil {
		return policyPage{}, false, err
	}
	result := policyPage{Selection: selection, Patterns: make([]string, 0, min(len(patterns)-offset, limit))}
	for _, pattern := range patterns[offset:] {
		if len(result.Patterns) == limit {
			break
		}
		candidate := result
		candidate.Patterns = append(append([]string(nil), result.Patterns...), pattern)
		if offset+len(candidate.Patterns) < len(patterns) {
			next := fmt.Sprintf("p1_%s_%d", fingerprint, offset+len(candidate.Patterns))
			candidate.NextCursor = &next
		}
		if !resultFitsControlFrame(requestID, candidate) {
			break
		}
		result = candidate
	}
	if len(patterns) > offset && len(result.Patterns) == 0 {
		return policyPage{}, false, newBrokerResponseError("protocol_error", "one policy pattern cannot fit the response frame")
	}
	return result, false, nil
}

func policyViewFingerprint(selection string, patterns []string) string {
	payload, _ := json.Marshal(struct {
		Selection string   `json:"selection"`
		Patterns  []string `json:"patterns"`
	}{selection, patterns})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:8])
}

func parsePolicyCursor(cursor, fingerprint string, maximum int) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	parts := strings.Split(cursor, "_")
	if len(parts) != 3 || parts[0] != "p1" || parts[1] != fingerprint {
		return 0, newBrokerResponseError("cursor_invalidated", "policy view changed; refresh required")
	}
	offset, err := strconv.Atoi(parts[2])
	if err != nil || offset < 0 || offset > maximum {
		return 0, newBrokerResponseError("cursor_invalidated", "policy view changed; refresh required")
	}
	return offset, nil
}

func projectSessionPage(requestID string, states []*synchronization.State, cursor string, limit int) (sessionListPage, error) {
	sorted := append([]*synchronization.State(nil), states...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Session.Identifier < sorted[j].Session.Identifier })
	identifiers := make([]string, len(sorted))
	for index, state := range sorted {
		identifiers[index] = state.Session.Identifier
	}
	fingerprint := sessionViewFingerprint(identifiers)
	offset, err := parseSessionCursor(cursor, fingerprint, len(sorted))
	if err != nil {
		return sessionListPage{}, err
	}
	eligible := sorted[offset:]
	result := sessionListPage{Sessions: make([]sessionSummary, 0, min(len(eligible), limit))}
	for index, state := range eligible {
		if index == limit {
			break
		}
		candidate := result
		candidate.Sessions = append(append([]sessionSummary(nil), result.Sessions...), summarizeSession(state))
		if offset+index+1 < len(sorted) {
			next := fmt.Sprintf("s1_%s_%d", fingerprint, offset+index+1)
			candidate.NextCursor = &next
		} else {
			candidate.NextCursor = nil
		}
		if !resultFitsControlFrame(requestID, candidate) {
			if len(result.Sessions) == 0 {
				return sessionListPage{}, newBrokerResponseError("protocol_error", "session summary exceeds response frame limit")
			}
			break
		}
		result = candidate
	}
	return result, nil
}

func sessionViewFingerprint(identifiers []string) string {
	payload, _ := json.Marshal(identifiers)
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:8])
}

func parseSessionCursor(cursor, fingerprint string, maximum int) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	parts := strings.Split(cursor, "_")
	if len(parts) != 3 || parts[0] != "s1" || parts[1] != fingerprint {
		return 0, newBrokerResponseError("cursor_invalidated", "session view changed; refresh required")
	}
	offset, err := strconv.Atoi(parts[2])
	if err != nil || offset < 0 || offset > maximum {
		return 0, newBrokerResponseError("cursor_invalidated", "session view changed; refresh required")
	}
	return offset, nil
}

func resultFitsControlFrame(requestID string, result any) bool {
	payload, err := json.Marshal(map[string]any{"t": "result", "requestId": requestID, "result": result})
	return err == nil && len(payload) <= MaximumControlFrameBytes
}

func getSession(ctx context.Context, manager *synchronization.Manager, identifier string) (sessionSummary, error) {
	result, found, err := findSession(ctx, manager, identifier)
	if err != nil {
		return sessionSummary{}, err
	}
	if !found {
		return sessionSummary{}, errors.New("unexpected selected session count")
	}
	return result, nil
}

func findSession(ctx context.Context, manager *synchronization.Manager, identifier string) (sessionSummary, bool, error) {
	_, states, err := manager.List(ctx, &selection.Selection{All: true}, 0)
	if err != nil {
		return sessionSummary{}, false, err
	}
	matches := states[:0]
	for _, state := range states {
		if state.Session.Identifier == identifier || state.Session.Name == identifier {
			matches = append(matches, state)
		}
	}
	if len(matches) == 0 {
		return sessionSummary{}, false, nil
	}
	if len(matches) != 1 {
		return sessionSummary{}, false, errors.New("unexpected selected session count")
	}
	return summarizeSession(matches[0]), true, nil
}
