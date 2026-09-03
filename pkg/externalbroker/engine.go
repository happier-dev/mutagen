package externalbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	IncludeGitDirectory  bool     `json:"includeGitDirectory"`
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
}

type listCommand struct {
	T         string `json:"t"`
	RequestID string `json:"requestId"`
}

type conflictProjection struct {
	TotalCount     int                           `json:"totalCount"`
	ShownCount     int                           `json:"shownCount"`
	TruncatedCount int                           `json:"truncatedCount"`
	Conflicts      []synchronizationapi.Conflict `json:"conflicts"`
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
	if err := decodeStrict(raw, &envelope); err != nil || envelope.T != "command" {
		return "", false
	}
	var tag commandTag
	if err := json.Unmarshal(envelope.Command, &tag); err != nil || tag.RequestID != envelope.RequestID {
		return "", false
	}
	switch tag.T {
	case "create":
		var command createCommand
		if err := decodeStrict(envelope.Command, &command); err == nil && command.Session.Name != "" {
			return "session:" + command.Session.Name, false
		}
	case "flush", "pause", "resume", "terminate":
		var command selectedCommand
		if err := decodeStrict(envelope.Command, &command); err == nil && validIdentifier(command.SessionIdentifier) {
			return "session:" + command.SessionIdentifier, false
		}
	case "shutdown":
		var command listCommand
		if err := decodeStrict(envelope.Command, &command); err == nil {
			return "", true
		}
	}
	return "", false
}

func executeCommand(ctx context.Context, manager *synchronization.Manager, raw json.RawMessage) (any, bool, error) {
	var envelope commandEnvelope
	if err := decodeStrict(raw, &envelope); err != nil || envelope.T != "command" || !validIdentifier(envelope.RequestID) {
		return nil, false, errors.New("malformed command envelope")
	}
	var tag commandTag
	if err := json.Unmarshal(envelope.Command, &tag); err != nil || tag.RequestID != envelope.RequestID {
		return nil, false, errors.New("malformed command")
	}
	switch tag.T {
	case "create":
		var command createCommand
		if err := decodeStrict(envelope.Command, &command); err != nil {
			return nil, false, err
		}
		identifier, err := createSession(ctx, manager, command.Session)
		if err != nil {
			return nil, false, err
		}
		status, err := getSession(ctx, manager, identifier)
		return status, false, err
	case "get", "flush", "pause", "resume", "terminate", "list_conflicts":
		var command selectedCommand
		if err := decodeStrict(envelope.Command, &command); err != nil || !validIdentifier(command.SessionIdentifier) {
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
		case "list_conflicts":
			if command.Limit < 1 || command.Limit > 100 {
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
			return projectConflicts(conflicts, states[0].ExcludedConflicts, command.Limit), false, nil
		}
		return nil, false, errors.New("invalid selected command")
	case "list":
		var command listCommand
		if err := decodeStrict(envelope.Command, &command); err != nil {
			return nil, false, err
		}
		_, states, err := manager.List(ctx, &selection.Selection{All: true}, 0)
		if err != nil {
			return nil, false, err
		}
		return synchronizationapi.ExportSessions(states), false, nil
	case "shutdown":
		var command listCommand
		if err := decodeStrict(envelope.Command, &command); err != nil {
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
	if len(policy.ExtraIgnorePatterns) > 256 || len(policy.ExtraIncludePatterns) > 256 {
		return "", errors.New("invalid content policy")
	}
	if policy.IncludeGitDirectory {
		return "", errors.New("includeGitDirectory is unsupported for continuous synchronization")
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

func projectConflicts(conflicts []synchronizationapi.Conflict, excluded uint64, limit int) conflictProjection {
	shown := conflicts
	if len(shown) > limit {
		shown = shown[:limit]
	}
	total := len(conflicts) + int(excluded)
	return conflictProjection{
		TotalCount: total, ShownCount: len(shown),
		TruncatedCount: total - len(shown), Conflicts: shown,
	}
}

func getSession(ctx context.Context, manager *synchronization.Manager, identifier string) (synchronizationapi.Session, error) {
	result, found, err := findSession(ctx, manager, identifier)
	if err != nil {
		return synchronizationapi.Session{}, err
	}
	if !found {
		return synchronizationapi.Session{}, errors.New("unexpected selected session count")
	}
	return result, nil
}

func findSession(ctx context.Context, manager *synchronization.Manager, identifier string) (synchronizationapi.Session, bool, error) {
	_, states, err := manager.List(ctx, &selection.Selection{All: true}, 0)
	if err != nil {
		return synchronizationapi.Session{}, false, err
	}
	matches := states[:0]
	for _, state := range states {
		if state.Session.Identifier == identifier || state.Session.Name == identifier {
			matches = append(matches, state)
		}
	}
	if len(matches) == 0 {
		return synchronizationapi.Session{}, false, nil
	}
	if len(matches) != 1 {
		return synchronizationapi.Session{}, false, errors.New("unexpected selected session count")
	}
	return synchronizationapi.ExportSessions(matches)[0], true, nil
}
