//go:build integration

// Command integrationclient is a source-only cross-language probe for the
// Happier TypeScript broker. It is never included in release artifacts.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/mutagen-io/mutagen/pkg/externalbroker"
	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: integrationclient <opaque-endpoint-id>")
	}
	descriptorStream := os.NewFile(3, "workspace-sync-broker-descriptor")
	if descriptorStream == nil {
		return errors.New("broker descriptor fd 3 is unavailable")
	}
	descriptor, err := externalbroker.ReadBrokerDescriptor(descriptorStream)
	if err != nil {
		return err
	}
	if err := descriptorStream.Close(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client, err := externalbroker.NewBrokerClient(ctx, descriptor)
	if err != nil {
		return err
	}
	defer client.Close()
	if stream, err := client.Dial(ctx, externalprotocol.DialRequest{EndpointIdentifier: os.Args[1]}); err == nil {
		stream.Close()
		return errors.New("broker unexpectedly accepted the injected data attachment")
	}
	select {
	case command, ok := <-client.Commands():
		if !ok {
			return fmt.Errorf("broker control closed before subsequent command: %w", client.Err())
		}
		defer client.FinishCommand(command.RequestID)
		return client.SendResult(command.RequestID, map[string]bool{"controlSurvived": true}, nil)
	case <-client.Done():
		return fmt.Errorf("broker control closed before subsequent command: %w", client.Err())
	case <-ctx.Done():
		return ctx.Err()
	}
}
