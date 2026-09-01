package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/mutagen-io/mutagen/cmd"

	"github.com/mutagen-io/mutagen/pkg/externalbroker"
	"github.com/mutagen-io/mutagen/pkg/logging"
	"github.com/mutagen-io/mutagen/pkg/mutagen"
	"github.com/mutagen-io/mutagen/pkg/synchronization"
	externalprotocol "github.com/mutagen-io/mutagen/pkg/synchronization/protocols/external"
	"github.com/mutagen-io/mutagen/pkg/url"
)

// rootMain is the entry point for the root command.
func rootMain(_ *cobra.Command, _ []string) error {
	if rootConfiguration.daemon {
		return runPrivateEngine()
	}
	// Set up signal handling.
	signalTermination := make(chan os.Signal, 1)
	signal.Notify(signalTermination, cmd.TerminationSignals...)

	// Wait for termination.
	<-signalTermination

	// Success.
	return nil
}

func runPrivateEngine() error {
	if rootConfiguration.dataDirectory == "" {
		return errors.New("missing required --data-directory")
	}
	if !filepath.IsAbs(rootConfiguration.dataDirectory) {
		return errors.New("--data-directory must be absolute")
	}
	descriptorNumber, err := strconv.Atoi(rootConfiguration.brokerDescriptor)
	if err != nil || descriptorNumber < 3 {
		return errors.New("invalid --broker-descriptor")
	}
	descriptorStream := os.NewFile(uintptr(descriptorNumber), "mutagen-engine-broker")
	if descriptorStream == nil {
		return errors.New("broker descriptor is unavailable")
	}
	descriptor, err := externalbroker.ReadBrokerDescriptor(descriptorStream)
	if err != nil {
		return err
	}
	if err := descriptorStream.Close(); err != nil {
		return fmt.Errorf("unable to close broker bootstrap descriptor: %w", err)
	}
	broker, err := externalbroker.NewBrokerClient(context.Background(), descriptor)
	if err != nil {
		return err
	}
	if err := os.Setenv("MUTAGEN_DATA_DIRECTORY", rootConfiguration.dataDirectory); err != nil {
		return fmt.Errorf("unable to set Mutagen data directory: %w", err)
	}
	logger := logging.NewLogger(logging.LevelInfo, os.Stderr)
	manager, err := synchronization.NewManager(logger.Sublogger("manager"))
	if err != nil {
		return fmt.Errorf("unable to initialize synchronization manager: %w", err)
	}
	synchronization.ProtocolHandlers[url.Protocol_External] = externalprotocol.NewProtocolHandler(broker)
	ctx, cancel := signal.NotifyContext(context.Background(), cmd.TerminationSignals...)
	defer cancel()
	return externalbroker.ServeEngine(ctx, broker, manager)
}

// rootCommand is the root command.
var rootCommand = &cobra.Command{
	Use:          "mutagen-sidecar",
	Version:      mutagen.Version,
	Short:        "Entry point for sidecar containers hosting Mutagen sessions",
	RunE:         rootMain,
	SilenceUsage: true,
}

// rootConfiguration stores configuration for the root command.
var rootConfiguration struct {
	// help indicates whether or not to show help information and exit.
	help             bool
	daemon           bool
	dataDirectory    string
	brokerDescriptor string
}

func init() {
	// Disable Cobra's command sorting behavior. By default, it sorts commands
	// alphabetically in the help output.
	cobra.EnableCommandSorting = false

	// Disable Cobra's use of mousetrap. This is primarily for consistency with
	// the main CLI, as it's not necessary for the sidecar.
	cobra.MousetrapHelpText = ""

	// Set the template used by the version flag.
	rootCommand.SetVersionTemplate("Mutagen sidecar version {{ .Version }}\n")

	// Grab a handle for the command line flags.
	flags := rootCommand.Flags()

	// Disable alphabetical sorting of flags in help output.
	flags.SortFlags = false

	// Manually add a help flag to override the default message. Cobra will
	// still implement its logic automatically.
	flags.BoolVarP(&rootConfiguration.help, "help", "h", false, "Show help information")
	flags.BoolVar(&rootConfiguration.daemon, "daemon", false, "Run the private managed synchronization engine")
	flags.StringVar(&rootConfiguration.dataDirectory, "data-directory", "", "Use an isolated manager data directory")
	flags.StringVar(&rootConfiguration.brokerDescriptor, "broker-descriptor", "", "Read broker control and launch secret from an inherited descriptor")

	// Hide Cobra's completion command.
	rootCommand.CompletionOptions.HiddenDefaultCmd = true

	// Register commands. We do this here (rather than in individual init
	// functions) so that we can control the order.
	rootCommand.AddCommand(
		versionCommand,
		legalCommand,
	)
}

func main() {
	// Execute the root command.
	if err := rootCommand.Execute(); err != nil {
		os.Exit(1)
	}
}
