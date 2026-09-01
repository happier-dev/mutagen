//go:build go1.22

package mutagen

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// VersionMajor represents the current major version of Mutagen.
	VersionMajor = 0
	// VersionMinor represents the current minor version of Mutagen.
	VersionMinor = 18
	// VersionPatch represents the current patch version of Mutagen.
	VersionPatch = 1
	// VersionTag represents a tag to be appended to the Mutagen version string.
	// It must not contain spaces. If empty, no tag is appended to the version
	// string.
	VersionTag = ""
)

// DevelopmentModeEnabled indicates that development mode is active. This is
// regulated via VersionTag and should not be set or updated explicitly.
const DevelopmentModeEnabled = VersionTag == "dev"

// Version provides a stringified version of the current Mutagen version.
var Version string

// BuildIdentity is injected with the immutable fork commit for release builds.
// Development builds share an explicit non-release identity so stock Mutagen
// binaries and differently pinned Happier artifacts fail the handshake.
var BuildIdentity = "happier-external-stream-v1+development"

// init performs global initialization.
func init() {
	// Compute the stringified version.
	if VersionTag != "" {
		Version = fmt.Sprintf("%d.%d.%d-%s", VersionMajor, VersionMinor, VersionPatch, VersionTag)
	} else {
		Version = fmt.Sprintf("%d.%d.%d", VersionMajor, VersionMinor, VersionPatch)
	}
}

// versionBytes is a type that can be used to send and receive version
// information over the wire.
type versionBytes [12]byte

// sendVersion writes the current version to the specified writer. Version tag
// components are neither transmitted nor received.
func sendVersion(writer io.Writer) error {
	// Compute the version bytes.
	var data versionBytes
	binary.BigEndian.PutUint32(data[:4], VersionMajor)
	binary.BigEndian.PutUint32(data[4:8], VersionMinor)
	binary.BigEndian.PutUint32(data[8:], VersionPatch)

	// Transmit the bytes.
	_, err := writer.Write(data[:])
	return err
}

// receiveVersion reads version information from the specified reader. Version
// tag components are neither transmitted nor received.
func receiveVersion(reader io.Reader) (uint32, uint32, uint32, error) {
	// Read the bytes.
	var data versionBytes
	if _, err := io.ReadFull(reader, data[:]); err != nil {
		return 0, 0, 0, err
	}

	// Decode components.
	major := binary.BigEndian.Uint32(data[:4])
	minor := binary.BigEndian.Uint32(data[4:8])
	patch := binary.BigEndian.Uint32(data[8:])

	// Done.
	return major, minor, patch, nil
}

func sendBuildIdentity(writer io.Writer) error {
	if BuildIdentity == "" || len(BuildIdentity) > 256 {
		return errors.New("invalid build identity")
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(BuildIdentity)))
	if _, err := writer.Write(size[:]); err != nil {
		return err
	}
	_, err := io.WriteString(writer, BuildIdentity)
	return err
}

func receiveBuildIdentity(reader io.Reader) (string, error) {
	var size [4]byte
	if _, err := io.ReadFull(reader, size[:]); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(size[:])
	if length == 0 || length > 256 {
		return "", errors.New("invalid build identity length")
	}
	identity := make([]byte, length)
	if _, err := io.ReadFull(reader, identity); err != nil {
		return "", err
	}
	return string(identity), nil
}

// ClientVersionHandshake performs the client side of a version handshake,
// returning an error if the received server version is not compatible with the
// client version.
//
// TODO: Add some ability to support version skew in this function.
func ClientVersionHandshake(stream io.ReadWriteCloser) error {
	// Receive the server's version.
	serverMajor, serverMinor, serverPatch, err := receiveVersion(stream)
	if err != nil {
		return fmt.Errorf("unable to receive server version: %w", err)
	}
	serverBuildIdentity, err := receiveBuildIdentity(stream)
	if err != nil {
		return fmt.Errorf("unable to receive server build identity: %w", err)
	}

	// Send our version to the server.
	if err := sendVersion(stream); err != nil {
		return fmt.Errorf("unable to send client version: %w", err)
	}
	if err := sendBuildIdentity(stream); err != nil {
		return fmt.Errorf("unable to send client build identity: %w", err)
	}

	// Ensure that our Mutagen versions are compatible. For now, we enforce that
	// they're equal.
	// TODO: Once we lock-in an internal protocol that we're going to support
	// for some time, we can allow some version skew. On the client side in
	// particular, we'll probably want to look out for the specific "locked-in"
	// server protocol that we support and instantiate some frozen client
	// implementation from that version.
	versionMatch := serverMajor == VersionMajor &&
		serverMinor == VersionMinor &&
		serverPatch == VersionPatch &&
		serverBuildIdentity == BuildIdentity
	if !versionMatch {
		return errors.New("version mismatch")
	}

	// Success.
	return nil
}

// ServerVersionHandshake performs the server side of a version handshake,
// returning an error if the received client version is not compatible with the
// server version.
//
// TODO: Add some ability to support version skew in this function.
func ServerVersionHandshake(stream io.ReadWriteCloser) error {
	// Send our version to the client.
	if err := sendVersion(stream); err != nil {
		return fmt.Errorf("unable to send server version: %w", err)
	}
	if err := sendBuildIdentity(stream); err != nil {
		return fmt.Errorf("unable to send server build identity: %w", err)
	}

	// Receive the client's version.
	clientMajor, clientMinor, clientPatch, err := receiveVersion(stream)
	if err != nil {
		return fmt.Errorf("unable to receive client version: %w", err)
	}
	clientBuildIdentity, err := receiveBuildIdentity(stream)
	if err != nil {
		return fmt.Errorf("unable to receive client build identity: %w", err)
	}

	// Ensure that our versions are compatible. For now, we enforce that they're
	// equal.
	versionMatch := clientMajor == VersionMajor &&
		clientMinor == VersionMinor &&
		clientPatch == VersionPatch &&
		clientBuildIdentity == BuildIdentity
	if !versionMatch {
		return errors.New("version mismatch")
	}

	// Success.
	return nil
}
