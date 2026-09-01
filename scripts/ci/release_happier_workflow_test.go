package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

func TestHappierReleasePreservesMixedLicenseBuildContract(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..")
	workflowPath := filepath.Join(repositoryRoot, ".github", "workflows", "release-happier.yml")
	workflowBytes, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(workflowBytes)
	var parsedWorkflow map[interface{}]interface{}
	if err := yaml.Unmarshal(workflowBytes, &parsedWorkflow); err != nil {
		t.Fatalf("Happier release workflow is not valid YAML: %v", err)
	}

	for _, required := range []string{
		"mutagensidecar,mutagensspl",
		"mutagenagent,mutagensspl",
		"watcher: polling",
		"runner: macos-15-intel",
		"licenses/SSPL-LICENSE",
		`"licensePolicy":"mixed-mit-sspl"`,
		`"ssplEnabled":true`,
		`"sourceTag":"${GITHUB_REF_NAME}"`,
		"git archive",
		"go run -tags mutagensspl ./scripts/ci/print_licenses",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("Happier release workflow is missing %q", required)
		}
	}

	for _, forbidden := range []string{
		"mutagenfanotify",
		"runner: macos-13",
		"test ! -e sspl",
		`"licensePolicy":"mit-only"`,
		`"ssplEnabled":false`,
		"missing dependency license",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("Happier release workflow still contains obsolete policy %q", forbidden)
		}
	}
}

func TestUpstreamSSPLAndNonSSPLBuildModesRemainPresent(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..")
	for _, relativePath := range []string{
		"sspl/LICENSE",
		"pkg/mutagen/licenses_nosspl.go",
		"pkg/mutagen/licenses_sspl.go",
		"pkg/synchronization/compression/zstandard_nosspl.go",
		"pkg/synchronization/compression/zstandard_sspl.go",
		"pkg/synchronization/hashing/xxh128_nosspl.go",
		"pkg/synchronization/hashing/xxh128_sspl.go",
	} {
		if _, err := os.Stat(filepath.Join(repositoryRoot, relativePath)); err != nil {
			t.Errorf("required upstream build-mode source %s is unavailable: %v", relativePath, err)
		}
	}
}
