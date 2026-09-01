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
	provenanceBytes, err := os.ReadFile(filepath.Join(repositoryRoot, "fork-provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	provenance := string(provenanceBytes)

	for _, required := range []string{
		"MINISIGN_SECRET_KEY: ${{ secrets.MINISIGN_SECRET_KEY }}",
		"mutagensidecar,mutagensspl",
		"mutagenagent,mutagensspl",
		"watcher: polling",
		"runner: macos-15-intel",
		"curl --retry 5 --retry-all-errors --retry-delay 2 --retry-max-time 120 -fsSLO",
		"sha256_file()",
		"licenses/SSPL-LICENSE",
		`"licensePolicy":"mixed-mit-sspl"`,
		`"ssplEnabled":true`,
		`"sourceTag":"${GITHUB_REF_NAME}"`,
		"git archive",
		"go run -tags mutagensspl ./scripts/ci/print_licenses",
		`windows-amd64) executable_suffix=".exe"`,
		`manager_relative_path="bin/happier-mutagen${executable_suffix}"`,
		`agent_relative_path="bin/happier-mutagen-agent${executable_suffix}"`,
		`-o "${root}/${manager_relative_path}"`,
		`-o "${root}/${agent_relative_path}"`,
		`"managerPath":"${manager_relative_path}"`,
		`"agentPath":"${agent_relative_path}"`,
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("Happier release workflow is missing %q", required)
		}
	}

	for _, forbidden := range []string{
		"MUTAGEN_RELEASE_MINISIGN_SECRET_KEY",
		"mutagenfanotify",
		"runner: macos-13",
		`manager_sha="$(shasum`,
		`agent_sha="$(shasum`,
		"test ! -e sspl",
		`"licensePolicy":"mit-only"`,
		`"ssplEnabled":false`,
		"missing dependency license",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Errorf("Happier release workflow still contains obsolete policy %q", forbidden)
		}
	}
	if strings.Contains(provenance, "mutagenfanotify") {
		t.Error("fork provenance still claims fanotify for the Happier managed release")
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
