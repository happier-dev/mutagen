package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

type releaseWorkflow struct {
	Permissions map[string]string             `yaml:"permissions"`
	Jobs        map[string]releaseWorkflowJob `yaml:"jobs"`
}

type releaseWorkflowJob struct {
	Environment string                `yaml:"environment"`
	If          string                `yaml:"if"`
	Needs       []string              `yaml:"needs"`
	Permissions map[string]string     `yaml:"permissions"`
	Steps       []releaseWorkflowStep `yaml:"steps"`
}

type releaseWorkflowStep struct {
	Name string            `yaml:"name"`
	Env  map[string]string `yaml:"env"`
	Run  string            `yaml:"run"`
}

func TestHappierReleasePreservesMixedLicenseBuildContract(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..")
	workflowPath := filepath.Join(repositoryRoot, ".github", "workflows", "release-happier.yml")
	workflowBytes, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(workflowBytes)
	var parsedWorkflow releaseWorkflow
	if err := yaml.Unmarshal(workflowBytes, &parsedWorkflow); err != nil {
		t.Fatalf("Happier release workflow is not valid YAML: %v", err)
	}
	publishJob, ok := parsedWorkflow.Jobs["publish"]
	if !ok {
		t.Fatal("Happier release workflow has no publish job")
	}
	if publishJob.Environment != "release" {
		t.Errorf("publish job environment is %q, want release", publishJob.Environment)
	}
	if parsedWorkflow.Permissions["contents"] != "read" {
		t.Errorf("workflow-wide contents permission is %q, want read", parsedWorkflow.Permissions["contents"])
	}
	if publishJob.Permissions["contents"] != "write" {
		t.Errorf("publish contents permission is %q, want write", publishJob.Permissions["contents"])
	}
	if strings.Join(publishJob.Needs, ",") != "test,build" {
		t.Errorf("publish dependencies are %v, want test and build", publishJob.Needs)
	}
	testJob, ok := parsedWorkflow.Jobs["test"]
	if !ok {
		t.Fatal("Happier release workflow has no exact-commit source test job")
	}
	testCommands := ""
	for _, step := range testJob.Steps {
		testCommands += step.Run
	}
	for _, command := range []string{"scripts/ci/setup_go.sh", "scripts/ci/setup_ssh.sh", "scripts/ci/setup_docker.sh", "scripts/ci/analyze.sh", "scripts/ci/test.sh"} {
		if !strings.Contains(testCommands, command) {
			t.Errorf("release source test job is missing %q", command)
		}
	}
	var setupStep *releaseWorkflowStep
	var setupStepIndex = -1
	var signingStep *releaseWorkflowStep
	var signingStepIndex = -1
	for index := range publishJob.Steps {
		if publishJob.Steps[index].Name == "Set up checksum-verified Minisign" {
			setupStep = &publishJob.Steps[index]
			setupStepIndex = index
		}
		if publishJob.Steps[index].Name == "Sign aggregate checksums" {
			signingStep = &publishJob.Steps[index]
			signingStepIndex = index
		}
	}
	if setupStep == nil {
		t.Fatal("publish job has no checksum-verified Minisign setup step")
	}
	if signingStep == nil {
		t.Fatal("publish job has no aggregate-checksum signing step")
	}
	if setupStepIndex >= signingStepIndex {
		t.Fatal("checksum-verified Minisign setup must precede the secret-bearing signing step")
	}
	if len(setupStep.Env) != 0 {
		t.Fatal("Minisign setup step must not receive signing secrets")
	}
	for _, requiredCommand := range []string{
		`minisign_version="0.12"`,
		`minisign_archive_sha256="9a599b48ba6eb7b1e80f12f36b94ceca7c00b7a5173c95c3efc88d9822957e73"`,
		`minisign_binary_sha256="2c74dffcc1c9a5ee55957c60971998ace2b89f22585631594ec2152c588af8db"`,
		`test "$(uname -m)" = "x86_64"`,
		`https://github.com/jedisct1/minisign/releases/download/${minisign_version}/minisign-${minisign_version}-linux.tar.gz`,
		`sha256sum --check --strict -`,
		`test "$("${minisign_root}/minisign" -v 2>&1)" = "minisign ${minisign_version}"`,
		`echo "${minisign_root}" >> "${GITHUB_PATH}"`,
	} {
		if !strings.Contains(setupStep.Run, requiredCommand) {
			t.Errorf("Minisign setup command is missing %q", requiredCommand)
		}
	}
	expectedSigningEnvironment := map[string]string{
		"MINISIGN_PASSPHRASE": "${{ secrets.MINISIGN_PASSPHRASE }}",
		"MINISIGN_SECRET_KEY": "${{ secrets.MINISIGN_SECRET_KEY }}",
	}
	if len(signingStep.Env) != len(expectedSigningEnvironment) {
		t.Errorf("signing step environment has %d entries, want %d", len(signingStep.Env), len(expectedSigningEnvironment))
	}
	for name, expectedValue := range expectedSigningEnvironment {
		if actualValue := signingStep.Env[name]; actualValue != expectedValue {
			t.Errorf("signing step environment %s is %q, want %q", name, actualValue, expectedValue)
		}
	}
	secretReferencePattern := regexp.MustCompile(`secrets\.([A-Za-z0-9_]+)`)
	secretReferences := secretReferencePattern.FindAllStringSubmatch(workflow, -1)
	if len(secretReferences) != 2 {
		t.Errorf("Happier release workflow has %d secret references, want 2", len(secretReferences))
	}
	for _, expectedSecret := range []string{"MINISIGN_PASSPHRASE", "MINISIGN_SECRET_KEY"} {
		count := 0
		for _, reference := range secretReferences {
			if reference[1] == expectedSecret {
				count++
			}
		}
		if count != 1 {
			t.Errorf("Happier release workflow references secret %s %d times, want once", expectedSecret, count)
		}
	}
	for _, requiredCommand := range []string{
		"set +x",
		"umask 077",
		`trap 'rm -f minisign.key' EXIT`,
		`test -n "${MINISIGN_SECRET_KEY}"`,
		`test -n "${MINISIGN_PASSPHRASE}"`,
		`printf '%s\n' "${MINISIGN_PASSPHRASE}" | minisign -Sm "dist/checksums-happier-${GITHUB_REF_NAME}.txt" -s minisign.key`,
	} {
		if !strings.Contains(signingStep.Run, requiredCommand) {
			t.Errorf("signing command is missing %q", requiredCommand)
		}
	}
	if strings.Count(signingStep.Run, "MINISIGN_PASSPHRASE") != 2 {
		t.Error("signing command must use its passphrase only for a non-empty check and minisign stdin")
	}
	for _, forbiddenCommand := range []string{
		"MINISIGN_PASSWORD",
		"set -x",
		"apt-get",
		"curl",
		"tar ",
		`echo "${MINISIGN_PASSPHRASE}"`,
	} {
		if strings.Contains(signingStep.Run, forbiddenCommand) {
			t.Errorf("signing command exposes or misroutes the passphrase via %q", forbiddenCommand)
		}
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
		`"${root}/${manager_relative_path}" version`,
		`"${root}/${agent_relative_path}" version`,
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
	upstreamWorkflowBytes, err := os.ReadFile(filepath.Join(repositoryRoot, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var parsedUpstreamWorkflow releaseWorkflow
	if err := yaml.Unmarshal(upstreamWorkflowBytes, &parsedUpstreamWorkflow); err != nil {
		t.Fatalf("upstream CI workflow is not valid YAML: %v", err)
	}
	upstreamRelease := parsedUpstreamWorkflow.Jobs["release"]
	if parsedUpstreamWorkflow.Permissions["contents"] != "read" || upstreamRelease.Permissions["contents"] != "write" {
		t.Error("inherited CI grants contents write outside its upstream-only publication job")
	}
	if upstreamRelease.If != "github.ref_type == 'tag' && github.repository == 'mutagen-io/mutagen'" {
		t.Error("inherited upstream publisher is not restricted to the upstream repository")
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
