package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateScenarioSnapshotRejectsPrototype(t *testing.T) {
	err := validateScenarioSnapshot("index-sequence-behavior", "go", map[string]any{
		"scenario": "index-sequence-behavior",
		"source":   "go",
		"status":   "prototype",
	})
	if err == nil {
		t.Fatal("expected prototype status to be rejected")
	}
}

func TestValidateScenarioSnapshotAcceptsValidatedPayload(t *testing.T) {
	err := validateScenarioSnapshot("index-sequence-behavior", "rust", map[string]any{
		"scenario": "index-sequence-behavior",
		"source":   "rust",
		"status":   "validated",
	})
	if err != nil {
		t.Fatalf("unexpected validation failure: %v", err)
	}
}

func TestValidateScenarioSnapshotRejectsSourceMismatch(t *testing.T) {
	err := validateScenarioSnapshot("index-sequence-behavior", "go", map[string]any{
		"scenario": "index-sequence-behavior",
		"source":   "rust",
		"status":   "validated",
	})
	if err == nil {
		t.Fatal("expected source mismatch to fail")
	}
}

func TestInferScenarioEvidenceDetectsSyntheticSnapshots(t *testing.T) {
	evidence := inferScenarioEvidence(harnessScenario{
		Go: sideConfig{
			Command: []string{"go", "run", "./script/parity_harness_go.go", "scenario", "index-sequence-behavior"},
		},
		Rust: sideConfig{
			Command: []string{"cargo", "run", "--manifest-path", "syncthing-rs/Cargo.toml", "--", "scenario", "index-sequence-behavior"},
		},
	})
	if evidence != "synthetic" {
		t.Fatalf("expected synthetic evidence, got %q", evidence)
	}
}

func TestInferScenarioEvidenceDetectsDaemonLevel(t *testing.T) {
	evidence := inferScenarioEvidence(harnessScenario{
		Go: sideConfig{
			Command: []string{"./bin/syncthing", "--home", "/tmp/node-a"},
		},
		Rust: sideConfig{
			Command: []string{"./target/debug/syncthing-rs", "daemon", "--folder-path", "/tmp/folder"},
		},
	})
	if evidence != "daemon" {
		t.Fatalf("expected daemon evidence, got %q", evidence)
	}
}

func TestInferSideEvidenceDetectsInteropScenarioCommand(t *testing.T) {
	evidence := inferSideEvidence(sideConfig{
		Command: []string{
			"cargo",
			"run",
			"--manifest-path",
			"syncthing-rs/Cargo.toml",
			"--",
			"interop-scenario",
			"protocol-state-transition",
		},
	})
	if evidence != "peer-interop" {
		t.Fatalf("expected peer-interop evidence, got %q", evidence)
	}
}

func TestInferSideEvidenceDetectsExternalSoakScenarioCommand(t *testing.T) {
	evidence := inferSideEvidence(sideConfig{
		Command: []string{
			"go",
			"run",
			"./script/parity_external_soak.go",
			"scenario",
			"external-soak-replacement",
			"--impl",
			"rust",
		},
	})
	if evidence != "external-soak" {
		t.Fatalf("expected external-soak evidence, got %q", evidence)
	}
}

func TestCompareSnapshotsEndpointSurfaceAcceptsSuperset(t *testing.T) {
	ok, msg := compareSnapshots(
		"daemon-api-surface",
		"endpoint-surface",
		map[string]any{
			"covered_endpoints": []string{
				"GET /rest/system/version",
				"GET /rest/system/status",
			},
		},
		map[string]any{
			"covered_endpoints": []string{
				"GET /rest/system/version",
				"GET /rest/system/status",
				"GET /rest/db/status",
			},
		},
	)
	if !ok {
		t.Fatalf("expected endpoint-surface comparator to pass, got msg=%q", msg)
	}
}

func TestCompareSnapshotsEndpointSurfaceRejectsMissingGoEndpoint(t *testing.T) {
	ok, msg := compareSnapshots(
		"daemon-api-surface",
		"endpoint-surface",
		map[string]any{
			"covered_endpoints": []string{
				"GET /rest/system/version",
				"GET /rest/system/status",
			},
		},
		map[string]any{
			"covered_endpoints": []string{
				"GET /rest/system/version",
			},
		},
	)
	if ok {
		t.Fatal("expected endpoint-surface comparator to fail")
	}
	if msg == "" {
		t.Fatal("expected mismatch message")
	}
}

func TestComputeInputDigestChangesWhenSourcesChange(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "input.txt")
	if err := os.WriteFile(file, []byte("alpha"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	first, err := computeInputDigest([]string{file})
	if err != nil {
		t.Fatalf("compute first digest: %v", err)
	}
	if err := os.WriteFile(file, []byte("beta"), 0o644); err != nil {
		t.Fatalf("rewrite file: %v", err)
	}
	second, err := computeInputDigest([]string{file})
	if err != nil {
		t.Fatalf("compute second digest: %v", err)
	}
	if first == second {
		t.Fatalf("expected digest to change after content update (digest=%s)", first)
	}
}

func TestCollectInputDigestFilesSkipsTargetAndGitDirs(t *testing.T) {
	root := t.TempDir()
	targetFile := filepath.Join(root, "target", "generated.bin")
	gitFile := filepath.Join(root, ".git", "index")
	includedFile := filepath.Join(root, "src", "main.rs")

	for _, path := range []string{targetFile, gitFile, includedFile} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	files, err := collectInputDigestFiles([]string{root})
	if err != nil {
		t.Fatalf("collect files: %v", err)
	}
	normalized := map[string]struct{}{}
	for _, path := range files {
		normalized[filepath.ToSlash(path)] = struct{}{}
	}
	if _, ok := normalized[filepath.ToSlash(includedFile)]; !ok {
		t.Fatalf("expected included file in digest file set: %#v", normalized)
	}
	if _, ok := normalized[filepath.ToSlash(targetFile)]; ok {
		t.Fatalf("target file must be excluded from digest file set: %#v", normalized)
	}
	if _, ok := normalized[filepath.ToSlash(gitFile)]; ok {
		t.Fatalf(".git file must be excluded from digest file set: %#v", normalized)
	}
}
