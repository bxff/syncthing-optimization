package main

import "testing"

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
