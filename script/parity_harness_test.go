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

func TestValidateScenarioSnapshotRejectsUnknownStatus(t *testing.T) {
	err := validateScenarioSnapshot("index-sequence-behavior", "go", map[string]any{
		"scenario": "index-sequence-behavior",
		"source":   "go",
		"status":   "stub",
	})
	if err == nil {
		t.Fatal("expected unknown status to fail validation")
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

func TestCompareSnapshotsEndpointSurfaceAcceptsEqualSets(t *testing.T) {
	ok, msg := compareSnapshots(
		"endpoint-test",
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
			},
		},
	)
	if !ok {
		t.Fatalf("expected endpoint-surface comparator to pass, got msg=%q", msg)
	}
}

func TestCompareSnapshotsEndpointSurfaceRejectsMissingGoEndpoint(t *testing.T) {
	ok, msg := compareSnapshots(
		"endpoint-test",
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

func TestCompareSnapshotsEndpointSurfaceRejectsRustOnlyEndpoint(t *testing.T) {
	ok, msg := compareSnapshots(
		"endpoint-test",
		"endpoint-surface",
		map[string]any{
			"covered_endpoints": []string{
				"GET /rest/system/version",
			},
		},
		map[string]any{
			"covered_endpoints": []string{
				"GET /rest/system/version",
				"GET /rest/system/status",
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

func TestCompareSnapshotsEndpointSurfaceEnforcesRequiredReplacementEndpoints(t *testing.T) {
	dir := t.TempDir()
	gatesPath := filepath.Join(dir, "replacement-gates.json")
	if err := os.WriteFile(gatesPath, []byte(`{"required_api_endpoints":["GET /rest/system/version","GET /rest/system/status"]}`), 0o644); err != nil {
		t.Fatalf("write replacement gates: %v", err)
	}
	t.Setenv("PARITY_REPLACEMENT_GATES_PATH", gatesPath)

	ok, _ := compareSnapshots(
		"daemon-api-surface",
		"endpoint-surface",
		map[string]any{
			"covered_endpoints": []string{"GET /rest/system/version"},
		},
		map[string]any{
			"covered_endpoints": []string{"GET /rest/system/version", "GET /rest/system/status"},
		},
	)
	if ok {
		t.Fatal("expected required endpoint enforcement to fail when go side is incomplete")
	}

	ok, msg := compareSnapshots(
		"daemon-api-surface",
		"endpoint-surface",
		map[string]any{
			"covered_endpoints": []string{"GET /rest/system/version", "GET /rest/system/status"},
		},
		map[string]any{
			"covered_endpoints": []string{"GET /rest/system/version", "GET /rest/system/status"},
		},
	)
	if !ok {
		t.Fatalf("expected required endpoint enforcement to pass, got msg=%q", msg)
	}
}

func TestCompareSnapshotsProtocolSemanticsIgnoresFrameSizeValues(t *testing.T) {
	ok, msg := compareSnapshots(
		"protocol-state-transition",
		"protocol-semantics",
		map[string]any{
			"transitions":   []any{"dial", "hello", "close"},
			"message_types": []any{"hello", "close"},
			"frame_sizes":   []any{10.0, 20.0, 30.0},
		},
		map[string]any{
			"transitions":   []any{"dial", "hello", "close"},
			"message_types": []any{"hello", "close"},
			"frame_sizes":   []any{12.0, 25.0, 31.0},
		},
	)
	if !ok {
		t.Fatalf("expected protocol-semantics comparator to pass, got msg=%q", msg)
	}
}

func TestCompareSnapshotsMemoryCapAllowsDifferentEstimatedBytesUnderBudget(t *testing.T) {
	ok, msg := compareSnapshots(
		"memory-cap-50mb",
		"memory-cap",
		map[string]any{
			"file_count":             float64(10_000),
			"page_count":             float64(10),
			"scanned_entries":        float64(10_000),
			"under_budget":           true,
			"memory_budget_bytes":    float64(52_428_800),
			"estimated_memory_bytes": float64(1_200_000),
		},
		map[string]any{
			"file_count":             float64(10_000),
			"page_count":             float64(10),
			"scanned_entries":        float64(10_000),
			"under_budget":           true,
			"memory_budget_bytes":    float64(52_428_800),
			"estimated_memory_bytes": float64(1_080_000),
		},
	)
	if !ok {
		t.Fatalf("expected memory-cap comparator to pass, got msg=%q", msg)
	}
}

func TestCompareSnapshotsMemoryCapRejectsOverBudget(t *testing.T) {
	ok, msg := compareSnapshots(
		"memory-cap-50mb",
		"memory-cap",
		map[string]any{
			"file_count":             float64(10_000),
			"page_count":             float64(10),
			"scanned_entries":        float64(10_000),
			"under_budget":           true,
			"memory_budget_bytes":    float64(100),
			"estimated_memory_bytes": float64(101),
		},
		map[string]any{
			"file_count":             float64(10_000),
			"page_count":             float64(10),
			"scanned_entries":        float64(10_000),
			"under_budget":           true,
			"memory_budget_bytes":    float64(100),
			"estimated_memory_bytes": float64(50),
		},
	)
	if ok {
		t.Fatal("expected memory-cap comparator to fail for over-budget sample")
	}
	if msg == "" {
		t.Fatal("expected mismatch message for over-budget sample")
	}
}

func TestSideWithSeedEnvAddsSeedWithoutDroppingExistingEnv(t *testing.T) {
	side := sideConfig{
		Command: []string{"go", "run", "./script/parity_external_soak.go"},
		Env: map[string]string{
			"FOO": "bar",
		},
	}
	out := sideWithSeedEnv(side, 42)
	if got := out.Env["FOO"]; got != "bar" {
		t.Fatalf("expected original env var to be preserved, got %q", got)
	}
	if got := out.Env["PARITY_SEED"]; got != "42" {
		t.Fatalf("expected PARITY_SEED=42, got %q", got)
	}
	if _, ok := side.Env["PARITY_SEED"]; ok {
		t.Fatal("expected original env map to remain unmodified")
	}
}

func TestEvidenceRankOrderingSupportsStrictRequiredThresholds(t *testing.T) {
	levels := []string{"synthetic", "component", "daemon", "peer-interop", "external-soak"}
	for i := 1; i < len(levels); i++ {
		prev := levels[i-1]
		cur := levels[i]
		if scenarioEvidenceRank(cur) <= scenarioEvidenceRank(prev) {
			t.Fatalf("expected %s to rank above %s", cur, prev)
		}
	}
}

func TestNormalizeScenarioEvidenceRejectsUnknownValues(t *testing.T) {
	if got := normalizeScenarioEvidence("nonsense"); got != "" {
		t.Fatalf("expected unknown evidence to normalize to empty, got %q", got)
	}
	if got := normalizeScenarioEvidence("  INTEROP  "); got != "peer-interop" {
		t.Fatalf("expected interop alias normalization, got %q", got)
	}
}

func TestFindFlagValueSupportsSingleDashAndDoubleDashForms(t *testing.T) {
	args := []string{"go", "run", "./script/parity_external_soak.go", "scenario", "x", "-impl", "rust"}
	if got, ok := findFlagValue(args, "--impl"); !ok || got != "rust" {
		t.Fatalf("expected to parse -impl rust using --impl lookup, got ok=%v value=%q", ok, got)
	}

	args = []string{"go", "run", "./script/parity_external_soak.go", "scenario", "x", "--impl=rust"}
	if got, ok := findFlagValue(args, "--impl"); !ok || got != "rust" {
		t.Fatalf("expected to parse --impl=rust, got ok=%v value=%q", ok, got)
	}
}

func TestFindFlagValueRejectsDuplicateFlags(t *testing.T) {
	args := []string{"go", "run", "./script/parity_external_soak.go", "scenario", "x", "--impl", "rust", "--impl", "go"}
	if got, ok := findFlagValue(args, "--impl"); ok {
		t.Fatalf("expected duplicate --impl to be rejected, got ok=%v value=%q", ok, got)
	}
}

func TestValidateSideCommandAllowsGoWrapperForRustWhenImplFlagIsRust(t *testing.T) {
	err := validateSideCommand("rust", sideConfig{
		Command: []string{
			"go", "run", "./script/parity_external_soak.go", "scenario", "external-soak-replacement", "--impl", "rust",
		},
	})
	if err != nil {
		t.Fatalf("expected go wrapper with --impl rust to be accepted, got %v", err)
	}
}

func TestValidateSideCommandRejectsSnapshotOnlyConfig(t *testing.T) {
	err := validateSideCommand("go", sideConfig{SnapshotPath: "/tmp/example.json"})
	if err == nil {
		t.Fatal("expected snapshot-only config to be rejected")
	}
}

func TestNormalizeComparisonSnapshotStripsMetadataKeys(t *testing.T) {
	got := normalizeComparisonSnapshot("x", map[string]any{
		"scenario":     "x",
		"source":       "go",
		"status":       "validated",
		"generated_at": "now",
		"version":      "1",
		"produced_by":  "tool",
		"payload":      42,
	})
	if _, ok := got["scenario"]; ok {
		t.Fatal("expected scenario key to be stripped")
	}
	if _, ok := got["payload"]; !ok {
		t.Fatal("expected payload key to remain")
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
