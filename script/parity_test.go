package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateManifestAgainstGeneratedFeaturesPassesOnExactMatch(t *testing.T) {
	report := &guardrailReport{}
	manifest := featureManifest{
		Items: []featureItem{
			{
				ID:        "feat-a",
				Source:    "lib/model/folder.go",
				Subsystem: "model",
				Kind:      "func",
				Symbol:    "folder.Scan",
			},
		},
	}
	generated := []featureItem{
		{
			ID:        "feat-a",
			Source:    "lib/model/folder.go",
			Subsystem: "model",
			Kind:      "func",
			Symbol:    "folder.Scan",
		},
	}

	validateManifestAgainstGeneratedFeatures(report, manifest, generated)

	if len(report.Failures) != 0 {
		t.Fatalf("expected no failures, got %#v", report.Failures)
	}
}

func TestValidateManifestAgainstGeneratedFeaturesDetectsShapeAndCoverageGaps(t *testing.T) {
	report := &guardrailReport{}
	manifest := featureManifest{
		Items: []featureItem{
			{
				ID:        "feat-a",
				Source:    "",
				Subsystem: "model",
				Kind:      "func",
				Symbol:    "folder.Scan",
			},
		},
	}
	generated := []featureItem{
		{
			ID:        "feat-b",
			Source:    "lib/model/folder.go",
			Subsystem: "model",
			Kind:      "func",
			Symbol:    "folder.Pull",
		},
	}

	validateManifestAgainstGeneratedFeatures(report, manifest, generated)

	rules := map[string]bool{}
	for _, failure := range report.Failures {
		rules[failure.Rule] = true
	}
	if !rules["manifest-shape"] {
		t.Fatalf("expected manifest-shape failure, got %#v", report.Failures)
	}
	if !rules["manifest-missing"] {
		t.Fatalf("expected manifest-missing failure, got %#v", report.Failures)
	}
}

func TestValidateRequiredScenarioCoverageReportsUnmappedRequiredScenario(t *testing.T) {
	report := &guardrailReport{}
	ev := requiredTestEvidence{
		RequiredScenarioIDs: map[string]struct{}{
			"memory-cap-50mb": {},
			"crash-recovery":  {},
		},
		ScenarioRefs: map[string]int{
			"memory-cap-50mb": 1,
		},
	}

	validateRequiredScenarioCoverage(report, ev)

	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %d", len(report.Failures))
	}
	if report.Failures[0].Rule != "required-scenario-unmapped" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "crash-recovery") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestValidateImplementedLikeRequiresPassingScenarioForParityVerified(t *testing.T) {
	report := &guardrailReport{}
	ev := &requiredTestEvidence{
		ScenarioIDs: map[string]struct{}{
			"index-sequence-behavior": {},
		},
		ScenarioOutcome: map[string]string{
			"index-sequence-behavior": "blocked",
		},
		ScenarioRefs: make(map[string]int),
	}
	feat := featureItem{
		ID:     "feat-test",
		Source: "internal/db/interface.go",
		Symbol: "DB.AllGlobalFiles",
	}
	mi := mappingItem{
		ID:            "feat-test",
		Status:        "parity-verified",
		RustComponent: "syncthing-rs/db",
		RustSymbol:    "Db",
		RequiredTests: []string{"scenario/index-sequence-behavior"},
	}

	validateImplementedLike(report, feat, mi, ev)

	if ev.ScenarioRefs["index-sequence-behavior"] != 1 {
		t.Fatalf("expected scenario ref to be counted, got %d", ev.ScenarioRefs["index-sequence-behavior"])
	}
	found := false
	for _, failure := range report.Failures {
		if failure.Rule == "mapping-required-tests" && strings.Contains(failure.Message, "current status=\"blocked\"") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected mapping-required-tests failure, got %#v", report.Failures)
	}
}

func TestValidateImplementedLikeAllowsImplementedWithoutPassingScenario(t *testing.T) {
	report := &guardrailReport{}
	ev := &requiredTestEvidence{
		ScenarioIDs: map[string]struct{}{
			"index-sequence-behavior": {},
		},
		ScenarioOutcome: map[string]string{
			"index-sequence-behavior": "blocked",
		},
		ScenarioRefs: make(map[string]int),
	}
	feat := featureItem{
		ID:     "feat-test",
		Source: "internal/db/interface.go",
		Symbol: "DB.AllGlobalFiles",
	}
	mi := mappingItem{
		ID:            "feat-test",
		Status:        "implemented",
		RustComponent: "syncthing-rs/db",
		RustSymbol:    "Db",
		RequiredTests: []string{"scenario/index-sequence-behavior"},
	}

	validateImplementedLike(report, feat, mi, ev)

	for _, failure := range report.Failures {
		if strings.Contains(failure.Message, "current status=") {
			t.Fatalf("did not expect pass-status failure for implemented feature: %#v", report.Failures)
		}
	}
}

func TestValidateReportFreshnessRejectsStaleReports(t *testing.T) {
	report := &guardrailReport{}
	stale := time.Now().UTC().Add(-(maxDiffReportAge + 2*time.Hour)).Format(time.RFC3339)
	validateReportFreshness(report, "parity/diff-reports/latest.json", "differential report", stale, maxDiffReportAge)

	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %d", len(report.Failures))
	}
	if report.Failures[0].Rule != "differential-report-stale" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "stale") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestValidateReportFreshnessAllowsRecentReports(t *testing.T) {
	report := &guardrailReport{}
	recent := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	validateReportFreshness(report, "parity/diff-reports/latest.json", "differential report", recent, maxDiffReportAge)

	if len(report.Failures) != 0 {
		t.Fatalf("expected no failures, got %#v", report.Failures)
	}
}

func TestValidateDifferentialEvidenceInputsRejectsMissingDigest(t *testing.T) {
	report := &guardrailReport{}
	validateDifferentialEvidenceInputs(report, "parity/diff-reports/latest.json", differentialReport{
		InputRoots: []string{"script/parity.go"},
	})
	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %#v", report.Failures)
	}
	if report.Failures[0].Rule != "differential-evidence-stale" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "inputs_digest") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestValidateDifferentialEvidenceInputsRejectsDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "probe.txt")
	if err := os.WriteFile(file, []byte("old"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	digest, err := computeInputDigest([]string{file})
	if err != nil {
		t.Fatalf("compute digest: %v", err)
	}
	if err := os.WriteFile(file, []byte("new"), 0o644); err != nil {
		t.Fatalf("rewrite file: %v", err)
	}

	report := &guardrailReport{}
	validateDifferentialEvidenceInputs(report, "parity/diff-reports/latest.json", differentialReport{
		InputRoots:   []string{file},
		InputsDigest: digest,
	})
	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %#v", report.Failures)
	}
	if report.Failures[0].Rule != "differential-evidence-stale" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "outdated") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestValidateDifferentialEvidenceInputsAcceptsMatchingDigest(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "probe.txt")
	if err := os.WriteFile(file, []byte("match"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	digest, err := computeInputDigest([]string{file})
	if err != nil {
		t.Fatalf("compute digest: %v", err)
	}

	report := &guardrailReport{}
	validateDifferentialEvidenceInputs(report, "parity/diff-reports/latest.json", differentialReport{
		InputRoots:   []string{file},
		InputsDigest: digest,
	})
	if len(report.Failures) != 0 {
		t.Fatalf("expected no failures, got %#v", report.Failures)
	}
}

func TestValidateMemoryCapStatusAcceptsPassing50MBProfile(t *testing.T) {
	report := &guardrailReport{}
	test := testStatusReport{
		MemoryCap: memoryCapStatus{
			Status:    "pass",
			Flaky:     false,
			ProfileMB: 50,
		},
	}

	validateMemoryCapStatus(report, "parity/diff-reports/test-status.json", test)

	if len(report.Failures) != 0 {
		t.Fatalf("expected no failures, got %#v", report.Failures)
	}
}

func TestValidateMemoryCapStatusRejectsNon50MBProfile(t *testing.T) {
	report := &guardrailReport{}
	test := testStatusReport{
		MemoryCap: memoryCapStatus{
			Status:    "pass",
			Flaky:     false,
			ProfileMB: 64,
		},
	}

	validateMemoryCapStatus(report, "parity/diff-reports/test-status.json", test)

	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %#v", report.Failures)
	}
	if report.Failures[0].Rule != "memory-cap-tests" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "50 MB") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestValidateReplacementScenarioEvidenceRejectsSyntheticEvidence(t *testing.T) {
	report := &guardrailReport{}
	ev := requiredTestEvidence{
		RequiredScenarioIDs: map[string]struct{}{
			"protocol-state-transition": {},
		},
		ScenarioOutcome: map[string]string{
			"protocol-state-transition": "pass",
		},
		ScenarioEvidence: map[string]string{
			"protocol-state-transition": "synthetic",
		},
		ScenarioTags: map[string]map[string]struct{}{
			"protocol-state-transition": {
				"interop": {},
			},
		},
	}

	validateReplacementScenarioEvidence(report, ev)

	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %#v", report.Failures)
	}
	if report.Failures[0].Rule != "replacement-scenario-evidence" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "peer-interop") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestValidateReplacementScenarioEvidenceAllowsStrongEvidence(t *testing.T) {
	report := &guardrailReport{}
	ev := requiredTestEvidence{
		RequiredScenarioIDs: map[string]struct{}{
			"memory-cap-50mb":           {},
			"protocol-state-transition": {},
		},
		ScenarioOutcome: map[string]string{
			"memory-cap-50mb":           "pass",
			"protocol-state-transition": "pass",
		},
		ScenarioEvidence: map[string]string{
			"memory-cap-50mb":           "daemon",
			"protocol-state-transition": "peer-interop",
		},
		ScenarioTags: map[string]map[string]struct{}{
			"memory-cap-50mb": {},
			"protocol-state-transition": {
				"interop": {},
			},
		},
	}

	validateReplacementScenarioEvidence(report, ev)

	if len(report.Failures) != 0 {
		t.Fatalf("expected no failures, got %#v", report.Failures)
	}
}

func TestValidateReplacementScenarioEvidenceRequiresExternalSoakWhenTagged(t *testing.T) {
	report := &guardrailReport{}
	ev := requiredTestEvidence{
		RequiredScenarioIDs: map[string]struct{}{
			"external-soak-replacement": {},
		},
		ScenarioOutcome: map[string]string{
			"external-soak-replacement": "pass",
		},
		ScenarioEvidence: map[string]string{
			"external-soak-replacement": "daemon",
		},
		ScenarioTags: map[string]map[string]struct{}{
			"external-soak-replacement": {
				"external-soak": {},
			},
		},
	}

	validateReplacementScenarioEvidence(report, ev)

	if len(report.Failures) != 1 {
		t.Fatalf("expected 1 failure, got %#v", report.Failures)
	}
	if report.Failures[0].Rule != "replacement-scenario-evidence" {
		t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
	}
	if !strings.Contains(report.Failures[0].Message, "external-soak") {
		t.Fatalf("unexpected message: %s", report.Failures[0].Message)
	}
}

func TestNormalizeScenarioEvidenceSupportsExternalSoak(t *testing.T) {
	if got := normalizeScenarioEvidence("soak"); got != "external-soak" {
		t.Fatalf("expected soak to normalize to external-soak, got %q", got)
	}
	if rank := scenarioEvidenceRank("external-soak"); rank <= scenarioEvidenceRank("peer-interop") {
		t.Fatalf("expected external-soak rank to be above peer-interop, got %d", rank)
	}
}

func TestValidateReplacementExternalSoakRejectsWeakEvidence(t *testing.T) {
	withTempRepoLayout(t, func() {
		writeLatestDiffReport(t, differentialReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Scenarios: []diffScenario{
				{
					ID:           "external-soak-replacement",
					Status:       "pass",
					GoEvidence:   "daemon",
					RustEvidence: "external-soak",
				},
			},
		})

		report := &guardrailReport{}
		validateReplacementExternalSoak(report, []string{"external-soak-replacement"})
		if len(report.Failures) != 1 {
			t.Fatalf("expected 1 failure, got %#v", report.Failures)
		}
		if report.Failures[0].Rule != "replacement-external-soak" {
			t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
		}
	})
}

func TestValidateReplacementExternalSoakAcceptsBothSidesSoakEvidence(t *testing.T) {
	withTempRepoLayout(t, func() {
		writeLatestDiffReport(t, differentialReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Scenarios: []diffScenario{
				{
					ID:           "external-soak-replacement",
					Status:       "pass",
					GoEvidence:   "external-soak",
					RustEvidence: "external-soak",
				},
			},
		})

		report := &guardrailReport{}
		validateReplacementExternalSoak(report, []string{"external-soak-replacement"})
		if len(report.Failures) != 0 {
			t.Fatalf("expected no failures, got %#v", report.Failures)
		}
	})
}

func TestToStringSetHandlesStringSlicesAndAnySlices(t *testing.T) {
	fromStrings := toStringSet([]string{"a", " ", "b"})
	if _, ok := fromStrings["a"]; !ok {
		t.Fatalf("expected key a in set: %#v", fromStrings)
	}
	if _, ok := fromStrings["b"]; !ok {
		t.Fatalf("expected key b in set: %#v", fromStrings)
	}
	if len(fromStrings) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(fromStrings))
	}

	fromAny := toStringSet([]any{"x", 7, "y"})
	if len(fromAny) != 2 {
		t.Fatalf("expected 2 entries from []any, got %d", len(fromAny))
	}
}

func TestMissingRequiredStringsReturnsSortedMissingEntries(t *testing.T) {
	covered := map[string]struct{}{
		"beta": {},
	}
	missing := missingRequiredStrings([]string{"gamma", "beta", "alpha"}, covered)
	if len(missing) != 2 {
		t.Fatalf("expected 2 missing entries, got %#v", missing)
	}
	if missing[0] != "alpha" || missing[1] != "gamma" {
		t.Fatalf("expected sorted missing entries, got %#v", missing)
	}
}

func TestValidateReplacementAPIEndpointCoverageRequiresEndpointAssertions(t *testing.T) {
	withTempRepoLayout(t, func() {
		writeLatestDiffReport(t, differentialReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Scenarios: []diffScenario{
				{
					ID:           "daemon-api-surface",
					Status:       "pass",
					RustEvidence: "daemon",
				},
			},
		})
		writeRustAPISnapshot(t, map[string]any{
			"scenario": "daemon-api-surface",
			"source":   "rust",
			"status":   "validated",
			"covered_endpoints": []string{
				"GET /rest/system/version",
			},
		})

		report := &guardrailReport{}
		validateReplacementAPIEndpointCoverage(report, []string{"GET /rest/system/version"})
		if len(report.Failures) != 1 {
			t.Fatalf("expected exactly one failure, got %#v", report.Failures)
		}
		if report.Failures[0].Rule != "replacement-api-coverage" {
			t.Fatalf("unexpected rule: %s", report.Failures[0].Rule)
		}
		if !strings.Contains(report.Failures[0].Message, "endpoint_assertions") {
			t.Fatalf("unexpected message: %s", report.Failures[0].Message)
		}
	})
}

func TestValidateReplacementAPIEndpointCoverageRequiresResponseKeys(t *testing.T) {
	withTempRepoLayout(t, func() {
		writeLatestDiffReport(t, differentialReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Scenarios: []diffScenario{
				{
					ID:           "daemon-api-surface",
					Status:       "pass",
					RustEvidence: "daemon",
				},
			},
		})
		writeRustAPISnapshot(t, map[string]any{
			"scenario":               "daemon-api-surface",
			"source":                 "rust",
			"status":                 "validated",
			"covered_endpoints":      []string{"GET /rest/system/version"},
			"covered_endpoint_count": 1,
			"endpoint_assertions": map[string]any{
				"GET /rest/system/version": map[string]any{
					"status_code":   200,
					"response_kind": "object",
					"required_keys": []string{"version", "longVersion"},
					"present_keys":  []string{"version"},
				},
			},
		})

		report := &guardrailReport{}
		validateReplacementAPIEndpointCoverage(report, []string{"GET /rest/system/version"})
		if len(report.Failures) == 0 {
			t.Fatalf("expected failures, got %#v", report.Failures)
		}
		found := false
		for _, failure := range report.Failures {
			if failure.Rule != "replacement-api-coverage" {
				continue
			}
			if strings.Contains(failure.Message, "missing required response key") {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected missing required response key failure, got %#v", report.Failures)
		}
	})
}

func TestValidateReplacementAPIEndpointCoverageAcceptsValidAssertions(t *testing.T) {
	withTempRepoLayout(t, func() {
		writeLatestDiffReport(t, differentialReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Scenarios: []diffScenario{
				{
					ID:           "daemon-api-surface",
					Status:       "pass",
					RustEvidence: "daemon",
				},
			},
		})
		writeRustAPISnapshot(t, map[string]any{
			"scenario":               "daemon-api-surface",
			"source":                 "rust",
			"status":                 "validated",
			"covered_endpoints":      []string{"GET /rest/system/version"},
			"covered_endpoint_count": 1,
			"endpoint_assertions": map[string]any{
				"GET /rest/system/version": map[string]any{
					"status_code":   200,
					"response_kind": "object",
					"required_keys": []string{"version", "longVersion"},
					"present_keys":  []string{"version", "longVersion", "os", "arch"},
				},
			},
		})

		report := &guardrailReport{}
		validateReplacementAPIEndpointCoverage(report, []string{"GET /rest/system/version"})
		if len(report.Failures) != 0 {
			t.Fatalf("expected no failures, got %#v", report.Failures)
		}
	})
}

func TestNormalizeBEPMessageTypeNormalizesCamelCaseAndDelimiters(t *testing.T) {
	cases := map[string]string{
		"ClusterConfig":               "cluster_config",
		"FileDownloadProgressUpdate":  "file_download_progress_update",
		"ClusterConfig_Folder_Device": "cluster_config_folder_device",
		"index-update":                "index_update",
	}

	for in, expected := range cases {
		if got := normalizeBEPMessageType(in); got != expected {
			t.Fatalf("normalizeBEPMessageType(%q) = %q, expected %q", in, got, expected)
		}
	}
}

func TestExtractGoBEPMessageTypesContainsCoreMessages(t *testing.T) {
	types, err := extractGoBEPMessageTypes()
	if err != nil {
		t.Fatalf("extractGoBEPMessageTypes error: %v", err)
	}

	for _, expected := range []string{
		"hello",
		"cluster_config",
		"index",
		"request",
		"response",
		"close",
	} {
		if _, ok := types[expected]; !ok {
			t.Fatalf("expected go bep surface to contain %q", expected)
		}
	}
}

func TestExtractRustBEPMessageTypesContainsCoreMessages(t *testing.T) {
	types, err := extractRustBEPMessageTypes()
	if err != nil {
		t.Fatalf("extractRustBEPMessageTypes error: %v", err)
	}

	for _, expected := range []string{
		"hello",
		"cluster_config",
		"index",
		"request",
		"response",
		"close",
		"file_info",
		"block_info",
		"platform_data",
		"xattr",
	} {
		if _, ok := types[expected]; !ok {
			t.Fatalf("expected rust bep surface to contain %q", expected)
		}
	}
}

func TestExtractGoFolderModesContainsCanonicalModes(t *testing.T) {
	modes, err := extractGoFolderModes()
	if err != nil {
		t.Fatalf("extractGoFolderModes error: %v", err)
	}

	for _, expected := range []string{"sendrecv", "sendonly", "recvonly", "recvenc"} {
		if _, ok := modes[expected]; !ok {
			t.Fatalf("expected go folder mode surface to contain %q", expected)
		}
	}
}

func TestExtractGoRESTSurfaceContainsKnownEndpoint(t *testing.T) {
	surface, err := extractGoRESTSurface()
	if err != nil {
		t.Fatalf("extractGoRESTSurface error: %v", err)
	}
	if _, ok := surface["GET /rest/system/version"]; !ok {
		t.Fatalf("expected go rest surface to contain GET /rest/system/version")
	}
}

func withTempRepoLayout(t *testing.T, fn func()) {
	t.Helper()
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		if chdirErr := os.Chdir(wd); chdirErr != nil {
			t.Fatalf("restore wd: %v", chdirErr)
		}
	})
	if err := os.MkdirAll(filepath.Join("parity", "diff-reports"), 0o755); err != nil {
		t.Fatalf("mkdir diff reports: %v", err)
	}
	fn()
}

func writeLatestDiffReport(t *testing.T, report differentialReport) {
	t.Helper()
	bs, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	path := filepath.Join("parity", "diff-reports", "latest.json")
	if err := os.WriteFile(path, bs, 0o644); err != nil {
		t.Fatalf("write report: %v", err)
	}
}

func writeRustAPISnapshot(t *testing.T, snapshot map[string]any) {
	t.Helper()
	path := filepath.Join("parity", "harness", "snapshots")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir snapshots: %v", err)
	}
	bs, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	fullPath := filepath.Join(path, "rust-daemon-api-surface.json")
	if err := os.WriteFile(fullPath, bs, 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
}
