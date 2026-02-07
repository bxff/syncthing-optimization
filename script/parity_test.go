package main

import (
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
