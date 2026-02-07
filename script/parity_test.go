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
