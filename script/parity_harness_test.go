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
