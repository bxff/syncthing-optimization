package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func FuzzFindFlagValue(f *testing.F) {
	f.Add("--impl", "rust", "impl")
	f.Add("--impl=rust", "", "impl")
	f.Add("-impl", "rust", "--impl")
	f.Add("", "", "")
	f.Fuzz(func(t *testing.T, arg1, arg2, flag string) {
		args := []string{}
		if arg1 != "" {
			args = append(args, arg1)
		}
		if arg2 != "" {
			args = append(args, arg2)
		}
		findFlagValue(args, flag)
	})
}

func FuzzValidateSideCommand(f *testing.F) {
	f.Add("go", "go run ./script/parity_external_soak.go scenario x --impl go")
	f.Add("rust", "go run ./script/parity_external_soak.go scenario x --impl rust")
	f.Add("rust", "cargo run --manifest-path syncthing-rs/Cargo.toml -- daemon")
	f.Fuzz(func(t *testing.T, source, cmdLine string) {
		side := sideConfig{Command: strings.Fields(cmdLine)}
		_ = validateSideCommand(source, side)
	})
}

func FuzzNormalizeScenarioEvidenceIdempotent(f *testing.F) {
	f.Add("synthetic")
	f.Add("DAEMON")
	f.Add("  interop ")
	f.Add("unknown")
	f.Fuzz(func(t *testing.T, input string) {
		once := normalizeScenarioEvidence(input)
		twice := normalizeScenarioEvidence(once)
		if once != twice {
			t.Fatalf("normalize is not idempotent: %q -> %q -> %q", input, once, twice)
		}
	})
}

func FuzzCanonicalJSONStable(f *testing.F) {
	f.Add(`{"b":1,"a":2}`)
	f.Add(`[1,null,"x"]`)
	f.Add(`{"nested":{"k":[3,2,1]}}`)
	f.Fuzz(func(t *testing.T, raw string) {
		var v any
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return
		}
		first, err := canonicalJSON(v)
		if err != nil {
			return
		}
		var decoded any
		if err := json.Unmarshal(first, &decoded); err != nil {
			t.Fatalf("canonical output must be valid json: %v", err)
		}
		second, err := canonicalJSON(decoded)
		if err != nil {
			t.Fatalf("second canonicalization failed: %v", err)
		}
		if !bytes.Equal(first, second) {
			t.Fatalf("canonical output not stable")
		}
	})
}

func FuzzCompareSnapshotsNoPanic(f *testing.F) {
	f.Add("daemon-api-surface", "endpoint-surface", `{"covered_endpoints":["GET /rest/system/status"]}`, `{"covered_endpoints":["GET /rest/system/status"]}`)
	f.Add("memory-cap-50mb", "json-equal", `{"a":1}`, `{"a":1}`)
	f.Add(
		"external-soak-replacement",
		"normalized-state-projection",
		`{"checks":{"scan_ok":true},"state_projection":{"my_id":"LOCAL","device_ids":["LOCAL"],"folder_summaries":[{"id":"default","local_files":1,"global_files":1,"need_files":0}],"pending_device_ids":[],"pending_folder_ids":[],"invalid_pending_device_ids":[],"invalid_pending_folder_ids":[]}}`,
		`{"checks":{"scan_ok":true},"state_projection":{"my_id":"LOCAL","device_ids":["LOCAL"],"folder_summaries":[{"id":"default","local_files":1,"global_files":1,"need_files":0}],"pending_device_ids":[],"pending_folder_ids":[],"invalid_pending_device_ids":[],"invalid_pending_folder_ids":[]}}`,
	)
	f.Add("x", "bogus-mode", `{}`, `{}`)
	f.Fuzz(func(t *testing.T, id, mode, goRaw, rustRaw string) {
		var goSnap map[string]any
		var rustSnap map[string]any
		if err := json.Unmarshal([]byte(goRaw), &goSnap); err != nil {
			return
		}
		if err := json.Unmarshal([]byte(rustRaw), &rustSnap); err != nil {
			return
		}
		_, _ = compareSnapshots(id, mode, goSnap, rustSnap)
	})
}
