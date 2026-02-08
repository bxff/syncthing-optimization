// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const harnessSchemaVersion = 1

var parityEvidenceInputRoots = []string{
	"syncthing-rs/src",
	"lib/api/api.go",
	"lib/config",
	"lib/model",
	"lib/protocol",
	"internal/db/interface.go",
	"internal/gen/bep/bep.pb.go",
	"parity/harness/scenarios.json",
	"parity/replacement-gates.json",
	"script/parity.go",
	"script/parity_external_soak.go",
	"script/parity_harness.go",
	"script/parity_harness_go.go",
}

type harnessConfig struct {
	SchemaVersion int               `json:"schema_version"`
	Description   string            `json:"description"`
	TimeoutSec    int               `json:"timeout_sec"`
	Scenarios     []harnessScenario `json:"scenarios"`
}

type harnessScenario struct {
	ID          string     `json:"id"`
	Description string     `json:"description"`
	Severity    string     `json:"severity"`
	Required    bool       `json:"required"`
	Tags        []string   `json:"tags"`
	Evidence    string     `json:"evidence"`
	Comparator  string     `json:"comparator"`
	Go          sideConfig `json:"go"`
	Rust        sideConfig `json:"rust"`
}

type sideConfig struct {
	Command      []string          `json:"command"`
	WorkingDir   string            `json:"working_dir"`
	Env          map[string]string `json:"env"`
	SnapshotPath string            `json:"snapshot_path"`
}

type latestReport struct {
	SchemaVersion int               `json:"schema_version"`
	GeneratedAt   string            `json:"generated_at"`
	ProducedBy    string            `json:"produced_by"`
	InputRoots    []string          `json:"input_roots,omitempty"`
	InputsDigest  string            `json:"inputs_digest,omitempty"`
	Match         bool              `json:"match"`
	Mismatches    []latestMismatch  `json:"mismatches"`
	Scenarios     []scenarioOutcome `json:"scenarios"`
}

type latestMismatch struct {
	ID       string `json:"id"`
	Severity string `json:"severity"`
	Open     bool   `json:"open"`
	Message  string `json:"message"`
}

type scenarioOutcome struct {
	ID               string `json:"id"`
	Severity         string `json:"severity"`
	Required         bool   `json:"required"`
	Status           string `json:"status"`
	Message          string `json:"message"`
	Evidence         string `json:"evidence"`
	GoEvidence       string `json:"go_evidence,omitempty"`
	RustEvidence     string `json:"rust_evidence,omitempty"`
	DeclaredEvidence string `json:"declared_evidence,omitempty"`
}

type testStatusReport struct {
	SchemaVersion int             `json:"schema_version"`
	GeneratedAt   string          `json:"generated_at"`
	MemoryCap     memoryCapStatus `json:"memory_cap"`
}

type memoryCapStatus struct {
	Status    string `json:"status"`
	Flaky     bool   `json:"flaky"`
	ProfileMB int    `json:"profile_mb"`
}

type interopReport struct {
	SchemaVersion int    `json:"schema_version"`
	GeneratedAt   string `json:"generated_at"`
	Pass          bool   `json:"pass"`
}

type durabilityReport struct {
	SchemaVersion int    `json:"schema_version"`
	GeneratedAt   string `json:"generated_at"`
	Durability    string `json:"durability"`
	CrashRecovery string `json:"crash_recovery"`
}

type replacementGatesConfig struct {
	RequiredAPIEndpoints []string `json:"required_api_endpoints"`
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: go run script/parity_harness.go run [flags]")
	}

	switch os.Args[1] {
	case "run":
		run(os.Args[2:])
	default:
		fatalf("unknown command %q", os.Args[1])
	}
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "parity/harness/scenarios.json", "harness scenario config")
	outLatest := fs.String("latest", "parity/diff-reports/latest.json", "latest differential report output")
	outTest := fs.String("test-status", "parity/diff-reports/test-status.json", "memory-cap test status output")
	outInterop := fs.String("interop", "parity/diff-reports/interop.json", "interop report output")
	outDur := fs.String("durability", "parity/diff-reports/durability.json", "durability report output")
	profileMB := fs.Int("profile-mb", 50, "memory cap profile used by tests")
	repeatExternal := fs.Int(
		"repeat-external-soak",
		1,
		"repeat each external-soak scenario this many times (seeded by --seed-base)",
	)
	seedBase := fs.Int64("seed-base", 1, "base seed for repeated external-soak runs")
	requiredEvidenceMin := fs.String(
		"required-evidence-min",
		"daemon",
		"when set (synthetic|component|daemon|peer-interop|external-soak), fail required scenarios with weaker inferred evidence",
	)
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}
	if *repeatExternal <= 0 {
		fatalf("--repeat-external-soak must be greater than zero")
	}
	requiredEvidence := normalizeScenarioEvidence(*requiredEvidenceMin)
	if strings.TrimSpace(*requiredEvidenceMin) != "" && requiredEvidence == "" {
		fatalf("--required-evidence-min must be one of: synthetic, component, daemon, peer-interop, external-soak")
	}

	cfg, err := readConfig(*configPath)
	if err != nil {
		fatalf("read config: %v", err)
	}

	defaultTimeout := time.Duration(cfg.TimeoutSec) * time.Second
	if cfg.TimeoutSec <= 0 {
		defaultTimeout = 120 * time.Second
	}
	now := time.Now().UTC().Format(time.RFC3339)

	report := latestReport{
		SchemaVersion: harnessSchemaVersion,
		GeneratedAt:   now,
		ProducedBy:    "parity-harness",
		Match:         true,
		Mismatches:    []latestMismatch{},
		Scenarios:     make([]scenarioOutcome, 0, len(cfg.Scenarios)),
	}

	memoryTotal, memoryPass := 0, 0
	interopTotal, interopPass := 0, 0
	durTotal, durPass := 0, 0
	crashTotal, crashPass := 0, 0

	for _, sc := range cfg.Scenarios {
		if strings.TrimSpace(sc.ID) == "" {
			continue
		}
		severity := normalizeSeverity(sc.Severity)
		if severity == "" {
			severity = "P1"
		}
		if sc.Comparator == "" {
			sc.Comparator = "json-equal"
		}

		outcome := scenarioOutcome{ID: sc.ID, Severity: severity, Required: sc.Required}
		outcome.GoEvidence = normalizeScenarioEvidence(inferSideEvidence(sc.Go))
		outcome.RustEvidence = normalizeScenarioEvidence(inferSideEvidence(sc.Rust))
		outcome.Evidence = inferScenarioEvidence(sc)
		if outcome.Evidence == "" {
			outcome.Evidence = "synthetic"
		}
		if outcome.GoEvidence == "" {
			outcome.GoEvidence = "synthetic"
		}
		if outcome.RustEvidence == "" {
			outcome.RustEvidence = "synthetic"
		}
		declaredEvidence := normalizeScenarioEvidence(sc.Evidence)
		declaredEvidenceOverclaim := false
		if declaredEvidence != "" {
			outcome.DeclaredEvidence = declaredEvidence
			if scenarioEvidenceRank(declaredEvidence) > scenarioEvidenceRank(outcome.Evidence) {
				declaredEvidenceOverclaim = true
			}
		}

		runs := 1
		if hasTag(sc.Tags, "external-soak") {
			runs = *repeatExternal
		}
		for runIdx := 0; runIdx < runs; runIdx++ {
			seed := *seedBase + int64(runIdx)
			goSide := sc.Go
			rustSide := sc.Rust
			if hasTag(sc.Tags, "external-soak") {
				goSide = sideWithSeedEnv(goSide, seed)
				rustSide = sideWithSeedEnv(rustSide, seed)
			}

			goErr := validateSideCommand("go", goSide)
			rustErr := validateSideCommand("rust", rustSide)
			var goSnap map[string]any
			var rustSnap map[string]any
			if goErr == nil {
				goSnap, goErr = loadSideSnapshot(goSide, defaultTimeout)
			}
			if rustErr == nil {
				rustSnap, rustErr = loadSideSnapshot(rustSide, defaultTimeout)
			}
			if goErr == nil {
				goErr = validateScenarioSnapshot(sc.ID, "go", goSnap)
			}
			if rustErr == nil {
				rustErr = validateScenarioSnapshot(sc.ID, "rust", rustSnap)
			}

			switch {
			case goErr != nil && rustErr != nil:
				outcome.Status = "blocked"
				outcome.Message = fmt.Sprintf("go and rust sides unavailable (run=%d seed=%d): %v | %v", runIdx+1, seed, goErr, rustErr)
			case goErr != nil:
				outcome.Status = "blocked"
				outcome.Message = fmt.Sprintf("go side unavailable (run=%d seed=%d): %v", runIdx+1, seed, goErr)
			case rustErr != nil:
				outcome.Status = "blocked"
				outcome.Message = fmt.Sprintf("rust side unavailable (run=%d seed=%d): %v", runIdx+1, seed, rustErr)
			default:
				ok, msg := compareSnapshots(sc.ID, sc.Comparator, goSnap, rustSnap)
				if ok {
					outcome.Status = "pass"
					outcome.Message = fmt.Sprintf("go and rust outputs match (runs=%d)", runs)
				} else {
					outcome.Status = "mismatch"
					outcome.Message = fmt.Sprintf("run=%d seed=%d: %s", runIdx+1, seed, msg)
				}
			}

			if outcome.Status != "pass" {
				report.Match = false
				report.Mismatches = append(report.Mismatches, latestMismatch{
					ID:       sc.ID,
					Severity: severity,
					Open:     true,
					Message:  outcome.Message,
				})
				break
			}
		}

		if outcome.Status == "pass" && sc.Required && requiredEvidence != "" {
			if scenarioEvidenceRank(outcome.Evidence) < scenarioEvidenceRank(requiredEvidence) {
				outcome.Status = "mismatch"
				outcome.Message = fmt.Sprintf(
					"required scenario evidence too weak: have=%s need>=%s",
					outcome.Evidence,
					requiredEvidence,
				)
				report.Match = false
				report.Mismatches = append(report.Mismatches, latestMismatch{
					ID:       sc.ID,
					Severity: severity,
					Open:     true,
					Message:  outcome.Message,
				})
			}
		}
		if outcome.Status == "pass" && declaredEvidenceOverclaim {
			outcome.Status = "mismatch"
			outcome.Message = fmt.Sprintf(
				"declared scenario evidence overclaims inferred evidence: declared=%s inferred=%s",
				declaredEvidence,
				outcome.Evidence,
			)
			report.Match = false
			report.Mismatches = append(report.Mismatches, latestMismatch{
				ID:       sc.ID,
				Severity: severity,
				Open:     true,
				Message:  outcome.Message,
			})
		}

		report.Scenarios = append(report.Scenarios, outcome)

		if hasTag(sc.Tags, "memory-cap") {
			memoryTotal++
			if outcome.Status == "pass" {
				memoryPass++
			}
		}
		if hasTag(sc.Tags, "interop") {
			interopTotal++
			if outcome.Status == "pass" {
				interopPass++
			}
		}
		if hasTag(sc.Tags, "durability") {
			durTotal++
			if outcome.Status == "pass" {
				durPass++
			}
		}
		if hasTag(sc.Tags, "crash-recovery") {
			crashTotal++
			if outcome.Status == "pass" {
				crashPass++
			}
		}
	}

	sort.Slice(report.Mismatches, func(i, j int) bool {
		if report.Mismatches[i].Severity != report.Mismatches[j].Severity {
			return report.Mismatches[i].Severity < report.Mismatches[j].Severity
		}
		return report.Mismatches[i].ID < report.Mismatches[j].ID
	})
	sort.Slice(report.Scenarios, func(i, j int) bool { return report.Scenarios[i].ID < report.Scenarios[j].ID })

	inputDigest, err := computeInputDigest(parityEvidenceInputRoots)
	if err != nil {
		fatalf("compute parity evidence digest: %v", err)
	}
	report.InputRoots = append([]string(nil), parityEvidenceInputRoots...)
	report.InputsDigest = inputDigest

	testStatus := testStatusReport{
		SchemaVersion: harnessSchemaVersion,
		GeneratedAt:   now,
		MemoryCap: memoryCapStatus{
			Status:    evaluateStatus(memoryTotal, memoryPass),
			Flaky:     false,
			ProfileMB: *profileMB,
		},
	}
	interop := interopReport{
		SchemaVersion: harnessSchemaVersion,
		GeneratedAt:   now,
		Pass:          interopTotal > 0 && interopPass == interopTotal,
	}
	durability := durabilityReport{
		SchemaVersion: harnessSchemaVersion,
		GeneratedAt:   now,
		Durability:    evaluateStatus(durTotal, durPass),
		CrashRecovery: evaluateStatus(crashTotal, crashPass),
	}

	if err := writeJSON(*outLatest, report); err != nil {
		fatalf("write latest report: %v", err)
	}
	if err := writeJSON(*outTest, testStatus); err != nil {
		fatalf("write test-status report: %v", err)
	}
	if err := writeJSON(*outInterop, interop); err != nil {
		fatalf("write interop report: %v", err)
	}
	if err := writeJSON(*outDur, durability); err != nil {
		fatalf("write durability report: %v", err)
	}

	if !report.Match {
		os.Exit(1)
	}
}

func sideWithSeedEnv(side sideConfig, seed int64) sideConfig {
	out := side
	env := make(map[string]string, len(side.Env)+1)
	for k, v := range side.Env {
		env[k] = v
	}
	env["PARITY_SEED"] = strconv.FormatInt(seed, 10)
	out.Env = env
	return out
}

func validateScenarioSnapshot(expectedID, expectedSource string, snap map[string]any) error {
	getString := func(key string) (string, error) {
		raw, ok := snap[key]
		if !ok {
			return "", fmt.Errorf("scenario payload missing %q", key)
		}
		str, ok := raw.(string)
		if !ok {
			return "", fmt.Errorf("scenario payload %q must be string", key)
		}
		str = strings.TrimSpace(str)
		if str == "" {
			return "", fmt.Errorf("scenario payload %q must not be empty", key)
		}
		return str, nil
	}

	id, err := getString("scenario")
	if err != nil {
		return err
	}
	if id != expectedID {
		return fmt.Errorf("scenario id mismatch: expected %q got %q", expectedID, id)
	}

	source, err := getString("source")
	if err != nil {
		return err
	}
	if !strings.EqualFold(source, expectedSource) {
		return fmt.Errorf("scenario source mismatch: expected %q got %q", expectedSource, source)
	}

	status, err := getString("status")
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "validated", "verified", "parity-verified":
		// Allowed.
	case "prototype":
		return errors.New("scenario status=prototype is not allowed for parity gating")
	default:
		return fmt.Errorf("unsupported scenario status %q (allowed: validated, verified, parity-verified)", status)
	}
	return nil
}

func validateSideCommand(expectedSource string, side sideConfig) error {
	if len(side.Command) == 0 {
		if strings.TrimSpace(side.SnapshotPath) != "" {
			return errors.New("snapshot-only side config is not allowed; command is required")
		}
		return nil
	}
	cmdLine := strings.ToLower(strings.Join(side.Command, " "))
	switch strings.ToLower(strings.TrimSpace(expectedSource)) {
	case "go":
		if value, ok := findFlagValue(side.Command, "--impl"); ok && strings.EqualFold(value, "rust") {
			return errors.New("go side command must not set --impl rust")
		}
		if strings.Contains(cmdLine, "syncthing-rs") && !strings.Contains(cmdLine, "parity_external_soak.go") {
			return errors.New("go side command unexpectedly references syncthing-rs")
		}
	case "rust":
		if strings.Contains(cmdLine, "parity_harness_go.go") {
			return errors.New("rust side command must not reference parity_harness_go.go")
		}
		if value, ok := findFlagValue(side.Command, "--impl"); ok && strings.EqualFold(value, "go") {
			return errors.New("rust side command must not set --impl go")
		}
		if strings.Contains(cmdLine, "parity_external_soak.go") {
			value, ok := findFlagValue(side.Command, "--impl")
			if !ok || !strings.EqualFold(value, "rust") {
				return errors.New("rust external soak command must include --impl rust")
			}
			return nil
		}
		if strings.EqualFold(side.Command[0], "go") {
			if value, ok := findFlagValue(side.Command, "--impl"); ok && strings.EqualFold(value, "rust") {
				return nil
			}
			return errors.New("rust side command must not start with go unless it dispatches --impl rust")
		}
	default:
		return fmt.Errorf("unsupported source %q", expectedSource)
	}
	return nil
}

func findFlagValue(args []string, flag string) (string, bool) {
	normalized := strings.TrimLeft(strings.TrimSpace(flag), "-")
	if normalized == "" {
		return "", false
	}
	var value string
	found := false
	matches := 0
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		trimmed := strings.TrimLeft(arg, "-")
		if trimmed == normalized {
			if i+1 >= len(args) {
				continue
			}
			value = strings.TrimSpace(args[i+1])
			found = true
			matches++
			i++
			continue
		}
		if strings.HasPrefix(trimmed, normalized+"=") {
			value = strings.TrimSpace(strings.TrimPrefix(trimmed, normalized+"="))
			found = true
			matches++
			continue
		}
	}
	if !found || matches != 1 {
		return "", false
	}
	return value, true
}

func readConfig(path string) (harnessConfig, error) {
	var cfg harnessConfig
	bs, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(bs, &cfg); err != nil {
		return cfg, err
	}
	if cfg.SchemaVersion == 0 {
		cfg.SchemaVersion = harnessSchemaVersion
	}
	return cfg, nil
}

func loadSideSnapshot(side sideConfig, timeout time.Duration) (map[string]any, error) {
	if len(side.Command) > 0 {
		snap, err := runCommand(side, timeout)
		if err != nil {
			return nil, err
		}
		if side.SnapshotPath != "" {
			if err := writeJSON(side.SnapshotPath, snap); err != nil {
				return nil, err
			}
		}
		return snap, nil
	}

	if side.SnapshotPath == "" {
		return nil, errors.New("neither command nor snapshot_path configured")
	}
	return readSnapshot(side.SnapshotPath)
}

func runCommand(side sideConfig, timeout time.Duration) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, side.Command[0], side.Command[1:]...)
	if side.WorkingDir != "" {
		cmd.Dir = side.WorkingDir
	}
	if len(side.Env) > 0 {
		env := os.Environ()
		for k, v := range side.Env {
			env = append(env, k+"="+v)
		}
		cmd.Env = env
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("command timed out after %s", timeout)
		}
		return nil, fmt.Errorf("command failed: %w (stderr=%s)", err, strings.TrimSpace(stderr.String()))
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("command output is not valid JSON: %w", err)
	}
	return out, nil
}

func readSnapshot(path string) (map[string]any, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snap map[string]any
	if err := json.Unmarshal(bs, &snap); err != nil {
		return nil, err
	}
	return snap, nil
}

func compareSnapshots(scenarioID, mode string, goSnap, rustSnap map[string]any) (bool, string) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "json-equal", "", "canonical-json-equal":
		goJSON, err := canonicalJSON(normalizeComparisonSnapshot(scenarioID, goSnap))
		if err != nil {
			return false, fmt.Sprintf("go snapshot canonicalization failed: %v", err)
		}
		rustJSON, err := canonicalJSON(normalizeComparisonSnapshot(scenarioID, rustSnap))
		if err != nil {
			return false, fmt.Sprintf("rust snapshot canonicalization failed: %v", err)
		}
		if bytes.Equal(goJSON, rustJSON) {
			return true, ""
		}
		return false, "json outputs differ"
	case "protocol-semantics":
		goComparable := normalizeComparisonSnapshot(scenarioID, goSnap)
		rustComparable := normalizeComparisonSnapshot(scenarioID, rustSnap)
		goFrameCount := countJSONArray(goComparable["frame_sizes"])
		rustFrameCount := countJSONArray(rustComparable["frame_sizes"])
		delete(goComparable, "frame_sizes")
		delete(rustComparable, "frame_sizes")
		goJSON, err := canonicalJSON(goComparable)
		if err != nil {
			return false, fmt.Sprintf("go snapshot canonicalization failed: %v", err)
		}
		rustJSON, err := canonicalJSON(rustComparable)
		if err != nil {
			return false, fmt.Sprintf("rust snapshot canonicalization failed: %v", err)
		}
		if !bytes.Equal(goJSON, rustJSON) {
			return false, "protocol semantic fields differ"
		}
		if goFrameCount != rustFrameCount {
			return false, fmt.Sprintf("protocol frame count differs (go=%d rust=%d)", goFrameCount, rustFrameCount)
		}
		return true, ""
	case "memory-cap":
		goComparable := normalizeComparisonSnapshot(scenarioID, goSnap)
		rustComparable := normalizeComparisonSnapshot(scenarioID, rustSnap)
		for _, key := range []string{"estimated_memory_bytes"} {
			delete(goComparable, key)
			delete(rustComparable, key)
		}
		goJSON, err := canonicalJSON(goComparable)
		if err != nil {
			return false, fmt.Sprintf("go snapshot canonicalization failed: %v", err)
		}
		rustJSON, err := canonicalJSON(rustComparable)
		if err != nil {
			return false, fmt.Sprintf("rust snapshot canonicalization failed: %v", err)
		}
		if !bytes.Equal(goJSON, rustJSON) {
			return false, "memory-cap semantic fields differ"
		}
		goEstimated, goEstimatedOK := numberAsInt64(goSnap["estimated_memory_bytes"])
		rustEstimated, rustEstimatedOK := numberAsInt64(rustSnap["estimated_memory_bytes"])
		goBudget, goBudgetOK := numberAsInt64(goSnap["memory_budget_bytes"])
		rustBudget, rustBudgetOK := numberAsInt64(rustSnap["memory_budget_bytes"])
		if !goEstimatedOK || !rustEstimatedOK || !goBudgetOK || !rustBudgetOK {
			return false, "memory-cap snapshots missing numeric memory fields"
		}
		if goEstimated < 0 || rustEstimated < 0 || goBudget <= 0 || rustBudget <= 0 {
			return false, "memory-cap snapshots contain invalid numeric memory values"
		}
		if goEstimated > goBudget || rustEstimated > rustBudget {
			return false, fmt.Sprintf(
				"memory cap exceeded (go=%d/%d rust=%d/%d)",
				goEstimated,
				goBudget,
				rustEstimated,
				rustBudget,
			)
		}
		return true, ""
	case "endpoint-surface":
		goCovered := toStringSet(goSnap["covered_endpoints"])
		rustCovered := toStringSet(rustSnap["covered_endpoints"])
		if scenarioID == "daemon-api-surface" {
			requiredEndpoints, err := loadRequiredAPIEndpoints(replacementGatesPath())
			if err != nil {
				return false, fmt.Sprintf("load required api endpoints: %v", err)
			}
			if len(requiredEndpoints) == 0 {
				return false, "required api endpoints list is empty"
			}
			missingGoRequired := make([]string, 0)
			missingRustRequired := make([]string, 0)
			for _, endpoint := range requiredEndpoints {
				if _, ok := goCovered[endpoint]; !ok {
					missingGoRequired = append(missingGoRequired, endpoint)
				}
				if _, ok := rustCovered[endpoint]; !ok {
					missingRustRequired = append(missingRustRequired, endpoint)
				}
			}
			if len(missingGoRequired) > 0 || len(missingRustRequired) > 0 {
				sort.Strings(missingGoRequired)
				sort.Strings(missingRustRequired)
				return false, fmt.Sprintf(
					"required endpoints missing (go_missing=%d rust_missing=%d)",
					len(missingGoRequired),
					len(missingRustRequired),
				)
			}
		}

		missingInRust := make([]string, 0)
		for endpoint := range goCovered {
			if _, ok := rustCovered[endpoint]; ok {
				continue
			}
			missingInRust = append(missingInRust, endpoint)
		}
		missingInGo := make([]string, 0)
		for endpoint := range rustCovered {
			if _, ok := goCovered[endpoint]; ok {
				continue
			}
			missingInGo = append(missingInGo, endpoint)
		}
		sort.Strings(missingInRust)
		sort.Strings(missingInGo)
		if len(missingInRust) == 0 && len(missingInGo) == 0 {
			return true, ""
		}
		previewRust := missingInRust
		previewGo := missingInGo
		if len(previewRust) > 12 {
			previewRust = previewRust[:12]
		}
		if len(previewGo) > 12 {
			previewGo = previewGo[:12]
		}
		return false, fmt.Sprintf(
			"endpoint sets differ (rust_missing_go=%d go_missing_rust=%d; rust_missing_preview=[%s]; go_missing_preview=[%s])",
			len(missingInRust),
			len(missingInGo),
			strings.Join(previewRust, ", "),
			strings.Join(previewGo, ", "),
		)
	default:
		return false, fmt.Sprintf("unsupported comparator %q", mode)
	}
}

func replacementGatesPath() string {
	if path := strings.TrimSpace(os.Getenv("PARITY_REPLACEMENT_GATES_PATH")); path != "" {
		return path
	}
	return "parity/replacement-gates.json"
}

func loadRequiredAPIEndpoints(path string) ([]string, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var gates replacementGatesConfig
	if err := json.Unmarshal(bs, &gates); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(gates.RequiredAPIEndpoints))
	seen := make(map[string]struct{}, len(gates.RequiredAPIEndpoints))
	for _, endpoint := range gates.RequiredAPIEndpoints {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		out = append(out, endpoint)
	}
	sort.Strings(out)
	return out, nil
}

func countJSONArray(value any) int {
	switch typed := value.(type) {
	case []any:
		return len(typed)
	case []string:
		return len(typed)
	default:
		return 0
	}
}

func numberAsInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int32:
		return int64(typed), true
	case int64:
		return typed, true
	case uint:
		return int64(typed), true
	case uint32:
		return int64(typed), true
	case uint64:
		if typed > uint64(1<<63-1) {
			return 0, false
		}
		return int64(typed), true
	case float32:
		return int64(typed), true
	case float64:
		return int64(typed), true
	case json.Number:
		if i, err := typed.Int64(); err == nil {
			return i, true
		}
		if f, err := typed.Float64(); err == nil {
			return int64(f), true
		}
		return 0, false
	default:
		return 0, false
	}
}

func inferScenarioEvidence(sc harnessScenario) string {
	goEvidence := normalizeScenarioEvidence(inferSideEvidence(sc.Go))
	rustEvidence := normalizeScenarioEvidence(inferSideEvidence(sc.Rust))
	if scenarioEvidenceRank(goEvidence) <= scenarioEvidenceRank(rustEvidence) {
		return goEvidence
	}
	return rustEvidence
}

func inferSideEvidence(side sideConfig) string {
	if len(side.Command) == 0 {
		if strings.TrimSpace(side.SnapshotPath) != "" {
			return "synthetic"
		}
		return "component"
	}

	cmd := strings.ToLower(strings.Join(side.Command, " "))
	switch {
	case strings.Contains(cmd, "parity_external_soak.go"):
		return "external-soak"
	case strings.Contains(cmd, "interop-scenario") || strings.Contains(cmd, "peer-interop"):
		return "peer-interop"
	case strings.Contains(cmd, "daemon-scenario"):
		return "daemon"
	case strings.Contains(cmd, "parity_harness_go.go"):
		return "synthetic"
	case strings.Contains(cmd, "syncthing-rs") && strings.Contains(cmd, " scenario"):
		return "synthetic"
	case strings.Contains(cmd, "syncthing-rs") && strings.Contains(cmd, " daemon"):
		return "daemon"
	case strings.Contains(cmd, "syncthing"):
		return "daemon"
	default:
		return "component"
	}
}

func normalizeScenarioEvidence(evidence string) string {
	switch strings.ToLower(strings.TrimSpace(evidence)) {
	case "synthetic":
		return "synthetic"
	case "component":
		return "component"
	case "daemon", "runtime":
		return "daemon"
	case "peer-interop", "interop":
		return "peer-interop"
	case "external-soak", "soak":
		return "external-soak"
	default:
		return ""
	}
}

func scenarioEvidenceRank(evidence string) int {
	switch normalizeScenarioEvidence(evidence) {
	case "synthetic":
		return 0
	case "component":
		return 1
	case "daemon":
		return 2
	case "peer-interop":
		return 3
	case "external-soak":
		return 4
	default:
		return 0
	}
}

func normalizeComparisonSnapshot(_ string, snap map[string]any) map[string]any {
	out := make(map[string]any, len(snap))
	for k, v := range snap {
		if k == "scenario" ||
			k == "source" ||
			k == "status" ||
			k == "generated_at" ||
			k == "version" ||
			k == "produced_by" {
			continue
		}
		out[k] = v
	}
	return out
}

func canonicalJSON(v any) ([]byte, error) {
	bs, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(bs, &out); err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func normalizeSeverity(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
	case "P0", "P1", "P2", "P3":
		return s
	default:
		return ""
	}
}

func hasTag(tags []string, target string) bool {
	target = strings.ToLower(strings.TrimSpace(target))
	for _, t := range tags {
		if strings.ToLower(strings.TrimSpace(t)) == target {
			return true
		}
	}
	return false
}

func evaluateStatus(total, passed int) string {
	if total == 0 {
		return "skipped"
	}
	if total == passed {
		return "passed"
	}
	return "failed"
}

func toStringSet(value any) map[string]struct{} {
	out := make(map[string]struct{})
	switch typed := value.(type) {
	case []string:
		for _, entry := range typed {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			out[entry] = struct{}{}
		}
	case []any:
		for _, raw := range typed {
			entry, ok := raw.(string)
			if !ok {
				continue
			}
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			out[entry] = struct{}{}
		}
	}
	return out
}

func computeInputDigest(roots []string) (string, error) {
	files, err := collectInputDigestFiles(roots)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", errors.New("no input files found for parity evidence digest")
	}

	h := sha256.New()
	for _, path := range files {
		bs, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		normalized := filepath.ToSlash(path)
		if _, err := h.Write([]byte(normalized)); err != nil {
			return "", err
		}
		if _, err := h.Write([]byte{0}); err != nil {
			return "", err
		}
		if _, err := h.Write(bs); err != nil {
			return "", err
		}
		if _, err := h.Write([]byte{0}); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func collectInputDigestFiles(roots []string) ([]string, error) {
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, root := range roots {
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "" {
			continue
		}
		info, err := os.Stat(root)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			normalized := filepath.ToSlash(root)
			if _, ok := seen[normalized]; !ok {
				seen[normalized] = struct{}{}
				out = append(out, root)
			}
			continue
		}
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				name := d.Name()
				if name == ".git" || name == "target" || name == ".gocache" {
					return filepath.SkipDir
				}
				return nil
			}
			normalized := filepath.ToSlash(path)
			if _, ok := seen[normalized]; ok {
				return nil
			}
			seen[normalized] = struct{}{}
			out = append(out, path)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(out)
	return out, nil
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	bs = append(bs, '\n')
	return os.WriteFile(path, bs, 0o644)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
