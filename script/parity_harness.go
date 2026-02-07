// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const harnessSchemaVersion = 1

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
	ID       string `json:"id"`
	Severity string `json:"severity"`
	Required bool   `json:"required"`
	Status   string `json:"status"`
	Message  string `json:"message"`
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
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
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

		goSnap, goErr := loadSideSnapshot(sc.Go, defaultTimeout)
		rustSnap, rustErr := loadSideSnapshot(sc.Rust, defaultTimeout)
		if goErr == nil {
			goErr = validateScenarioSnapshot(sc.ID, "go", goSnap)
		}
		if rustErr == nil {
			rustErr = validateScenarioSnapshot(sc.ID, "rust", rustSnap)
		}

		outcome := scenarioOutcome{ID: sc.ID, Severity: severity, Required: sc.Required}

		switch {
		case goErr != nil && rustErr != nil:
			outcome.Status = "blocked"
			outcome.Message = fmt.Sprintf("go and rust sides unavailable: %v | %v", goErr, rustErr)
			report.Match = false
			report.Mismatches = append(report.Mismatches, latestMismatch{ID: sc.ID, Severity: severity, Open: true, Message: outcome.Message})
		case goErr != nil:
			outcome.Status = "blocked"
			outcome.Message = fmt.Sprintf("go side unavailable: %v", goErr)
			report.Match = false
			report.Mismatches = append(report.Mismatches, latestMismatch{ID: sc.ID, Severity: severity, Open: true, Message: outcome.Message})
		case rustErr != nil:
			outcome.Status = "blocked"
			outcome.Message = fmt.Sprintf("rust side unavailable: %v", rustErr)
			report.Match = false
			report.Mismatches = append(report.Mismatches, latestMismatch{ID: sc.ID, Severity: severity, Open: true, Message: outcome.Message})
		default:
			ok, msg := compareSnapshots(sc.Comparator, goSnap, rustSnap)
			if ok {
				outcome.Status = "pass"
				outcome.Message = "go and rust outputs match"
			} else {
				outcome.Status = "mismatch"
				outcome.Message = msg
				report.Match = false
				report.Mismatches = append(report.Mismatches, latestMismatch{ID: sc.ID, Severity: severity, Open: true, Message: msg})
			}
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
	if strings.EqualFold(status, "prototype") {
		return errors.New("scenario status=prototype is not allowed for parity gating")
	}
	return nil
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

func compareSnapshots(mode string, goSnap, rustSnap map[string]any) (bool, string) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "json-equal", "", "canonical-json-equal":
		goJSON, err := canonicalJSON(normalizeComparisonSnapshot(goSnap))
		if err != nil {
			return false, fmt.Sprintf("go snapshot canonicalization failed: %v", err)
		}
		rustJSON, err := canonicalJSON(normalizeComparisonSnapshot(rustSnap))
		if err != nil {
			return false, fmt.Sprintf("rust snapshot canonicalization failed: %v", err)
		}
		if bytes.Equal(goJSON, rustJSON) {
			return true, ""
		}
		return false, "json outputs differ"
	default:
		return false, fmt.Sprintf("unsupported comparator %q", mode)
	}
}

func normalizeComparisonSnapshot(snap map[string]any) map[string]any {
	out := make(map[string]any, len(snap))
	for k, v := range snap {
		if k == "source" || k == "status" {
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
