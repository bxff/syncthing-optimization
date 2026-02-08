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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	externalSoakScenarioID        = "external-soak-replacement"
	externalMultiFolderScenarioID = "external-multifolder-replacement"
	externalFolderModesScenarioID = "external-folder-modes-replacement"
	externalDurabilityScenarioID  = "external-durability-restart"
	externalCrashScenarioID       = "external-crash-recovery-restart"
	httpTimeout                   = 5 * time.Second
	startupTimeout                = 30 * time.Second
	statusTimeout                 = 30 * time.Second
	shutdownTimeout               = 10 * time.Second
	expectedLocalFiles            = 2
	expectedMultiFolderFiles      = 6
)

var expectedOrderedPaths = []string{
	"a/x.txt",
	"a/z.txt",
	"a.d/x.txt",
	"a.d/y.txt",
	"a.txt",
	"nested/b.txt",
}

var expectedFolderModeTypes = map[string]string{
	"default":  "sendrecv",
	"sendonly": "sendonly",
	"recvonly": "recvonly",
	"recvenc":  "recvenc",
}

func main() {
	if len(os.Args) < 3 || os.Args[1] != "scenario" {
		fatalf(
			"usage: go run ./script/parity_external_soak.go scenario <%s|%s|%s|%s|%s> --impl <go|rust>",
			externalSoakScenarioID,
			externalMultiFolderScenarioID,
			externalFolderModesScenarioID,
			externalDurabilityScenarioID,
			externalCrashScenarioID,
		)
	}
	id := strings.TrimSpace(os.Args[2])
	if id != externalSoakScenarioID &&
		id != externalMultiFolderScenarioID &&
		id != externalFolderModesScenarioID &&
		id != externalDurabilityScenarioID &&
		id != externalCrashScenarioID {
		fatalf("unsupported scenario id %q", id)
	}

	fs := flag.NewFlagSet("scenario", flag.ExitOnError)
	impl := fs.String("impl", "", "implementation to run (go or rust)")
	seed := fs.Int64("seed", seedFromEnv(), "deterministic workload seed")
	if err := fs.Parse(os.Args[3:]); err != nil {
		fatalf("parse flags: %v", err)
	}
	if *impl != "go" && *impl != "rust" {
		fatalf("--impl must be go or rust")
	}

	checks, metrics, stateProjection, err := runScenario(id, *impl, *seed)
	if err != nil {
		fatalf("external soak failed: %v", err)
	}

	out := map[string]any{
		"scenario":         id,
		"source":           *impl,
		"status":           "validated",
		"checks":           checks,
		"metrics":          metrics,
		"state_projection": stateProjection,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fatalf("encode output: %v", err)
	}
}

func runScenario(id, impl string, seed int64) (map[string]any, map[string]any, map[string]any, error) {
	switch id {
	case externalSoakScenarioID:
		return runScenarioExternalSoak(impl, seed)
	case externalMultiFolderScenarioID:
		return runScenarioExternalMultiFolder(impl, seed)
	case externalFolderModesScenarioID:
		return runScenarioExternalFolderModes(impl, seed)
	case externalDurabilityScenarioID:
		return runScenarioExternalDurability(impl, seed)
	case externalCrashScenarioID:
		return runScenarioExternalCrashRecovery(impl, seed)
	default:
		return nil, nil, nil, fmt.Errorf("unsupported scenario %q", id)
	}
}

func runScenarioExternalSoak(impl string, seed int64) (map[string]any, map[string]any, map[string]any, error) {
	var (
		metrics soakMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalSoak(seed)
	case "rust":
		metrics, err = runRustExternalSoak(seed)
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	checks := map[string]any{
		"daemon_boot_ok":                 true,
		"folder_configured":              metrics.FolderConfigured,
		"scan_ok":                        metrics.ScanOK,
		"status_ok":                      metrics.StatusOK,
		"status_state_valid":             stateIsRunnable(metrics.State),
		"local_files_at_least_expected":  metrics.LocalFiles >= expectedLocalFiles,
		"global_files_at_least_expected": metrics.GlobalFiles >= expectedLocalFiles,
		"need_files_zero":                metrics.NeedFiles == 0,
		"shutdown_requested":             metrics.ShutdownRequested,
	}
	m := map[string]any{
		"local_files":          metrics.LocalFiles,
		"global_files":         metrics.GlobalFiles,
		"need_files":           metrics.NeedFiles,
		"expected_local_files": expectedLocalFiles,
		"scan_attempts":        metrics.ScanAttempts,
		"status_poll_attempts": metrics.StatusPollAttempts,
		"seed":                 metrics.Seed,
		"workload_digest":      metrics.WorkloadDigest,
	}
	return checks, m, metrics.StateProjection, nil
}

func runScenarioExternalMultiFolder(impl string, seed int64) (map[string]any, map[string]any, map[string]any, error) {
	var (
		metrics multiFolderMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalMultiFolderSoak(seed)
	case "rust":
		metrics, err = runRustExternalMultiFolderSoak(seed)
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	checks := map[string]any{
		"default_folder_configured":              metrics.DefaultFolderConfigured,
		"media_folder_configured":                metrics.MediaFolderConfigured,
		"scan_ok":                                metrics.ScanOK,
		"default_status_ok":                      metrics.StatusOK,
		"browse_order_ok":                        metrics.BrowseOrderOK,
		"default_status_state_valid":             stateIsRunnable(metrics.DefaultState),
		"media_status_state_valid":               stateIsRunnable(metrics.MediaState),
		"default_local_files_at_least_expected":  metrics.DefaultLocalFiles >= expectedMultiFolderFiles,
		"default_global_files_at_least_expected": metrics.DefaultGlobalFiles >= expectedMultiFolderFiles,
		"default_need_files_zero":                metrics.DefaultNeedFiles == 0,
		"media_local_files_at_least_expected":    metrics.MediaLocalFiles >= expectedMultiFolderFiles,
		"media_global_files_at_least_expected":   metrics.MediaGlobalFiles >= expectedMultiFolderFiles,
		"media_need_files_zero":                  metrics.MediaNeedFiles == 0,
		"shutdown_requested":                     metrics.ShutdownRequested,
	}
	m := map[string]any{
		"default_local_files":    metrics.DefaultLocalFiles,
		"default_global_files":   metrics.DefaultGlobalFiles,
		"default_need_files":     metrics.DefaultNeedFiles,
		"media_local_files":      metrics.MediaLocalFiles,
		"media_global_files":     metrics.MediaGlobalFiles,
		"media_need_files":       metrics.MediaNeedFiles,
		"default_browse_paths":   metrics.DefaultBrowsePaths,
		"media_browse_paths":     metrics.MediaBrowsePaths,
		"expected_local_files":   expectedMultiFolderFiles,
		"expected_ordered_paths": expectedOrderedPaths,
		"scan_attempts":          metrics.ScanAttempts,
		"status_poll_attempts":   metrics.StatusPollAttempts,
		"seed":                   metrics.Seed,
		"workload_digest":        metrics.WorkloadDigest,
	}
	return checks, m, metrics.StateProjection, nil
}

func runScenarioExternalFolderModes(impl string, seed int64) (map[string]any, map[string]any, map[string]any, error) {
	var (
		metrics folderModeMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalFolderModes(seed)
	case "rust":
		metrics, err = runRustExternalFolderModes(seed)
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	checks := map[string]any{
		"folders_configured":        metrics.FoldersConfigured,
		"scan_ok":                   metrics.ScanOK,
		"status_ok":                 metrics.StatusOK,
		"type_map_matches_expected": metrics.TypeMapMatches,
		"shutdown_requested":        metrics.ShutdownRequested,
	}
	m := map[string]any{
		"type_map":             metrics.TypeMap,
		"local_files_ok":       metrics.LocalFilesOK,
		"state_ok":             metrics.StateOK,
		"need_files_zero":      metrics.NeedFilesZero,
		"expected_type_map":    expectedFolderModeTypes,
		"scan_attempts":        metrics.ScanAttempts,
		"status_poll_attempts": metrics.StatusPollAttempts,
		"seed":                 metrics.Seed,
		"workload_digest":      metrics.WorkloadDigest,
	}
	return checks, m, metrics.StateProjection, nil
}

func runScenarioExternalDurability(impl string, seed int64) (map[string]any, map[string]any, map[string]any, error) {
	var (
		metrics durabilityMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalDurabilityRestart(seed)
	case "rust":
		metrics, err = runRustExternalDurabilityRestart(seed)
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	checks := map[string]any{
		"first_boot_ok":              metrics.FirstBootOK,
		"folder_configured":          metrics.FolderConfigured,
		"scan_ok":                    metrics.ScanOK,
		"pre_restart_files_indexed":  metrics.PreRestartFilesIndexed,
		"restart_boot_ok":            metrics.RestartBootOK,
		"post_restart_files_indexed": metrics.PostRestartFilesIndexed,
		"need_files_zero":            metrics.PostRestartNeedFiles == 0,
		"shutdown_requested":         metrics.ShutdownRequested,
		"restart_shutdown_requested": metrics.RestartShutdownRequested,
	}
	m := map[string]any{
		"expected_local_files":      expectedLocalFiles,
		"pre_restart_local_files":   metrics.PreRestartLocalFiles,
		"pre_restart_global_files":  metrics.PreRestartGlobalFiles,
		"post_restart_local_files":  metrics.PostRestartLocalFiles,
		"post_restart_global_files": metrics.PostRestartGlobalFiles,
		"post_restart_need_files":   metrics.PostRestartNeedFiles,
		"status_poll_attempts":      metrics.StatusPollAttempts,
		"seed":                      metrics.Seed,
		"workload_digest":           metrics.WorkloadDigest,
	}
	return checks, m, metrics.StateProjection, nil
}

func runScenarioExternalCrashRecovery(impl string, seed int64) (map[string]any, map[string]any, map[string]any, error) {
	var (
		metrics crashMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalCrashRecoveryRestart(seed)
	case "rust":
		metrics, err = runRustExternalCrashRecoveryRestart(seed)
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, nil, err
	}

	checks := map[string]any{
		"first_boot_ok":                metrics.FirstBootOK,
		"folder_configured":            metrics.FolderConfigured,
		"scan_ok":                      metrics.ScanOK,
		"pre_crash_files_indexed":      metrics.PreCrashFilesIndexed,
		"crash_kill_ok":                metrics.CrashKillOK,
		"restart_boot_ok":              metrics.RestartBootOK,
		"post_restart_files_indexed":   metrics.PostRestartFilesIndexed,
		"post_restart_need_files_zero": metrics.PostRestartNeedFiles == 0,
		"restart_shutdown_requested":   metrics.RestartShutdownRequested,
	}
	m := map[string]any{
		"expected_local_files":      expectedLocalFiles,
		"pre_crash_local_files":     metrics.PreCrashLocalFiles,
		"pre_crash_global_files":    metrics.PreCrashGlobalFiles,
		"post_restart_local_files":  metrics.PostRestartLocalFiles,
		"post_restart_global_files": metrics.PostRestartGlobalFiles,
		"post_restart_need_files":   metrics.PostRestartNeedFiles,
		"status_poll_attempts":      metrics.StatusPollAttempts,
		"seed":                      metrics.Seed,
		"workload_digest":           metrics.WorkloadDigest,
	}
	return checks, m, metrics.StateProjection, nil
}

type soakMetrics struct {
	FolderConfigured   bool
	ScanOK             bool
	StatusOK           bool
	ShutdownRequested  bool
	LocalFiles         int
	GlobalFiles        int
	NeedFiles          int
	State              string
	ScanAttempts       int
	StatusPollAttempts int
	Seed               int64
	WorkloadDigest     string
	StateProjection    map[string]any
}

type multiFolderMetrics struct {
	DefaultFolderConfigured bool
	MediaFolderConfigured   bool
	ScanOK                  bool
	StatusOK                bool
	BrowseOrderOK           bool
	ShutdownRequested       bool
	DefaultLocalFiles       int
	DefaultGlobalFiles      int
	DefaultNeedFiles        int
	DefaultState            string
	MediaLocalFiles         int
	MediaGlobalFiles        int
	MediaNeedFiles          int
	MediaState              string
	DefaultBrowsePaths      int
	MediaBrowsePaths        int
	ScanAttempts            int
	StatusPollAttempts      int
	Seed                    int64
	WorkloadDigest          string
	StateProjection         map[string]any
}

type folderModeMetrics struct {
	FoldersConfigured  bool
	ScanOK             bool
	StatusOK           bool
	TypeMapMatches     bool
	ShutdownRequested  bool
	TypeMap            map[string]string
	LocalFilesOK       map[string]bool
	StateOK            map[string]bool
	NeedFilesZero      map[string]bool
	ScanAttempts       int
	StatusPollAttempts int
	Seed               int64
	WorkloadDigest     string
	StateProjection    map[string]any
}

type durabilityMetrics struct {
	FirstBootOK              bool
	FolderConfigured         bool
	ScanOK                   bool
	PreRestartFilesIndexed   bool
	RestartBootOK            bool
	PostRestartFilesIndexed  bool
	ShutdownRequested        bool
	RestartShutdownRequested bool
	PreRestartLocalFiles     int
	PreRestartGlobalFiles    int
	PostRestartLocalFiles    int
	PostRestartGlobalFiles   int
	PostRestartNeedFiles     int
	StatusPollAttempts       int
	Seed                     int64
	WorkloadDigest           string
	StateProjection          map[string]any
}

type crashMetrics struct {
	FirstBootOK              bool
	FolderConfigured         bool
	ScanOK                   bool
	PreCrashFilesIndexed     bool
	CrashKillOK              bool
	RestartBootOK            bool
	PostRestartFilesIndexed  bool
	RestartShutdownRequested bool
	PreCrashLocalFiles       int
	PreCrashGlobalFiles      int
	PostRestartLocalFiles    int
	PostRestartGlobalFiles   int
	PostRestartNeedFiles     int
	StatusPollAttempts       int
	Seed                     int64
	WorkloadDigest           string
	StateProjection          map[string]any
}

type daemonProc struct {
	cmd    *exec.Cmd
	stderr bytes.Buffer
	waited bool
}

func (p *daemonProc) start() error {
	p.cmd.Stdout = &p.stderr
	p.cmd.Stderr = &p.stderr
	return p.cmd.Start()
}

func (p *daemonProc) stop() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if p.waited {
		return nil
	}
	_ = p.cmd.Process.Kill()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := waitWithContext(waitCtx, p.cmd)
	if err == nil || strings.Contains(err.Error(), "Wait was already called") {
		p.waited = true
		return nil
	}
	return err
}

func (p *daemonProc) crashKill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if p.waited {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := waitWithContext(waitCtx, p.cmd)
	if err == nil {
		p.waited = true
		return nil
	}
	// `go run`/`cargo run` wrappers can return deadline/signal errors while still
	// ensuring abrupt process termination semantics for crash-recovery testing.
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "signal: killed") {
		p.waited = true
		return nil
	}
	return err
}

func (p *daemonProc) wait(timeout time.Duration) error {
	if p == nil || p.cmd == nil {
		return nil
	}
	if p.waited {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := waitWithContext(waitCtx, p.cmd)
	if err == nil || strings.Contains(err.Error(), "Wait was already called") {
		p.waited = true
		return nil
	}
	return err
}

func runGoExternalSoak(seed int64) (soakMetrics, error) {
	metrics := soakMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-soak", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-go-external-soak-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	folderDir := filepath.Join(root, "folder")
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return metrics, err
	}

	if err := runCmd(20*time.Second, "go", "run", "./cmd/syncthing", "generate", "--home", homeDir, "--no-port-probing"); err != nil {
		return metrics, fmt.Errorf("generate go config: %w", err)
	}
	apiKey, err := parseAPIKey(filepath.Join(homeDir, "config.xml"))
	if err != nil {
		return metrics, err
	}
	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}

	proc := &daemonProc{
		cmd: exec.Command(
			"go", "run", "./cmd/syncthing", "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", fmt.Sprintf("http://127.0.0.1:%d", apiPort),
			"--log-file=-",
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon: %w", err)
	}
	defer proc.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	if err := waitForPing(baseURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go runtime identity check: %w", err)
	}

	if err := runCmd(
		20*time.Second,
		"go", "run", "./cmd/syncthing", "cli",
		"--home", homeDir,
		"--gui-address", fmt.Sprintf("127.0.0.1:%d", apiPort),
		"--gui-apikey", apiKey,
		"config", "folders", "add",
		"--id", "default",
		"--path", folderDir,
		"--type", "sendreceive",
	); err != nil {
		return metrics, fmt.Errorf("configure go folder: %w", err)
	}
	metrics.FolderConfigured = true

	metrics.ScanAttempts++
	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", apiKey); err != nil {
		return metrics, fmt.Errorf("go scan request: %w", err)
	}
	metrics.ScanOK = true

	status, polls, err := pollStatus(baseURL, apiKey, statusTimeout)
	metrics.StatusPollAttempts = polls
	if err != nil {
		return metrics, fmt.Errorf("go status poll: %w", err)
	}
	metrics.StatusOK = true
	metrics.LocalFiles = intField(status, "localFiles")
	metrics.GlobalFiles = intField(status, "globalFiles")
	metrics.NeedFiles = intField(status, "needFiles")
	metrics.State = stringField(status, "state")
	if metrics.LocalFiles < expectedLocalFiles || metrics.GlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("go status did not observe expected files (local=%d global=%d need=%d)",
			metrics.LocalFiles, metrics.GlobalFiles, metrics.NeedFiles)
	}
	if !stateIsRunnable(metrics.State) {
		return metrics, fmt.Errorf("go status returned unexpected state %q", metrics.State)
	}
	if metrics.NeedFiles != 0 {
		return metrics, fmt.Errorf("go status expected needFiles=0, got %d", metrics.NeedFiles)
	}
	metrics.StateProjection, err = buildStateProjection(baseURL, apiKey, []string{"default"}, true)
	if err != nil {
		return metrics, fmt.Errorf("go state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalSoak(seed int64) (soakMetrics, error) {
	metrics := soakMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-soak", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-rs-external-soak-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	folderDir := filepath.Join(root, "folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return metrics, err
	}
	if err := os.MkdirAll(dbRoot, 0o755); err != nil {
		return metrics, fmt.Errorf("create rust db root: %w", err)
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	bepPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	proc := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", folderDir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", bepPort),
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon: %w", err)
	}
	defer proc.stop()
	metrics.FolderConfigured = true

	if err := waitForPing(baseURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust runtime identity check: %w", err)
	}

	metrics.ScanAttempts++
	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", ""); err != nil {
		return metrics, fmt.Errorf("rust scan request: %w", err)
	}
	metrics.ScanOK = true

	status, polls, err := pollStatus(baseURL, "", statusTimeout)
	metrics.StatusPollAttempts = polls
	if err != nil {
		return metrics, fmt.Errorf("rust status poll: %w", err)
	}
	metrics.StatusOK = true
	metrics.LocalFiles = intField(status, "localFiles")
	metrics.GlobalFiles = intField(status, "globalFiles")
	metrics.NeedFiles = intField(status, "needFiles")
	metrics.State = stringField(status, "state")
	if metrics.LocalFiles < expectedLocalFiles || metrics.GlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("rust status did not observe expected files (local=%d global=%d need=%d)",
			metrics.LocalFiles, metrics.GlobalFiles, metrics.NeedFiles)
	}
	if !stateIsRunnable(metrics.State) {
		return metrics, fmt.Errorf("rust status returned unexpected state %q", metrics.State)
	}
	if metrics.NeedFiles != 0 {
		return metrics, fmt.Errorf("rust status expected needFiles=0, got %d", metrics.NeedFiles)
	}
	metrics.StateProjection, err = buildStateProjection(baseURL, "", []string{"default"}, true)
	if err != nil {
		return metrics, fmt.Errorf("rust state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runGoExternalMultiFolderSoak(seed int64) (multiFolderMetrics, error) {
	metrics := multiFolderMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-multifolder", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-go-external-multifolder-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	defaultFolderDir := filepath.Join(root, "default-folder")
	mediaFolderDir := filepath.Join(root, "media-folder")
	if err := prepareMultiFolderSoakFolder(defaultFolderDir, seed+11); err != nil {
		return metrics, fmt.Errorf("prepare default folder: %w", err)
	}
	if err := prepareMultiFolderSoakFolder(mediaFolderDir, seed+29); err != nil {
		return metrics, fmt.Errorf("prepare media folder: %w", err)
	}

	if err := runCmd(20*time.Second, "go", "run", "./cmd/syncthing", "generate", "--home", homeDir, "--no-port-probing"); err != nil {
		return metrics, fmt.Errorf("generate go config: %w", err)
	}
	apiKey, err := parseAPIKey(filepath.Join(homeDir, "config.xml"))
	if err != nil {
		return metrics, err
	}
	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}

	proc := &daemonProc{
		cmd: exec.Command(
			"go", "run", "./cmd/syncthing", "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", fmt.Sprintf("http://127.0.0.1:%d", apiPort),
			"--log-file=-",
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon: %w", err)
	}
	defer proc.stop()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	if err := waitForPing(baseURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go runtime identity check: %w", err)
	}

	if err := runCmd(
		20*time.Second,
		"go", "run", "./cmd/syncthing", "cli",
		"--home", homeDir,
		"--gui-address", fmt.Sprintf("127.0.0.1:%d", apiPort),
		"--gui-apikey", apiKey,
		"config", "folders", "add",
		"--id", "default",
		"--path", defaultFolderDir,
		"--type", "sendreceive",
	); err != nil {
		return metrics, fmt.Errorf("configure go default folder: %w", err)
	}
	metrics.DefaultFolderConfigured = true

	if err := runCmd(
		20*time.Second,
		"go", "run", "./cmd/syncthing", "cli",
		"--home", homeDir,
		"--gui-address", fmt.Sprintf("127.0.0.1:%d", apiPort),
		"--gui-apikey", apiKey,
		"config", "folders", "add",
		"--id", "media",
		"--path", mediaFolderDir,
		"--type", "sendreceive",
	); err != nil {
		return metrics, fmt.Errorf("configure go media folder: %w", err)
	}
	metrics.MediaFolderConfigured = true

	for _, folder := range []string{"default", "media"} {
		metrics.ScanAttempts++
		scanURL := encodeURLQuery(baseURL+"/rest/db/scan", url.Values{"folder": []string{folder}})
		if _, _, err := requestJSON(http.MethodPost, scanURL, apiKey); err != nil {
			return metrics, fmt.Errorf("go scan request (%s): %w", folder, err)
		}
	}
	metrics.ScanOK = true

	defaultStatus, defaultPolls, err := pollStatusForFolder(
		baseURL,
		apiKey,
		"default",
		expectedMultiFolderFiles,
		expectedMultiFolderFiles,
		true,
		statusTimeout,
	)
	metrics.StatusPollAttempts += defaultPolls
	if err != nil {
		return metrics, fmt.Errorf("go default status poll: %w", err)
	}
	mediaStatus, mediaPolls, err := pollStatusForFolder(
		baseURL,
		apiKey,
		"media",
		expectedMultiFolderFiles,
		expectedMultiFolderFiles,
		true,
		statusTimeout,
	)
	metrics.StatusPollAttempts += mediaPolls
	if err != nil {
		return metrics, fmt.Errorf("go media status poll: %w", err)
	}
	metrics.StatusOK = true

	metrics.DefaultLocalFiles = intField(defaultStatus, "localFiles")
	metrics.DefaultGlobalFiles = intField(defaultStatus, "globalFiles")
	metrics.DefaultNeedFiles = intField(defaultStatus, "needFiles")
	metrics.DefaultState = stringField(defaultStatus, "state")
	if metrics.DefaultLocalFiles < expectedMultiFolderFiles || metrics.DefaultGlobalFiles < expectedMultiFolderFiles {
		return metrics, fmt.Errorf("go default status did not observe expected files (local=%d global=%d need=%d)",
			metrics.DefaultLocalFiles, metrics.DefaultGlobalFiles, metrics.DefaultNeedFiles)
	}
	if !stateIsRunnable(metrics.DefaultState) {
		return metrics, fmt.Errorf("go default status returned unexpected state %q", metrics.DefaultState)
	}
	if metrics.DefaultNeedFiles != 0 {
		return metrics, fmt.Errorf("go default status expected needFiles=0, got %d", metrics.DefaultNeedFiles)
	}

	metrics.MediaLocalFiles = intField(mediaStatus, "localFiles")
	metrics.MediaGlobalFiles = intField(mediaStatus, "globalFiles")
	metrics.MediaNeedFiles = intField(mediaStatus, "needFiles")
	metrics.MediaState = stringField(mediaStatus, "state")
	if metrics.MediaLocalFiles < expectedMultiFolderFiles || metrics.MediaGlobalFiles < expectedMultiFolderFiles {
		return metrics, fmt.Errorf("go media status did not observe expected files (local=%d global=%d need=%d)",
			metrics.MediaLocalFiles, metrics.MediaGlobalFiles, metrics.MediaNeedFiles)
	}
	if !stateIsRunnable(metrics.MediaState) {
		return metrics, fmt.Errorf("go media status returned unexpected state %q", metrics.MediaState)
	}
	if metrics.MediaNeedFiles != 0 {
		return metrics, fmt.Errorf("go media status expected needFiles=0, got %d", metrics.MediaNeedFiles)
	}

	if err := verifyIndexedFiles(baseURL, apiKey, "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("go default file verification: %w", err)
	}
	if err := verifyIndexedFiles(baseURL, apiKey, "media", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("go media file verification: %w", err)
	}
	defaultPaths, err := browseOrderedPaths(baseURL, apiKey, "default", 128)
	if err != nil {
		return metrics, fmt.Errorf("go default browse order: %w", err)
	}
	metrics.DefaultBrowsePaths = len(defaultPaths)
	if !equalStringSlices(defaultPaths, expectedOrderedPaths) {
		return metrics, fmt.Errorf("go default browse order mismatch: got=%v want=%v", defaultPaths, expectedOrderedPaths)
	}
	mediaPaths, err := browseOrderedPaths(baseURL, apiKey, "media", 128)
	if err != nil {
		return metrics, fmt.Errorf("go media browse order: %w", err)
	}
	metrics.MediaBrowsePaths = len(mediaPaths)
	if !equalStringSlices(mediaPaths, expectedOrderedPaths) {
		return metrics, fmt.Errorf("go media browse order mismatch: got=%v want=%v", mediaPaths, expectedOrderedPaths)
	}
	metrics.BrowseOrderOK = true
	metrics.StateProjection, err = buildStateProjection(baseURL, apiKey, []string{"default", "media"}, true)
	if err != nil {
		return metrics, fmt.Errorf("go state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalMultiFolderSoak(seed int64) (multiFolderMetrics, error) {
	metrics := multiFolderMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-multifolder", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-rs-external-multifolder-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	defaultFolderDir := filepath.Join(root, "default-folder")
	mediaFolderDir := filepath.Join(root, "media-folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareMultiFolderSoakFolder(defaultFolderDir, seed+11); err != nil {
		return metrics, fmt.Errorf("prepare default folder: %w", err)
	}
	if err := prepareMultiFolderSoakFolder(mediaFolderDir, seed+29); err != nil {
		return metrics, fmt.Errorf("prepare media folder: %w", err)
	}
	if err := os.MkdirAll(dbRoot, 0o755); err != nil {
		return metrics, fmt.Errorf("create rust db root: %w", err)
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	bepPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	proc := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", defaultFolderDir),
			"--folder", fmt.Sprintf("media:%s", mediaFolderDir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", bepPort),
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon: %w", err)
	}
	defer proc.stop()
	metrics.DefaultFolderConfigured = true
	metrics.MediaFolderConfigured = true

	if err := waitForPing(baseURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust runtime identity check: %w", err)
	}

	for _, folder := range []string{"default", "media"} {
		metrics.ScanAttempts++
		scanURL := encodeURLQuery(baseURL+"/rest/db/scan", url.Values{"folder": []string{folder}})
		if _, _, err := requestJSON(http.MethodPost, scanURL, ""); err != nil {
			return metrics, fmt.Errorf("rust scan request (%s): %w", folder, err)
		}
	}
	metrics.ScanOK = true

	defaultStatus, defaultPolls, err := pollStatusForFolder(
		baseURL,
		"",
		"default",
		expectedMultiFolderFiles,
		expectedMultiFolderFiles,
		true,
		statusTimeout,
	)
	metrics.StatusPollAttempts += defaultPolls
	if err != nil {
		return metrics, fmt.Errorf("rust default status poll: %w", err)
	}
	mediaStatus, mediaPolls, err := pollStatusForFolder(
		baseURL,
		"",
		"media",
		expectedMultiFolderFiles,
		expectedMultiFolderFiles,
		true,
		statusTimeout,
	)
	metrics.StatusPollAttempts += mediaPolls
	if err != nil {
		return metrics, fmt.Errorf("rust media status poll: %w", err)
	}
	metrics.StatusOK = true

	metrics.DefaultLocalFiles = intField(defaultStatus, "localFiles")
	metrics.DefaultGlobalFiles = intField(defaultStatus, "globalFiles")
	metrics.DefaultNeedFiles = intField(defaultStatus, "needFiles")
	metrics.DefaultState = stringField(defaultStatus, "state")
	if metrics.DefaultLocalFiles < expectedMultiFolderFiles || metrics.DefaultGlobalFiles < expectedMultiFolderFiles {
		return metrics, fmt.Errorf("rust default status did not observe expected files (local=%d global=%d need=%d)",
			metrics.DefaultLocalFiles, metrics.DefaultGlobalFiles, metrics.DefaultNeedFiles)
	}
	if !stateIsRunnable(metrics.DefaultState) {
		return metrics, fmt.Errorf("rust default status returned unexpected state %q", metrics.DefaultState)
	}
	if metrics.DefaultNeedFiles != 0 {
		return metrics, fmt.Errorf("rust default status expected needFiles=0, got %d", metrics.DefaultNeedFiles)
	}

	metrics.MediaLocalFiles = intField(mediaStatus, "localFiles")
	metrics.MediaGlobalFiles = intField(mediaStatus, "globalFiles")
	metrics.MediaNeedFiles = intField(mediaStatus, "needFiles")
	metrics.MediaState = stringField(mediaStatus, "state")
	if metrics.MediaLocalFiles < expectedMultiFolderFiles || metrics.MediaGlobalFiles < expectedMultiFolderFiles {
		return metrics, fmt.Errorf("rust media status did not observe expected files (local=%d global=%d need=%d)",
			metrics.MediaLocalFiles, metrics.MediaGlobalFiles, metrics.MediaNeedFiles)
	}
	if !stateIsRunnable(metrics.MediaState) {
		return metrics, fmt.Errorf("rust media status returned unexpected state %q", metrics.MediaState)
	}
	if metrics.MediaNeedFiles != 0 {
		return metrics, fmt.Errorf("rust media status expected needFiles=0, got %d", metrics.MediaNeedFiles)
	}

	if err := verifyIndexedFiles(baseURL, "", "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("rust default file verification: %w", err)
	}
	if err := verifyIndexedFiles(baseURL, "", "media", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("rust media file verification: %w", err)
	}
	defaultPaths, err := browseOrderedPaths(baseURL, "", "default", 128)
	if err != nil {
		return metrics, fmt.Errorf("rust default browse order: %w", err)
	}
	metrics.DefaultBrowsePaths = len(defaultPaths)
	if !equalStringSlices(defaultPaths, expectedOrderedPaths) {
		return metrics, fmt.Errorf("rust default browse order mismatch: got=%v want=%v", defaultPaths, expectedOrderedPaths)
	}
	mediaPaths, err := browseOrderedPaths(baseURL, "", "media", 128)
	if err != nil {
		return metrics, fmt.Errorf("rust media browse order: %w", err)
	}
	metrics.MediaBrowsePaths = len(mediaPaths)
	if !equalStringSlices(mediaPaths, expectedOrderedPaths) {
		return metrics, fmt.Errorf("rust media browse order mismatch: got=%v want=%v", mediaPaths, expectedOrderedPaths)
	}
	metrics.BrowseOrderOK = true
	metrics.StateProjection, err = buildStateProjection(baseURL, "", []string{"default", "media"}, true)
	if err != nil {
		return metrics, fmt.Errorf("rust state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

type folderModeSpec struct {
	id   string
	mode string
	dir  string
}

func runGoExternalFolderModes(seed int64) (folderModeMetrics, error) {
	metrics := folderModeMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-folder-modes", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-go-external-folder-modes-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	modeSpecs := []folderModeSpec{
		{id: "default", mode: "sendreceive", dir: filepath.Join(root, "default")},
		{id: "sendonly", mode: "sendonly", dir: filepath.Join(root, "sendonly")},
		{id: "recvonly", mode: "receiveonly", dir: filepath.Join(root, "recvonly")},
		{id: "recvenc", mode: "receiveencrypted", dir: filepath.Join(root, "recvenc")},
	}
	for _, spec := range modeSpecs {
		if err := prepareSoakFolder(spec.dir, seed+int64(len(spec.id))); err != nil {
			return metrics, fmt.Errorf("prepare folder %s: %w", spec.id, err)
		}
	}

	if err := runCmd(20*time.Second, "go", "run", "./cmd/syncthing", "generate", "--home", homeDir, "--no-port-probing"); err != nil {
		return metrics, fmt.Errorf("generate go config: %w", err)
	}
	apiKey, err := parseAPIKey(filepath.Join(homeDir, "config.xml"))
	if err != nil {
		return metrics, err
	}
	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	proc := &daemonProc{
		cmd: exec.Command(
			"go", "run", "./cmd/syncthing", "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", baseURL,
			"--log-file=-",
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon: %w", err)
	}
	defer proc.stop()
	if err := waitForPing(baseURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go runtime identity check: %w", err)
	}

	for _, spec := range modeSpecs {
		if err := runCmd(
			20*time.Second,
			"go", "run", "./cmd/syncthing", "cli",
			"--home", homeDir,
			"--gui-address", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"--gui-apikey", apiKey,
			"config", "folders", "add",
			"--id", spec.id,
			"--path", spec.dir,
			"--type", spec.mode,
		); err != nil {
			return metrics, fmt.Errorf("configure go folder %s (%s): %w", spec.id, spec.mode, err)
		}
	}
	metrics.FoldersConfigured = true

	metrics.LocalFilesOK = make(map[string]bool, len(modeSpecs))
	metrics.StateOK = make(map[string]bool, len(modeSpecs))
	metrics.NeedFilesZero = make(map[string]bool, len(modeSpecs))
	for _, spec := range modeSpecs {
		metrics.ScanAttempts++
		scanURL := encodeURLQuery(baseURL+"/rest/db/scan", url.Values{"folder": []string{spec.id}})
		if _, _, err := requestJSON(http.MethodPost, scanURL, apiKey); err != nil {
			return metrics, fmt.Errorf("go scan request (%s): %w", spec.id, err)
		}

		status, polls, err := pollStatusForFolderLocalThreshold(
			baseURL,
			apiKey,
			spec.id,
			expectedLocalFiles,
			true,
			statusTimeout,
		)
		metrics.StatusPollAttempts += polls
		if err != nil {
			return metrics, fmt.Errorf("go status poll (%s): %w (last=%v)", spec.id, err, status)
		}
		localFiles := intField(status, "localFiles")
		needFiles := intField(status, "needFiles")
		state := stringField(status, "state")
		metrics.LocalFilesOK[spec.id] = localFiles >= expectedLocalFiles
		metrics.NeedFilesZero[spec.id] = needFiles == 0
		metrics.StateOK[spec.id] = stateIsRunnable(state)
		if !metrics.LocalFilesOK[spec.id] {
			return metrics, fmt.Errorf("go folder %s local files below expected: %d", spec.id, localFiles)
		}
		if !metrics.StateOK[spec.id] {
			return metrics, fmt.Errorf("go folder %s has non-runnable state %q", spec.id, state)
		}
		if !metrics.NeedFilesZero[spec.id] {
			return metrics, fmt.Errorf("go folder %s expected needFiles=0, got %d", spec.id, needFiles)
		}
	}
	metrics.ScanOK = true
	metrics.StatusOK = true

	types, err := fetchFolderTypeMap(baseURL, apiKey)
	if err != nil {
		return metrics, fmt.Errorf("go fetch folder type map: %w", err)
	}
	metrics.TypeMap = types
	metrics.TypeMapMatches = stringMapEquals(types, expectedFolderModeTypes)
	if !metrics.TypeMapMatches {
		return metrics, fmt.Errorf("go folder type map mismatch: got=%v want=%v", types, expectedFolderModeTypes)
	}
	metrics.StateProjection, err = buildStateProjection(
		baseURL,
		apiKey,
		[]string{"default", "sendonly", "recvonly", "recvenc"},
		false,
	)
	if err != nil {
		return metrics, fmt.Errorf("go state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalFolderModes(seed int64) (folderModeMetrics, error) {
	metrics := folderModeMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-folder-modes", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-rs-external-folder-modes-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	modeSpecs := []folderModeSpec{
		{id: "default", mode: "sendreceive", dir: filepath.Join(root, "default")},
		{id: "sendonly", mode: "sendonly", dir: filepath.Join(root, "sendonly")},
		{id: "recvonly", mode: "receiveonly", dir: filepath.Join(root, "recvonly")},
		{id: "recvenc", mode: "receiveencrypted", dir: filepath.Join(root, "recvenc")},
	}
	for _, spec := range modeSpecs {
		if err := prepareSoakFolder(spec.dir, seed+int64(len(spec.id))); err != nil {
			return metrics, fmt.Errorf("prepare folder %s: %w", spec.id, err)
		}
	}
	dbRoot := filepath.Join(root, "db")
	if err := os.MkdirAll(dbRoot, 0o755); err != nil {
		return metrics, fmt.Errorf("create rust db root: %w", err)
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	bepPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	proc := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", modeSpecs[0].dir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", bepPort),
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon: %w", err)
	}
	defer proc.stop()
	if err := waitForPing(baseURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust runtime identity check: %w", err)
	}

	for _, spec := range modeSpecs {
		query := url.Values{}
		query.Set("id", spec.id)
		query.Set("path", spec.dir)
		query.Set("type", spec.mode)
		endpoint := encodeURLQuery(baseURL+"/rest/config/folders", query)
		method := http.MethodPost
		if spec.id == "default" {
			method = http.MethodPut
		}
		if _, _, err := requestJSON(method, endpoint, ""); err != nil {
			return metrics, fmt.Errorf("configure rust folder %s (%s): %w", spec.id, spec.mode, err)
		}
	}
	metrics.FoldersConfigured = true

	metrics.LocalFilesOK = make(map[string]bool, len(modeSpecs))
	metrics.StateOK = make(map[string]bool, len(modeSpecs))
	metrics.NeedFilesZero = make(map[string]bool, len(modeSpecs))
	for _, spec := range modeSpecs {
		metrics.ScanAttempts++
		scanURL := encodeURLQuery(baseURL+"/rest/db/scan", url.Values{"folder": []string{spec.id}})
		if _, _, err := requestJSON(http.MethodPost, scanURL, ""); err != nil {
			return metrics, fmt.Errorf("rust scan request (%s): %w", spec.id, err)
		}

		status, polls, err := pollStatusForFolderLocalThreshold(
			baseURL,
			"",
			spec.id,
			expectedLocalFiles,
			true,
			statusTimeout,
		)
		metrics.StatusPollAttempts += polls
		if err != nil {
			return metrics, fmt.Errorf("rust status poll (%s): %w (last=%v)", spec.id, err, status)
		}
		localFiles := intField(status, "localFiles")
		needFiles := intField(status, "needFiles")
		state := stringField(status, "state")
		metrics.LocalFilesOK[spec.id] = localFiles >= expectedLocalFiles
		metrics.NeedFilesZero[spec.id] = needFiles == 0
		metrics.StateOK[spec.id] = stateIsRunnable(state)
		if !metrics.LocalFilesOK[spec.id] {
			return metrics, fmt.Errorf("rust folder %s local files below expected: %d", spec.id, localFiles)
		}
		if !metrics.StateOK[spec.id] {
			return metrics, fmt.Errorf("rust folder %s has non-runnable state %q", spec.id, state)
		}
		if !metrics.NeedFilesZero[spec.id] {
			return metrics, fmt.Errorf("rust folder %s expected needFiles=0, got %d", spec.id, needFiles)
		}
	}
	metrics.ScanOK = true
	metrics.StatusOK = true

	types, err := fetchFolderTypeMap(baseURL, "")
	if err != nil {
		return metrics, fmt.Errorf("rust fetch folder type map: %w", err)
	}
	metrics.TypeMap = types
	metrics.TypeMapMatches = stringMapEquals(types, expectedFolderModeTypes)
	if !metrics.TypeMapMatches {
		return metrics, fmt.Errorf("rust folder type map mismatch: got=%v want=%v", types, expectedFolderModeTypes)
	}
	metrics.StateProjection, err = buildStateProjection(
		baseURL,
		"",
		[]string{"default", "sendonly", "recvonly", "recvenc"},
		false,
	)
	if err != nil {
		return metrics, fmt.Errorf("rust state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runGoExternalDurabilityRestart(seed int64) (durabilityMetrics, error) {
	metrics := durabilityMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-durability", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-go-external-durability-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	folderDir := filepath.Join(root, "folder")
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return metrics, err
	}
	if err := runCmd(20*time.Second, "go", "run", "./cmd/syncthing", "generate", "--home", homeDir, "--no-port-probing"); err != nil {
		return metrics, fmt.Errorf("generate go config: %w", err)
	}
	apiKey, err := parseAPIKey(filepath.Join(homeDir, "config.xml"))
	if err != nil {
		return metrics, err
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	proc := &daemonProc{
		cmd: exec.Command(
			"go", "run", "./cmd/syncthing", "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", baseURL,
			"--log-file=-",
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon: %w", err)
	}
	defer proc.stop()
	if err := waitForPing(baseURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go first boot startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go first boot runtime identity: %w", err)
	}
	metrics.FirstBootOK = true

	if err := runCmd(
		20*time.Second,
		"go", "run", "./cmd/syncthing", "cli",
		"--home", homeDir,
		"--gui-address", fmt.Sprintf("127.0.0.1:%d", apiPort),
		"--gui-apikey", apiKey,
		"config", "folders", "add",
		"--id", "default",
		"--path", folderDir,
		"--type", "sendreceive",
	); err != nil {
		return metrics, fmt.Errorf("configure go folder: %w", err)
	}
	metrics.FolderConfigured = true

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", apiKey); err != nil {
		return metrics, fmt.Errorf("go first scan request: %w", err)
	}
	metrics.ScanOK = true
	status, polls, err := pollStatus(baseURL, apiKey, statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("go first status poll: %w", err)
	}
	metrics.PreRestartLocalFiles = intField(status, "localFiles")
	metrics.PreRestartGlobalFiles = intField(status, "globalFiles")
	if metrics.PreRestartLocalFiles < expectedLocalFiles || metrics.PreRestartGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("go pre-restart status did not observe expected files (local=%d global=%d)",
			metrics.PreRestartLocalFiles, metrics.PreRestartGlobalFiles)
	}
	if err := verifyIndexedFiles(baseURL, apiKey, "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("go pre-restart file verification: %w", err)
	}
	metrics.PreRestartFilesIndexed = true
	preProjection, err := buildStateProjection(baseURL, apiKey, []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("go pre-restart state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go first shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go first shutdown wait: %w", err)
	}

	restartPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	restartURL := fmt.Sprintf("http://127.0.0.1:%d", restartPort)
	restarted := &daemonProc{
		cmd: exec.Command(
			"go", "run", "./cmd/syncthing", "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", restartURL,
			"--log-file=-",
		),
	}
	if err := restarted.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon restart: %w", err)
	}
	defer restarted.stop()
	if err := waitForPing(restartURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go restart boot startup: %w (stderr=%s)", err, strings.TrimSpace(restarted.stderr.String()))
	}
	if err := verifyRuntimeIdentity(restartURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go restart runtime identity: %w", err)
	}
	metrics.RestartBootOK = true

	restartStatus, polls, err := pollStatus(restartURL, apiKey, statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("go restart status poll: %w", err)
	}
	metrics.PostRestartLocalFiles = intField(restartStatus, "localFiles")
	metrics.PostRestartGlobalFiles = intField(restartStatus, "globalFiles")
	metrics.PostRestartNeedFiles = intField(restartStatus, "needFiles")
	if metrics.PostRestartLocalFiles < expectedLocalFiles || metrics.PostRestartGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("go post-restart status did not observe expected files (local=%d global=%d need=%d)",
			metrics.PostRestartLocalFiles, metrics.PostRestartGlobalFiles, metrics.PostRestartNeedFiles)
	}
	if err := verifyIndexedFiles(restartURL, apiKey, "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("go post-restart file verification: %w", err)
	}
	metrics.PostRestartFilesIndexed = true
	postProjection, err := buildStateProjection(restartURL, apiKey, []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("go post-restart state projection: %w", err)
	}
	metrics.StateProjection = mergeProjectionWithPhases(postProjection, map[string]any{
		"pre_restart":  preProjection,
		"post_restart": postProjection,
	})

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalDurabilityRestart(seed int64) (durabilityMetrics, error) {
	metrics := durabilityMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-durability", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-rs-external-durability-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	folderDir := filepath.Join(root, "folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return metrics, err
	}
	if err := os.MkdirAll(dbRoot, 0o755); err != nil {
		return metrics, fmt.Errorf("create rust db root: %w", err)
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	bepPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	proc := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", folderDir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", bepPort),
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon: %w", err)
	}
	defer proc.stop()
	if err := waitForPing(baseURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust first boot startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust first boot runtime identity: %w", err)
	}
	metrics.FirstBootOK = true
	metrics.FolderConfigured = true

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", ""); err != nil {
		return metrics, fmt.Errorf("rust first scan request: %w", err)
	}
	metrics.ScanOK = true
	status, polls, err := pollStatus(baseURL, "", statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("rust first status poll: %w", err)
	}
	metrics.PreRestartLocalFiles = intField(status, "localFiles")
	metrics.PreRestartGlobalFiles = intField(status, "globalFiles")
	if metrics.PreRestartLocalFiles < expectedLocalFiles || metrics.PreRestartGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("rust pre-restart status did not observe expected files (local=%d global=%d)",
			metrics.PreRestartLocalFiles, metrics.PreRestartGlobalFiles)
	}
	if err := verifyIndexedFiles(baseURL, "", "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("rust pre-restart file verification: %w", err)
	}
	metrics.PreRestartFilesIndexed = true
	preProjection, err := buildStateProjection(baseURL, "", []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("rust pre-restart state projection: %w", err)
	}

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust first shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust first shutdown wait: %w", err)
	}

	restartAPIPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	restartBEPPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	restartURL := fmt.Sprintf("http://127.0.0.1:%d", restartAPIPort)
	restarted := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", folderDir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", restartAPIPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", restartBEPPort),
		),
	}
	if err := restarted.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon restart: %w", err)
	}
	defer restarted.stop()
	if err := waitForPing(restartURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust restart boot startup: %w (stderr=%s)", err, strings.TrimSpace(restarted.stderr.String()))
	}
	if err := verifyRuntimeIdentity(restartURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust restart runtime identity: %w", err)
	}
	metrics.RestartBootOK = true

	restartStatus, polls, err := pollStatus(restartURL, "", statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("rust restart status poll: %w", err)
	}
	metrics.PostRestartLocalFiles = intField(restartStatus, "localFiles")
	metrics.PostRestartGlobalFiles = intField(restartStatus, "globalFiles")
	metrics.PostRestartNeedFiles = intField(restartStatus, "needFiles")
	if metrics.PostRestartLocalFiles < expectedLocalFiles || metrics.PostRestartGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("rust post-restart status did not observe expected files (local=%d global=%d need=%d)",
			metrics.PostRestartLocalFiles, metrics.PostRestartGlobalFiles, metrics.PostRestartNeedFiles)
	}
	if err := verifyIndexedFiles(restartURL, "", "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("rust post-restart file verification: %w", err)
	}
	metrics.PostRestartFilesIndexed = true
	postProjection, err := buildStateProjection(restartURL, "", []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("rust post-restart state projection: %w", err)
	}
	metrics.StateProjection = mergeProjectionWithPhases(postProjection, map[string]any{
		"pre_restart":  preProjection,
		"post_restart": postProjection,
	})

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func runGoExternalCrashRecoveryRestart(seed int64) (crashMetrics, error) {
	metrics := crashMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-crash-recovery", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-go-external-crash-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	folderDir := filepath.Join(root, "folder")
	goBinary := filepath.Join(root, "syncthing-go")
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return metrics, err
	}
	if err := runCmd(90*time.Second, "go", "build", "-o", goBinary, "./cmd/syncthing"); err != nil {
		return metrics, fmt.Errorf("build go daemon binary: %w", err)
	}
	if err := runCmd(20*time.Second, goBinary, "generate", "--home", homeDir, "--no-port-probing"); err != nil {
		return metrics, fmt.Errorf("generate go config: %w", err)
	}
	apiKey, err := parseAPIKey(filepath.Join(homeDir, "config.xml"))
	if err != nil {
		return metrics, err
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)
	proc := &daemonProc{
		cmd: exec.Command(
			goBinary, "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", baseURL,
			"--log-file=-",
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon: %w", err)
	}
	defer proc.stop()
	if err := waitForPing(baseURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go first boot startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go first boot runtime identity: %w", err)
	}
	metrics.FirstBootOK = true

	if err := runCmd(
		20*time.Second,
		goBinary, "cli",
		"--home", homeDir,
		"--gui-address", fmt.Sprintf("127.0.0.1:%d", apiPort),
		"--gui-apikey", apiKey,
		"config", "folders", "add",
		"--id", "default",
		"--path", folderDir,
		"--type", "sendreceive",
	); err != nil {
		return metrics, fmt.Errorf("configure go folder: %w", err)
	}
	metrics.FolderConfigured = true

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", apiKey); err != nil {
		return metrics, fmt.Errorf("go first scan request: %w", err)
	}
	metrics.ScanOK = true
	status, polls, err := pollStatus(baseURL, apiKey, statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("go first status poll: %w", err)
	}
	metrics.PreCrashLocalFiles = intField(status, "localFiles")
	metrics.PreCrashGlobalFiles = intField(status, "globalFiles")
	if metrics.PreCrashLocalFiles < expectedLocalFiles || metrics.PreCrashGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("go pre-crash status did not observe expected files (local=%d global=%d)",
			metrics.PreCrashLocalFiles, metrics.PreCrashGlobalFiles)
	}
	if err := verifyIndexedFiles(baseURL, apiKey, "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("go pre-crash file verification: %w", err)
	}
	metrics.PreCrashFilesIndexed = true
	preProjection, err := buildStateProjection(baseURL, apiKey, []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("go pre-crash state projection: %w", err)
	}

	if err := proc.crashKill(); err != nil {
		return metrics, fmt.Errorf("go crash kill stop: %w", err)
	}
	metrics.CrashKillOK = true
	clearGoHomeLocks(homeDir)

	restartPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	restartURL := fmt.Sprintf("http://127.0.0.1:%d", restartPort)
	restarted := &daemonProc{
		cmd: exec.Command(
			goBinary, "serve",
			"--home", homeDir,
			"--no-browser",
			"--no-restart",
			"--gui-address", restartURL,
			"--log-file=-",
		),
	}
	if err := restarted.start(); err != nil {
		return metrics, fmt.Errorf("start go daemon restart: %w", err)
	}
	defer restarted.stop()
	if err := waitForPing(restartURL, apiKey, startupTimeout); err != nil {
		return metrics, fmt.Errorf("go restart boot startup: %w (stderr=%s)", err, strings.TrimSpace(restarted.stderr.String()))
	}
	if err := verifyRuntimeIdentity(restartURL, apiKey, "go"); err != nil {
		return metrics, fmt.Errorf("go restart runtime identity: %w", err)
	}
	metrics.RestartBootOK = true

	restartStatus, polls, err := pollStatus(restartURL, apiKey, statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("go restart status poll: %w", err)
	}
	metrics.PostRestartLocalFiles = intField(restartStatus, "localFiles")
	metrics.PostRestartGlobalFiles = intField(restartStatus, "globalFiles")
	metrics.PostRestartNeedFiles = intField(restartStatus, "needFiles")
	if metrics.PostRestartLocalFiles < expectedLocalFiles || metrics.PostRestartGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("go post-restart status did not observe expected files (local=%d global=%d need=%d)",
			metrics.PostRestartLocalFiles, metrics.PostRestartGlobalFiles, metrics.PostRestartNeedFiles)
	}
	if err := verifyIndexedFiles(restartURL, apiKey, "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("go post-restart file verification: %w", err)
	}
	metrics.PostRestartFilesIndexed = true
	postProjection, err := buildStateProjection(restartURL, apiKey, []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("go post-restart state projection: %w", err)
	}
	metrics.StateProjection = mergeProjectionWithPhases(postProjection, map[string]any{
		"pre_crash":    preProjection,
		"post_restart": postProjection,
	})

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalCrashRecoveryRestart(seed int64) (crashMetrics, error) {
	metrics := crashMetrics{
		Seed:           seed,
		WorkloadDigest: workloadDigest("external-crash-recovery", seed),
	}
	root, err := os.MkdirTemp("", "syncthing-rs-external-crash-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	folderDir := filepath.Join(root, "folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return metrics, err
	}
	if err := os.MkdirAll(dbRoot, 0o755); err != nil {
		return metrics, fmt.Errorf("create rust db root: %w", err)
	}

	apiPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	bepPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", apiPort)

	proc := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", folderDir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", apiPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", bepPort),
		),
	}
	if err := proc.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon: %w", err)
	}
	defer proc.stop()
	if err := waitForPing(baseURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust first boot startup: %w (stderr=%s)", err, strings.TrimSpace(proc.stderr.String()))
	}
	if err := verifyRuntimeIdentity(baseURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust first boot runtime identity: %w", err)
	}
	metrics.FirstBootOK = true
	metrics.FolderConfigured = true

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", ""); err != nil {
		return metrics, fmt.Errorf("rust first scan request: %w", err)
	}
	metrics.ScanOK = true
	status, polls, err := pollStatus(baseURL, "", statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("rust first status poll: %w", err)
	}
	metrics.PreCrashLocalFiles = intField(status, "localFiles")
	metrics.PreCrashGlobalFiles = intField(status, "globalFiles")
	if metrics.PreCrashLocalFiles < expectedLocalFiles || metrics.PreCrashGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("rust pre-crash status did not observe expected files (local=%d global=%d)",
			metrics.PreCrashLocalFiles, metrics.PreCrashGlobalFiles)
	}
	if err := verifyIndexedFiles(baseURL, "", "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("rust pre-crash file verification: %w", err)
	}
	metrics.PreCrashFilesIndexed = true
	preProjection, err := buildStateProjection(baseURL, "", []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("rust pre-crash state projection: %w", err)
	}

	if err := proc.crashKill(); err != nil {
		return metrics, fmt.Errorf("rust crash kill stop: %w", err)
	}
	metrics.CrashKillOK = true

	restartAPIPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	restartBEPPort, err := freePort()
	if err != nil {
		return metrics, err
	}
	restartURL := fmt.Sprintf("http://127.0.0.1:%d", restartAPIPort)
	restarted := &daemonProc{
		cmd: exec.Command(
			"cargo", "run", "--quiet", "--manifest-path", "syncthing-rs/Cargo.toml", "--",
			"daemon",
			"--folder", fmt.Sprintf("default:%s", folderDir),
			"--db-root", dbRoot,
			"--api-listen", fmt.Sprintf("127.0.0.1:%d", restartAPIPort),
			"--listen", fmt.Sprintf("127.0.0.1:%d", restartBEPPort),
		),
	}
	if err := restarted.start(); err != nil {
		return metrics, fmt.Errorf("start rust daemon restart: %w", err)
	}
	defer restarted.stop()
	if err := waitForPing(restartURL, "", startupTimeout); err != nil {
		return metrics, fmt.Errorf("rust restart boot startup: %w (stderr=%s)", err, strings.TrimSpace(restarted.stderr.String()))
	}
	if err := verifyRuntimeIdentity(restartURL, "", "rust"); err != nil {
		return metrics, fmt.Errorf("rust restart runtime identity: %w", err)
	}
	metrics.RestartBootOK = true

	restartStatus, polls, err := pollStatus(restartURL, "", statusTimeout)
	metrics.StatusPollAttempts += polls
	if err != nil {
		return metrics, fmt.Errorf("rust restart status poll: %w", err)
	}
	metrics.PostRestartLocalFiles = intField(restartStatus, "localFiles")
	metrics.PostRestartGlobalFiles = intField(restartStatus, "globalFiles")
	metrics.PostRestartNeedFiles = intField(restartStatus, "needFiles")
	if metrics.PostRestartLocalFiles < expectedLocalFiles || metrics.PostRestartGlobalFiles < expectedLocalFiles {
		return metrics, fmt.Errorf("rust post-restart status did not observe expected files (local=%d global=%d need=%d)",
			metrics.PostRestartLocalFiles, metrics.PostRestartGlobalFiles, metrics.PostRestartNeedFiles)
	}
	if err := verifyIndexedFiles(restartURL, "", "default", []string{"a.txt", "nested/b.txt"}); err != nil {
		return metrics, fmt.Errorf("rust post-restart file verification: %w", err)
	}
	metrics.PostRestartFilesIndexed = true
	postProjection, err := buildStateProjection(restartURL, "", []string{"default"}, false)
	if err != nil {
		return metrics, fmt.Errorf("rust post-restart state projection: %w", err)
	}
	metrics.StateProjection = mergeProjectionWithPhases(postProjection, map[string]any{
		"pre_crash":    preProjection,
		"post_restart": postProjection,
	})

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func prepareSoakFolder(folderDir string, seed int64) error {
	if err := os.MkdirAll(filepath.Join(folderDir, "nested"), 0o755); err != nil {
		return fmt.Errorf("create folder tree: %w", err)
	}
	if err := os.WriteFile(filepath.Join(folderDir, ".stfolder"), []byte(""), 0o644); err != nil {
		return fmt.Errorf("write marker file: %w", err)
	}
	if err := os.WriteFile(
		filepath.Join(folderDir, "a.txt"),
		[]byte(seededPayload("hello", seed)),
		0o644,
	); err != nil {
		return fmt.Errorf("write a.txt: %w", err)
	}
	if err := os.WriteFile(
		filepath.Join(folderDir, "nested", "b.txt"),
		[]byte(seededPayload("world", seed+1)),
		0o644,
	); err != nil {
		return fmt.Errorf("write nested/b.txt: %w", err)
	}
	return nil
}

func prepareMultiFolderSoakFolder(folderDir string, seed int64) error {
	if err := prepareSoakFolder(folderDir, seed); err != nil {
		return err
	}
	for _, dir := range []string{"a", "a.d"} {
		if err := os.MkdirAll(filepath.Join(folderDir, dir), 0o755); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	files := map[string]string{
		"a/x.txt":   seededPayload("ax", seed+2),
		"a/z.txt":   seededPayload("az", seed+3),
		"a.d/x.txt": seededPayload("adx", seed+4),
		"a.d/y.txt": seededPayload("ady", seed+5),
	}
	for rel, content := range files {
		if err := os.WriteFile(filepath.Join(folderDir, rel), []byte(content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", rel, err)
		}
	}
	return nil
}

func seedFromEnv() int64 {
	seedRaw := strings.TrimSpace(os.Getenv("PARITY_SEED"))
	if seedRaw == "" {
		return 1
	}
	seed, err := strconv.ParseInt(seedRaw, 10, 64)
	if err != nil {
		return 1
	}
	return seed
}

func seededPayload(prefix string, seed int64) string {
	return fmt.Sprintf("%s-%d", prefix, seed)
}

func workloadDigest(tag string, seed int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", tag, seed)))
	return hex.EncodeToString(sum[:])
}

func parseAPIKey(configPath string) (string, error) {
	bs, err := os.ReadFile(configPath)
	if err != nil {
		return "", fmt.Errorf("read config %s: %w", configPath, err)
	}
	re := regexp.MustCompile(`<apikey>([^<]+)</apikey>`)
	match := re.FindStringSubmatch(string(bs))
	if len(match) != 2 {
		return "", fmt.Errorf("parse api key from %s", configPath)
	}
	key := strings.TrimSpace(match[1])
	if key == "" {
		return "", fmt.Errorf("empty api key in %s", configPath)
	}
	return key, nil
}

func runCmd(timeout time.Duration, name string, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = io.Discard
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("timeout running %s %s", name, strings.Join(args, " "))
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s %s: %s", name, strings.Join(args, " "), msg)
	}
	return nil
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocate free port: %w", err)
	}
	defer ln.Close()
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener addr type %T", ln.Addr())
	}
	return addr.Port, nil
}

func waitForPing(baseURL, apiKey string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, status, err := requestJSON(http.MethodGet, baseURL+"/rest/system/ping", apiKey)
		if err == nil && status == http.StatusOK {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for ping")
}

func verifyRuntimeIdentity(baseURL, apiKey, expectedSource string) error {
	payload, status, err := requestJSON(http.MethodGet, baseURL+"/rest/system/version", apiKey)
	if err != nil {
		return fmt.Errorf("request version endpoint: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("version endpoint returned status %d", status)
	}

	longVersion := strings.ToLower(strings.TrimSpace(stringField(payload, "longVersion")))
	version := strings.ToLower(strings.TrimSpace(stringField(payload, "version")))
	isRustSignature := strings.Contains(longVersion, "syncthing-rs") || strings.Contains(version, "syncthing-rs")
	switch strings.ToLower(strings.TrimSpace(expectedSource)) {
	case "go":
		if isRustSignature {
			return fmt.Errorf("expected go runtime but version payload reports rust: longVersion=%q version=%q", longVersion, version)
		}
	case "rust":
		if !isRustSignature {
			return fmt.Errorf("expected rust runtime signature in version payload, got longVersion=%q version=%q", longVersion, version)
		}
	default:
		return fmt.Errorf("unsupported runtime source expectation %q", expectedSource)
	}
	return nil
}

func pollStatus(baseURL, apiKey string, timeout time.Duration) (map[string]any, int, error) {
	return pollStatusForFolder(
		baseURL,
		apiKey,
		"default",
		expectedLocalFiles,
		expectedLocalFiles,
		true,
		timeout,
	)
}

func pollStatusForFolder(
	baseURL, apiKey, folder string,
	minLocalFiles, minGlobalFiles int,
	requireRunnableState bool,
	timeout time.Duration,
) (map[string]any, int, error) {
	deadline := time.Now().Add(timeout)
	polls := 0
	var last map[string]any
	target := encodeURLQuery(baseURL+"/rest/db/status", url.Values{"folder": []string{folder}})
	for time.Now().Before(deadline) {
		polls++
		payload, status, err := requestJSON(http.MethodGet, target, apiKey)
		if err == nil && status == http.StatusOK {
			last = payload
			localOK := intField(payload, "localFiles") >= minLocalFiles
			globalOK := intField(payload, "globalFiles") >= minGlobalFiles
			stateOK := true
			if requireRunnableState {
				stateOK = stateIsRunnable(stringField(payload, "state"))
			}
			if localOK && globalOK && stateOK {
				return payload, polls, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == nil {
		last = map[string]any{}
	}
	return last, polls, fmt.Errorf("timed out waiting for status convergence for folder %q", folder)
}

func pollStatusForFolderLocalThreshold(
	baseURL, apiKey, folder string,
	minLocalFiles int,
	requireRunnableState bool,
	timeout time.Duration,
) (map[string]any, int, error) {
	deadline := time.Now().Add(timeout)
	polls := 0
	var last map[string]any
	target := encodeURLQuery(baseURL+"/rest/db/status", url.Values{"folder": []string{folder}})
	for time.Now().Before(deadline) {
		polls++
		payload, status, err := requestJSON(http.MethodGet, target, apiKey)
		if err == nil && status == http.StatusOK {
			last = payload
			localOK := intField(payload, "localFiles") >= minLocalFiles
			stateOK := true
			if requireRunnableState {
				stateOK = stateIsRunnable(stringField(payload, "state"))
			}
			if localOK && stateOK {
				return payload, polls, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if last == nil {
		last = map[string]any{}
	}
	return last, polls, fmt.Errorf("timed out waiting for local status convergence for folder %q", folder)
}

func verifyIndexedFiles(baseURL, apiKey, folder string, files []string) error {
	for _, file := range files {
		query := url.Values{}
		query.Set("folder", folder)
		query.Set("file", file)
		target := encodeURLQuery(baseURL+"/rest/db/file", query)
		payload, status, err := requestJSON(http.MethodGet, target, apiKey)
		if err != nil {
			return fmt.Errorf("request %s: %w", target, err)
		}
		if status != http.StatusOK {
			return fmt.Errorf("request %s returned status %d", target, status)
		}
		if err := validateIndexedFilePayload(payload, file); err != nil {
			return fmt.Errorf("request %s %w payload=%v", target, err, payload)
		}
	}
	return nil
}

func validateIndexedFilePayload(payload map[string]any, expectedFile string) error {
	if len(payload) == 0 {
		return fmt.Errorf("returned empty payload")
	}
	if existsRaw, hasExists := payload["exists"]; hasExists {
		exists, ok := existsRaw.(bool)
		if !ok || !exists {
			return fmt.Errorf("returned non-existing entry")
		}
		entry, ok := payload["entry"].(map[string]any)
		if !ok {
			return fmt.Errorf("returned invalid entry object")
		}
		return validateIndexedEntry(entry, expectedFile)
	}

	// Go's /rest/db/file shape exposes local/global objects instead of exists/entry.
	if local, ok := payload["local"].(map[string]any); ok && len(local) > 0 {
		if err := validateIndexedEntry(local, expectedFile); err == nil {
			return nil
		}
	}
	if global, ok := payload["global"].(map[string]any); ok && len(global) > 0 {
		if err := validateIndexedEntry(global, expectedFile); err == nil {
			return nil
		}
	}
	return fmt.Errorf("returned no recognizable indexed entry")
}

func validateIndexedEntry(entry map[string]any, expectedFile string) error {
	if len(entry) == 0 {
		return fmt.Errorf("entry missing")
	}
	if deleted, ok := entry["deleted"].(bool); ok && deleted {
		return fmt.Errorf("entry marked deleted")
	}
	name := strings.TrimSpace(stringField(entry, "path"))
	if name == "" {
		name = strings.TrimSpace(stringField(entry, "name"))
	}
	if name == "" {
		return fmt.Errorf("entry missing name/path")
	}
	if expectedFile != "" && name != expectedFile {
		return fmt.Errorf("entry path mismatch")
	}
	if _, ok := entry["size"]; !ok {
		return fmt.Errorf("entry missing size")
	}
	if _, ok := entry["blockHashes"]; ok {
		return nil
	}
	if _, ok := entry["blocksHash"]; ok {
		return nil
	}
	if _, ok := entry["numBlocks"]; ok {
		return nil
	}
	return fmt.Errorf("entry missing block hash metadata")
}

func fetchFolderTypeMap(baseURL, apiKey string) (map[string]string, error) {
	payload, status, err := requestJSONAny(http.MethodGet, baseURL+"/rest/config/folders", apiKey)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("config folders request returned status %d", status)
	}
	return decodeFolderTypeMap(payload)
}

func decodeFolderTypeMap(payload any) (map[string]string, error) {
	out := make(map[string]string)
	switch typed := payload.(type) {
	case []any:
		for _, item := range typed {
			entry, ok := item.(map[string]any)
			if !ok {
				continue
			}
			id := strings.TrimSpace(stringField(entry, "id"))
			if id == "" {
				continue
			}
			folderType := canonicalFolderType(stringField(entry, "type"))
			if folderType == "" {
				folderType = canonicalFolderType(stringField(entry, "folderType"))
			}
			if folderType == "" {
				continue
			}
			out[id] = folderType
		}
	case map[string]any:
		return decodeFolderTypeMap(typed["folders"])
	default:
		return nil, fmt.Errorf("unsupported /rest/config/folders payload shape %T", payload)
	}
	return out, nil
}

func canonicalFolderType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "sendreceive", "sendrecv", "readwrite":
		return "sendrecv"
	case "sendonly", "readonly":
		return "sendonly"
	case "receiveonly", "recvonly":
		return "recvonly"
	case "receiveencrypted", "recvenc":
		return "recvenc"
	default:
		return ""
	}
}

func stringMapEquals(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		if right[key] != leftValue {
			return false
		}
	}
	return true
}

func browseOrderedPaths(baseURL, apiKey, folder string, limit int) ([]string, error) {
	query := url.Values{}
	query.Set("folder", folder)
	query.Set("limit", strconv.Itoa(limit))
	target := encodeURLQuery(baseURL+"/rest/db/browse", query)
	payloadAny, status, err := requestJSONAny(http.MethodGet, target, apiKey)
	if err != nil {
		return nil, fmt.Errorf("request %s: %w", target, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("request %s returned status %d", target, status)
	}
	paths := make([]string, 0, 16)
	if err := appendBrowsePaths(&paths, "", payloadAny); err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(paths))
	for _, p := range paths {
		p = strings.Trim(strings.ReplaceAll(p, "\\", "/"), "/")
		if p == "" || strings.HasSuffix(p, "/.stfolder") || p == ".stfolder" {
			continue
		}
		filtered = append(filtered, p)
	}
	return filtered, nil
}

func appendBrowsePaths(out *[]string, parent string, payload any) error {
	switch typed := payload.(type) {
	case []any:
		for _, item := range typed {
			if err := appendBrowsePaths(out, parent, item); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if items, ok := typed["items"]; ok {
			return appendBrowsePaths(out, parent, items)
		}
		if path := stringField(typed, "path"); strings.TrimSpace(path) != "" {
			*out = append(*out, path)
			return nil
		}
		name := stringField(typed, "name")
		if strings.TrimSpace(name) == "" {
			return nil
		}
		current := name
		if parent != "" {
			current = parent + "/" + name
		}
		if children, ok := typed["children"]; ok {
			return appendBrowsePaths(out, current, children)
		}
		*out = append(*out, current)
		return nil
	default:
		return fmt.Errorf("unsupported /rest/db/browse payload shape %T", payload)
	}
}

func buildStateProjection(baseURL, apiKey string, folders []string, includeBrowse bool) (map[string]any, error) {
	systemStatus, statusCode, err := requestJSON(http.MethodGet, baseURL+"/rest/system/status", apiKey)
	if err != nil {
		return nil, fmt.Errorf("request /rest/system/status: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("/rest/system/status returned status %d", statusCode)
	}
	configPayload, statusCode, err := requestJSON(http.MethodGet, baseURL+"/rest/config", apiKey)
	if err != nil {
		return nil, fmt.Errorf("request /rest/config: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("/rest/config returned status %d", statusCode)
	}

	pendingDevicesPayload, statusCode, err := requestJSONAny(http.MethodGet, baseURL+"/rest/cluster/pending/devices", apiKey)
	if err != nil {
		return nil, fmt.Errorf("request /rest/cluster/pending/devices: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("/rest/cluster/pending/devices returned status %d", statusCode)
	}
	pendingFoldersPayload, statusCode, err := requestJSONAny(http.MethodGet, baseURL+"/rest/cluster/pending/folders", apiKey)
	if err != nil {
		return nil, fmt.Errorf("request /rest/cluster/pending/folders: %w", err)
	}
	if statusCode != http.StatusOK {
		return nil, fmt.Errorf("/rest/cluster/pending/folders returned status %d", statusCode)
	}

	myID := strings.TrimSpace(stringField(systemStatus, "myID"))
	configuredDeviceIDs := dedupeSortedNonEmpty(extractConfigDeviceIDs(configPayload))
	if myID == "" && len(configuredDeviceIDs) > 0 {
		myID = configuredDeviceIDs[0]
	}
	deviceIDs := []string{}
	if myID != "" {
		deviceIDs = []string{myID}
	}

	folderTypeMap := extractConfigFolderTypeMap(configPayload)
	folderIDs := dedupeSortedNonEmpty(folders)
	if len(folderIDs) == 0 {
		for folderID := range folderTypeMap {
			folderIDs = append(folderIDs, folderID)
		}
		folderIDs = dedupeSortedNonEmpty(folderIDs)
	}

	folderSummaries := make([]map[string]any, 0, len(folderIDs))
	for _, folderID := range folderIDs {
		target := encodeURLQuery(baseURL+"/rest/db/status", url.Values{"folder": []string{folderID}})
		payload, statusCode, err := requestJSON(http.MethodGet, target, apiKey)
		if err != nil {
			return nil, fmt.Errorf("request folder status %q: %w", folderID, err)
		}
		if statusCode != http.StatusOK {
			return nil, fmt.Errorf("folder status %q returned status %d", folderID, statusCode)
		}

		summary := map[string]any{
			"id":           folderID,
			"type":         folderTypeMap[folderID],
			"state":        classifyRunnableState(stringField(payload, "state")),
			"local_files":  intField(payload, "localFiles"),
			"global_files": intField(payload, "globalFiles"),
			"need_files":   intField(payload, "needFiles"),
		}
		if includeBrowse {
			paths, err := browseOrderedPaths(baseURL, apiKey, folderID, 1024)
			if err != nil {
				return nil, fmt.Errorf("browse ordered paths for %q: %w", folderID, err)
			}
			summary["browse_paths"] = paths
			summary["browse_path_count"] = len(paths)
		}
		folderSummaries = append(folderSummaries, summary)
	}
	sort.Slice(folderSummaries, func(i, j int) bool {
		return stringField(folderSummaries[i], "id") < stringField(folderSummaries[j], "id")
	})

	pendingDeviceIDs, invalidPendingDeviceIDs := extractPendingIDs(pendingDevicesPayload, []string{"deviceID", "id", "name"})
	pendingFolderIDs, invalidPendingFolderIDs := extractPendingIDs(pendingFoldersPayload, []string{"folder", "id", "label", "name"})

	return map[string]any{
		"my_id":                      myID,
		"device_ids":                 deviceIDs,
		"folder_summaries":           folderSummaries,
		"pending_device_ids":         pendingDeviceIDs,
		"pending_folder_ids":         pendingFolderIDs,
		"invalid_pending_device_ids": invalidPendingDeviceIDs,
		"invalid_pending_folder_ids": invalidPendingFolderIDs,
	}, nil
}

func classifyRunnableState(state string) string {
	if stateIsRunnable(state) {
		return "runnable"
	}
	return "not-runnable"
}

func extractConfigDeviceIDs(config map[string]any) []string {
	raw, ok := config["devices"]
	if !ok {
		return nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id := strings.TrimSpace(stringField(entry, "deviceID"))
		if id == "" {
			id = strings.TrimSpace(stringField(entry, "id"))
		}
		if id == "" {
			continue
		}
		out = append(out, id)
	}
	return out
}

func extractConfigFolderTypeMap(config map[string]any) map[string]string {
	out := make(map[string]string)
	raw, ok := config["folders"]
	if !ok {
		return out
	}
	items, ok := raw.([]any)
	if !ok {
		return out
	}
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id := strings.TrimSpace(stringField(entry, "id"))
		if id == "" {
			continue
		}
		folderType := canonicalFolderType(stringField(entry, "type"))
		if folderType == "" {
			folderType = canonicalFolderType(stringField(entry, "folderType"))
		}
		out[id] = folderType
	}
	return out
}

func extractPendingIDs(payload any, fieldCandidates []string) ([]string, []string) {
	ids := make([]string, 0, 8)
	invalid := make([]string, 0, 4)

	extractEntryID := func(entry map[string]any) string {
		for _, candidate := range fieldCandidates {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			id := strings.TrimSpace(stringField(entry, candidate))
			if id != "" {
				return id
			}
		}
		return ""
	}
	isEntryMap := func(entry map[string]any) bool {
		if len(entry) == 0 {
			return false
		}
		for _, candidate := range fieldCandidates {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			if _, ok := entry[candidate]; ok {
				return true
			}
		}
		// Common pending entry fields from /rest/cluster/pending/* endpoints.
		for _, field := range []string{"name", "address", "time", "offeredBy"} {
			if _, ok := entry[field]; ok {
				return true
			}
		}
		return false
	}

	var walk func(any)
	walk = func(node any) {
		switch typed := node.(type) {
		case map[string]any:
			if value, ok := typed["devices"]; ok {
				walk(value)
			}
			if value, ok := typed["folders"]; ok {
				walk(value)
			}
			if isEntryMap(typed) {
				if id := extractEntryID(typed); id != "" {
					ids = append(ids, id)
				} else {
					invalid = append(invalid, "<empty>")
				}
				return
			}
			for key, value := range typed {
				key = strings.TrimSpace(key)
				switch key {
				case "count", "devices", "folders":
					continue
				}
				if key != "" {
					ids = append(ids, key)
					continue
				}
				if entry, ok := value.(map[string]any); ok {
					if id := extractEntryID(entry); id != "" {
						ids = append(ids, id)
					} else {
						invalid = append(invalid, "<empty>")
					}
					continue
				}
				walk(value)
			}
		case []any:
			for _, item := range typed {
				walk(item)
			}
		}
	}

	walk(payload)
	return dedupeSortedNonEmpty(ids), dedupeSortedNonEmpty(invalid)
}

func dedupeSortedNonEmpty(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return []string{}
	}
	return out
}

func mergeProjectionWithPhases(base map[string]any, phases map[string]any) map[string]any {
	out := make(map[string]any, len(base)+1)
	for key, value := range base {
		out[key] = value
	}
	out["phase_projection"] = phases
	return out
}

func equalStringSlices(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func requestJSON(method, rawURL, apiKey string) (map[string]any, int, error) {
	payloadAny, status, err := requestJSONAny(method, rawURL, apiKey)
	if err != nil {
		if payload, ok := payloadAny.(map[string]any); ok {
			return payload, status, err
		}
		return nil, status, err
	}
	payload, ok := payloadAny.(map[string]any)
	if !ok {
		return nil, status, fmt.Errorf("expected object json response from %s %s", method, rawURL)
	}
	return payload, status, nil
}

func requestJSONAny(method, rawURL, apiKey string) (any, int, error) {
	client := &http.Client{Timeout: httpTimeout}
	req, err := http.NewRequest(method, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	if apiKey != "" {
		req.Header.Set("X-API-Key", apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	var payload any = map[string]any{}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, resp.StatusCode, fmt.Errorf("decode json response from %s: %w", rawURL, err)
		}
	}
	if resp.StatusCode >= 400 {
		return payload, resp.StatusCode, fmt.Errorf("http %d for %s %s", resp.StatusCode, method, rawURL)
	}
	return payload, resp.StatusCode, nil
}

func intField(payload map[string]any, key string) int {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return 0
	}
	switch v := raw.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case json.Number:
		i, _ := v.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(v)
		return i
	default:
		return 0
	}
}

func stringField(payload map[string]any, key string) string {
	raw, ok := payload[key]
	if !ok || raw == nil {
		return ""
	}
	if str, ok := raw.(string); ok {
		return str
	}
	return fmt.Sprintf("%v", raw)
}

func stateIsRunnable(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "running", "idle", "runnable":
		return true
	default:
		return false
	}
}

func clearGoHomeLocks(homeDir string) {
	for _, name := range []string{"lock", ".lock", "syncthing.lock", ".syncthing.lock"} {
		_ = os.Remove(filepath.Join(homeDir, name))
	}
}

func waitWithContext(ctx context.Context, cmd *exec.Cmd) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func encodeURLQuery(path string, values url.Values) string {
	if len(values) == 0 {
		return path
	}
	return path + "?" + values.Encode()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
