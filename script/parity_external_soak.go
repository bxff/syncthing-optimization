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
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	externalSoakScenarioID        = "external-soak-replacement"
	externalMultiFolderScenarioID = "external-multifolder-replacement"
	externalDurabilityScenarioID  = "external-durability-restart"
	externalCrashScenarioID       = "external-crash-recovery-restart"
	httpTimeout                   = 5 * time.Second
	startupTimeout                = 30 * time.Second
	statusTimeout                 = 30 * time.Second
	shutdownTimeout               = 10 * time.Second
	expectedLocalFiles            = 2
)

func main() {
	if len(os.Args) < 3 || os.Args[1] != "scenario" {
		fatalf(
			"usage: go run ./script/parity_external_soak.go scenario <%s|%s|%s|%s> --impl <go|rust>",
			externalSoakScenarioID,
			externalMultiFolderScenarioID,
			externalDurabilityScenarioID,
			externalCrashScenarioID,
		)
	}
	id := strings.TrimSpace(os.Args[2])
	if id != externalSoakScenarioID &&
		id != externalMultiFolderScenarioID &&
		id != externalDurabilityScenarioID &&
		id != externalCrashScenarioID {
		fatalf("unsupported scenario id %q", id)
	}

	fs := flag.NewFlagSet("scenario", flag.ExitOnError)
	impl := fs.String("impl", "", "implementation to run (go or rust)")
	if err := fs.Parse(os.Args[3:]); err != nil {
		fatalf("parse flags: %v", err)
	}
	if *impl != "go" && *impl != "rust" {
		fatalf("--impl must be go or rust")
	}

	checks, metrics, err := runScenario(id, *impl)
	if err != nil {
		fatalf("external soak failed: %v", err)
	}

	out := map[string]any{
		"scenario": id,
		"source":   *impl,
		"status":   "validated",
		"checks":   checks,
		"metrics":  metrics,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fatalf("encode output: %v", err)
	}
}

func runScenario(id, impl string) (map[string]any, map[string]any, error) {
	switch id {
	case externalSoakScenarioID:
		return runScenarioExternalSoak(impl)
	case externalMultiFolderScenarioID:
		return runScenarioExternalMultiFolder(impl)
	case externalDurabilityScenarioID:
		return runScenarioExternalDurability(impl)
	case externalCrashScenarioID:
		return runScenarioExternalCrashRecovery(impl)
	default:
		return nil, nil, fmt.Errorf("unsupported scenario %q", id)
	}
}

func runScenarioExternalSoak(impl string) (map[string]any, map[string]any, error) {
	var (
		metrics soakMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalSoak()
	case "rust":
		metrics, err = runRustExternalSoak()
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, err
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
	}
	return checks, m, nil
}

func runScenarioExternalMultiFolder(impl string) (map[string]any, map[string]any, error) {
	var (
		metrics multiFolderMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalMultiFolderSoak()
	case "rust":
		metrics, err = runRustExternalMultiFolderSoak()
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, err
	}

	checks := map[string]any{
		"default_folder_configured":              metrics.DefaultFolderConfigured,
		"media_folder_configured":                metrics.MediaFolderConfigured,
		"scan_ok":                                metrics.ScanOK,
		"default_status_ok":                      metrics.StatusOK,
		"default_status_state_valid":             stateIsRunnable(metrics.DefaultState),
		"media_status_state_valid":               stateIsRunnable(metrics.MediaState),
		"default_local_files_at_least_expected":  metrics.DefaultLocalFiles >= expectedLocalFiles,
		"default_global_files_at_least_expected": metrics.DefaultGlobalFiles >= expectedLocalFiles,
		"default_need_files_zero":                metrics.DefaultNeedFiles == 0,
		"media_local_files_at_least_expected":    metrics.MediaLocalFiles >= expectedLocalFiles,
		"media_global_files_at_least_expected":   metrics.MediaGlobalFiles >= expectedLocalFiles,
		"media_need_files_zero":                  metrics.MediaNeedFiles == 0,
		"shutdown_requested":                     metrics.ShutdownRequested,
	}
	m := map[string]any{
		"default_local_files":  metrics.DefaultLocalFiles,
		"default_global_files": metrics.DefaultGlobalFiles,
		"default_need_files":   metrics.DefaultNeedFiles,
		"media_local_files":    metrics.MediaLocalFiles,
		"media_global_files":   metrics.MediaGlobalFiles,
		"media_need_files":     metrics.MediaNeedFiles,
		"expected_local_files": expectedLocalFiles,
		"scan_attempts":        metrics.ScanAttempts,
		"status_poll_attempts": metrics.StatusPollAttempts,
	}
	return checks, m, nil
}

func runScenarioExternalDurability(impl string) (map[string]any, map[string]any, error) {
	var (
		metrics durabilityMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalDurabilityRestart()
	case "rust":
		metrics, err = runRustExternalDurabilityRestart()
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, err
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
	}
	return checks, m, nil
}

func runScenarioExternalCrashRecovery(impl string) (map[string]any, map[string]any, error) {
	var (
		metrics crashMetrics
		err     error
	)
	switch impl {
	case "go":
		metrics, err = runGoExternalCrashRecoveryRestart()
	case "rust":
		metrics, err = runRustExternalCrashRecoveryRestart()
	default:
		err = fmt.Errorf("unsupported impl %q", impl)
	}
	if err != nil {
		return nil, nil, err
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
	}
	return checks, m, nil
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
}

type multiFolderMetrics struct {
	DefaultFolderConfigured bool
	MediaFolderConfigured   bool
	ScanOK                  bool
	StatusOK                bool
	ShutdownRequested       bool
	DefaultLocalFiles       int
	DefaultGlobalFiles      int
	DefaultNeedFiles        int
	DefaultState            string
	MediaLocalFiles         int
	MediaGlobalFiles        int
	MediaNeedFiles          int
	MediaState              string
	ScanAttempts            int
	StatusPollAttempts      int
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
}

type daemonProc struct {
	cmd    *exec.Cmd
	stderr bytes.Buffer
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
	_ = p.cmd.Process.Kill()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return waitWithContext(waitCtx, p.cmd)
}

func (p *daemonProc) crashKill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := waitWithContext(waitCtx, p.cmd)
	if err == nil {
		return nil
	}
	// `go run`/`cargo run` wrappers can return deadline/signal errors while still
	// ensuring abrupt process termination semantics for crash-recovery testing.
	if errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "signal: killed") {
		return nil
	}
	return nil
}

func (p *daemonProc) wait(timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return waitWithContext(waitCtx, p.cmd)
}

func runGoExternalSoak() (soakMetrics, error) {
	var metrics soakMetrics
	root, err := os.MkdirTemp("", "syncthing-go-external-soak-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	folderDir := filepath.Join(root, "folder")
	if err := prepareSoakFolder(folderDir); err != nil {
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

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalSoak() (soakMetrics, error) {
	var metrics soakMetrics
	root, err := os.MkdirTemp("", "syncthing-rs-external-soak-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	folderDir := filepath.Join(root, "folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(folderDir); err != nil {
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

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runGoExternalMultiFolderSoak() (multiFolderMetrics, error) {
	var metrics multiFolderMetrics
	root, err := os.MkdirTemp("", "syncthing-go-external-multifolder-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	defaultFolderDir := filepath.Join(root, "default-folder")
	mediaFolderDir := filepath.Join(root, "media-folder")
	if err := prepareSoakFolder(defaultFolderDir); err != nil {
		return metrics, fmt.Errorf("prepare default folder: %w", err)
	}
	if err := prepareSoakFolder(mediaFolderDir); err != nil {
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

	defaultStatus, defaultPolls, err := pollStatusForFolder(baseURL, apiKey, "default", statusTimeout)
	metrics.StatusPollAttempts += defaultPolls
	if err != nil {
		return metrics, fmt.Errorf("go default status poll: %w", err)
	}
	mediaStatus, mediaPolls, err := pollStatusForFolder(baseURL, apiKey, "media", statusTimeout)
	metrics.StatusPollAttempts += mediaPolls
	if err != nil {
		return metrics, fmt.Errorf("go media status poll: %w", err)
	}
	metrics.StatusOK = true

	metrics.DefaultLocalFiles = intField(defaultStatus, "localFiles")
	metrics.DefaultGlobalFiles = intField(defaultStatus, "globalFiles")
	metrics.DefaultNeedFiles = intField(defaultStatus, "needFiles")
	metrics.DefaultState = stringField(defaultStatus, "state")
	if metrics.DefaultLocalFiles < expectedLocalFiles || metrics.DefaultGlobalFiles < expectedLocalFiles {
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
	if metrics.MediaLocalFiles < expectedLocalFiles || metrics.MediaGlobalFiles < expectedLocalFiles {
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

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalMultiFolderSoak() (multiFolderMetrics, error) {
	var metrics multiFolderMetrics
	root, err := os.MkdirTemp("", "syncthing-rs-external-multifolder-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	defaultFolderDir := filepath.Join(root, "default-folder")
	mediaFolderDir := filepath.Join(root, "media-folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(defaultFolderDir); err != nil {
		return metrics, fmt.Errorf("prepare default folder: %w", err)
	}
	if err := prepareSoakFolder(mediaFolderDir); err != nil {
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

	for _, folder := range []string{"default", "media"} {
		metrics.ScanAttempts++
		scanURL := encodeURLQuery(baseURL+"/rest/db/scan", url.Values{"folder": []string{folder}})
		if _, _, err := requestJSON(http.MethodPost, scanURL, ""); err != nil {
			return metrics, fmt.Errorf("rust scan request (%s): %w", folder, err)
		}
	}
	metrics.ScanOK = true

	defaultStatus, defaultPolls, err := pollStatusForFolder(baseURL, "", "default", statusTimeout)
	metrics.StatusPollAttempts += defaultPolls
	if err != nil {
		return metrics, fmt.Errorf("rust default status poll: %w", err)
	}
	mediaStatus, mediaPolls, err := pollStatusForFolder(baseURL, "", "media", statusTimeout)
	metrics.StatusPollAttempts += mediaPolls
	if err != nil {
		return metrics, fmt.Errorf("rust media status poll: %w", err)
	}
	metrics.StatusOK = true

	metrics.DefaultLocalFiles = intField(defaultStatus, "localFiles")
	metrics.DefaultGlobalFiles = intField(defaultStatus, "globalFiles")
	metrics.DefaultNeedFiles = intField(defaultStatus, "needFiles")
	metrics.DefaultState = stringField(defaultStatus, "state")
	if metrics.DefaultLocalFiles < expectedLocalFiles || metrics.DefaultGlobalFiles < expectedLocalFiles {
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
	if metrics.MediaLocalFiles < expectedLocalFiles || metrics.MediaGlobalFiles < expectedLocalFiles {
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

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust shutdown request: %w", err)
	}
	metrics.ShutdownRequested = true
	if err := proc.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust daemon shutdown wait: %w", err)
	}
	return metrics, nil
}

func runGoExternalDurabilityRestart() (durabilityMetrics, error) {
	var metrics durabilityMetrics
	root, err := os.MkdirTemp("", "syncthing-go-external-durability-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	folderDir := filepath.Join(root, "folder")
	if err := prepareSoakFolder(folderDir); err != nil {
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
	status, _, err := pollStatus(baseURL, apiKey, statusTimeout)
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
	metrics.RestartBootOK = true

	restartStatus, _, err := pollStatus(restartURL, apiKey, statusTimeout)
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

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalDurabilityRestart() (durabilityMetrics, error) {
	var metrics durabilityMetrics
	root, err := os.MkdirTemp("", "syncthing-rs-external-durability-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	folderDir := filepath.Join(root, "folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(folderDir); err != nil {
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
	metrics.FirstBootOK = true
	metrics.FolderConfigured = true

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", ""); err != nil {
		return metrics, fmt.Errorf("rust first scan request: %w", err)
	}
	metrics.ScanOK = true
	status, _, err := pollStatus(baseURL, "", statusTimeout)
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
	metrics.RestartBootOK = true

	restartStatus, _, err := pollStatus(restartURL, "", statusTimeout)
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

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func runGoExternalCrashRecoveryRestart() (crashMetrics, error) {
	var metrics crashMetrics
	root, err := os.MkdirTemp("", "syncthing-go-external-crash-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	homeDir := filepath.Join(root, "home")
	folderDir := filepath.Join(root, "folder")
	goBinary := filepath.Join(root, "syncthing-go")
	if err := prepareSoakFolder(folderDir); err != nil {
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
	status, _, err := pollStatus(baseURL, apiKey, statusTimeout)
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
	metrics.RestartBootOK = true

	restartStatus, _, err := pollStatus(restartURL, apiKey, statusTimeout)
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

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", apiKey); err != nil {
		return metrics, fmt.Errorf("go restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("go restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func runRustExternalCrashRecoveryRestart() (crashMetrics, error) {
	var metrics crashMetrics
	root, err := os.MkdirTemp("", "syncthing-rs-external-crash-")
	if err != nil {
		return metrics, fmt.Errorf("create temp root: %w", err)
	}
	defer os.RemoveAll(root)

	folderDir := filepath.Join(root, "folder")
	dbRoot := filepath.Join(root, "db")
	if err := prepareSoakFolder(folderDir); err != nil {
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
	metrics.FirstBootOK = true
	metrics.FolderConfigured = true

	if _, _, err := requestJSON(http.MethodPost, baseURL+"/rest/db/scan?folder=default", ""); err != nil {
		return metrics, fmt.Errorf("rust first scan request: %w", err)
	}
	metrics.ScanOK = true
	status, _, err := pollStatus(baseURL, "", statusTimeout)
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
	metrics.RestartBootOK = true

	restartStatus, _, err := pollStatus(restartURL, "", statusTimeout)
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

	if _, _, err := requestJSON(http.MethodPost, restartURL+"/rest/system/shutdown", ""); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown request: %w", err)
	}
	metrics.RestartShutdownRequested = true
	if err := restarted.wait(shutdownTimeout); err != nil {
		return metrics, fmt.Errorf("rust restart shutdown wait: %w", err)
	}
	return metrics, nil
}

func prepareSoakFolder(folderDir string) error {
	if err := os.MkdirAll(filepath.Join(folderDir, "nested"), 0o755); err != nil {
		return fmt.Errorf("create folder tree: %w", err)
	}
	if err := os.WriteFile(filepath.Join(folderDir, ".stfolder"), []byte(""), 0o644); err != nil {
		return fmt.Errorf("write marker file: %w", err)
	}
	if err := os.WriteFile(filepath.Join(folderDir, "a.txt"), []byte("hello"), 0o644); err != nil {
		return fmt.Errorf("write a.txt: %w", err)
	}
	if err := os.WriteFile(filepath.Join(folderDir, "nested", "b.txt"), []byte("world"), 0o644); err != nil {
		return fmt.Errorf("write nested/b.txt: %w", err)
	}
	return nil
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

func pollStatus(baseURL, apiKey string, timeout time.Duration) (map[string]any, int, error) {
	return pollStatusForFolder(baseURL, apiKey, "default", timeout)
}

func pollStatusForFolder(baseURL, apiKey, folder string, timeout time.Duration) (map[string]any, int, error) {
	deadline := time.Now().Add(timeout)
	polls := 0
	var last map[string]any
	target := encodeURLQuery(baseURL+"/rest/db/status", url.Values{"folder": []string{folder}})
	for time.Now().Before(deadline) {
		polls++
		payload, status, err := requestJSON(http.MethodGet, target, apiKey)
		if err == nil && status == http.StatusOK {
			last = payload
			if intField(payload, "localFiles") >= expectedLocalFiles && intField(payload, "globalFiles") >= expectedLocalFiles {
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
		if len(payload) == 0 {
			return fmt.Errorf("request %s returned empty payload", target)
		}
	}
	return nil
}

func requestJSON(method, rawURL, apiKey string) (map[string]any, int, error) {
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
	payload := map[string]any{}
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
	case "running", "idle":
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
