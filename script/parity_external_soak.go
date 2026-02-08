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
	externalSoakScenarioID = "external-soak-replacement"
	httpTimeout            = 5 * time.Second
	startupTimeout         = 30 * time.Second
	statusTimeout          = 30 * time.Second
	shutdownTimeout        = 10 * time.Second
	expectedLocalFiles     = 2
)

func main() {
	if len(os.Args) < 3 || os.Args[1] != "scenario" {
		fatalf("usage: go run ./script/parity_external_soak.go scenario %s --impl <go|rust>", externalSoakScenarioID)
	}
	id := strings.TrimSpace(os.Args[2])
	if id != externalSoakScenarioID {
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

	var (
		metrics soakMetrics
		err     error
	)
	switch *impl {
	case "go":
		metrics, err = runGoExternalSoak()
	case "rust":
		metrics, err = runRustExternalSoak()
	default:
		err = fmt.Errorf("unsupported impl %q", *impl)
	}
	if err != nil {
		fatalf("external soak failed: %v", err)
	}

	out := map[string]any{
		"scenario": externalSoakScenarioID,
		"source":   *impl,
		"status":   "validated",
		"checks": map[string]any{
			"daemon_boot_ok":                 true,
			"folder_configured":              metrics.FolderConfigured,
			"scan_ok":                        metrics.ScanOK,
			"status_ok":                      metrics.StatusOK,
			"status_state_valid":             stateIsRunnable(metrics.State),
			"local_files_at_least_expected":  metrics.LocalFiles >= expectedLocalFiles,
			"global_files_at_least_expected": metrics.GlobalFiles >= expectedLocalFiles,
			"need_files_zero":                metrics.NeedFiles == 0,
			"shutdown_requested":             metrics.ShutdownRequested,
		},
		"metrics": map[string]any{
			"local_files":          metrics.LocalFiles,
			"global_files":         metrics.GlobalFiles,
			"need_files":           metrics.NeedFiles,
			"expected_local_files": expectedLocalFiles,
			"scan_attempts":        metrics.ScanAttempts,
			"status_poll_attempts": metrics.StatusPollAttempts,
		},
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fatalf("encode output: %v", err)
	}
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

type daemonProc struct {
	cmd    *exec.Cmd
	stderr bytes.Buffer
}

func (p *daemonProc) start() error {
	p.cmd.Stdout = io.Discard
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
	deadline := time.Now().Add(timeout)
	polls := 0
	var last map[string]any
	for time.Now().Before(deadline) {
		polls++
		payload, status, err := requestJSON(http.MethodGet, baseURL+"/rest/db/status?folder=default", apiKey)
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
	return last, polls, fmt.Errorf("timed out waiting for status convergence")
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
