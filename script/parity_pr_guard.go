// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var surfacePatterns = []string{
	"lib/config/folderconfiguration.go",
	"lib/model/model.go",
	"lib/model/folder.go",
	"lib/model/folder_sendrecv.go",
	"lib/model/folder_recvonly.go",
	"lib/model/folder_recvenc.go",
	"lib/protocol/bep_*.go",
	"internal/gen/bep/bep.pb.go",
	"internal/db/interface.go",
}

var rustRoots = []string{"rust/", "crates/", "surf/"}

func main() {
	fs := flag.NewFlagSet("parity-pr-guard", flag.ExitOnError)
	base := fs.String("base", "origin/main", "base ref for diff")
	head := fs.String("head", "HEAD", "head ref for diff")
	if err := fs.Parse(os.Args[1:]); err != nil {
		fatalf("parse flags: %v", err)
	}

	changed, err := changedFiles(*base, *head)
	if err != nil {
		fatalf("compute changed files: %v", err)
	}
	if len(changed) == 0 {
		fmt.Println("parity-pr-guard: no changed files")
		return
	}

	changedSet := make(map[string]struct{}, len(changed))
	for _, file := range changed {
		changedSet[file] = struct{}{}
	}

	criticalFailures := []string{}

	rustChanged := hasPrefix(changed, rustRoots)
	if rustChanged {
		required := []string{
			"parity/mapping-rust.json",
			"parity/diff-reports/latest.json",
			"parity/diff-reports/test-status.json",
			"parity/diff-reports/interop.json",
			"parity/diff-reports/durability.json",
		}
		for _, req := range required {
			if _, ok := changedSet[req]; !ok {
				criticalFailures = append(criticalFailures, fmt.Sprintf("rust changes require updating %s", req))
			}
		}
	}

	surfaceFiles, err := resolveSurfaceFiles(surfacePatterns)
	if err != nil {
		fatalf("resolve surface files: %v", err)
	}
	surfaceChanged := false
	for _, sf := range surfaceFiles {
		if _, ok := changedSet[sf]; ok {
			surfaceChanged = true
			break
		}
	}
	if surfaceChanged {
		if _, ok := changedSet["parity/feature-manifest.json"]; !ok {
			criticalFailures = append(criticalFailures, "go surface changes require updating parity/feature-manifest.json")
		}
	}

	if _, ok := changedSet["parity/harness/scenarios.json"]; ok {
		if _, ok2 := changedSet["parity/diff-reports/latest.json"]; !ok2 {
			criticalFailures = append(criticalFailures, "scenario changes require updating parity/diff-reports/latest.json")
		}
	}

	if len(criticalFailures) > 0 {
		fmt.Fprintln(os.Stderr, "parity-pr-guard: FAIL")
		for _, f := range criticalFailures {
			fmt.Fprintf(os.Stderr, " - %s\n", f)
		}
		os.Exit(1)
	}

	fmt.Println("parity-pr-guard: PASS")
}

func changedFiles(base, head string) ([]string, error) {
	cmd := exec.Command("git", "diff", "--name-only", base+"..."+head)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("git diff failed: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, err
	}

	s := bufio.NewScanner(strings.NewReader(string(out)))
	files := []string{}
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" {
			continue
		}
		files = append(files, filepath.ToSlash(line))
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return files, nil
}

func hasPrefix(files []string, prefixes []string) bool {
	for _, file := range files {
		for _, p := range prefixes {
			if strings.HasPrefix(file, p) {
				return true
			}
		}
	}
	return false
}

func resolveSurfaceFiles(patterns []string) ([]string, error) {
	set := make(map[string]struct{})
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			set[filepath.ToSlash(match)] = struct{}{}
		}
	}
	files := make([]string, 0, len(set))
	for file := range set {
		files = append(files, file)
	}
	return files, nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
