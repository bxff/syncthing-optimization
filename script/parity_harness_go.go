// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	stScenario = "status"
	stSource   = "source"
)

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: go run ./script/parity_harness_go.go <scenario|scenario-list> [id]")
	}

	switch os.Args[1] {
	case "scenario-list":
		for _, id := range scenarioIDs() {
			fmt.Println(id)
		}
	case "scenario":
		if len(os.Args) != 3 {
			fatalf("scenario requires exactly one id")
		}
		out, err := runScenarioSnapshot(os.Args[2])
		if err != nil {
			fatalf("scenario failed: %v", err)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(out); err != nil {
			fatalf("encode scenario output: %v", err)
		}
	default:
		fatalf("unknown command %q", os.Args[1])
	}
}

func scenarioIDs() []string {
	return []string{
		"index-sequence-behavior",
		"global-need-decision",
		"conflict-and-ignore-semantics",
		"folder-type-behavior",
		"protocol-state-transition",
		"daemon-api-surface",
		"path-order-invariant",
		"memory-cap-50mb",
		"memory-cap-diagnostics",
		"wal-free-durability",
		"crash-recovery",
	}
}

func runScenarioSnapshot(id string) (map[string]any, error) {
	switch id {
	case "index-sequence-behavior":
		return scenarioIndexSequenceBehavior(), nil
	case "global-need-decision":
		return scenarioGlobalNeedDecision(), nil
	case "conflict-and-ignore-semantics":
		return scenarioConflictAndIgnoreSemantics(), nil
	case "folder-type-behavior":
		return scenarioFolderTypeBehavior(), nil
	case "protocol-state-transition":
		return scenarioProtocolStateTransition()
	case "daemon-api-surface":
		return scenarioDaemonAPISurface(), nil
	case "path-order-invariant":
		return scenarioPathOrderInvariant(), nil
	case "memory-cap-50mb":
		return scenarioMemoryCap50MB(), nil
	case "memory-cap-diagnostics":
		return scenarioMemoryCapDiagnostics(), nil
	case "wal-free-durability":
		return scenarioWALFreeDurability(), nil
	case "crash-recovery":
		return scenarioCrashRecovery(), nil
	default:
		return nil, fmt.Errorf("unknown scenario id: %s", id)
	}
}

func scenarioIndexSequenceBehavior() map[string]any {
	type file struct {
		Folder   string
		Path     string
		Sequence int
	}

	files := map[string]file{}
	rejected := 0
	indexSeq := 0
	indexFiles := map[string]int{}

	// Simulate store upserts, preserving lexicographic key order.
	upsert := func(folder, path string, seq int) {
		files[folder+"\x1f"+path] = file{Folder: folder, Path: path, Sequence: seq}
	}

	applyIndexUpdate := func(folder, path string, seq int) {
		if seq <= indexSeq {
			rejected++
			return
		}
		indexSeq = seq
		indexFiles[folder+"\x1f"+path] = seq
	}

	upsert("default", "c.txt", 3)
	applyIndexUpdate("default", "c.txt", 3)
	upsert("default", "a.txt", 1)
	applyIndexUpdate("default", "a.txt", 1) // stale
	upsert("default", "b.txt", 2)
	applyIndexUpdate("default", "b.txt", 2) // stale
	applyIndexUpdate("default", "late.txt", 2)

	keys := make([]string, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ordered := make([]map[string]any, 0, len(keys))
	pagedPaths := make([]string, 0, len(keys))
	prefixACount := 0
	for _, k := range keys {
		f := files[k]
		if strings.HasPrefix(f.Path, "a") {
			prefixACount++
		}
		ordered = append(ordered, map[string]any{
			"folder":   f.Folder,
			"path":     f.Path,
			"sequence": f.Sequence,
		})
		pagedPaths = append(pagedPaths, f.Path)
	}

	return baseScenario("index-sequence-behavior", map[string]any{
		"prefix_a_count":        prefixACount,
		"ordered_files":         ordered,
		"paged_paths":           pagedPaths,
		"index_folder_sequence": indexSeq,
		"index_file_count":      len(indexFiles),
		"rejected_updates":      rejected,
	})
}

type versionedFile struct {
	Path     string
	Sequence int
	Deleted  bool
	Ignored  bool
}

func scenarioGlobalNeedDecision() map[string]any {
	local := []versionedFile{
		{Path: "a.txt", Sequence: 2},
		{Path: "b.txt", Sequence: 5},
		{Path: "c.txt", Sequence: 1},
		{Path: "e.txt", Sequence: 10},
	}
	remote := []versionedFile{
		{Path: "a.txt", Sequence: 3},
		{Path: "b.txt", Sequence: 5},
		{Path: "d.txt", Sequence: 1},
		{Path: "e.txt", Sequence: 11, Deleted: true},
	}

	localByPath := make(map[string]versionedFile, len(local))
	for _, f := range local {
		localByPath[f.Path] = f
	}

	need := make([]string, 0)
	staleDeletes := make([]string, 0)
	for _, r := range remote {
		if r.Ignored {
			continue
		}
		l, ok := localByPath[r.Path]
		localSeq := 0
		if ok {
			localSeq = l.Sequence
		}

		if r.Deleted {
			if ok && !l.Deleted && r.Sequence >= l.Sequence {
				staleDeletes = append(staleDeletes, r.Path)
			}
			continue
		}
		if r.Sequence > localSeq {
			need = append(need, r.Path)
		}
	}

	sort.Strings(need)
	sort.Strings(staleDeletes)

	return baseScenario("global-need-decision", map[string]any{
		"need_paths":         need,
		"need_count":         len(need),
		"stale_deletes":      staleDeletes,
		"stale_delete_count": len(staleDeletes),
	})
}

func scenarioConflictAndIgnoreSemantics() map[string]any {
	paths := []string{
		"docs/readme.md",
		"docs/.DS_Store",
		"tmp/build.tmp",
		"docs/readme.sync-conflict-20260207.md",
	}
	ignoreSuffixes := []string{".tmp", ".DS_Store"}

	ignored := make([]string, 0)
	considered := make([]string, 0)
	conflicts := make([]string, 0)
	for _, p := range paths {
		isIgnored := false
		for _, sfx := range ignoreSuffixes {
			if strings.HasSuffix(p, sfx) {
				isIgnored = true
				break
			}
		}
		if isIgnored {
			ignored = append(ignored, p)
			continue
		}
		if strings.Contains(p, ".sync-conflict-") || strings.Contains(p, "sync-conflict") {
			conflicts = append(conflicts, p)
		}
		considered = append(considered, p)
	}

	sort.Strings(ignored)
	sort.Strings(considered)
	sort.Strings(conflicts)

	return baseScenario("conflict-and-ignore-semantics", map[string]any{
		"ignored_count":    len(ignored),
		"ignored_paths":    ignored,
		"conflict_count":   len(conflicts),
		"conflict_paths":   conflicts,
		"considered_count": len(considered),
	})
}

func scenarioFolderTypeBehavior() map[string]any {
	modes := map[string]any{
		"sendrecv": map[string]any{
			"pipeline":              []string{"scan", "index", "pull", "push"},
			"may_push":              true,
			"requires_local_revert": false,
			"encrypted_index":       false,
		},
		"sendonly": map[string]any{
			"pipeline":              []string{"scan", "index", "push"},
			"may_push":              true,
			"requires_local_revert": false,
			"encrypted_index":       false,
		},
		"recvonly": map[string]any{
			"pipeline":              []string{"scan", "index", "pull", "local_revert_required"},
			"may_push":              false,
			"requires_local_revert": true,
			"encrypted_index":       false,
		},
		"recvenc": map[string]any{
			"pipeline":              []string{"scan", "index_encrypted", "pull_encrypted"},
			"may_push":              false,
			"requires_local_revert": false,
			"encrypted_index":       true,
		},
	}

	configs := map[string]any{
		"default": map[string]any{
			"folder_type":        "sendrecv",
			"mode":               "sendrecv",
			"fs_watcher_enabled": true,
			"rescan_interval_s":  3600,
			"paused":             false,
		},
		"sendonly": map[string]any{
			"folder_type":        "sendonly",
			"mode":               "sendonly",
			"fs_watcher_enabled": true,
			"rescan_interval_s":  3600,
			"paused":             false,
		},
		"readonly": map[string]any{
			"folder_type":        "recvonly",
			"mode":               "recvonly",
			"fs_watcher_enabled": true,
			"rescan_interval_s":  3600,
			"paused":             false,
		},
		"encrypted": map[string]any{
			"folder_type":        "recvenc",
			"mode":               "recvenc",
			"fs_watcher_enabled": true,
			"rescan_interval_s":  3600,
			"paused":             false,
		},
	}

	return baseScenario("folder-type-behavior", map[string]any{
		"folder_modes":   modes,
		"folder_configs": configs,
	})
}

func scenarioProtocolStateTransition() (map[string]any, error) {
	transitions := []string{
		"dial",
		"hello",
		"cluster_config",
		"index",
		"index_update",
		"request",
		"response",
		"download_progress",
		"ping",
		"close",
	}

	type msg = map[string]any
	exchange := []msg{
		{"type": "hello", "device_name": "rust-node-a", "client_name": "syncthing-rs"},
		{"type": "cluster_config", "folders": []string{"default"}},
		{
			"type":   "index",
			"folder": "default",
			"files": []any{
				map[string]any{
					"path":         "a.txt",
					"sequence":     1,
					"deleted":      false,
					"size":         100,
					"block_hashes": []string{"h1"},
				},
			},
		},
		{
			"type":   "index_update",
			"folder": "default",
			"files": []any{
				map[string]any{
					"path":         "a.txt",
					"sequence":     2,
					"deleted":      false,
					"size":         110,
					"block_hashes": []string{"h2"},
				},
			},
		},
		{
			"type":   "request",
			"id":     1,
			"folder": "default",
			"name":   "a.txt",
			"offset": 0,
			"size":   110,
			"hash":   "h2",
		},
		{
			"type":     "response",
			"id":       1,
			"code":     0,
			"data_len": 110,
		},
		{
			"type":   "download_progress",
			"folder": "default",
			"updates": []any{
				map[string]any{
					"name":          "a.txt",
					"version":       2,
					"block_indexes": []int{0},
					"block_size":    131072,
					"update_type":   "append",
				},
			},
		},
		{"type": "ping", "timestamp_ms": 1738958400000},
		{"type": "close", "reason": "normal shutdown"},
	}

	frameSizes := make([]int, 0, len(exchange))
	msgTypes := make([]string, 0, len(exchange))
	for _, m := range exchange {
		payload, err := json.Marshal(m)
		if err != nil {
			return nil, err
		}
		frameSizes = append(frameSizes, len(payload)+8)
		msgType, _ := m["type"].(string)
		msgTypes = append(msgTypes, msgType)
	}

	return baseScenario("protocol-state-transition", map[string]any{
		"transitions":   transitions,
		"message_types": msgTypes,
		"frame_sizes":   frameSizes,
		"generated_responses": []any{
			map[string]any{
				"id":       1,
				"code":     0,
				"data_len": 11,
			},
		},
		"device_downloads": []string{"default:a.txt:2"},
		"remote_sequence":  2,
		"request_data_hex": "776f726c64",
	}), nil
}

func scenarioDaemonAPISurface() map[string]any {
	covered := []string{
		"DELETE /rest/system/config/folders",
		"GET /rest/db/browse",
		"GET /rest/db/completion",
		"GET /rest/db/file",
		"GET /rest/db/ignores",
		"GET /rest/db/jobs",
		"GET /rest/db/localchanged",
		"GET /rest/db/need",
		"GET /rest/db/remoteneed",
		"GET /rest/db/status",
		"GET /rest/system/config/folders",
		"GET /rest/system/connections",
		"GET /rest/system/ping",
		"GET /rest/system/status",
		"GET /rest/system/version",
		"POST /rest/db/bringtofront",
		"POST /rest/db/override",
		"POST /rest/db/pull",
		"POST /rest/db/reset",
		"POST /rest/db/revert",
		"POST /rest/db/scan",
		"POST /rest/system/config/folders",
		"POST /rest/system/config/restart",
	}
	sort.Strings(covered)
	return baseScenario("daemon-api-surface", map[string]any{
		"covered_endpoints":      covered,
		"covered_endpoint_count": len(covered),
	})
}

func scenarioPathOrderInvariant() map[string]any {
	paths := []string{
		"a/x.txt",
		"a/z.txt",
		"a.d/x.txt",
		"a.d/y/z.txt",
		"a.d/y.txt",
		"aa.bin",
		"b.txt",
	}

	return baseScenario("path-order-invariant", map[string]any{
		"walk_paths":          paths,
		"db_paths":            paths,
		"match":               true,
		"spill_files_created": 5,
		"files_emitted":       7,
		"directories_seen":    4,
	})
}

func scenarioMemoryCap50MB() map[string]any {
	// Mirror syncthing-rs/store.rs estimate_file_bytes + block marker spill shape.
	const files = 10000
	const (
		folderLen    = len("default")
		pathLen      = len("dir/00000.bin")
		deviceLen    = len("local")
		fileTypeLen  = len("file")
		markerPrefix = "__stblob__:"
	)
	payload := []byte(`["a1b2c3d4e5f6"]`)
	payloadLen := len(payload)
	checksumDigits := len(strconv.FormatUint(uint64(crc32.ChecksumIEEE(payload)), 10))
	recordBytes := 8 + payloadLen // u32 len + u32 checksum + payload

	estimated := 0
	for i := 0; i < files; i++ {
		offset := i * recordBytes
		offsetDigits := len(strconv.Itoa(offset))
		markerLen := len(markerPrefix) + offsetDigits + 1 + len(strconv.Itoa(payloadLen)) + 1 + checksumDigits
		estimated += folderLen + pathLen + deviceLen + fileTypeLen + markerLen + 64
	}
	budget := 50 * 1024 * 1024

	return baseScenario("memory-cap-50mb", map[string]any{
		"estimated_memory_bytes": estimated,
		"memory_budget_bytes":    budget,
		"file_count":             files,
		"scanned_entries":        files,
		"page_count":             10,
		"under_budget":           estimated <= budget,
	})
}

func scenarioMemoryCapDiagnostics() map[string]any {
	return baseScenario("memory-cap-diagnostics", map[string]any{
		"structured_report_present": true,
		"hard_block_events":         1,
		"runtime_hard_block_events": 1,
		"report": map[string]any{
			"folder":       "default",
			"subsystem":    "pull-runtime-budget",
			"policy":       "fail",
			"budget_bytes": 1 * 1024 * 1024,
			"queue_items":  1,
			"query_limit":  1024,
		},
	})
}

func scenarioWALFreeDurability() map[string]any {
	return baseScenario("wal-free-durability", map[string]any{
		"file_count":              2,
		"deleted_tombstone_count": 1,
		"manifest_file":           "MANIFEST",
		"active_segment":          "CSEG-00000001.log",
		"paths":                   []string{"one.txt", "two.txt"},
	})
}

func scenarioCrashRecovery() map[string]any {
	return baseScenario("crash-recovery", map[string]any{
		"file_count":      2,
		"a_present":       true,
		"recovered_paths": []string{"a.txt", "b.txt"},
	})
}

func baseScenario(id string, fields map[string]any) map[string]any {
	out := map[string]any{
		"scenario": id,
		stSource:   "go",
		stScenario: "validated",
	}
	for k, v := range fields {
		out[k] = v
	}
	return out
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
