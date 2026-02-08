package main

import (
	"encoding/json"
	"testing"
)

func FuzzCanonicalFolderTypeIdempotent(f *testing.F) {
	f.Add("sendreceive")
	f.Add("READONLY")
	f.Add(" recvenc ")
	f.Add("unknown")
	f.Fuzz(func(t *testing.T, input string) {
		once := canonicalFolderType(input)
		twice := canonicalFolderType(once)
		if once != twice {
			t.Fatalf("canonical folder type not idempotent: %q -> %q -> %q", input, once, twice)
		}
	})
}

func FuzzDecodeFolderTypeMapNoPanic(f *testing.F) {
	f.Add(`[{"id":"x","type":"sendreceive"}]`)
	f.Add(`{"folders":[{"id":"x","type":"sendonly"}]}`)
	f.Add(`{"folders":{"folders":[{"id":"x","type":"recvonly"}]}}`)
	f.Fuzz(func(t *testing.T, raw string) {
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return
		}
		_, _ = decodeFolderTypeMap(payload)
	})
}

func FuzzAppendBrowsePathsNoPanic(f *testing.F) {
	f.Add(`[{"name":"a","children":[{"name":"b"}]}]`, "")
	f.Add(`[{"path":"x/y"}]`, "root")
	f.Add(`{"name":"single"}`, "p")
	f.Fuzz(func(t *testing.T, raw, parent string) {
		var payload any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return
		}
		var out []string
		_ = appendBrowsePaths(&out, parent, payload)
	})
}

func FuzzIntFieldNoPanic(f *testing.F) {
	f.Add(`{"k":42}`, "k")
	f.Add(`{"k":"999"}`, "k")
	f.Add(`{"k":null}`, "k")
	f.Add(`{"x":1}`, "k")
	f.Fuzz(func(t *testing.T, raw, key string) {
		var payload map[string]any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return
		}
		_ = intField(payload, key)
	})
}

func TestValidateIndexedFilePayloadAcceptsRustShape(t *testing.T) {
	payload := map[string]any{
		"exists": true,
		"entry": map[string]any{
			"path":        "a.txt",
			"size":        float64(7),
			"blockHashes": []any{"abc"},
			"deleted":     false,
		},
	}
	if err := validateIndexedFilePayload(payload, "a.txt"); err != nil {
		t.Fatalf("expected rust shape to validate: %v", err)
	}
}

func TestValidateIndexedFilePayloadAcceptsGoShape(t *testing.T) {
	payload := map[string]any{
		"local": map[string]any{
			"name":       "a.txt",
			"size":       float64(7),
			"blocksHash": "abc",
			"numBlocks":  float64(1),
			"deleted":    false,
		},
		"global": map[string]any{
			"name":       "a.txt",
			"size":       float64(7),
			"blocksHash": "abc",
			"numBlocks":  float64(1),
			"deleted":    false,
		},
	}
	if err := validateIndexedFilePayload(payload, "a.txt"); err != nil {
		t.Fatalf("expected go shape to validate: %v", err)
	}
}

func TestValidateIndexedFilePayloadRejectsMissingEntry(t *testing.T) {
	payload := map[string]any{
		"exists": true,
		"entry":  map[string]any{},
	}
	if err := validateIndexedFilePayload(payload, "a.txt"); err == nil {
		t.Fatal("expected missing entry to be rejected")
	}
}

func TestExtractPendingIDsHandlesObjectShapes(t *testing.T) {
	ids, invalid := extractPendingIDs(
		map[string]any{
			"PEER-A": map[string]any{"name": "peer-a"},
			"devices": map[string]any{
				"PEER-B": map[string]any{"name": "peer-b"},
			},
		},
		[]string{"deviceID", "id", "name"},
	)
	if len(invalid) != 0 {
		t.Fatalf("expected no invalid pending IDs, got %v", invalid)
	}
	if len(ids) != 2 || ids[0] != "PEER-A" || ids[1] != "PEER-B" {
		t.Fatalf("unexpected pending IDs: %v", ids)
	}
}

func TestExtractPendingIDsReportsInvalidEmptyIDs(t *testing.T) {
	ids, invalid := extractPendingIDs(
		[]any{
			map[string]any{"name": "peer-a"},
			map[string]any{},
		},
		[]string{"deviceID", "id"},
	)
	if len(ids) != 0 {
		t.Fatalf("expected no valid IDs, got %v", ids)
	}
	if len(invalid) == 0 {
		t.Fatal("expected invalid pending IDs to be reported")
	}
}
