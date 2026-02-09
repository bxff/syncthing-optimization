package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestClassifyTier(t *testing.T) {
	tests := []struct {
		name string
		item mappingItem
		want string
	}{
		{
			name: "data-kind-is-t2",
			item: mappingItem{
				Kind:          "struct_field",
				Symbol:        "FileInfo.Sequence",
				RustSymbol:    "Sequence",
				RustComponent: "syncthing-rs/db",
			},
			want: "T2",
		},
		{
			name: "core-logic-method-is-t1",
			item: mappingItem{
				Kind:          "method",
				Symbol:        "model.ScanFolderSubdirs",
				RustSymbol:    "ScanFolderSubdirs",
				RustComponent: "syncthing-rs/model-core",
			},
			want: "T1",
		},
		{
			name: "low-risk-method-is-t3",
			item: mappingItem{
				Kind:          "method",
				Symbol:        "config.String",
				RustSymbol:    "String",
				RustComponent: "syncthing-rs/config",
			},
			want: "T3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyTier(tt.item); got != tt.want {
				t.Fatalf("classifyTier()=%s want=%s", got, tt.want)
			}
		})
	}
}

func TestParseReviewFindings(t *testing.T) {
	item := reviewPlanItem{ID: "feat-1", Tier: "T1"}

	noFindings, err := parseReviewFindings("NO_FINDINGS\n", item)
	if err != nil {
		t.Fatalf("parse no findings: %v", err)
	}
	if len(noFindings) != 0 {
		t.Fatalf("expected zero findings, got %d", len(noFindings))
	}

	raw := "FINDING|P1|Missing branch|lib/model/model.go:123|Rust path skips recvonly-specific branch\n"
	findings, err := parseReviewFindings(raw, item)
	if err != nil {
		t.Fatalf("parse findings: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Priority != "P1" {
		t.Fatalf("unexpected priority: %s", findings[0].Priority)
	}
}

func TestParseReviewFindingsRejectsInvalidPriority(t *testing.T) {
	item := reviewPlanItem{ID: "feat-1", Tier: "T1"}
	_, err := parseReviewFindings("FINDING|PX|Title|a.go:1|bad priority\n", item)
	if err == nil {
		t.Fatal("expected error for invalid priority")
	}
}

func TestParseReviewFindingsIgnoresPreambleLines(t *testing.T) {
	item := reviewPlanItem{ID: "feat-1", Tier: "T1"}
	raw := "I need more context first.\nFINDING|P2|Title|a.go:1|detail\n"
	findings, err := parseReviewFindings(raw, item)
	if err != nil {
		t.Fatalf("parse with preamble: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Title != "Title" {
		t.Fatalf("unexpected finding title: %s", findings[0].Title)
	}
}

func TestParseReviewFindingsSupportsJSONEnvelope(t *testing.T) {
	item := reviewPlanItem{ID: "feat-json", Tier: "T1", RustPath: "syncthing-rs/src/db.rs"}
	raw := `{
  "findings": [
    {
      "priority": "P1",
      "title": "Missing error propagation",
      "path": "syncthing-rs/src/db.rs",
      "line": 42,
      "message": "Rust path swallows a store read error."
    }
  ]
}`
	findings, err := parseReviewFindings(raw, item)
	if err != nil {
		t.Fatalf("parse json findings: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(findings))
	}
	if findings[0].Priority != "P1" {
		t.Fatalf("unexpected priority: %s", findings[0].Priority)
	}
	if findings[0].Location != "syncthing-rs/src/db.rs:42" {
		t.Fatalf("unexpected location: %s", findings[0].Location)
	}
}

func TestIsInfraOpencodeError(t *testing.T) {
	if !isInfraOpencodeError(errors.New("opencode returned error event: Error: All Antigravity endpoints failed")) {
		t.Fatal("expected endpoints failure to be infra error")
	}
	if !isInfraOpencodeError(errors.New("Quota protection: All 1 account(s) are over 90% usage for gemini. Quota resets in 2h.")) {
		t.Fatal("expected quota protection to be infra error")
	}
	if !isInfraOpencodeError(errors.New("opencode timed out after 2m0s")) {
		t.Fatal("expected timeout to be infra error")
	}
	if !isInfraOpencodeError(errors.New("opencode run failed: exit status 1 (stderr=ERROR service=models.dev error=Unable to connect. Is the computer able to access the url? Failed to fetch models.dev)")) {
		t.Fatal("expected models.dev connectivity failure to be infra error")
	}
	if isInfraOpencodeError(errors.New("agent response did not contain parseable FINDING lines")) {
		t.Fatal("did not expect parse error to be infra error")
	}
}

func TestRustComponentPath(t *testing.T) {
	path, err := rustComponentPath("syncthing-rs/db")
	if err != nil {
		t.Fatalf("rustComponentPath: %v", err)
	}
	if path != "syncthing-rs/src/db.rs" {
		t.Fatalf("unexpected path %q", path)
	}
}

func TestExtractOpencodeText(t *testing.T) {
	raw := []byte(`{"type":"step_start","part":{"text":""}}
{"type":"text","part":{"text":"NO_FINDINGS"}}
{"type":"step_finish","part":{"text":""}}
`)
	got := extractOpencodeText(raw)
	if got != "NO_FINDINGS" {
		t.Fatalf("extractOpencodeText=%q", got)
	}
}

func TestExtractOpencodeError(t *testing.T) {
	raw := []byte(`{"type":"step_start","part":{"text":""}}
{"type":"error","error":{"name":"UnknownError","data":{"message":"rate limit"}}}
{"type":"step_finish","part":{"text":""}}
`)
	got := extractOpencodeError(raw)
	if got != "rate limit" {
		t.Fatalf("extractOpencodeError=%q", got)
	}
}

func TestExtractOpencodeErrorEmptyWhenNoErrorEvent(t *testing.T) {
	raw := []byte(`{"type":"text","part":{"text":"NO_FINDINGS"}}`)
	got := extractOpencodeError(raw)
	if got != "" {
		t.Fatalf("extractOpencodeError=%q want empty", got)
	}
}

func TestFindFunctionBlockPrefersRustImplBodyOverTraitDecl(t *testing.T) {
	lines := []string{
		"pub(crate) trait Db {",
		"    fn all_global_files(&self, folder: &str) -> Result<Vec<FileMetadata>, String>;",
		"}",
		"",
		"impl Db for WalFreeDb {",
		"    fn all_global_files(&self, folder: &str) -> Result<Vec<FileMetadata>, String> {",
		"        self.ensure_open()?;",
		"        Ok(Vec::new())",
		"    }",
		"}",
	}
	got, ok := findFunctionBlock(lines, "all_global_files", true)
	if !ok {
		t.Fatal("findFunctionBlock did not find method")
	}
	want := lineRange{Start: 6, End: 9}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("findFunctionBlock=%+v want=%+v", got, want)
	}
}

func TestNormalizeArtifactMode(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "", want: "jsonl"},
		{in: "jsonl", want: "jsonl"},
		{in: "bundle", want: "jsonl"},
		{in: "files", want: "files"},
		{in: "per-item", want: "files"},
		{in: "none", want: "none"},
		{in: "OFF", want: "none"},
		{in: "unknown", want: ""},
	}
	for _, tt := range tests {
		if got := normalizeArtifactMode(tt.in); got != tt.want {
			t.Fatalf("normalizeArtifactMode(%q)=%q want=%q", tt.in, got, tt.want)
		}
	}
}

func TestCollectLocalFindingsDetectsMissingSnippetsAndPlaceholders(t *testing.T) {
	item := reviewPlanItem{
		ID:         "feat-1",
		Tier:       "T1",
		Symbol:     "GoSymbol",
		Source:     "lib/model/folder.go",
		RustSymbol: "rust_symbol",
		RustPath:   "syncthing-rs/src/folder_core.rs",
	}
	rustSnippet := "" +
		"   40 | pub fn apply() {\n" +
		"   41 |     // TODO: finish this behavior\n" +
		"   42 |     todo!(\"later\");\n" +
		"   43 | }\n"

	findings := collectLocalFindings(item, "", rustSnippet, errors.New("go not found"), nil)
	if len(findings) < 2 {
		t.Fatalf("expected at least 2 findings, got %d", len(findings))
	}
	titles := make([]string, 0, len(findings))
	for _, finding := range findings {
		titles = append(titles, finding.Title)
	}
	joined := strings.Join(titles, " | ")
	if !strings.Contains(joined, "Go symbol snippet missing") {
		t.Fatalf("missing Go snippet finding in %q", joined)
	}
	if !strings.Contains(joined, "todo! placeholder in Rust logic") {
		t.Fatalf("missing Rust placeholder finding in %q", joined)
	}
}

func TestCollectLocalFindingsDetectsConstantDrift(t *testing.T) {
	item := reviewPlanItem{
		ID:         "feat-const",
		Tier:       "T1",
		Kind:       "const",
		Symbol:     "retainBits",
		Source:     "lib/model/folder.go",
		RustSymbol: "retain_bits",
		RustPath:   "syncthing-rs/src/folder_core.rs",
	}
	goSnippet := "" +
		"  100 | const retainBits = fs.ModeSetgid | fs.ModeSetuid | fs.ModeSticky\n"
	rustSnippet := "" +
		"  120 | pub(crate) const retain_bits: u32 = 0xFFFF;\n"

	findings := collectLocalFindings(item, goSnippet, rustSnippet, nil, nil)
	if len(findings) == 0 {
		t.Fatal("expected local findings for constant drift")
	}
	foundConst := false
	for _, finding := range findings {
		if finding.Title == "Constant/value drift between Go and Rust" {
			foundConst = true
		}
	}
	if !foundConst {
		t.Fatalf("expected constant drift finding, got %+v", findings)
	}
}

func TestCollectLocalFindingsDetectsFunctionSignatureDrift(t *testing.T) {
	item := reviewPlanItem{
		ID:         "feat-func",
		Tier:       "T1",
		Kind:       "method",
		Symbol:     "apply",
		Source:     "lib/model/folder.go",
		RustSymbol: "apply",
		RustPath:   "syncthing-rs/src/folder_core.rs",
	}
	goSnippet := "" +
		"  200 | func apply(a int, b int) error {\n" +
		"  201 |     if a > 0 { return nil }\n" +
		"  202 |     return nil\n" +
		"  203 | }\n"
	rustSnippet := "" +
		"  220 | pub(crate) fn apply() {\n" +
		"  221 | }\n"

	findings := collectLocalFindings(item, goSnippet, rustSnippet, nil, nil)
	foundSig := false
	for _, finding := range findings {
		if finding.Title == "Function parameter count drift" {
			foundSig = true
			break
		}
	}
	if !foundSig {
		t.Fatalf("expected signature drift finding, got %+v", findings)
	}
}

func TestPersistArtifactJSONLWritesSingleFile(t *testing.T) {
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "prompt.jsonl")
	rawPath := filepath.Join(dir, "raw.jsonl")
	if err := initializeArtifactJSONL(promptPath); err != nil {
		t.Fatalf("initialize prompt jsonl: %v", err)
	}
	if err := initializeArtifactJSONL(rawPath); err != nil {
		t.Fatalf("initialize raw jsonl: %v", err)
	}

	cfg := runConfig{
		ArtifactMode:     "jsonl",
		PromptJSONLPath:  promptPath,
		RawJSONLPath:     rawPath,
		MaxArtifactBytes: 16,
		artifactMu:       &sync.Mutex{},
	}
	item := reviewPlanItem{
		ID:         "feat-1",
		Tier:       "T1",
		Kind:       "method",
		Symbol:     "model.Apply",
		RustSymbol: "apply",
	}
	if err := persistArtifact(cfg, item, "prompt", "", strings.Repeat("x", 64)); err != nil {
		t.Fatalf("persist prompt artifact: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(mustReadFile(t, promptPath))), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 jsonl record, got %d", len(lines))
	}
	var record artifactRecord
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("unmarshal jsonl record: %v", err)
	}
	if record.Artifact != "prompt" {
		t.Fatalf("unexpected artifact kind: %s", record.Artifact)
	}
	if record.ID != item.ID {
		t.Fatalf("unexpected artifact id: %s", record.ID)
	}
	if !record.Truncated {
		t.Fatalf("expected truncated artifact content")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	bs, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return bs
}

func TestWriteReviewRollupsPartialRunPreservesUntouchedFiles(t *testing.T) {
	dir := t.TempDir()
	untouchedPath := "syncthing-rs/src/untouched.rs"
	touchedPath := "syncthing-rs/src/touched.rs"
	untouchedFile := filepath.Join(dir, rollupFileName(untouchedPath))
	if err := os.WriteFile(untouchedFile, []byte(
		renderRollupMeta(rollupMeta{
			RustPath: untouchedPath,
			Findings: 2,
			P1:       1,
			P2:       1,
		})+"\n# Existing\n",
	), 0o644); err != nil {
		t.Fatalf("write untouched rollup: %v", err)
	}

	items := []reviewPlanItem{
		{ID: "id-1", RustPath: touchedPath},
	}
	report := reviewReport{
		Findings: []pairFinding{
			{
				ID:       "id-1",
				Priority: "P0",
				Title:    "Critical drift",
				Location: "syncthing-rs/src/touched.rs:42",
				Message:  "behavior differs",
			},
		},
	}
	if err := writeReviewRollups(dir, items, report, 8); err != nil {
		t.Fatalf("writeReviewRollups: %v", err)
	}

	if _, err := os.Stat(untouchedFile); err != nil {
		t.Fatalf("untouched rollup removed: %v", err)
	}

	touchedBytes, err := os.ReadFile(filepath.Join(dir, rollupFileName(touchedPath)))
	if err != nil {
		t.Fatalf("read touched rollup: %v", err)
	}
	meta, ok := parseRollupMeta(string(touchedBytes))
	if !ok {
		t.Fatal("missing rollup metadata for touched file")
	}
	if meta.RustPath != touchedPath || meta.Findings != 1 || meta.P0 != 1 {
		t.Fatalf("unexpected touched meta: %+v", meta)
	}

	indexBytes, err := os.ReadFile(filepath.Join(dir, "_index.md"))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	index := string(indexBytes)
	if !strings.Contains(index, "`"+untouchedPath+"`") {
		t.Fatalf("index missing untouched path: %s", index)
	}
	if !strings.Contains(index, "`"+touchedPath+"`") {
		t.Fatalf("index missing touched path: %s", index)
	}
}

func TestWriteReviewRollupsPartialRunRemovesTouchedFileWhenNoFindings(t *testing.T) {
	dir := t.TempDir()
	touchedPath := "syncthing-rs/src/touched.rs"
	touchedFile := filepath.Join(dir, rollupFileName(touchedPath))
	if err := os.WriteFile(touchedFile, []byte(
		renderRollupMeta(rollupMeta{
			RustPath: touchedPath,
			Findings: 1,
			P1:       1,
		})+"\n# Existing\n",
	), 0o644); err != nil {
		t.Fatalf("write touched rollup: %v", err)
	}

	items := []reviewPlanItem{
		{ID: "id-1", RustPath: touchedPath},
	}
	report := reviewReport{}
	if err := writeReviewRollups(dir, items, report, 8); err != nil {
		t.Fatalf("writeReviewRollups: %v", err)
	}

	if _, err := os.Stat(touchedFile); !os.IsNotExist(err) {
		t.Fatalf("expected touched rollup removal, got err=%v", err)
	}

	indexBytes, err := os.ReadFile(filepath.Join(dir, "_index.md"))
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	if strings.Contains(string(indexBytes), "`"+touchedPath+"`") {
		t.Fatalf("index still contains removed touched path: %s", string(indexBytes))
	}
}
