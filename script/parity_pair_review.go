// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pairReviewSchemaVersion = 1
	defaultMappingPath      = "parity/mapping-rust.json"
	defaultManifestPath     = "parity/feature-manifest.json"
	defaultPlanPath         = "parity/diff-reports/function-pair-review-plan.json"
	defaultReportPath       = "parity/diff-reports/function-pair-discrepancy-report.json"
	defaultRawDir           = "parity/diff-reports/function-pair-raw"
	defaultPromptDir        = "parity/diff-reports/function-pair-prompts"
	defaultRawJSONLPath     = "parity/diff-reports/function-pair-raw.jsonl"
	defaultPromptJSONLPath  = "parity/diff-reports/function-pair-prompts.jsonl"
	defaultCachePath        = "parity/diff-reports/function-pair-cache.json"
	defaultRollupDir        = "parity/diff-reports/function-pair-rollups"
	defaultModel            = "google/antigravity-claude-opus-4-6-thinking"
)

var (
	logicNamePattern            = regexp.MustCompile(`(?i)(scan|index|update|need|global|local|override|revert|pull|conflict|sync|status|state|sequence|recover|durab|manifest|compact|browse|completion|pending|connect|request|response|serve|cluster|folder|device)`)
	criticalLogicPattern        = regexp.MustCompile(`(?i)(scan|index|update|need|revert|override|completion|browse|pending|availability|global|local|sequence|manifest|compact|recover|durab|version|config)`)
	panicPlaceholderPattern     = regexp.MustCompile(`(?i)\bpanic!\s*\(\s*"(todo|not implemented|unimplemented|placeholder)`)
	todoFixmePattern            = regexp.MustCompile(`(?i)\b(TODO|FIXME)\b`)
	syntheticPlaceholderPattern = regexp.MustCompile(`(?i)\b(synthetic|placeholder|stub)\b`)
	numberedSnippetPattern      = regexp.MustCompile(`^\s*(\d+)\s+\|\s?(.*)$`)
	goConstAssignPattern        = regexp.MustCompile(`^\s*(const|var)\s+([A-Za-z_]\w*)\b[^=]*=\s*(.+)$`)
	rustConstAssignPattern      = regexp.MustCompile(`^\s*(?:pub(?:\([^)]+\))?\s+)?(?:const|static)\s+([A-Za-z_]\w*)\b[^=]*=\s*(.+);`)
	goFuncSigPattern            = regexp.MustCompile(`^\s*func\s*(?:\([^)]*\)\s*)?([A-Za-z_]\w*)\s*\(([^)]*)\)\s*(.*)$`)
	rustFuncSigPattern          = regexp.MustCompile(`^\s*(?:pub(?:\([^)]+\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_]\w*)\s*\(([^)]*)\)\s*(?:->\s*(.+?))?\s*\{?\s*$`)
	goBranchTokenPattern        = regexp.MustCompile(`\b(if|switch|for|select)\b`)
	rustBranchTokenPattern      = regexp.MustCompile(`\b(if|match|for|while|loop)\b`)
	rustUnwrapPattern           = regexp.MustCompile(`\.(unwrap|expect)\s*\(`)
)

type parityMapping struct {
	SchemaVersion int           `json:"schema_version"`
	Items         []mappingItem `json:"items"`
}

type parityManifest struct {
	SchemaVersion int            `json:"schema_version"`
	Items         []manifestItem `json:"items"`
}

type mappingItem struct {
	ID            string   `json:"id"`
	Kind          string   `json:"kind"`
	Source        string   `json:"source"`
	Symbol        string   `json:"symbol"`
	Subsystem     string   `json:"subsystem"`
	RustComponent string   `json:"rust_component"`
	RustSymbol    string   `json:"rust_symbol"`
	RequiredTests []string `json:"required_tests"`
	Status        string   `json:"status"`
}

type manifestItem struct {
	ID       string `json:"id"`
	Behavior string `json:"behavior"`
}

type reviewPlan struct {
	SchemaVersion int               `json:"schema_version"`
	GeneratedAt   string            `json:"generated_at"`
	MappingPath   string            `json:"mapping_path"`
	ManifestPath  string            `json:"manifest_path"`
	TotalItems    int               `json:"total_items"`
	TierCounts    map[string]int    `json:"tier_counts"`
	Items         []reviewPlanItem  `json:"items"`
	Filters       map[string]string `json:"filters,omitempty"`
	Warnings      []string          `json:"warnings,omitempty"`
}

type reviewPlanItem struct {
	Index         int      `json:"index"`
	ID            string   `json:"id"`
	Kind          string   `json:"kind"`
	Symbol        string   `json:"symbol"`
	Source        string   `json:"source"`
	Subsystem     string   `json:"subsystem"`
	Behavior      string   `json:"behavior"`
	RustSymbol    string   `json:"rust_symbol"`
	RustComponent string   `json:"rust_component"`
	RustPath      string   `json:"rust_path"`
	Tier          string   `json:"tier"`
	RequiredTests []string `json:"required_tests"`
}

type reviewReport struct {
	SchemaVersion    int                `json:"schema_version"`
	GeneratedAt      string             `json:"generated_at"`
	PlanPath         string             `json:"plan_path"`
	Model            string             `json:"model"`
	TotalItems       int                `json:"total_items"`
	Completed        int                `json:"completed"`
	NoFindings       int                `json:"no_findings"`
	WithFindings     int                `json:"with_findings"`
	AgentUnavailable int                `json:"agent_unavailable"`
	Errors           int                `json:"errors"`
	TierCounts       map[string]int     `json:"tier_counts"`
	SeverityCount    map[string]int     `json:"severity_count"`
	Findings         []pairFinding      `json:"findings"`
	Results          []pairReviewResult `json:"results"`
}

type pairReviewResult struct {
	ID           string `json:"id"`
	Tier         string `json:"tier"`
	Kind         string `json:"kind"`
	Symbol       string `json:"symbol"`
	RustSymbol   string `json:"rust_symbol"`
	Status       string `json:"status"`
	RawPath      string `json:"raw_path,omitempty"`
	PromptPath   string `json:"prompt_path,omitempty"`
	DurationMS   int64  `json:"duration_ms"`
	FindingCount int    `json:"finding_count"`
	Error        string `json:"error,omitempty"`
}

type pairFinding struct {
	ID       string `json:"id"`
	Tier     string `json:"tier"`
	Priority string `json:"priority"`
	Title    string `json:"title"`
	Location string `json:"location"`
	Message  string `json:"message"`
}

type lineRange struct {
	Start int
	End   int
}

type opencodeEvent struct {
	Type string `json:"type"`
	Part struct {
		Text string `json:"text"`
	} `json:"part"`
}

type opencodeErrorEvent struct {
	Type  string `json:"type"`
	Error struct {
		Name string `json:"name"`
		Data struct {
			Message string `json:"message"`
		} `json:"data"`
	} `json:"error"`
}

type runConfig struct {
	PlanPath                  string
	ReportPath                string
	RawDir                    string
	PromptDir                 string
	RawJSONLPath              string
	PromptJSONLPath           string
	ArtifactMode              string
	WriteRaw                  bool
	WritePrompts              bool
	MaxArtifactBytes          int
	CachePath                 string
	UseCache                  bool
	WriteRollups              bool
	RollupDir                 string
	RollupMaxFindings         int
	OpencodeBin               string
	Model                     string
	Workers                   int
	Timeout                   time.Duration
	MaxRetries                int
	RetryDelay                time.Duration
	MaxSnippetLines           int
	MaxSnippetBytes           int
	CheckpointEvery           int
	TierFilter                string
	Limit                     int
	Offset                    int
	DryRun                    bool
	LocalOnly                 bool
	SkipAgentOnLocal          bool
	FallbackLocalOnInfraError bool
	cache                     *reviewCacheStore
	artifactMu                *sync.Mutex
	agentState                *agentAvailabilityState
}

type reviewCacheFile struct {
	SchemaVersion int                         `json:"schema_version"`
	GeneratedAt   string                      `json:"generated_at"`
	Items         map[string]reviewCacheEntry `json:"items"`
}

type reviewCacheEntry struct {
	ID         string        `json:"id"`
	Model      string        `json:"model"`
	GoHash     string        `json:"go_hash"`
	RustHash   string        `json:"rust_hash"`
	ReviewedAt string        `json:"reviewed_at"`
	Findings   []pairFinding `json:"findings"`
}

type reviewCacheStore struct {
	mu    sync.RWMutex
	items map[string]reviewCacheEntry
}

type artifactRecord struct {
	SchemaVersion int    `json:"schema_version"`
	GeneratedAt   string `json:"generated_at"`
	Kind          string `json:"kind"`
	ID            string `json:"id"`
	Tier          string `json:"tier"`
	Symbol        string `json:"symbol"`
	RustSymbol    string `json:"rust_symbol"`
	Artifact      string `json:"artifact"`
	Content       string `json:"content"`
	Truncated     bool   `json:"truncated"`
}

type agentAvailabilityState struct {
	disabled atomic.Bool
	mu       sync.RWMutex
	reason   string
}

func newReviewCacheStore() *reviewCacheStore {
	return &reviewCacheStore{
		items: map[string]reviewCacheEntry{},
	}
}

func newAgentAvailabilityState() *agentAvailabilityState {
	return &agentAvailabilityState{}
}

func (s *agentAvailabilityState) IsDisabled() bool {
	if s == nil {
		return false
	}
	return s.disabled.Load()
}

func (s *agentAvailabilityState) Disable(reason string) {
	if s == nil {
		return
	}
	s.disabled.Store(true)
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(reason) != "" {
		s.reason = reason
	}
}

func (s *agentAvailabilityState) Reason() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reason
}

func (s *reviewCacheStore) Get(id string) (reviewCacheEntry, bool) {
	if s == nil {
		return reviewCacheEntry{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.items[id]
	if !ok {
		return reviewCacheEntry{}, false
	}
	entry.Findings = cloneFindings(entry.Findings)
	return entry, true
}

func (s *reviewCacheStore) Set(entry reviewCacheEntry) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entry.Findings = cloneFindings(entry.Findings)
	s.items[entry.ID] = entry
}

func (s *reviewCacheStore) Snapshot() map[string]reviewCacheEntry {
	if s == nil {
		return map[string]reviewCacheEntry{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]reviewCacheEntry, len(s.items))
	for id, entry := range s.items {
		entry.Findings = cloneFindings(entry.Findings)
		out[id] = entry
	}
	return out
}

func cloneFindings(in []pairFinding) []pairFinding {
	if len(in) == 0 {
		return nil
	}
	out := make([]pairFinding, len(in))
	copy(out, in)
	return out
}

type reviewTask struct {
	PlanItem reviewPlanItem
}

type reviewOutcome struct {
	Result   pairReviewResult
	Findings []pairFinding
}

type rollupMeta struct {
	RustPath string `json:"rust_path"`
	Findings int    `json:"findings"`
	P0       int    `json:"p0"`
	P1       int    `json:"p1"`
	P2       int    `json:"p2"`
	P3       int    `json:"p3"`
}

const rollupMetaPrefix = "<!-- parity-rollup-meta: "

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "plan":
		cmdPlan(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: go run ./script/parity_pair_review.go <plan|run> [flags]\n")
	os.Exit(2)
}

func cmdPlan(args []string) {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)
	mappingPath := fs.String("mapping", defaultMappingPath, "path to parity/mapping-rust.json")
	manifestPath := fs.String("manifest", defaultManifestPath, "path to parity/feature-manifest.json")
	outPath := fs.String("out", defaultPlanPath, "output review plan path")
	tier := fs.String("tier", "all", "tier filter: all|T1|T2|T3")
	limit := fs.Int("limit", 0, "maximum items to include (0 = all)")
	offset := fs.Int("offset", 0, "skip first N filtered items")
	status := fs.String("status", "all", "status filter: all|missing|implemented|parity-verified")
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}

	plan, err := buildPlan(*mappingPath, *manifestPath, *tier, *status, *offset, *limit)
	if err != nil {
		fatalf("build plan: %v", err)
	}
	if err := writeJSON(*outPath, plan); err != nil {
		fatalf("write plan: %v", err)
	}
	fmt.Printf("wrote plan: %s (items=%d)\n", *outPath, len(plan.Items))
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	cfg := runConfig{}
	fs.StringVar(&cfg.PlanPath, "plan", defaultPlanPath, "review plan path")
	fs.StringVar(&cfg.ReportPath, "out", defaultReportPath, "output discrepancy report path")
	fs.StringVar(&cfg.RawDir, "raw-dir", defaultRawDir, "directory for raw agent responses")
	fs.StringVar(&cfg.PromptDir, "prompt-dir", defaultPromptDir, "directory for prompt artifacts")
	fs.StringVar(&cfg.RawJSONLPath, "raw-jsonl", defaultRawJSONLPath, "jsonl file for raw agent responses when --artifact-mode=jsonl")
	fs.StringVar(&cfg.PromptJSONLPath, "prompt-jsonl", defaultPromptJSONLPath, "jsonl file for prompt artifacts when --artifact-mode=jsonl")
	fs.StringVar(&cfg.ArtifactMode, "artifact-mode", "jsonl", "artifact persistence mode when write flags are enabled: jsonl|files|none")
	fs.BoolVar(&cfg.WriteRaw, "write-raw", false, "persist per-item raw agent output artifacts")
	fs.BoolVar(&cfg.WritePrompts, "write-prompts", false, "persist per-item prompt artifacts")
	fs.IntVar(&cfg.MaxArtifactBytes, "max-artifact-bytes", 8*1024, "max bytes to persist per artifact (0 = unlimited)")
	fs.StringVar(&cfg.CachePath, "cache", defaultCachePath, "cache file for unchanged function-pair reviews")
	fs.BoolVar(&cfg.UseCache, "use-cache", true, "reuse cached findings when Go/Rust snippets are unchanged")
	fs.BoolVar(&cfg.WriteRollups, "write-rollups", true, "write short markdown rollups grouped by Rust file")
	fs.StringVar(&cfg.RollupDir, "rollup-dir", defaultRollupDir, "directory for markdown rollups")
	fs.IntVar(&cfg.RollupMaxFindings, "rollup-max-findings", 12, "max findings to keep per file rollup")
	fs.StringVar(&cfg.OpencodeBin, "opencode-bin", "opencode", "opencode CLI binary path")
	fs.StringVar(&cfg.Model, "model", defaultModel, "agent model for opencode run")
	fs.IntVar(&cfg.Workers, "workers", 4, "number of concurrent agent reviews")
	fs.DurationVar(&cfg.Timeout, "timeout", 15*time.Minute, "per-item agent review timeout")
	fs.IntVar(&cfg.MaxRetries, "max-retries", 2, "max retry attempts for transient opencode errors")
	fs.DurationVar(&cfg.RetryDelay, "retry-delay", 2*time.Second, "initial delay between opencode retries")
	fs.IntVar(&cfg.MaxSnippetLines, "max-snippet-lines", 240, "max snippet lines per side")
	fs.IntVar(&cfg.MaxSnippetBytes, "max-snippet-bytes", 24000, "max snippet bytes per side")
	fs.IntVar(&cfg.CheckpointEvery, "checkpoint-every", 25, "persist progress report/cache every N completed reviews (0 disables)")
	fs.StringVar(&cfg.TierFilter, "tier", "all", "tier filter: all|T1|T2|T3")
	fs.IntVar(&cfg.Limit, "limit", 0, "maximum items to review (0 = all)")
	fs.IntVar(&cfg.Offset, "offset", 0, "skip first N filtered items")
	fs.BoolVar(&cfg.DryRun, "dry-run", false, "skip opencode calls; only emit prompts and report skeleton")
	fs.BoolVar(&cfg.LocalOnly, "local-only", false, "run deterministic local checks only and skip opencode model calls")
	fs.BoolVar(&cfg.SkipAgentOnLocal, "skip-agent-on-local-findings", true, "skip opencode call when local placeholder/findings are already detected")
	fs.BoolVar(&cfg.FallbackLocalOnInfraError, "fallback-local-on-infra-error", true, "when opencode infra is unavailable, fallback to local checks without counting hard errors")
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}

	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Minute
	}
	if cfg.MaxSnippetLines <= 0 {
		cfg.MaxSnippetLines = 240
	}
	if cfg.MaxSnippetBytes <= 0 {
		cfg.MaxSnippetBytes = 24000
	}
	if cfg.MaxArtifactBytes < 0 {
		cfg.MaxArtifactBytes = 0
	}
	if cfg.RollupMaxFindings <= 0 {
		cfg.RollupMaxFindings = 12
	}
	if cfg.CheckpointEvery < 0 {
		cfg.CheckpointEvery = 0
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 2 * time.Second
	}
	rawArtifactMode := cfg.ArtifactMode
	cfg.ArtifactMode = normalizeArtifactMode(cfg.ArtifactMode)
	if cfg.ArtifactMode == "" {
		fatalf("invalid --artifact-mode %q (expected jsonl|files|none)", strings.TrimSpace(rawArtifactMode))
	}
	cfg.artifactMu = &sync.Mutex{}
	cfg.agentState = newAgentAvailabilityState()
	if cfg.ArtifactMode == "none" {
		cfg.WriteRaw = false
		cfg.WritePrompts = false
	}

	plan := reviewPlan{}
	if err := readJSON(cfg.PlanPath, &plan); err != nil {
		fatalf("read plan: %v", err)
	}

	selected := filterPlanItems(plan.Items, cfg.TierFilter, cfg.Offset, cfg.Limit)
	if len(selected) == 0 {
		fatalf("no plan items selected (tier=%q offset=%d limit=%d)", cfg.TierFilter, cfg.Offset, cfg.Limit)
	}

	if cfg.UseCache {
		cache, err := loadReviewCache(cfg.CachePath)
		if err != nil {
			fatalf("load cache: %v", err)
		}
		cfg.cache = cache
	}

	if cfg.WriteRaw {
		switch cfg.ArtifactMode {
		case "files":
			if err := os.MkdirAll(cfg.RawDir, 0o755); err != nil {
				fatalf("create raw dir: %v", err)
			}
		case "jsonl":
			if err := initializeArtifactJSONL(cfg.RawJSONLPath); err != nil {
				fatalf("initialize raw jsonl: %v", err)
			}
		}
	}
	if cfg.WritePrompts {
		switch cfg.ArtifactMode {
		case "files":
			if err := os.MkdirAll(cfg.PromptDir, 0o755); err != nil {
				fatalf("create prompt dir: %v", err)
			}
		case "jsonl":
			if err := initializeArtifactJSONL(cfg.PromptJSONLPath); err != nil {
				fatalf("initialize prompt jsonl: %v", err)
			}
		}
	}
	if cfg.WriteRollups {
		if err := os.MkdirAll(cfg.RollupDir, 0o755); err != nil {
			fatalf("create rollup dir: %v", err)
		}
	}

	report := runReview(plan, selected, cfg)
	if err := writeJSON(cfg.ReportPath, report); err != nil {
		fatalf("write report: %v", err)
	}
	if cfg.UseCache {
		if err := saveReviewCache(cfg.CachePath, cfg.cache); err != nil {
			fatalf("write cache: %v", err)
		}
	}
	if cfg.WriteRollups {
		if err := writeReviewRollups(cfg.RollupDir, selected, report, cfg.RollupMaxFindings); err != nil {
			fatalf("write rollups: %v", err)
		}
	}
	fmt.Printf(
		"wrote report: %s (completed=%d findings=%d agent_unavailable=%d errors=%d)\n",
		cfg.ReportPath,
		report.Completed,
		len(report.Findings),
		report.AgentUnavailable,
		report.Errors,
	)
}

func buildPlan(mappingPath, manifestPath, tierFilter, statusFilter string, offset, limit int) (reviewPlan, error) {
	mapping := parityMapping{}
	if err := readJSON(mappingPath, &mapping); err != nil {
		return reviewPlan{}, err
	}
	manifest := parityManifest{}
	if err := readJSON(manifestPath, &manifest); err != nil {
		return reviewPlan{}, err
	}

	behaviorByID := make(map[string]string, len(manifest.Items))
	for _, item := range manifest.Items {
		behaviorByID[strings.TrimSpace(item.ID)] = strings.TrimSpace(item.Behavior)
	}

	items := make([]reviewPlanItem, 0, len(mapping.Items))
	warnings := make([]string, 0)
	for i, item := range mapping.Items {
		if !statusAllowed(item.Status, statusFilter) {
			continue
		}
		rustPath, err := rustComponentPath(item.RustComponent)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("skip %s: %v", item.ID, err))
			continue
		}
		planItem := reviewPlanItem{
			Index:         i,
			ID:            strings.TrimSpace(item.ID),
			Kind:          strings.TrimSpace(item.Kind),
			Symbol:        strings.TrimSpace(item.Symbol),
			Source:        filepath.ToSlash(strings.TrimSpace(item.Source)),
			Subsystem:     strings.TrimSpace(item.Subsystem),
			Behavior:      strings.TrimSpace(behaviorByID[strings.TrimSpace(item.ID)]),
			RustSymbol:    strings.TrimSpace(item.RustSymbol),
			RustComponent: strings.TrimSpace(item.RustComponent),
			RustPath:      rustPath,
			Tier:          classifyTier(item),
			RequiredTests: append([]string(nil), item.RequiredTests...),
		}
		if planItem.ID == "" || planItem.Symbol == "" || planItem.Source == "" || planItem.RustSymbol == "" {
			warnings = append(warnings, fmt.Sprintf("skip malformed mapping item at index %d", i))
			continue
		}
		items = append(items, planItem)
	}

	items = filterPlanItems(items, tierFilter, offset, limit)

	tierCounts := map[string]int{"T1": 0, "T2": 0, "T3": 0}
	for _, item := range items {
		tierCounts[item.Tier]++
	}

	return reviewPlan{
		SchemaVersion: pairReviewSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		MappingPath:   mappingPath,
		ManifestPath:  manifestPath,
		TotalItems:    len(items),
		TierCounts:    tierCounts,
		Items:         items,
		Filters: map[string]string{
			"tier":   normalizeTier(tierFilter),
			"status": normalizeStatusFilter(statusFilter),
			"offset": strconv.Itoa(max(offset, 0)),
			"limit":  strconv.Itoa(max(limit, 0)),
		},
		Warnings: warnings,
	}, nil
}

func runReview(plan reviewPlan, items []reviewPlanItem, cfg runConfig) reviewReport {
	report := reviewReport{
		SchemaVersion: pairReviewSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		PlanPath:      cfg.PlanPath,
		Model:         cfg.Model,
		TotalItems:    len(items),
		TierCounts:    map[string]int{"T1": 0, "T2": 0, "T3": 0},
		SeverityCount: map[string]int{"P0": 0, "P1": 0, "P2": 0, "P3": 0},
		Findings:      make([]pairFinding, 0),
		Results:       make([]pairReviewResult, 0, len(items)),
	}

	taskCh := make(chan reviewTask)
	outCh := make(chan reviewOutcome)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				outCh <- executeReviewTask(task.PlanItem, cfg)
			}
		}()
	}

	go func() {
		for _, item := range items {
			taskCh <- reviewTask{PlanItem: item}
		}
		close(taskCh)
		wg.Wait()
		close(outCh)
	}()

	for outcome := range outCh {
		res := outcome.Result
		report.Completed++
		report.Results = append(report.Results, res)
		if _, ok := report.TierCounts[res.Tier]; !ok {
			report.TierCounts[res.Tier] = 0
		}
		report.TierCounts[res.Tier]++
		switch res.Status {
		case "findings", "cached-findings":
			report.WithFindings++
		case "no-findings", "dry-run", "cached-no-findings", "local-no-findings":
			report.NoFindings++
		case "local-findings":
			report.WithFindings++
		case "agent-unavailable":
			report.AgentUnavailable++
		default:
			report.Errors++
		}
		for _, finding := range outcome.Findings {
			report.Findings = append(report.Findings, finding)
			if _, ok := report.SeverityCount[finding.Priority]; !ok {
				report.SeverityCount[finding.Priority] = 0
			}
			report.SeverityCount[finding.Priority]++
		}
		if cfg.CheckpointEvery > 0 && report.Completed%cfg.CheckpointEvery == 0 {
			if err := checkpointProgress(report, cfg); err != nil {
				fmt.Fprintf(
					os.Stderr,
					"checkpoint failed at completed=%d: %v\n",
					report.Completed,
					err,
				)
			}
			fmt.Printf(
				"progress: completed=%d/%d findings=%d agent_unavailable=%d errors=%d\n",
				report.Completed,
				report.TotalItems,
				len(report.Findings),
				report.AgentUnavailable,
				report.Errors,
			)
		}
	}

	sort.Slice(report.Results, func(i, j int) bool { return report.Results[i].ID < report.Results[j].ID })
	sort.Slice(report.Findings, func(i, j int) bool {
		if report.Findings[i].Priority == report.Findings[j].Priority {
			if report.Findings[i].ID == report.Findings[j].ID {
				return report.Findings[i].Title < report.Findings[j].Title
			}
			return report.Findings[i].ID < report.Findings[j].ID
		}
		return report.Findings[i].Priority < report.Findings[j].Priority
	})
	return report
}

func checkpointProgress(report reviewReport, cfg runConfig) error {
	if err := writeJSON(cfg.ReportPath, report); err != nil {
		return err
	}
	if cfg.UseCache {
		if err := saveReviewCache(cfg.CachePath, cfg.cache); err != nil {
			return err
		}
	}
	return nil
}

func executeReviewTask(item reviewPlanItem, cfg runConfig) reviewOutcome {
	start := time.Now()
	result := pairReviewResult{
		ID:         item.ID,
		Tier:       item.Tier,
		Kind:       item.Kind,
		Symbol:     item.Symbol,
		RustSymbol: item.RustSymbol,
	}

	safeItemID := safeID(item.ID)
	promptPath := filepath.Join(cfg.PromptDir, safeItemID+".txt")
	rawPath := filepath.Join(cfg.RawDir, safeItemID+".txt")
	if cfg.WritePrompts {
		result.PromptPath = artifactReferencePath(cfg, "prompt", promptPath, safeItemID)
	}
	if cfg.WriteRaw {
		result.RawPath = artifactReferencePath(cfg, "raw", rawPath, safeItemID)
	}

	goSnippet, goRange, goErr := extractSymbolSnippet(item.Source, item.Kind, item.Symbol, false, cfg.MaxSnippetLines, cfg.MaxSnippetBytes)
	rustSnippet, rustRange, rustErr := extractSymbolSnippet(item.RustPath, item.Kind, item.RustSymbol, true, cfg.MaxSnippetLines, cfg.MaxSnippetBytes)
	localFindings := collectLocalFindings(item, goSnippet, rustSnippet, goErr, rustErr)
	goHash := hashSnippet(goSnippet, goErr)
	rustHash := hashSnippet(rustSnippet, rustErr)

	if cfg.UseCache && !cfg.DryRun && len(localFindings) == 0 {
		if cached, ok := cfg.cache.Get(item.ID); ok &&
			cached.Model == cfg.Model &&
			cached.GoHash == goHash &&
			cached.RustHash == rustHash {
			result.FindingCount = len(cached.Findings)
			if len(cached.Findings) == 0 {
				result.Status = "cached-no-findings"
			} else {
				result.Status = "cached-findings"
			}
			result.DurationMS = time.Since(start).Milliseconds()
			return reviewOutcome{Result: result, Findings: cloneFindings(cached.Findings)}
		}
	}

	prompt := buildReviewPrompt(item, goSnippet, rustSnippet, goRange, rustRange, goErr, rustErr)
	if cfg.WritePrompts {
		if err := persistArtifact(cfg, item, "prompt", promptPath, prompt); err != nil {
			result.Status = "error"
			result.Error = fmt.Sprintf("write prompt: %v", err)
			result.DurationMS = time.Since(start).Milliseconds()
			return reviewOutcome{Result: result}
		}
	}

	if cfg.DryRun {
		if len(localFindings) > 0 {
			result.Status = "local-findings"
		} else {
			result.Status = "dry-run"
		}
		result.FindingCount = len(localFindings)
		result.DurationMS = time.Since(start).Milliseconds()
		return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
	}

	if cfg.LocalOnly {
		if len(localFindings) > 0 {
			result.Status = "local-findings"
		} else {
			result.Status = "local-no-findings"
		}
		result.FindingCount = len(localFindings)
		if cfg.UseCache {
			cfg.cache.Set(reviewCacheEntry{
				ID:         item.ID,
				Model:      cfg.Model,
				GoHash:     goHash,
				RustHash:   rustHash,
				ReviewedAt: time.Now().UTC().Format(time.RFC3339),
				Findings:   cloneFindings(localFindings),
			})
		}
		result.DurationMS = time.Since(start).Milliseconds()
		return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
	}

	if cfg.SkipAgentOnLocal && len(localFindings) > 0 {
		result.Status = "local-findings"
		result.FindingCount = len(localFindings)
		if cfg.UseCache {
			cfg.cache.Set(reviewCacheEntry{
				ID:         item.ID,
				Model:      cfg.Model,
				GoHash:     goHash,
				RustHash:   rustHash,
				ReviewedAt: time.Now().UTC().Format(time.RFC3339),
				Findings:   cloneFindings(localFindings),
			})
		}
		result.DurationMS = time.Since(start).Milliseconds()
		return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
	}

	if cfg.agentState != nil && cfg.agentState.IsDisabled() {
		if len(localFindings) > 0 {
			result.Status = "local-findings"
		} else {
			result.Status = "local-no-findings"
		}
		result.Error = cfg.agentState.Reason()
		result.FindingCount = len(localFindings)
		if cfg.UseCache {
			cfg.cache.Set(reviewCacheEntry{
				ID:         item.ID,
				Model:      cfg.Model,
				GoHash:     goHash,
				RustHash:   rustHash,
				ReviewedAt: time.Now().UTC().Format(time.RFC3339),
				Findings:   cloneFindings(localFindings),
			})
		}
		result.DurationMS = time.Since(start).Milliseconds()
		return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
	}

	rawText, err := runOpencodeWithRetry(cfg, prompt)
	if err != nil {
		if cfg.FallbackLocalOnInfraError && isInfraOpencodeError(err) {
			if cfg.agentState != nil {
				cfg.agentState.Disable(err.Error())
			}
			if len(localFindings) > 0 {
				result.Status = "local-findings"
			} else {
				result.Status = "local-no-findings"
			}
			result.Error = err.Error()
			result.FindingCount = len(localFindings)
			if cfg.UseCache {
				cfg.cache.Set(reviewCacheEntry{
					ID:         item.ID,
					Model:      cfg.Model,
					GoHash:     goHash,
					RustHash:   rustHash,
					ReviewedAt: time.Now().UTC().Format(time.RFC3339),
					Findings:   cloneFindings(localFindings),
				})
			}
			result.DurationMS = time.Since(start).Milliseconds()
			if cfg.WriteRaw {
				_ = persistArtifact(cfg, item, "raw", rawPath, rawText)
			}
			return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
		}
		result.Status = "error"
		result.Error = err.Error()
		result.DurationMS = time.Since(start).Milliseconds()
		if cfg.WriteRaw {
			_ = persistArtifact(cfg, item, "raw", rawPath, rawText)
		}
		return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
	}
	if cfg.WriteRaw {
		_ = persistArtifact(cfg, item, "raw", rawPath, rawText)
	}

	findings, parseErr := parseReviewFindings(rawText, item)
	if parseErr != nil {
		result.Status = "error"
		result.Error = parseErr.Error()
		result.FindingCount = len(localFindings)
		result.DurationMS = time.Since(start).Milliseconds()
		return reviewOutcome{Result: result, Findings: cloneFindings(localFindings)}
	}
	mergedFindings := mergeFindings(localFindings, findings)
	result.FindingCount = len(mergedFindings)
	if len(mergedFindings) == 0 {
		result.Status = "no-findings"
	} else {
		result.Status = "findings"
	}
	if cfg.UseCache {
		cfg.cache.Set(reviewCacheEntry{
			ID:         item.ID,
			Model:      cfg.Model,
			GoHash:     goHash,
			RustHash:   rustHash,
			ReviewedAt: time.Now().UTC().Format(time.RFC3339),
			Findings:   cloneFindings(mergedFindings),
		})
	}
	result.DurationMS = time.Since(start).Milliseconds()
	return reviewOutcome{Result: result, Findings: mergedFindings}
}

func writeArtifact(path, text string, maxBytes int) error {
	content, _ := trimArtifactContent(text, maxBytes)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

func persistArtifact(cfg runConfig, item reviewPlanItem, artifact, filePath, text string) error {
	switch cfg.ArtifactMode {
	case "none":
		return nil
	case "files":
		return writeArtifact(filePath, text, cfg.MaxArtifactBytes)
	case "jsonl":
		var jsonlPath string
		switch artifact {
		case "prompt":
			jsonlPath = strings.TrimSpace(cfg.PromptJSONLPath)
		case "raw":
			jsonlPath = strings.TrimSpace(cfg.RawJSONLPath)
		default:
			return fmt.Errorf("unknown artifact kind %q", artifact)
		}
		if jsonlPath == "" {
			return fmt.Errorf("empty jsonl path for %s artifacts", artifact)
		}
		content, truncated := trimArtifactContent(text, cfg.MaxArtifactBytes)
		record := artifactRecord{
			SchemaVersion: pairReviewSchemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
			Kind:          item.Kind,
			ID:            item.ID,
			Tier:          item.Tier,
			Symbol:        item.Symbol,
			RustSymbol:    item.RustSymbol,
			Artifact:      artifact,
			Content:       content,
			Truncated:     truncated,
		}
		return appendArtifactJSONL(jsonlPath, record, cfg.artifactMu)
	default:
		return fmt.Errorf("unsupported artifact mode %q", cfg.ArtifactMode)
	}
}

func artifactReferencePath(cfg runConfig, artifact, filePath, safeItemID string) string {
	switch cfg.ArtifactMode {
	case "none":
		return ""
	case "files":
		return filepath.ToSlash(filePath)
	case "jsonl":
		switch artifact {
		case "prompt":
			return filepath.ToSlash(strings.TrimSpace(cfg.PromptJSONLPath)) + "#" + safeItemID
		case "raw":
			return filepath.ToSlash(strings.TrimSpace(cfg.RawJSONLPath)) + "#" + safeItemID
		default:
			return ""
		}
	default:
		return ""
	}
}

func trimArtifactContent(text string, maxBytes int) (string, bool) {
	content := []byte(text)
	if maxBytes <= 0 || len(content) <= maxBytes {
		return text, false
	}
	trimTo := maxBytes
	if trimTo > len(content) {
		trimTo = len(content)
	}
	trimmed := append([]byte{}, content[:trimTo]...)
	if len(trimmed) == 0 || trimmed[len(trimmed)-1] != '\n' {
		trimmed = append(trimmed, '\n')
	}
	trimmed = append(trimmed, []byte("...[truncated]\n")...)
	return string(trimmed), true
}

func initializeArtifactJSONL(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("jsonl path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte{}, 0o644)
}

func appendArtifactJSONL(path string, record artifactRecord, mu *sync.Mutex) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("jsonl path is empty")
	}
	bs, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(bs, '\n')); err != nil {
		return err
	}
	return nil
}

func normalizeArtifactMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "jsonl", "bundle":
		return "jsonl"
	case "files", "file", "per-item":
		return "files"
	case "none", "off", "disabled":
		return "none"
	default:
		return ""
	}
}

func collectLocalFindings(item reviewPlanItem, goSnippet, rustSnippet string, goErr, rustErr error) []pairFinding {
	out := make([]pairFinding, 0, 4)
	seen := map[string]struct{}{}
	appendFinding := func(priority, title, location, message string) {
		priority = strings.ToUpper(strings.TrimSpace(priority))
		if priority == "" {
			priority = "P2"
		}
		location = strings.TrimSpace(location)
		if location == "" {
			location = fmt.Sprintf("%s:%d", item.RustPath, 1)
		}
		message = strings.TrimSpace(message)
		if message == "" {
			return
		}
		key := strings.Join([]string{priority, title, location, message}, "|")
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, pairFinding{
			ID:       item.ID,
			Tier:     item.Tier,
			Priority: priority,
			Title:    strings.TrimSpace(title),
			Location: location,
			Message:  message,
		})
	}

	if goErr != nil {
		appendFinding(
			"P0",
			"Go symbol snippet missing",
			fmt.Sprintf("%s:%d", item.Source, 1),
			fmt.Sprintf("Failed to extract Go symbol %q from %s: %v", item.Symbol, item.Source, goErr),
		)
	}
	if rustErr != nil {
		appendFinding(
			"P0",
			"Rust symbol snippet missing",
			fmt.Sprintf("%s:%d", item.RustPath, 1),
			fmt.Sprintf("Failed to extract Rust symbol %q from %s: %v", item.RustSymbol, item.RustPath, rustErr),
		)
	}

	lines := parseNumberedSnippetLines(rustSnippet)
	for _, line := range lines {
		text := strings.TrimSpace(line.Text)
		location := fmt.Sprintf("%s:%d", item.RustPath, max(line.Number, 1))
		switch {
		case strings.Contains(text, "todo!("):
			appendFinding("P0", "todo! placeholder in Rust logic", location, "Rust implementation contains `todo!`, which means the behavior is not fully implemented.")
		case strings.Contains(text, "unimplemented!("):
			appendFinding("P0", "unimplemented! placeholder in Rust logic", location, "Rust implementation contains `unimplemented!`, so parity for this path cannot be verified.")
		case panicPlaceholderPattern.MatchString(text):
			appendFinding("P0", "panic placeholder in Rust logic", location, "Rust implementation panics with a placeholder/not-implemented message in a mapped parity path.")
		case todoFixmePattern.MatchString(text):
			appendFinding("P1", "TODO/FIXME marker in parity path", location, "Rust parity path still contains TODO/FIXME markers; this often indicates incomplete or deferred behavior.")
		case syntheticPlaceholderPattern.MatchString(text):
			appendFinding("P1", "Synthetic/placeholder marker in parity path", location, "Rust parity path uses synthetic/placeholder/stub language, which may indicate non-production behavior.")
		case item.Tier == "T1" && rustUnwrapPattern.MatchString(text):
			appendFinding("P2", "Unchecked unwrap/expect in T1 parity path", location, "Rust logic uses unwrap/expect in a T1 path; this may panic on inputs where Go returned an error.")
		}
	}

	if goErr == nil && rustErr == nil {
		goAssign, goAssignLine, goAssignOK := extractGoAssignment(goSnippet)
		rustAssign, rustAssignLine, rustAssignOK := extractRustAssignment(rustSnippet)
		if goAssignOK && rustAssignOK && looksLikeConstantComparison(item) {
			goNorm := normalizeAssignedValue(goAssign)
			rustNorm := normalizeAssignedValue(rustAssign)
			if goNorm != "" && rustNorm != "" && goNorm != rustNorm {
				priority := "P1"
				title := "Constant/value drift between Go and Rust"
				if isStringLiteral(goAssign) && isStringLiteral(rustAssign) {
					title = "String sentinel/value drift between Go and Rust"
					if strings.Contains(strings.ToLower(item.Symbol), "err") || strings.Contains(strings.ToLower(item.RustSymbol), "err") {
						priority = "P0"
					}
				}
				appendFinding(
					priority,
					title,
					fmt.Sprintf("%s:%d | %s:%d", item.Source, goAssignLine, item.RustPath, rustAssignLine),
					fmt.Sprintf("Go assignment value `%s` differs from Rust value `%s`; this can change compatibility/behavior.", strings.TrimSpace(goAssign), strings.TrimSpace(rustAssign)),
				)
			}
		}

		goSig, goSigLine, goSigOK := extractGoFunctionSignature(goSnippet)
		rustSig, rustSigLine, rustSigOK := extractRustFunctionSignature(rustSnippet)
		if goSigOK && rustSigOK && looksLikeFunctionComparison(item) {
			if goSig.ParamCount != rustSig.ParamCount {
				appendFinding(
					"P1",
					"Function parameter count drift",
					fmt.Sprintf("%s:%d | %s:%d", item.Source, goSigLine, item.RustPath, rustSigLine),
					fmt.Sprintf("Go signature exposes %d params while Rust exposes %d params; this often indicates dropped or added behavior.", goSig.ParamCount, rustSig.ParamCount),
				)
			}
			if goSig.ReturnsError && !rustSig.ReturnsResult {
				appendFinding(
					"P1",
					"Error-return contract drift",
					fmt.Sprintf("%s:%d", item.RustPath, rustSigLine),
					"Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.",
				)
			}
		}

		goBranches := len(goBranchTokenPattern.FindAllString(goSnippet, -1))
		rustBranches := len(rustBranchTokenPattern.FindAllString(rustSnippet, -1))
		if looksLikeFunctionComparison(item) && goBranches >= 3 && rustBranches == 0 {
			appendFinding(
				"P1",
				"Potential missing branch logic in Rust",
				fmt.Sprintf("%s:%d", item.RustPath, 1),
				fmt.Sprintf("Go snippet includes %d control-flow branches while Rust snippet includes %d; verify no branch paths were dropped.", goBranches, rustBranches),
			)
		}
	}

	return out
}

func parseNumberedSnippetLines(snippet string) []snippetLine {
	if strings.TrimSpace(snippet) == "" {
		return nil
	}
	lines := strings.Split(snippet, "\n")
	out := make([]snippetLine, 0, len(lines))
	for _, raw := range lines {
		matches := numberedSnippetPattern.FindStringSubmatch(raw)
		if len(matches) != 3 {
			continue
		}
		number, err := strconv.Atoi(matches[1])
		if err != nil {
			continue
		}
		out = append(out, snippetLine{
			Number: number,
			Text:   matches[2],
		})
	}
	return out
}

type functionSignature struct {
	ParamCount    int
	ReturnsError  bool
	ReturnsResult bool
}

func extractGoAssignment(snippet string) (value string, line int, ok bool) {
	lines := parseNumberedSnippetLines(snippet)
	if len(lines) == 0 {
		return "", 0, false
	}
	for _, line := range lines {
		text := strings.TrimSpace(stripInlineComment(line.Text))
		m := goConstAssignPattern.FindStringSubmatch(text)
		if len(m) != 4 {
			continue
		}
		value = strings.TrimSpace(m[3])
		if value == "" {
			continue
		}
		return value, max(line.Number, 1), true
	}
	return "", 0, false
}

func extractRustAssignment(snippet string) (value string, line int, ok bool) {
	lines := parseNumberedSnippetLines(snippet)
	if len(lines) == 0 {
		return "", 0, false
	}
	for _, line := range lines {
		text := strings.TrimSpace(stripInlineComment(line.Text))
		m := rustConstAssignPattern.FindStringSubmatch(text)
		if len(m) != 3 {
			continue
		}
		value = strings.TrimSpace(m[2])
		if value == "" {
			continue
		}
		return value, max(line.Number, 1), true
	}
	return "", 0, false
}

func extractGoFunctionSignature(snippet string) (functionSignature, int, bool) {
	lines := parseNumberedSnippetLines(snippet)
	for _, line := range lines {
		text := strings.TrimSpace(line.Text)
		m := goFuncSigPattern.FindStringSubmatch(text)
		if len(m) != 4 {
			continue
		}
		params := splitTopLevelComma(strings.TrimSpace(m[2]))
		paramCount := len(params)
		returnText := strings.TrimSpace(m[3])
		return functionSignature{
			ParamCount:   paramCount,
			ReturnsError: strings.Contains(returnText, "error"),
		}, max(line.Number, 1), true
	}
	return functionSignature{}, 0, false
}

func extractRustFunctionSignature(snippet string) (functionSignature, int, bool) {
	lines := parseNumberedSnippetLines(snippet)
	for _, line := range lines {
		text := strings.TrimSpace(line.Text)
		m := rustFuncSigPattern.FindStringSubmatch(text)
		if len(m) != 4 {
			continue
		}
		params := splitTopLevelComma(strings.TrimSpace(m[2]))
		paramCount := 0
		for _, p := range params {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				continue
			}
			if trimmed == "self" || trimmed == "&self" || trimmed == "&mut self" {
				continue
			}
			paramCount++
		}
		returnText := strings.TrimSpace(m[3])
		return functionSignature{
			ParamCount:    paramCount,
			ReturnsResult: strings.Contains(returnText, "Result<") || strings.Contains(returnText, "Result <"),
		}, max(line.Number, 1), true
	}
	return functionSignature{}, 0, false
}

func splitTopLevelComma(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	out := make([]string, 0, 4)
	start := 0
	angle, paren, bracket, brace := 0, 0, 0, 0
	for i, r := range value {
		switch r {
		case '<':
			angle++
		case '>':
			if angle > 0 {
				angle--
			}
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case '{':
			brace++
		case '}':
			if brace > 0 {
				brace--
			}
		case ',':
			if angle == 0 && paren == 0 && bracket == 0 && brace == 0 {
				segment := strings.TrimSpace(value[start:i])
				if segment != "" {
					out = append(out, segment)
				}
				start = i + 1
			}
		}
	}
	if start < len(value) {
		segment := strings.TrimSpace(value[start:])
		if segment != "" {
			out = append(out, segment)
		}
	}
	return out
}

func looksLikeConstantComparison(item reviewPlanItem) bool {
	kind := strings.ToLower(strings.TrimSpace(item.Kind))
	if strings.Contains(kind, "const") || strings.Contains(kind, "var") || strings.Contains(kind, "value") {
		return true
	}
	symbolToken, _ := splitSymbol(item.Symbol)
	if symbolToken != "" && strings.ToUpper(symbolToken) == symbolToken {
		return true
	}
	return false
}

func looksLikeFunctionComparison(item reviewPlanItem) bool {
	kind := strings.ToLower(strings.TrimSpace(item.Kind))
	return strings.Contains(kind, "func") || strings.Contains(kind, "method")
}

func normalizeAssignedValue(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimSuffix(value, ";")
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, "\t", "")
	return value
}

func stripInlineComment(line string) string {
	if idx := strings.Index(line, "//"); idx >= 0 {
		return line[:idx]
	}
	return line
}

func isStringLiteral(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
		return true
	}
	if strings.HasPrefix(value, "`") && strings.HasSuffix(value, "`") {
		return true
	}
	return false
}

func mergeFindings(groups ...[]pairFinding) []pairFinding {
	if len(groups) == 0 {
		return nil
	}
	merged := make([]pairFinding, 0, 8)
	seen := map[string]struct{}{}
	for _, findings := range groups {
		for _, finding := range findings {
			key := strings.Join([]string{
				finding.ID,
				strings.ToUpper(strings.TrimSpace(finding.Priority)),
				strings.TrimSpace(finding.Title),
				strings.TrimSpace(finding.Location),
				strings.TrimSpace(finding.Message),
			}, "|")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			merged = append(merged, finding)
		}
	}
	return merged
}

type snippetLine struct {
	Number int
	Text   string
}

func hashSnippet(snippet string, err error) string {
	h := sha256.New()
	if err != nil {
		_, _ = h.Write([]byte("error:"))
		_, _ = h.Write([]byte(err.Error()))
	} else {
		_, _ = h.Write([]byte(snippet))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func buildReviewPrompt(item reviewPlanItem, goSnippet, rustSnippet string, goRange, rustRange lineRange, goErr, rustErr error) string {
	var b strings.Builder
	b.WriteString("You are reviewing a Go-to-Rust port for behavioral equivalence.\n")
	b.WriteString("Report only concrete discrepancies, with line references.\n\n")
	b.WriteString("Review Target:\n")
	b.WriteString(fmt.Sprintf("- Mapping ID: %s\n", item.ID))
	b.WriteString(fmt.Sprintf("- Tier: %s\n", item.Tier))
	b.WriteString(fmt.Sprintf("- Kind: %s\n", item.Kind))
	b.WriteString(fmt.Sprintf("- Go symbol: %s\n", item.Symbol))
	b.WriteString(fmt.Sprintf("- Go source: %s\n", item.Source))
	b.WriteString(fmt.Sprintf("- Rust symbol: %s\n", item.RustSymbol))
	b.WriteString(fmt.Sprintf("- Rust component: %s\n", item.RustComponent))
	b.WriteString(fmt.Sprintf("- Rust source: %s\n", item.RustPath))
	if item.Behavior != "" {
		b.WriteString(fmt.Sprintf("- Behavior intent: %s\n", item.Behavior))
	}
	if len(item.RequiredTests) > 0 {
		b.WriteString(fmt.Sprintf("- Required parity tests: %s\n", strings.Join(item.RequiredTests, ", ")))
	}
	b.WriteString("\nChecks:\n")
	b.WriteString("1. Semantic equivalence: same inputs produce same outputs/side effects.\n")
	b.WriteString("2. Edge cases: empty inputs, nil/None, error paths, overflow/underflow.\n")
	b.WriteString("3. Off-by-one and ordering behavior.\n")
	b.WriteString("4. Missing branches or missing behavior paths.\n")
	b.WriteString("5. Type fidelity and wire/schema compatibility.\n")
	b.WriteString("6. Constant/string/value drift that changes behavior or compatibility.\n\n")
	b.WriteString("Output Contract (strict):\n")
	b.WriteString("- If no concrete discrepancy exists, output exactly: NO_FINDINGS\n")
	b.WriteString("- Otherwise output one or more lines exactly in this format:\n")
	b.WriteString("  FINDING|<P0-P3>|<short-title>|<path:line>|<one-paragraph discrepancy>\n")
	b.WriteString("- Do not output markdown, bullets, code fences, or extra prose.\n\n")

	b.WriteString("Go snippet:\n")
	if goErr != nil {
		b.WriteString(fmt.Sprintf("ERROR: %v\n", goErr))
	} else {
		b.WriteString(fmt.Sprintf("Source window: %s:%d-%d\n", item.Source, goRange.Start, goRange.End))
		b.WriteString(goSnippet)
		if !strings.HasSuffix(goSnippet, "\n") {
			b.WriteByte('\n')
		}
	}

	b.WriteString("\nRust snippet:\n")
	if rustErr != nil {
		b.WriteString(fmt.Sprintf("ERROR: %v\n", rustErr))
	} else {
		b.WriteString(fmt.Sprintf("Source window: %s:%d-%d\n", item.RustPath, rustRange.Start, rustRange.End))
		b.WriteString(rustSnippet)
		if !strings.HasSuffix(rustSnippet, "\n") {
			b.WriteByte('\n')
		}
	}

	return b.String()
}

func parseReviewFindings(raw string, item reviewPlanItem) ([]pairFinding, error) {
	lines := splitNonEmptyLines(raw)
	if len(lines) == 0 {
		return nil, errors.New("agent response was empty")
	}
	if len(lines) == 1 && strings.TrimSpace(lines[0]) == "NO_FINDINGS" {
		return []pairFinding{}, nil
	}

	out := make([]pairFinding, 0, len(lines))
	sawNoFindings := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if trimmed == "NO_FINDINGS" {
			sawNoFindings = true
			continue
		}
		if !strings.HasPrefix(trimmed, "FINDING|") {
			// Some models emit brief scratchpad lines before the structured output.
			// Keep strict parsing for FINDING lines while ignoring unrelated prose.
			continue
		}
		parts := strings.SplitN(trimmed, "|", 5)
		if len(parts) != 5 {
			return nil, fmt.Errorf("unparseable finding line: %q", trimmed)
		}
		priority := strings.ToUpper(strings.TrimSpace(parts[1]))
		if priority != "P0" && priority != "P1" && priority != "P2" && priority != "P3" {
			return nil, fmt.Errorf("invalid finding priority %q", priority)
		}
		title := strings.TrimSpace(parts[2])
		location := strings.TrimSpace(parts[3])
		message := strings.TrimSpace(parts[4])
		if title == "" || location == "" || message == "" {
			return nil, fmt.Errorf("finding fields must be non-empty: %q", trimmed)
		}
		out = append(out, pairFinding{
			ID:       item.ID,
			Tier:     item.Tier,
			Priority: priority,
			Title:    title,
			Location: location,
			Message:  message,
		})
	}
	if len(out) == 0 {
		if sawNoFindings {
			return []pairFinding{}, nil
		}
		if parsed, ok, err := parseFindingsJSON(raw, item); ok {
			return parsed, err
		}
		return nil, errors.New("agent response did not contain parseable FINDING lines")
	}
	return out, nil
}

type findingsJSONRecord struct {
	Priority string `json:"priority"`
	Severity string `json:"severity"`
	Title    string `json:"title"`
	Location string `json:"location"`
	Path     string `json:"path"`
	Line     int    `json:"line"`
	Message  string `json:"message"`
	Body     string `json:"body"`
}

type findingsJSONEnvelope struct {
	Findings []findingsJSONRecord `json:"findings"`
}

func parseFindingsJSON(raw string, item reviewPlanItem) ([]pairFinding, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, false, nil
	}

	normalize := func(rec findingsJSONRecord) (pairFinding, bool, error) {
		priority := strings.ToUpper(strings.TrimSpace(rec.Priority))
		if priority == "" {
			priority = strings.ToUpper(strings.TrimSpace(rec.Severity))
		}
		if priority == "" {
			priority = "P2"
		}
		if priority != "P0" && priority != "P1" && priority != "P2" && priority != "P3" {
			return pairFinding{}, false, fmt.Errorf("invalid finding priority %q", priority)
		}
		title := strings.TrimSpace(rec.Title)
		if title == "" {
			title = "Discrepancy"
		}
		location := strings.TrimSpace(rec.Location)
		if location == "" && strings.TrimSpace(rec.Path) != "" {
			if rec.Line > 0 {
				location = fmt.Sprintf("%s:%d", strings.TrimSpace(rec.Path), rec.Line)
			} else {
				location = strings.TrimSpace(rec.Path)
			}
		}
		if location == "" {
			location = fmt.Sprintf("%s:1", item.RustPath)
		}
		message := strings.TrimSpace(rec.Message)
		if message == "" {
			message = strings.TrimSpace(rec.Body)
		}
		if message == "" {
			return pairFinding{}, false, errors.New("finding message is empty")
		}
		return pairFinding{
			ID:       item.ID,
			Tier:     item.Tier,
			Priority: priority,
			Title:    title,
			Location: location,
			Message:  message,
		}, true, nil
	}

	if strings.HasPrefix(trimmed, "{") {
		var env findingsJSONEnvelope
		if err := json.Unmarshal([]byte(trimmed), &env); err == nil {
			if len(env.Findings) == 0 {
				return []pairFinding{}, true, nil
			}
			out := make([]pairFinding, 0, len(env.Findings))
			for _, rec := range env.Findings {
				finding, ok, err := normalize(rec)
				if err != nil {
					return nil, true, err
				}
				if ok {
					out = append(out, finding)
				}
			}
			return out, true, nil
		}
	}

	if strings.HasPrefix(trimmed, "[") {
		var records []findingsJSONRecord
		if err := json.Unmarshal([]byte(trimmed), &records); err == nil {
			if len(records) == 0 {
				return []pairFinding{}, true, nil
			}
			out := make([]pairFinding, 0, len(records))
			for _, rec := range records {
				finding, ok, err := normalize(rec)
				if err != nil {
					return nil, true, err
				}
				if ok {
					out = append(out, finding)
				}
			}
			return out, true, nil
		}
	}

	return nil, false, nil
}

func isInfraOpencodeError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if msg == "" {
		return false
	}
	needles := []string{
		"all antigravity endpoints failed",
		"rate-limited",
		"quota protection",
		"over 90% usage",
		"soft_quota_threshold_percent",
		"quota reset",
		"quota resets",
		"failed to fetch models.dev",
		"unable to connect. is the computer able to access the url",
		"unable to connect",
		"eperm: operation not permitted",
		"operation not permitted, open",
		"unexpected error, check log file",
		"timed out",
		"deadline exceeded",
		"connection reset",
		"connection refused",
		"temporarily unavailable",
		"network error",
		"tls handshake timeout",
		"service unavailable",
	}
	for _, needle := range needles {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

func runOpencodeWithRetry(cfg runConfig, prompt string) (string, error) {
	attempts := cfg.MaxRetries + 1
	if attempts < 1 {
		attempts = 1
	}
	delay := cfg.RetryDelay
	if delay <= 0 {
		delay = 2 * time.Second
	}

	var raw string
	var err error
	for i := 0; i < attempts; i++ {
		raw, err = runOpencode(cfg.OpencodeBin, cfg.Model, cfg.Timeout, prompt)
		if err == nil {
			return raw, nil
		}
		if !isInfraOpencodeError(err) || i == attempts-1 {
			break
		}
		time.Sleep(delay)
		if delay < 30*time.Second {
			delay = time.Duration(float64(delay) * 1.8)
		}
	}
	return raw, err
}

func runOpencode(bin, model string, timeout time.Duration, prompt string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(
		ctx,
		bin,
		"run",
		"--model",
		model,
		"--format",
		"json",
		prompt,
	)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if opErr := strings.TrimSpace(extractOpencodeError(stdout.Bytes())); opErr != "" {
		return "", fmt.Errorf("opencode returned error event: %s", opErr)
	}
	raw := strings.TrimSpace(extractOpencodeText(stdout.Bytes()))
	if raw == "" {
		raw = strings.TrimSpace(stdout.String())
	}
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return raw, fmt.Errorf("opencode timed out after %s", timeout)
		}
		return raw, fmt.Errorf("opencode run failed: %v (stderr=%s)", err, strings.TrimSpace(stderr.String()))
	}
	if raw == "" {
		return "", errors.New("opencode produced no text response")
	}
	return raw, nil
}

func extractOpencodeText(raw []byte) string {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	out := make([]string, 0, 4)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev opencodeEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if strings.ToLower(strings.TrimSpace(ev.Type)) != "text" {
			continue
		}
		text := strings.TrimSpace(ev.Part.Text)
		if text == "" {
			continue
		}
		out = append(out, text)
	}
	return strings.Join(out, "\n")
}

func extractOpencodeError(raw []byte) string {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var ev opencodeErrorEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(ev.Type), "error") {
			continue
		}
		msg := strings.TrimSpace(ev.Error.Data.Message)
		if msg != "" {
			return msg
		}
		name := strings.TrimSpace(ev.Error.Name)
		if name != "" {
			return name
		}
		return "unknown opencode error"
	}
	return ""
}

func extractSymbolSnippet(path, kind, symbol string, rust bool, maxLines, maxBytes int) (string, lineRange, error) {
	bs, err := os.ReadFile(path)
	if err != nil {
		return "", lineRange{}, fmt.Errorf("read %s: %w", path, err)
	}
	text := string(bs)
	lines := strings.Split(text, "\n")

	token, owner := splitSymbol(symbol)
	if token == "" {
		return "", lineRange{}, fmt.Errorf("empty symbol token for %q", symbol)
	}

	rg, ok := locateSnippetRange(lines, kind, token, owner, rust)
	if !ok {
		return "", lineRange{}, fmt.Errorf("could not locate symbol %q in %s", symbol, path)
	}
	rg = normalizeRange(rg, len(lines))
	if rg.Start <= 0 || rg.End <= 0 {
		return "", lineRange{}, fmt.Errorf("invalid snippet range for %q in %s", symbol, path)
	}
	if maxLines > 0 && rg.End-rg.Start+1 > maxLines {
		rg.End = rg.Start + maxLines - 1
	}

	snippet := formatNumberedLines(lines, rg.Start, rg.End)
	if maxBytes > 0 && len(snippet) > maxBytes {
		snippet = snippet[:maxBytes]
		if !strings.HasSuffix(snippet, "\n") {
			snippet += "\n"
		}
		snippet += "... <truncated>\n"
	}
	return snippet, rg, nil
}

func locateSnippetRange(lines []string, kind, token, owner string, rust bool) (lineRange, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))

	// Struct/interface methods and fields should prioritize owner blocks.
	switch kind {
	case "struct_field":
		if owner != "" {
			if rg, ok := findTypeBlock(lines, owner, rust); ok {
				return rg, true
			}
		}
	case "interface_method":
		if owner != "" {
			if rg, ok := findInterfaceBlock(lines, owner, rust); ok {
				return rg, true
			}
		}
	}

	switch kind {
	case "function", "method", "interface_method":
		if rg, ok := findFunctionBlock(lines, token, rust); ok {
			return rg, true
		}
	case "type":
		if rg, ok := findTypeBlock(lines, token, rust); ok {
			return rg, true
		}
	case "const", "var":
		if rg, ok := findConstVarBlock(lines, token, rust); ok {
			return rg, true
		}
	}

	if owner != "" {
		if rg, ok := findTypeBlock(lines, owner, rust); ok {
			return rg, true
		}
	}

	if idx, ok := findTokenLine(lines, token); ok {
		start := max(1, idx-15)
		end := min(len(lines), idx+20)
		return lineRange{Start: start, End: end}, true
	}
	return lineRange{}, false
}

func splitSymbol(symbol string) (token string, owner string) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return "", ""
	}
	parts := strings.Split(symbol, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	token = parts[len(parts)-1]
	if len(parts) >= 2 {
		owner = parts[len(parts)-2]
	}
	token = strings.TrimPrefix(token, "*")
	owner = strings.TrimPrefix(owner, "*")
	return token, owner
}

func findFunctionBlock(lines []string, name string, rust bool) (lineRange, bool) {
	pat := ""
	if rust {
		pat = `^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+` + regexp.QuoteMeta(name) + `\b`
	} else {
		pat = `^\s*func\s*(?:\([^)]*\)\s*)?` + regexp.QuoteMeta(name) + `(?:\[[^\]]+\])?\s*\(`
	}
	re := regexp.MustCompile(pat)
	fallback := lineRange{}
	hasFallback := false
	for i, line := range lines {
		if !re.MatchString(line) {
			continue
		}
		start := i + 1
		if rust && isRustFnDeclarationOnly(line) {
			if !hasFallback {
				hasFallback = true
				fallback = lineRange{Start: start, End: min(len(lines), start+30)}
			}
			continue
		}
		if rg, ok := findBracedBlock(lines, start); ok {
			return rg, true
		}
		if !hasFallback {
			hasFallback = true
			fallback = lineRange{Start: start, End: min(len(lines), start+30)}
		}
	}
	if hasFallback {
		return fallback, true
	}
	return lineRange{}, false
}

func isRustFnDeclarationOnly(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}
	hasBrace := strings.Contains(trimmed, "{")
	hasSemi := strings.HasSuffix(trimmed, ";")
	return hasSemi && !hasBrace
}

func findTypeBlock(lines []string, name string, rust bool) (lineRange, bool) {
	pat := ""
	if rust {
		pat = `^\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum|trait|type)\s+` + regexp.QuoteMeta(name) + `\b`
	} else {
		pat = `^\s*type\s+` + regexp.QuoteMeta(name) + `\b`
	}
	re := regexp.MustCompile(pat)
	for i, line := range lines {
		if !re.MatchString(line) {
			continue
		}
		start := i + 1
		if rg, ok := findBracedBlock(lines, start); ok {
			return rg, true
		}
		if rg, ok := findParenBlock(lines, start); ok {
			return rg, true
		}
		end := min(len(lines), start+25)
		return lineRange{Start: start, End: end}, true
	}
	return lineRange{}, false
}

func findInterfaceBlock(lines []string, name string, rust bool) (lineRange, bool) {
	pat := ""
	if rust {
		pat = `^\s*(?:pub(?:\([^)]*\))?\s+)?trait\s+` + regexp.QuoteMeta(name) + `\b`
	} else {
		pat = `^\s*type\s+` + regexp.QuoteMeta(name) + `\s+interface\b`
	}
	re := regexp.MustCompile(pat)
	for i, line := range lines {
		if !re.MatchString(line) {
			continue
		}
		start := i + 1
		if rg, ok := findBracedBlock(lines, start); ok {
			return rg, true
		}
		end := min(len(lines), start+40)
		return lineRange{Start: start, End: end}, true
	}
	return lineRange{}, false
}

func findConstVarBlock(lines []string, name string, rust bool) (lineRange, bool) {
	var patterns []string
	if rust {
		patterns = []string{
			`^\s*(?:pub(?:\([^)]*\))?\s+)?const\s+` + regexp.QuoteMeta(name) + `\b`,
			`^\s*(?:pub(?:\([^)]*\))?\s+)?static\s+` + regexp.QuoteMeta(name) + `\b`,
		}
	} else {
		patterns = []string{
			`^\s*const\s+` + regexp.QuoteMeta(name) + `\b`,
			`^\s*var\s+` + regexp.QuoteMeta(name) + `\b`,
			`\b` + regexp.QuoteMeta(name) + `\b`,
		}
	}

	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		for i, line := range lines {
			if !re.MatchString(line) {
				continue
			}
			start := i + 1
			if rg, ok := maybeGroupedDeclBlock(lines, start); ok {
				return rg, true
			}
			return lineRange{Start: max(1, start-3), End: min(len(lines), start+12)}, true
		}
	}
	return lineRange{}, false
}

func maybeGroupedDeclBlock(lines []string, start int) (lineRange, bool) {
	if start <= 0 || start > len(lines) {
		return lineRange{}, false
	}
	line := lines[start-1]
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "const") && !strings.HasPrefix(trimmed, "var") && !strings.HasPrefix(trimmed, "type") {
		return lineRange{}, false
	}
	if !strings.Contains(trimmed, "(") {
		return lineRange{}, false
	}
	return findParenBlock(lines, start)
}

func findParenBlock(lines []string, start int) (lineRange, bool) {
	if start <= 0 || start > len(lines) {
		return lineRange{}, false
	}
	openSeen := false
	depth := 0
	for i := start - 1; i < len(lines); i++ {
		line := lines[i]
		for _, ch := range line {
			switch ch {
			case '(':
				openSeen = true
				depth++
			case ')':
				if openSeen {
					depth--
				}
			}
		}
		if openSeen && depth <= 0 {
			return lineRange{Start: start, End: i + 1}, true
		}
	}
	return lineRange{}, false
}

func findBracedBlock(lines []string, start int) (lineRange, bool) {
	if start <= 0 || start > len(lines) {
		return lineRange{}, false
	}
	openSeen := false
	depth := 0
	for i := start - 1; i < len(lines); i++ {
		line := lines[i]
		for _, ch := range line {
			switch ch {
			case '{':
				openSeen = true
				depth++
			case '}':
				if openSeen {
					depth--
				}
			}
		}
		if openSeen && depth <= 0 {
			return lineRange{Start: start, End: i + 1}, true
		}
	}
	return lineRange{}, false
}

func findTokenLine(lines []string, token string) (int, bool) {
	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(token) + `\b`)
	for i, line := range lines {
		if re.MatchString(line) {
			return i + 1, true
		}
	}
	return 0, false
}

func normalizeRange(rg lineRange, total int) lineRange {
	if rg.Start < 1 {
		rg.Start = 1
	}
	if rg.End < rg.Start {
		rg.End = rg.Start
	}
	if rg.End > total {
		rg.End = total
	}
	return rg
}

func formatNumberedLines(lines []string, start, end int) string {
	var b strings.Builder
	for i := start; i <= end; i++ {
		line := ""
		if i-1 >= 0 && i-1 < len(lines) {
			line = lines[i-1]
		}
		b.WriteString(fmt.Sprintf("%6d | %s\n", i, line))
	}
	return b.String()
}

func rustComponentPath(component string) (string, error) {
	switch strings.TrimSpace(component) {
	case "syncthing-rs/db":
		return "syncthing-rs/src/db.rs", nil
	case "syncthing-rs/model-core":
		return "syncthing-rs/src/model_core.rs", nil
	case "syncthing-rs/folder-core":
		return "syncthing-rs/src/folder_core.rs", nil
	case "syncthing-rs/config":
		return "syncthing-rs/src/config.rs", nil
	case "syncthing-rs/bep-core":
		return "syncthing-rs/src/bep_core.rs", nil
	case "syncthing-rs/bep-compat":
		return "syncthing-rs/src/bep_compat.rs", nil
	default:
		return "", fmt.Errorf("unknown rust component %q", component)
	}
}

func classifyTier(item mappingItem) string {
	kind := strings.ToLower(strings.TrimSpace(item.Kind))
	component := strings.TrimSpace(item.RustComponent)
	nameBlob := strings.ToLower(item.Symbol + " " + item.RustSymbol)

	switch kind {
	case "struct_field", "type", "const", "var":
		return "T2"
	}

	score := 0
	switch kind {
	case "function", "method":
		score += 4
	case "interface_method":
		score += 6
	default:
		score += 1
	}

	switch component {
	case "syncthing-rs/model-core", "syncthing-rs/folder-core", "syncthing-rs/db", "syncthing-rs/bep-compat":
		score += 4
	case "syncthing-rs/config", "syncthing-rs/bep-core":
		score += 1
	}

	if logicNamePattern.MatchString(nameBlob) {
		score += 3
	}
	if criticalLogicPattern.MatchString(nameBlob) {
		score += 2
	}

	if score >= 13 {
		return "T1"
	}
	return "T3"
}

func filterPlanItems(items []reviewPlanItem, tierFilter string, offset, limit int) []reviewPlanItem {
	filtered := make([]reviewPlanItem, 0, len(items))
	tierFilter = normalizeTier(tierFilter)
	for _, item := range items {
		if tierFilter != "all" && strings.ToUpper(item.Tier) != tierFilter {
			continue
		}
		filtered = append(filtered, item)
	}
	if offset > 0 {
		if offset >= len(filtered) {
			return []reviewPlanItem{}
		}
		filtered = filtered[offset:]
	}
	if limit > 0 && limit < len(filtered) {
		filtered = filtered[:limit]
	}
	return filtered
}

func normalizeTier(tier string) string {
	switch strings.ToUpper(strings.TrimSpace(tier)) {
	case "T1":
		return "T1"
	case "T2":
		return "T2"
	case "T3":
		return "T3"
	default:
		return "all"
	}
}

func statusAllowed(status, filter string) bool {
	filter = normalizeStatusFilter(filter)
	if filter == "all" {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(status), filter)
}

func normalizeStatusFilter(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "missing":
		return "missing"
	case "implemented":
		return "implemented"
	case "parity-verified":
		return "parity-verified"
	default:
		return "all"
	}
}

func splitNonEmptyLines(raw string) []string {
	lines := strings.Split(raw, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func safeID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		id = "item"
	}
	re := regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
	id = re.ReplaceAllString(id, "_")
	return id
}

func loadReviewCache(path string) (*reviewCacheStore, error) {
	store := newReviewCacheStore()
	path = strings.TrimSpace(path)
	if path == "" {
		return store, nil
	}
	bs, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	payload := reviewCacheFile{}
	if len(bytes.TrimSpace(bs)) == 0 {
		return store, nil
	}
	if err := json.Unmarshal(bs, &payload); err != nil {
		return nil, err
	}
	for id, entry := range payload.Items {
		if entry.ID == "" {
			entry.ID = id
		}
		store.items[id] = entry
	}
	return store, nil
}

func saveReviewCache(path string, store *reviewCacheStore) error {
	path = strings.TrimSpace(path)
	if path == "" || store == nil {
		return nil
	}
	payload := reviewCacheFile{
		SchemaVersion: pairReviewSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Items:         store.Snapshot(),
	}
	return writeJSON(path, payload)
}

func writeReviewRollups(
	outDir string,
	items []reviewPlanItem,
	report reviewReport,
	maxFindings int,
) error {
	if maxFindings <= 0 {
		maxFindings = 12
	}
	idToItem := make(map[string]reviewPlanItem, len(items))
	for _, item := range items {
		idToItem[item.ID] = item
	}

	type fileBucket struct {
		RustPath string
		Findings []pairFinding
	}
	buckets := map[string]*fileBucket{}
	touchedPaths := map[string]struct{}{}
	for _, item := range items {
		rustPath := strings.TrimSpace(item.RustPath)
		if rustPath == "" {
			continue
		}
		touchedPaths[rustPath] = struct{}{}
	}
	for _, finding := range report.Findings {
		rustPath := ""
		if item, ok := idToItem[finding.ID]; ok {
			rustPath = strings.TrimSpace(item.RustPath)
		}
		if rustPath == "" {
			rustPath = locationFilePath(finding.Location)
		}
		if rustPath == "" {
			rustPath = "unknown"
		}
		bucket, ok := buckets[rustPath]
		if !ok {
			bucket = &fileBucket{RustPath: rustPath}
			buckets[rustPath] = bucket
		}
		bucket.Findings = append(bucket.Findings, finding)
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	paths := make([]string, 0, len(touchedPaths))
	for path := range touchedPaths {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		bucket, ok := buckets[path]
		if !ok || len(bucket.Findings) == 0 {
			_ = os.Remove(filepath.Join(outDir, rollupFileName(path)))
			continue
		}
		sort.Slice(bucket.Findings, func(i, j int) bool {
			ri := severityRank(bucket.Findings[i].Priority)
			rj := severityRank(bucket.Findings[j].Priority)
			if ri == rj {
				if bucket.Findings[i].Title == bucket.Findings[j].Title {
					return bucket.Findings[i].Location < bucket.Findings[j].Location
				}
				return bucket.Findings[i].Title < bucket.Findings[j].Title
			}
			return ri < rj
		})
		counts := findingPriorityCounts(bucket.Findings)
		fileName := rollupFileName(path)

		var b strings.Builder
		b.WriteString(fmt.Sprintf("# File Review: `%s`\n\n", path))
		meta := rollupMeta{
			RustPath: path,
			Findings: len(bucket.Findings),
			P0:       counts["P0"],
			P1:       counts["P1"],
			P2:       counts["P2"],
			P3:       counts["P3"],
		}
		b.WriteString(renderRollupMeta(meta))
		b.WriteByte('\n')
		b.WriteString(fmt.Sprintf(
			"- Updated: %s\n- Findings: %d (P0=%d, P1=%d, P2=%d, P3=%d)\n\n",
			time.Now().UTC().Format(time.RFC3339),
			len(bucket.Findings),
			counts["P0"],
			counts["P1"],
			counts["P2"],
			counts["P3"],
		))
		b.WriteString("## Top Findings\n")
		limit := min(maxFindings, len(bucket.Findings))
		for i := 0; i < limit; i++ {
			f := bucket.Findings[i]
			summary := trimForRollup(f.Message, 220)
			b.WriteString(fmt.Sprintf(
				"%d. [%s] %s (`%s`) %s\n",
				i+1,
				f.Priority,
				trimForRollup(f.Title, 80),
				trimForRollup(f.Location, 120),
				summary,
			))
		}
		if len(bucket.Findings) > limit {
			b.WriteString(fmt.Sprintf(
				"\n_Additional findings omitted: %d_\n",
				len(bucket.Findings)-limit,
			))
		}

		if err := os.WriteFile(filepath.Join(outDir, fileName), []byte(b.String()), 0o644); err != nil {
			return err
		}
	}

	metas, err := readRollupMetas(outDir)
	if err != nil {
		return err
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].RustPath < metas[j].RustPath })

	indexRows := make([]string, 0, len(metas))
	totalFindings := 0
	for _, meta := range metas {
		totalFindings += meta.Findings
		indexRows = append(indexRows, fmt.Sprintf(
			"| `%s` | %d | %d | %d | %d | %d |",
			meta.RustPath,
			meta.Findings,
			meta.P0,
			meta.P1,
			meta.P2,
			meta.P3,
		))
	}

	var index strings.Builder
	index.WriteString("# Function-Pair Review Rollups\n\n")
	index.WriteString(fmt.Sprintf("- Updated: %s\n", time.Now().UTC().Format(time.RFC3339)))
	index.WriteString(fmt.Sprintf("- Files with findings: %d\n", len(metas)))
	index.WriteString(fmt.Sprintf("- Total findings: %d\n\n", totalFindings))
	index.WriteString("| Rust file | Findings | P0 | P1 | P2 | P3 |\n")
	index.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, row := range indexRows {
		index.WriteString(row)
		index.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(outDir, "_index.md"), []byte(index.String()), 0o644); err != nil {
		return err
	}
	return nil
}

func findingPriorityCounts(findings []pairFinding) map[string]int {
	out := map[string]int{"P0": 0, "P1": 0, "P2": 0, "P3": 0}
	for _, finding := range findings {
		if _, ok := out[finding.Priority]; !ok {
			out[finding.Priority] = 0
		}
		out[finding.Priority]++
	}
	return out
}

func severityRank(priority string) int {
	switch strings.ToUpper(strings.TrimSpace(priority)) {
	case "P0":
		return 0
	case "P1":
		return 1
	case "P2":
		return 2
	case "P3":
		return 3
	default:
		return 4
	}
}

func locationFilePath(location string) string {
	location = strings.TrimSpace(location)
	if location == "" {
		return ""
	}
	if idx := strings.Index(location, ":"); idx > 0 {
		return filepath.ToSlash(location[:idx])
	}
	return filepath.ToSlash(location)
}

func rollupFileName(path string) string {
	file := strings.TrimSpace(path)
	if file == "" {
		file = "unknown"
	}
	file = strings.ReplaceAll(file, "/", "__")
	file = strings.ReplaceAll(file, "\\", "__")
	file = strings.ReplaceAll(file, ":", "_")
	return safeID(file) + ".md"
}

func trimForRollup(text string, maxLen int) string {
	text = strings.TrimSpace(strings.ReplaceAll(text, "\n", " "))
	if maxLen <= 0 || len(text) <= maxLen {
		return text
	}
	if maxLen <= 3 {
		return text[:maxLen]
	}
	return text[:maxLen-3] + "..."
}

func renderRollupMeta(meta rollupMeta) string {
	bs, err := json.Marshal(meta)
	if err != nil {
		return ""
	}
	return rollupMetaPrefix + string(bs) + " -->"
}

func parseRollupMeta(content string) (rollupMeta, bool) {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, rollupMetaPrefix) || !strings.HasSuffix(line, " -->") {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(line, rollupMetaPrefix), " -->")
		meta := rollupMeta{}
		if err := json.Unmarshal([]byte(raw), &meta); err != nil {
			continue
		}
		if strings.TrimSpace(meta.RustPath) == "" {
			continue
		}
		return meta, true
	}
	return rollupMeta{}, false
}

func readRollupMetas(outDir string) ([]rollupMeta, error) {
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return nil, err
	}
	metas := make([]rollupMeta, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") || entry.Name() == "_index.md" {
			continue
		}
		path := filepath.Join(outDir, entry.Name())
		bs, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		meta, ok := parseRollupMeta(string(bs))
		if !ok {
			continue
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

func readJSON(path string, out any) error {
	bs, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(bs, out)
}

func writeJSON(path string, v any) error {
	bs, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	bs = append(bs, '\n')
	return os.WriteFile(path, bs, 0o644)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
