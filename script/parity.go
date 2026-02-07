// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	schemaVersion = 1
)

var sourcePatterns = []string{
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

var parityCriticalRoots = []string{"rust", "crates", "syncthing-rs"}

var allowedExceptionRules = map[string]struct{}{
	"status-missing":        {},
	"rust-surface-missing":  {},
	"rust-surface-unmapped": {},
	"todo-fixme":            {},
}

type featureManifest struct {
	SchemaVersion int           `json:"schema_version"`
	Sources       []string      `json:"sources"`
	Items         []featureItem `json:"items"`
}

type featureItem struct {
	ID            string   `json:"id"`
	Source        string   `json:"source"`
	Subsystem     string   `json:"subsystem"`
	Kind          string   `json:"kind"`
	Symbol        string   `json:"symbol"`
	Behavior      string   `json:"behavior"`
	RustComponent string   `json:"rust_component"`
	RustSymbol    string   `json:"rust_symbol"`
	RequiredTests []string `json:"required_tests"`
	Status        string   `json:"status"`
}

type mappingFile struct {
	SchemaVersion int           `json:"schema_version"`
	Items         []mappingItem `json:"items"`
}

type mappingItem struct {
	ID            string   `json:"id"`
	Source        string   `json:"source"`
	Subsystem     string   `json:"subsystem"`
	Kind          string   `json:"kind"`
	Symbol        string   `json:"symbol"`
	RustComponent string   `json:"rust_component"`
	RustSymbol    string   `json:"rust_symbol"`
	RequiredTests []string `json:"required_tests"`
	Status        string   `json:"status"`
}

type exceptionsFile struct {
	SchemaVersion int              `json:"schema_version"`
	Items         []guardException `json:"items"`
}

type guardException struct {
	Rule           string `json:"rule"`
	ID             string `json:"id,omitempty"`
	Path           string `json:"path,omitempty"`
	Reason         string `json:"reason"`
	ApprovedBy     string `json:"approved_by"`
	DecisionRecord string `json:"decision_record"`
	ExpiresAt      string `json:"expires_at,omitempty"`
}

type guardrailReport struct {
	SchemaVersion int             `json:"schema_version"`
	GeneratedAt   string          `json:"generated_at"`
	Mode          string          `json:"mode"`
	Summary       reportSummary   `json:"summary"`
	Failures      []reportFailure `json:"failures"`
}

type reportSummary struct {
	TotalFeatures  int `json:"total_features"`
	Missing        int `json:"missing"`
	Implemented    int `json:"implemented"`
	ParityVerified int `json:"parity_verified"`
	Failures       int `json:"failures"`
}

type reportFailure struct {
	Rule    string `json:"rule"`
	ID      string `json:"id,omitempty"`
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

type differentialReport struct {
	SchemaVersion int            `json:"schema_version"`
	GeneratedAt   string         `json:"generated_at"`
	ProducedBy    string         `json:"produced_by,omitempty"`
	Match         bool           `json:"match"`
	Mismatches    []diffMismatch `json:"mismatches"`
	Scenarios     []diffScenario `json:"scenarios"`
}

type diffMismatch struct {
	ID       string `json:"id"`
	Severity string `json:"severity"`
	Open     bool   `json:"open"`
	Message  string `json:"message"`
}

type diffScenario struct {
	ID       string `json:"id"`
	Severity string `json:"severity"`
	Required bool   `json:"required"`
	Status   string `json:"status"`
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

type harnessScenarioFile struct {
	SchemaVersion int               `json:"schema_version"`
	Scenarios     []harnessScenario `json:"scenarios"`
}

type harnessScenario struct {
	ID string `json:"id"`
}

type requiredTestEvidence struct {
	ScenarioIDs     map[string]struct{}
	ScenarioOutcome map[string]string
}

type dashboardJSON struct {
	SchemaVersion int                `json:"schema_version"`
	GeneratedAt   string             `json:"generated_at"`
	Totals        reportSummary      `json:"totals"`
	Subsystems    []dashboardSection `json:"subsystems"`
}

type dashboardSection struct {
	Name           string  `json:"name"`
	Total          int     `json:"total"`
	Missing        int     `json:"missing"`
	Implemented    int     `json:"implemented"`
	ParityVerified int     `json:"parity_verified"`
	Coverage       float64 `json:"coverage_percent"`
}

type featureKey struct {
	Source string
	Kind   string
	Symbol string
}

func main() {
	if len(os.Args) < 2 {
		fatalf("usage: go run script/parity.go <generate|check|dashboard>")
	}

	switch os.Args[1] {
	case "generate":
		runGenerate(os.Args[2:])
	case "check":
		runCheck(os.Args[2:])
	case "dashboard":
		runDashboard(os.Args[2:])
	default:
		fatalf("unknown command %q", os.Args[1])
	}
}

func runGenerate(args []string) {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	manifestPath := fs.String("manifest", "parity/feature-manifest.json", "manifest output path")
	mappingPath := fs.String("mapping", "parity/mapping-rust.json", "mapping output path")
	exceptionsPath := fs.String("exceptions", "parity/exceptions.json", "exceptions output path")
	ensureDiff := fs.Bool("ensure-diff", true, "ensure differential report stubs exist")
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}

	sources, err := resolveSources(sourcePatterns)
	if err != nil {
		fatalf("resolve sources: %v", err)
	}

	items, err := generateFeatures(sources)
	if err != nil {
		fatalf("generate features: %v", err)
	}

	mapping := loadMapping(*mappingPath)
	exceptions := loadOrInitExceptions(*exceptionsPath)

	mappingByID := make(map[string]mappingItem, len(mapping.Items))
	for _, it := range mapping.Items {
		mappingByID[it.ID] = it
	}

	manifestItems := make([]featureItem, 0, len(items))
	mappingItems := make([]mappingItem, 0, len(items))

	for _, item := range items {
		existing, ok := mappingByID[item.ID]
		if ok {
			if existing.RustComponent != "" {
				item.RustComponent = existing.RustComponent
			}
			if existing.RustSymbol != "" {
				item.RustSymbol = existing.RustSymbol
			}
			if len(existing.RequiredTests) > 0 {
				item.RequiredTests = existing.RequiredTests
			}
			if existing.Status != "" {
				item.Status = existing.Status
			}
		}
		if len(item.RequiredTests) == 0 {
			item.RequiredTests = []string{fmt.Sprintf("parity/%s", item.ID)}
		}
		if item.Status == "" {
			item.Status = "missing"
		}
		manifestItems = append(manifestItems, item)
		mappingItems = append(mappingItems, mappingItem{
			ID:            item.ID,
			Source:        item.Source,
			Subsystem:     item.Subsystem,
			Kind:          item.Kind,
			Symbol:        item.Symbol,
			RustComponent: item.RustComponent,
			RustSymbol:    item.RustSymbol,
			RequiredTests: item.RequiredTests,
			Status:        item.Status,
		})
	}

	sort.Slice(manifestItems, func(i, j int) bool {
		if manifestItems[i].Source != manifestItems[j].Source {
			return manifestItems[i].Source < manifestItems[j].Source
		}
		if manifestItems[i].Kind != manifestItems[j].Kind {
			return manifestItems[i].Kind < manifestItems[j].Kind
		}
		return manifestItems[i].Symbol < manifestItems[j].Symbol
	})

	sort.Slice(mappingItems, func(i, j int) bool {
		if mappingItems[i].Source != mappingItems[j].Source {
			return mappingItems[i].Source < mappingItems[j].Source
		}
		if mappingItems[i].Kind != mappingItems[j].Kind {
			return mappingItems[i].Kind < mappingItems[j].Kind
		}
		return mappingItems[i].Symbol < mappingItems[j].Symbol
	})

	manifest := featureManifest{
		SchemaVersion: schemaVersion,
		Sources:       sources,
		Items:         manifestItems,
	}

	mapping = mappingFile{
		SchemaVersion: schemaVersion,
		Items:         mappingItems,
	}

	if err := writeJSON(*manifestPath, manifest); err != nil {
		fatalf("write manifest: %v", err)
	}
	if err := writeJSON(*mappingPath, mapping); err != nil {
		fatalf("write mapping: %v", err)
	}
	if err := writeJSON(*exceptionsPath, exceptions); err != nil {
		fatalf("write exceptions: %v", err)
	}

	if *ensureDiff {
		if err := ensureDiffReports(); err != nil {
			fatalf("ensure diff reports: %v", err)
		}
	}
}

func runCheck(args []string) {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	manifestPath := fs.String("manifest", "parity/feature-manifest.json", "manifest input path")
	mappingPath := fs.String("mapping", "parity/mapping-rust.json", "mapping input path")
	exceptionsPath := fs.String("exceptions", "parity/exceptions.json", "exceptions input path")
	mode := fs.String("mode", "ci", "check mode: ci or release")
	reportPath := fs.String("report", "parity/guardrail-report.json", "report output path")
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}
	if *mode != "ci" && *mode != "release" {
		fatalf("invalid mode %q", *mode)
	}

	manifest, err := readManifest(*manifestPath)
	if err != nil {
		fatalf("read manifest: %v", err)
	}
	mapping, err := readMapping(*mappingPath)
	if err != nil {
		fatalf("read mapping: %v", err)
	}
	exceptions, err := readExceptions(*exceptionsPath)
	if err != nil {
		fatalf("read exceptions: %v", err)
	}

	mappingByID := make(map[string]mappingItem, len(mapping.Items))
	for _, it := range mapping.Items {
		mappingByID[it.ID] = it
	}
	manifestByID := make(map[string]featureItem, len(manifest.Items))
	for _, feat := range manifest.Items {
		manifestByID[feat.ID] = feat
	}

	report := guardrailReport{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Mode:          *mode,
	}
	testEvidence := loadRequiredTestEvidence(&report)

	validateExceptions(&report, exceptions, *mode)

	for _, feat := range manifest.Items {
		mi, ok := mappingByID[feat.ID]
		if !ok {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "mapping-missing",
				ID:      feat.ID,
				Message: "feature is missing from mapping-rust.json",
			})
			continue
		}

		status := mi.Status
		switch status {
		case "missing", "implemented", "parity-verified":
		default:
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "status-invalid",
				ID:      mi.ID,
				Message: fmt.Sprintf("invalid status %q", status),
			})
		}

		switch status {
		case "missing":
			report.Summary.Missing++
			if !exceptionAllowed(exceptions.Items, "status-missing", mi.ID, "") {
				report.Failures = append(report.Failures, reportFailure{
					Rule:    "status-missing",
					ID:      mi.ID,
					Message: "feature status is missing",
				})
			}
		case "implemented":
			report.Summary.Implemented++
			validateImplementedLike(&report, feat, mi, testEvidence)
		case "parity-verified":
			report.Summary.ParityVerified++
			validateImplementedLike(&report, feat, mi, testEvidence)
		}
	}

	report.Summary.TotalFeatures = len(manifest.Items)

	for _, mi := range mapping.Items {
		feat, ok := manifestByID[mi.ID]
		if !ok {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "mapping-stale",
				ID:      mi.ID,
				Message: "mapping item does not exist in feature-manifest.json",
			})
			continue
		}
		if mi.Source != feat.Source || mi.Subsystem != feat.Subsystem || mi.Kind != feat.Kind || mi.Symbol != feat.Symbol {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "mapping-drift",
				ID:      mi.ID,
				Message: "mapping source metadata differs from manifest entry",
			})
		}
	}

	rustSymbols, publicRustSymbols := scanRustSurfaceSymbols()
	if len(rustSymbols) > 0 {
		mappedRustSymbols := make(map[string]mappingItem)
		for _, mi := range mapping.Items {
			if mi.RustSymbol == "" {
				continue
			}
			mappedRustSymbols[mi.RustSymbol] = mi
			if (mi.Status == "implemented" || mi.Status == "parity-verified") && !rustSymbols[mi.RustSymbol] {
				if !exceptionAllowed(exceptions.Items, "rust-surface-missing", mi.ID, "") {
					report.Failures = append(report.Failures, reportFailure{
						Rule:    "rust-surface-missing",
						ID:      mi.ID,
						Message: fmt.Sprintf("rust symbol %q not found in rust/crates/syncthing-rs sources", mi.RustSymbol),
					})
				}
			}
		}
		for sym := range publicRustSymbols {
			if _, ok := mappedRustSymbols[sym]; !ok {
				if !exceptionAllowed(exceptions.Items, "rust-surface-unmapped", "", sym) {
					report.Failures = append(report.Failures, reportFailure{
						Rule:    "rust-surface-unmapped",
						Path:    sym,
						Message: "rust public symbol has no mapping entry",
					})
				}
			}
		}
	}

	scanTODOFixme(&report, exceptions.Items)
	enforceDiffReports(&report, *mode)

	if *mode == "release" {
		if report.Summary.TotalFeatures == 0 || report.Summary.ParityVerified != report.Summary.TotalFeatures {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "release-gate",
				Message: fmt.Sprintf("release mode requires 100%% parity-verified: have %d/%d", report.Summary.ParityVerified, report.Summary.TotalFeatures),
			})
		}
	}

	report.Summary.Failures = len(report.Failures)

	if err := writeJSON(*reportPath, report); err != nil {
		fatalf("write report: %v", err)
	}

	if len(report.Failures) > 0 {
		os.Exit(1)
	}
}

func runDashboard(args []string) {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	manifestPath := fs.String("manifest", "parity/feature-manifest.json", "manifest input path")
	mappingPath := fs.String("mapping", "parity/mapping-rust.json", "mapping input path")
	outJSON := fs.String("json", "parity/dashboard.json", "dashboard json path")
	outMD := fs.String("markdown", "parity/dashboard.md", "dashboard markdown path")
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}

	manifest, err := readManifest(*manifestPath)
	if err != nil {
		fatalf("read manifest: %v", err)
	}
	mapping, err := readMapping(*mappingPath)
	if err != nil {
		fatalf("read mapping: %v", err)
	}

	mappingByID := make(map[string]mappingItem, len(mapping.Items))
	for _, it := range mapping.Items {
		mappingByID[it.ID] = it
	}

	totals := reportSummary{TotalFeatures: len(manifest.Items)}
	bySubsystem := make(map[string]*dashboardSection)

	for _, feat := range manifest.Items {
		mi := mappingByID[feat.ID]
		section, ok := bySubsystem[feat.Subsystem]
		if !ok {
			section = &dashboardSection{Name: feat.Subsystem}
			bySubsystem[feat.Subsystem] = section
		}
		section.Total++

		switch mi.Status {
		case "parity-verified":
			totals.ParityVerified++
			section.ParityVerified++
		case "implemented":
			totals.Implemented++
			section.Implemented++
		default:
			totals.Missing++
			section.Missing++
		}
	}

	sections := make([]dashboardSection, 0, len(bySubsystem))
	for _, section := range bySubsystem {
		if section.Total > 0 {
			section.Coverage = 100 * float64(section.ParityVerified) / float64(section.Total)
		}
		sections = append(sections, *section)
	}
	sort.Slice(sections, func(i, j int) bool { return sections[i].Name < sections[j].Name })

	dashboard := dashboardJSON{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Totals:        totals,
		Subsystems:    sections,
	}

	if err := writeJSON(*outJSON, dashboard); err != nil {
		fatalf("write dashboard json: %v", err)
	}

	var b strings.Builder
	b.WriteString("# Parity Dashboard\n\n")
	b.WriteString(fmt.Sprintf("Generated: %s UTC\n\n", time.Now().UTC().Format(time.RFC3339)))
	b.WriteString("## Totals\n\n")
	b.WriteString("| Metric | Value |\n")
	b.WriteString("|---|---:|\n")
	b.WriteString(fmt.Sprintf("| Total features | %d |\n", totals.TotalFeatures))
	b.WriteString(fmt.Sprintf("| Missing | %d |\n", totals.Missing))
	b.WriteString(fmt.Sprintf("| Implemented | %d |\n", totals.Implemented))
	b.WriteString(fmt.Sprintf("| Parity verified | %d |\n", totals.ParityVerified))

	b.WriteString("\n## By Subsystem\n\n")
	b.WriteString("| Subsystem | Total | Missing | Implemented | Parity Verified | Coverage |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|\n")
	for _, section := range sections {
		b.WriteString(fmt.Sprintf("| %s | %d | %d | %d | %d | %.2f%% |\n",
			section.Name,
			section.Total,
			section.Missing,
			section.Implemented,
			section.ParityVerified,
			section.Coverage,
		))
	}

	if err := writeText(*outMD, b.String()); err != nil {
		fatalf("write dashboard markdown: %v", err)
	}
}

func generateFeatures(sources []string) ([]featureItem, error) {
	fset := token.NewFileSet()
	seen := make(map[featureKey]featureItem)

	for _, source := range sources {
		file, err := parser.ParseFile(fset, source, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", source, err)
		}

		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				kind := "function"
				symbol := d.Name.Name
				if d.Recv != nil {
					kind = "method"
					recv := receiverType(d.Recv.List)
					symbol = recv + "." + d.Name.Name
				}
				addFeature(seen, makeFeature(source, subsystemForPath(source), kind, symbol, nodeDoc(d.Doc)))

			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch s := spec.(type) {
					case *ast.TypeSpec:
						specDoc := nodeDoc(d.Doc, s.Doc)
						addFeature(seen, makeFeature(source, subsystemForPath(source), "type", s.Name.Name, specDoc))
						switch t := s.Type.(type) {
						case *ast.StructType:
							for _, field := range t.Fields.List {
								fieldDoc := nodeDoc(field.Doc, field.Comment)
								if fieldDoc == "" {
									fieldDoc = specDoc
								}
								names := fieldNames(field)
								for _, name := range names {
									addFeature(seen, makeFeature(source, subsystemForPath(source), "struct_field", s.Name.Name+"."+name, fieldDoc))
								}
							}
						case *ast.InterfaceType:
							for _, field := range t.Methods.List {
								methodDoc := nodeDoc(field.Doc, field.Comment)
								if methodDoc == "" {
									methodDoc = specDoc
								}
								names := fieldNames(field)
								for _, name := range names {
									addFeature(seen, makeFeature(source, subsystemForPath(source), "interface_method", s.Name.Name+"."+name, methodDoc))
								}
							}
						}
					case *ast.ValueSpec:
						kind := "var"
						if d.Tok == token.CONST {
							kind = "const"
						}
						comment := nodeDoc(d.Doc, s.Doc)
						for _, n := range s.Names {
							addFeature(seen, makeFeature(source, subsystemForPath(source), kind, n.Name, comment))
						}
					}
				}
			}
		}
	}

	items := make([]featureItem, 0, len(seen))
	for _, feat := range seen {
		if feat.Behavior == "" {
			feat.Behavior = fmt.Sprintf("Auto-generated from %s `%s` in `%s`.", feat.Kind, feat.Symbol, feat.Source)
		}
		feat.ID = featureID(feat.Source, feat.Kind, feat.Symbol)
		feat.RequiredTests = []string{fmt.Sprintf("parity/%s", feat.ID)}
		feat.Status = "missing"
		items = append(items, feat)
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Source != items[j].Source {
			return items[i].Source < items[j].Source
		}
		if items[i].Kind != items[j].Kind {
			return items[i].Kind < items[j].Kind
		}
		return items[i].Symbol < items[j].Symbol
	})

	return items, nil
}

func receiverType(list []*ast.Field) string {
	if len(list) == 0 {
		return "<unknown>"
	}
	expr := list[0].Type
	for {
		switch t := expr.(type) {
		case *ast.Ident:
			return t.Name
		case *ast.StarExpr:
			expr = t.X
		case *ast.IndexExpr:
			expr = t.X
		case *ast.IndexListExpr:
			expr = t.X
		default:
			return exprString(expr)
		}
	}
}

func fieldNames(field *ast.Field) []string {
	if len(field.Names) == 0 {
		return []string{exprString(field.Type)}
	}
	names := make([]string, 0, len(field.Names))
	for _, n := range field.Names {
		names = append(names, n.Name)
	}
	return names
}

func exprString(expr ast.Expr) string {
	var b strings.Builder
	_ = printer.Fprint(&b, token.NewFileSet(), expr)
	return b.String()
}

func makeFeature(source, subsystem, kind, symbol, behavior string) featureItem {
	return featureItem{
		Source:    source,
		Subsystem: subsystem,
		Kind:      kind,
		Symbol:    symbol,
		Behavior:  cleanDoc(behavior),
	}
}

func addFeature(seen map[featureKey]featureItem, feat featureItem) {
	key := featureKey{Source: feat.Source, Kind: feat.Kind, Symbol: feat.Symbol}
	if existing, ok := seen[key]; ok {
		if existing.Behavior == "" && feat.Behavior != "" {
			seen[key] = feat
		}
		return
	}
	seen[key] = feat
}

func nodeDoc(groups ...*ast.CommentGroup) string {
	for _, g := range groups {
		if g == nil {
			continue
		}
		t := strings.TrimSpace(g.Text())
		if t != "" {
			return t
		}
	}
	return ""
}

func cleanDoc(doc string) string {
	doc = strings.ReplaceAll(doc, "\n", " ")
	doc = strings.Join(strings.Fields(doc), " ")
	return strings.TrimSpace(doc)
}

func featureID(source, kind, symbol string) string {
	sum := sha1.Sum([]byte(source + "|" + kind + "|" + symbol))
	return "feat-" + hex.EncodeToString(sum[:])[:12]
}

func subsystemForPath(path string) string {
	switch {
	case strings.HasPrefix(path, "lib/config/"):
		return "config"
	case strings.HasPrefix(path, "lib/model/"):
		return "model"
	case strings.HasPrefix(path, "lib/protocol/"):
		return "protocol"
	case strings.HasPrefix(path, "internal/gen/bep/"):
		return "protocol"
	case strings.HasPrefix(path, "internal/db/"):
		return "db"
	default:
		parts := strings.Split(path, "/")
		if len(parts) > 0 {
			return parts[0]
		}
		return "unknown"
	}
}

func resolveSources(patterns []string) ([]string, error) {
	set := make(map[string]struct{})
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("glob %q: %w", pattern, err)
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("pattern %q matched no files", pattern)
		}
		for _, match := range matches {
			rel := filepath.ToSlash(match)
			set[rel] = struct{}{}
		}
	}

	sources := make([]string, 0, len(set))
	for source := range set {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	return sources, nil
}

func loadMapping(path string) mappingFile {
	mapping, err := readMapping(path)
	if err != nil {
		return mappingFile{SchemaVersion: schemaVersion}
	}
	return mapping
}

func loadOrInitExceptions(path string) exceptionsFile {
	ex, err := readExceptions(path)
	if err != nil {
		return exceptionsFile{SchemaVersion: schemaVersion, Items: []guardException{}}
	}
	if ex.Items == nil {
		ex.Items = []guardException{}
	}
	return ex
}

func loadRequiredTestEvidence(report *guardrailReport) requiredTestEvidence {
	ev := requiredTestEvidence{
		ScenarioIDs:     make(map[string]struct{}),
		ScenarioOutcome: make(map[string]string),
	}

	cfg := harnessScenarioFile{}
	if err := readJSON("parity/harness/scenarios.json", &cfg); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "required-tests",
			Path:    "parity/harness/scenarios.json",
			Message: "missing or invalid harness scenarios file",
		})
		return ev
	}

	for _, sc := range cfg.Scenarios {
		id := strings.TrimSpace(sc.ID)
		if id == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "required-tests",
				Path:    "parity/harness/scenarios.json",
				Message: "scenario entry with empty id",
			})
			continue
		}
		if _, exists := ev.ScenarioIDs[id]; exists {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "required-tests",
				Path:    "parity/harness/scenarios.json",
				Message: fmt.Sprintf("duplicate scenario id %q", id),
			})
			continue
		}
		ev.ScenarioIDs[id] = struct{}{}
	}

	latest := differentialReport{}
	if err := readJSON("parity/diff-reports/latest.json", &latest); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "required-tests",
			Path:    "parity/diff-reports/latest.json",
			Message: "missing or invalid latest differential report for scenario outcomes",
		})
		return ev
	}
	for _, sc := range latest.Scenarios {
		id := strings.TrimSpace(sc.ID)
		if id == "" {
			continue
		}
		ev.ScenarioOutcome[id] = strings.TrimSpace(sc.Status)
	}
	return ev
}

func validateImplementedLike(report *guardrailReport, feat featureItem, mi mappingItem, ev requiredTestEvidence) {
	if strings.TrimSpace(mi.RustComponent) == "" {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "mapping-rust-component",
			ID:      mi.ID,
			Message: "implemented feature is missing rust_component",
		})
	}
	if strings.TrimSpace(mi.RustSymbol) == "" {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "mapping-rust-symbol",
			ID:      mi.ID,
			Message: "implemented feature is missing rust_symbol",
		})
	}
	if len(mi.RequiredTests) == 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "mapping-required-tests",
			ID:      mi.ID,
			Message: "implemented feature has no required_tests",
		})
		return
	}

	scenarioRefs := 0
	for _, testRef := range mi.RequiredTests {
		testRef = strings.TrimSpace(testRef)
		if testRef == "" {
			continue
		}
		if !strings.HasPrefix(testRef, "scenario/") {
			continue
		}
		scenarioRefs++
		scenarioID := strings.TrimSpace(strings.TrimPrefix(testRef, "scenario/"))
		if scenarioID == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "mapping-required-tests",
				ID:      mi.ID,
				Message: "scenario/ required_test has empty scenario id",
			})
			continue
		}
		if _, ok := ev.ScenarioIDs[scenarioID]; !ok {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "mapping-required-tests",
				ID:      mi.ID,
				Message: fmt.Sprintf("required_test %q does not map to parity/harness/scenarios.json", testRef),
			})
			continue
		}
		if mi.Status == "parity-verified" {
			status := strings.ToLower(strings.TrimSpace(ev.ScenarioOutcome[scenarioID]))
			if status != "pass" {
				report.Failures = append(report.Failures, reportFailure{
					Rule:    "mapping-required-tests",
					ID:      mi.ID,
					Message: fmt.Sprintf("parity-verified feature requires passing %q (current status=%q)", testRef, status),
				})
			}
		}
	}
	if scenarioRefs == 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "mapping-required-tests",
			ID:      mi.ID,
			Message: fmt.Sprintf("feature %s (%s) must include at least one scenario/<id> required_test", feat.Symbol, feat.Source),
		})
	}
}

func validateExceptions(report *guardrailReport, ex exceptionsFile, mode string) {
	now := time.Now().UTC()

	if mode == "release" && len(ex.Items) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "release-exceptions",
			Path:    "parity/exceptions.json",
			Message: "release mode requires parity/exceptions.json to be empty",
		})
	}

	for i, item := range ex.Items {
		loc := fmt.Sprintf("parity/exceptions.json:item[%d]", i)
		if _, ok := allowedExceptionRules[item.Rule]; !ok {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "exception-invalid-rule",
				Path:    loc,
				Message: fmt.Sprintf("unsupported exception rule %q", item.Rule),
			})
		}
		if strings.TrimSpace(item.Reason) == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "exception-metadata",
				Path:    loc,
				Message: "exception missing reason",
			})
		}
		if strings.TrimSpace(item.ApprovedBy) == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "exception-metadata",
				Path:    loc,
				Message: "exception missing approved_by",
			})
		}
		if strings.TrimSpace(item.DecisionRecord) == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "exception-metadata",
				Path:    loc,
				Message: "exception missing decision_record",
			})
		}
		if item.ExpiresAt != "" {
			expires, err := time.Parse(time.RFC3339, item.ExpiresAt)
			if err != nil {
				report.Failures = append(report.Failures, reportFailure{
					Rule:    "exception-expiry",
					Path:    loc,
					Message: fmt.Sprintf("invalid expires_at %q", item.ExpiresAt),
				})
			} else if now.After(expires) {
				report.Failures = append(report.Failures, reportFailure{
					Rule:    "exception-expiry",
					Path:    loc,
					Message: "exception is expired",
				})
			}
		}
	}
}

type rustSurfacePrefix struct {
	Prefix   string
	IsPublic bool
}

func scanRustSurfaceSymbols() (map[string]bool, map[string]bool) {
	symbols := make(map[string]bool)
	publicSymbols := make(map[string]bool)
	prefixes := []rustSurfacePrefix{
		{Prefix: "pub fn ", IsPublic: true},
		{Prefix: "pub struct ", IsPublic: true},
		{Prefix: "pub enum ", IsPublic: true},
		{Prefix: "pub trait ", IsPublic: true},
		{Prefix: "pub(crate) fn ", IsPublic: false},
		{Prefix: "pub(crate) struct ", IsPublic: false},
		{Prefix: "pub(crate) enum ", IsPublic: false},
		{Prefix: "pub(crate) trait ", IsPublic: false},
	}

	for _, root := range parityCriticalRoots {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			continue
		}
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".rs") {
				return nil
			}
			bs, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			for _, line := range strings.Split(string(bs), "\n") {
				line = strings.TrimSpace(line)
				for _, prefix := range prefixes {
					if !strings.HasPrefix(line, prefix.Prefix) {
						continue
					}
					name := strings.Fields(strings.TrimPrefix(line, prefix.Prefix))
					if len(name) == 0 {
						continue
					}
					sym := name[0]
					if idx := strings.Index(sym, "("); idx >= 0 {
						sym = sym[:idx]
					}
					if idx := strings.Index(sym, "<"); idx >= 0 {
						sym = sym[:idx]
					}
					sym = strings.TrimSuffix(sym, "{")
					sym = strings.TrimSuffix(sym, ";")
					sym = strings.TrimSpace(sym)
					if sym == "" {
						continue
					}
					symbols[sym] = true
					if prefix.IsPublic {
						publicSymbols[sym] = true
					}
					break
				}
			}
			return nil
		})
	}
	return symbols, publicSymbols
}

func scanTODOFixme(report *guardrailReport, exceptions []guardException) {
	for _, root := range parityCriticalRoots {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() {
			continue
		}
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".rs") {
				return nil
			}
			bs, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			lines := strings.Split(string(bs), "\n")
			for i, line := range lines {
				if !strings.Contains(line, "TODO") && !strings.Contains(line, "FIXME") {
					continue
				}
				if strings.Contains(line, "PARITY-EXCEPTION:") {
					token := strings.TrimSpace(line[strings.Index(line, "PARITY-EXCEPTION:")+len("PARITY-EXCEPTION:"):])
					if token != "" && exceptionAllowed(exceptions, "todo-fixme", token, filepath.ToSlash(path)) {
						continue
					}
				}
				if !exceptionAllowed(exceptions, "todo-fixme", "", filepath.ToSlash(path)) {
					report.Failures = append(report.Failures, reportFailure{
						Rule:    "todo-fixme",
						Path:    fmt.Sprintf("%s:%d", filepath.ToSlash(path), i+1),
						Message: "TODO/FIXME found in parity-critical path without approved exception",
					})
				}
			}
			return nil
		})
	}
}

func enforceDiffReports(report *guardrailReport, mode string) {
	latestPath := "parity/diff-reports/latest.json"
	latest := differentialReport{}
	if err := readJSON(latestPath, &latest); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-report",
			Path:    latestPath,
			Message: "missing or invalid differential report",
		})
	} else {
		if strings.TrimSpace(latest.ProducedBy) != "parity-harness" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "differential-report",
				Path:    latestPath,
				Message: "differential report must be produced by parity-harness",
			})
		}
		if !latest.Match {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "differential-mismatch",
				Path:    latestPath,
				Message: "differential report indicates mismatch",
			})
		}
		for _, mm := range latest.Mismatches {
			sev := strings.ToLower(strings.TrimSpace(mm.Severity))
			if mm.Open && (sev == "p0" || sev == "p1") {
				report.Failures = append(report.Failures, reportFailure{
					Rule:    "differential-open-critical",
					ID:      mm.ID,
					Path:    latestPath,
					Message: fmt.Sprintf("open %s mismatch: %s", strings.ToUpper(mm.Severity), mm.Message),
				})
			}
		}
	}

	testPath := "parity/diff-reports/test-status.json"
	test := testStatusReport{}
	if err := readJSON(testPath, &test); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "memory-cap-tests",
			Path:    testPath,
			Message: "missing or invalid memory-cap test status report",
		})
	} else {
		status := strings.ToLower(strings.TrimSpace(test.MemoryCap.Status))
		if status == "" || status == "skipped" || (status != "pass" && status != "passed") {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "memory-cap-tests",
				Path:    testPath,
				Message: fmt.Sprintf("memory-cap tests must pass and not be skipped (status=%q)", test.MemoryCap.Status),
			})
		}
		if test.MemoryCap.Flaky {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "memory-cap-tests",
				Path:    testPath,
				Message: "memory-cap tests must not be marked flaky",
			})
		}
	}

	if mode != "release" {
		return
	}

	interopPath := "parity/diff-reports/interop.json"
	interop := interopReport{}
	if err := readJSON(interopPath, &interop); err != nil || !interop.Pass {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "release-interop",
			Path:    interopPath,
			Message: "release mode requires interop pass",
		})
	}

	durPath := "parity/diff-reports/durability.json"
	dur := durabilityReport{}
	if err := readJSON(durPath, &dur); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "release-durability",
			Path:    durPath,
			Message: "missing or invalid durability report",
		})
	} else {
		if strings.ToLower(strings.TrimSpace(dur.Durability)) != "passed" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "release-durability",
				Path:    durPath,
				Message: fmt.Sprintf("durability=%s", dur.Durability),
			})
		}
		if strings.ToLower(strings.TrimSpace(dur.CrashRecovery)) != "passed" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "release-durability",
				Path:    durPath,
				Message: fmt.Sprintf("crash_recovery=%s", dur.CrashRecovery),
			})
		}
	}
}

func exceptionAllowed(items []guardException, rule, id, path string) bool {
	now := time.Now().UTC()
	for _, it := range items {
		if it.Rule != rule {
			continue
		}
		if it.ID != "" && it.ID != id {
			continue
		}
		if it.Path != "" {
			matched := it.Path == path
			if !matched {
				ok, err := filepath.Match(it.Path, path)
				matched = err == nil && ok
			}
			if !matched {
				continue
			}
		}
		if it.ExpiresAt != "" {
			expires, err := time.Parse(time.RFC3339, it.ExpiresAt)
			if err == nil && now.After(expires) {
				continue
			}
		}
		return true
	}
	return false
}

func ensureDiffReports() error {
	if err := os.MkdirAll("parity/diff-reports", 0o755); err != nil {
		return err
	}

	latestPath := "parity/diff-reports/latest.json"
	if _, err := os.Stat(latestPath); errors.Is(err, os.ErrNotExist) {
		report := differentialReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Match:         false,
			Mismatches: []diffMismatch{{
				ID:       "bootstrap",
				Severity: "P0",
				Open:     true,
				Message:  "Differential parity harness not executed yet",
			}},
		}
		if err := writeJSON(latestPath, report); err != nil {
			return err
		}
	}

	testPath := "parity/diff-reports/test-status.json"
	if _, err := os.Stat(testPath); errors.Is(err, os.ErrNotExist) {
		report := testStatusReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			MemoryCap: memoryCapStatus{
				Status:    "skipped",
				Flaky:     false,
				ProfileMB: 50,
			},
		}
		if err := writeJSON(testPath, report); err != nil {
			return err
		}
	}

	durPath := "parity/diff-reports/durability.json"
	if _, err := os.Stat(durPath); errors.Is(err, os.ErrNotExist) {
		report := durabilityReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Durability:    "failed",
			CrashRecovery: "failed",
		}
		if err := writeJSON(durPath, report); err != nil {
			return err
		}
	}

	interopPath := "parity/diff-reports/interop.json"
	if _, err := os.Stat(interopPath); errors.Is(err, os.ErrNotExist) {
		report := interopReport{
			SchemaVersion: schemaVersion,
			GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
			Pass:          false,
		}
		if err := writeJSON(interopPath, report); err != nil {
			return err
		}
	}

	return nil
}

func readManifest(path string) (featureManifest, error) {
	var m featureManifest
	err := readJSON(path, &m)
	return m, err
}

func readMapping(path string) (mappingFile, error) {
	var m mappingFile
	err := readJSON(path, &m)
	return m, err
}

func readExceptions(path string) (exceptionsFile, error) {
	var e exceptionsFile
	err := readJSON(path, &e)
	return e, err
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

func writeText(path string, text string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(text), 0o644)
}

func readJSON(path string, out any) error {
	bs, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(bs, out); err != nil {
		return err
	}
	return nil
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
