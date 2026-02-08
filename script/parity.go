// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"crypto/sha1"
	"crypto/sha256"
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
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	schemaVersion    = 1
	maxDiffReportAge = 72 * time.Hour
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

var parityEvidenceInputRoots = []string{
	"syncthing-rs/src",
	"lib/api/api.go",
	"lib/config",
	"lib/model",
	"lib/protocol",
	"internal/db/interface.go",
	"internal/gen/bep/bep.pb.go",
	"parity/harness/scenarios.json",
	"parity/replacement-gates.json",
	"script/parity.go",
	"script/parity_external_soak.go",
	"script/parity_harness.go",
	"script/parity_harness_go.go",
}

var parityCriticalRoots = []string{"rust", "crates", "syncthing-rs"}

var allowedExceptionRules = map[string]struct{}{
	"status-missing":        {},
	"rust-surface-missing":  {},
	"rust-surface-unmapped": {},
	"implementation-stub":   {},
	"todo-fixme":            {},
}

var disallowedStubComponents = map[string]struct{}{
	"syncthing-rs/model-surface":    {},
	"syncthing-rs/protocol-surface": {},
	"syncthing-rs/parity-symbols":   {},
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
	InputRoots    []string       `json:"input_roots,omitempty"`
	InputsDigest  string         `json:"inputs_digest,omitempty"`
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
	ID           string `json:"id"`
	Severity     string `json:"severity"`
	Required     bool   `json:"required"`
	Status       string `json:"status"`
	Evidence     string `json:"evidence,omitempty"`
	GoEvidence   string `json:"go_evidence,omitempty"`
	RustEvidence string `json:"rust_evidence,omitempty"`
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

type replacementGates struct {
	SchemaVersion                  int      `json:"schema_version"`
	RequiredAPIEndpoints           []string `json:"required_api_endpoints"`
	RequiredBEPMessageTypes        []string `json:"required_bep_message_types"`
	RequiredFolderModes            []string `json:"required_folder_modes"`
	RequiredExternalSoakScenarios  []string `json:"required_external_soak_scenarios"`
	RequiredMemoryDiagnosticFields []string `json:"required_memory_diagnostic_fields"`
	RequiredDurabilityFields       []string `json:"required_durability_fields"`
}

type harnessScenarioFile struct {
	SchemaVersion int               `json:"schema_version"`
	Scenarios     []harnessScenario `json:"scenarios"`
}

type harnessScenario struct {
	ID       string   `json:"id"`
	Required bool     `json:"required"`
	Tags     []string `json:"tags"`
}

type requiredTestEvidence struct {
	ScenarioIDs          map[string]struct{}
	RequiredScenarioIDs  map[string]struct{}
	ScenarioOutcome      map[string]string
	ScenarioEvidence     map[string]string
	ScenarioGoEvidence   map[string]string
	ScenarioRustEvidence map[string]string
	ScenarioTags         map[string]map[string]struct{}
	ScenarioRefs         map[string]int
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

type replacementGapReport struct {
	SchemaVersion int      `json:"schema_version"`
	GeneratedAt   string   `json:"generated_at"`
	Rule          string   `json:"rule"`
	SourceCount   int      `json:"source_count"`
	TargetCount   int      `json:"target_count"`
	Missing       []string `json:"missing"`
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
	mode := fs.String("mode", "ci", "check mode: ci, release, or replacement")
	reportPath := fs.String("report", "parity/guardrail-report.json", "report output path")
	if err := fs.Parse(args); err != nil {
		fatalf("parse flags: %v", err)
	}
	if *mode != "ci" && *mode != "release" && *mode != "replacement" {
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
	sources, err := resolveSources(sourcePatterns)
	if err != nil {
		fatalf("resolve sources: %v", err)
	}
	generatedFeatures, err := generateFeatures(sources)
	if err != nil {
		fatalf("generate features: %v", err)
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
	validateManifestAgainstGeneratedFeatures(&report, manifest, generatedFeatures)

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
			validateImplementedLike(&report, feat, mi, &testEvidence)
		case "parity-verified":
			report.Summary.ParityVerified++
			validateImplementedLike(&report, feat, mi, &testEvidence)
		}
	}

	validateRequiredScenarioCoverage(&report, testEvidence)
	if *mode == "replacement" {
		validateReplacementScenarioEvidence(&report, testEvidence)
		validateReplacementCapabilityCoverage(&report)
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

	if *mode == "release" || *mode == "replacement" {
		if report.Summary.TotalFeatures == 0 || report.Summary.ParityVerified != report.Summary.TotalFeatures {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "release-gate",
				Message: fmt.Sprintf("%s mode requires 100%% parity-verified: have %d/%d", *mode, report.Summary.ParityVerified, report.Summary.TotalFeatures),
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
		ScenarioIDs:          make(map[string]struct{}),
		RequiredScenarioIDs:  make(map[string]struct{}),
		ScenarioOutcome:      make(map[string]string),
		ScenarioEvidence:     make(map[string]string),
		ScenarioGoEvidence:   make(map[string]string),
		ScenarioRustEvidence: make(map[string]string),
		ScenarioTags:         make(map[string]map[string]struct{}),
		ScenarioRefs:         make(map[string]int),
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
		tagSet := make(map[string]struct{})
		for _, tag := range sc.Tags {
			normalized := strings.ToLower(strings.TrimSpace(tag))
			if normalized == "" {
				continue
			}
			tagSet[normalized] = struct{}{}
		}
		ev.ScenarioTags[id] = tagSet
		if sc.Required {
			ev.RequiredScenarioIDs[id] = struct{}{}
		}
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
		ev.ScenarioEvidence[id] = normalizeScenarioEvidence(sc.Evidence)
		ev.ScenarioGoEvidence[id] = normalizeScenarioEvidence(sc.GoEvidence)
		ev.ScenarioRustEvidence[id] = normalizeScenarioEvidence(sc.RustEvidence)
	}
	return ev
}

func validateManifestAgainstGeneratedFeatures(report *guardrailReport, manifest featureManifest, generated []featureItem) {
	generatedByID := make(map[string]featureItem, len(generated))
	for _, feat := range generated {
		generatedByID[feat.ID] = feat
	}

	manifestByID := make(map[string]featureItem, len(manifest.Items))
	for _, feat := range manifest.Items {
		if strings.TrimSpace(feat.ID) == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "manifest-shape",
				Path:    "parity/feature-manifest.json",
				Message: "feature is missing id",
			})
			continue
		}
		if strings.TrimSpace(feat.Source) == "" || strings.TrimSpace(feat.Kind) == "" || strings.TrimSpace(feat.Symbol) == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "manifest-shape",
				ID:      feat.ID,
				Path:    "parity/feature-manifest.json",
				Message: "feature is missing source/kind/symbol metadata",
			})
			continue
		}
		manifestByID[feat.ID] = feat

		expected, ok := generatedByID[feat.ID]
		if !ok {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "manifest-stale",
				ID:      feat.ID,
				Path:    "parity/feature-manifest.json",
				Message: "feature id does not exist in current Go source-of-truth inventory",
			})
			continue
		}
		if feat.Source != expected.Source || feat.Subsystem != expected.Subsystem || feat.Kind != expected.Kind || feat.Symbol != expected.Symbol {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "manifest-drift",
				ID:      feat.ID,
				Path:    "parity/feature-manifest.json",
				Message: "manifest metadata differs from current generated Go feature inventory",
			})
		}
	}

	for _, feat := range generated {
		if _, ok := manifestByID[feat.ID]; ok {
			continue
		}
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "manifest-missing",
			ID:      feat.ID,
			Path:    "parity/feature-manifest.json",
			Message: "generated feature is missing from manifest",
		})
	}
}

func validateImplementedLike(report *guardrailReport, feat featureItem, mi mappingItem, ev *requiredTestEvidence) {
	if strings.TrimSpace(mi.RustComponent) == "" {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "mapping-rust-component",
			ID:      mi.ID,
			Message: "implemented feature is missing rust_component",
		})
	}
	if _, isStub := disallowedStubComponents[strings.TrimSpace(mi.RustComponent)]; isStub {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "implementation-stub",
			ID:      mi.ID,
			Message: fmt.Sprintf("feature is mapped to stub/surface component %q", mi.RustComponent),
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
		ev.ScenarioRefs[scenarioID]++
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

func validateRequiredScenarioCoverage(report *guardrailReport, ev requiredTestEvidence) {
	ids := make([]string, 0, len(ev.RequiredScenarioIDs))
	for id := range ev.RequiredScenarioIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if ev.ScenarioRefs[id] > 0 {
			continue
		}
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "required-scenario-unmapped",
			Path:    "parity/harness/scenarios.json",
			Message: fmt.Sprintf("required scenario %q is not referenced by any feature required_tests", id),
		})
	}
}

func validateReplacementScenarioEvidence(report *guardrailReport, ev requiredTestEvidence) {
	ids := make([]string, 0, len(ev.RequiredScenarioIDs))
	for id := range ev.RequiredScenarioIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		status := strings.ToLower(strings.TrimSpace(ev.ScenarioOutcome[id]))
		if status == "" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-scenario-status",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("required scenario %q is missing from latest differential report", id),
			})
			continue
		}
		if status != "pass" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-scenario-status",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("required scenario %q must pass for replacement readiness (status=%q)", id, status),
			})
		}

		goObserved := normalizeScenarioEvidence(ev.ScenarioGoEvidence[id])
		if goObserved == "" {
			goObserved = normalizeScenarioEvidence(ev.ScenarioEvidence[id])
		}
		if goObserved == "" {
			goObserved = "synthetic"
		}
		rustObserved := normalizeScenarioEvidence(ev.ScenarioRustEvidence[id])
		if rustObserved == "" {
			rustObserved = normalizeScenarioEvidence(ev.ScenarioEvidence[id])
		}
		if rustObserved == "" {
			rustObserved = "synthetic"
		}
		required := "daemon"
		if scenarioHasTag(ev, id, "external-soak") {
			required = "external-soak"
		} else if scenarioHasTag(ev, id, "interop") {
			required = "peer-interop"
		}

		requiredRank := scenarioEvidenceRank(required)
		if scenarioEvidenceRank(goObserved) < requiredRank {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-scenario-evidence",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("required scenario %q has insufficient go evidence level %q (need >= %q)", id, goObserved, required),
			})
		}
		if scenarioEvidenceRank(rustObserved) < requiredRank {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-scenario-evidence",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("required scenario %q has insufficient rust evidence level %q (need >= %q)", id, rustObserved, required),
			})
		}
	}
}

func scenarioHasTag(ev requiredTestEvidence, scenarioID, tag string) bool {
	tags := ev.ScenarioTags[scenarioID]
	if len(tags) == 0 {
		return false
	}
	_, ok := tags[strings.ToLower(strings.TrimSpace(tag))]
	return ok
}

func validateReplacementCapabilityCoverage(report *guardrailReport) {
	gates := replacementGates{}
	if err := readJSON("parity/replacement-gates.json", &gates); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-capability-gates",
			Path:    "parity/replacement-gates.json",
			Message: "missing or invalid replacement gates configuration",
		})
		return
	}

	validateReplacementAPIEndpointCoverage(report, gates.RequiredAPIEndpoints)
	validateReplacementProtocolCoverage(report, gates.RequiredBEPMessageTypes)
	validateReplacementFolderModeCoverage(report, gates.RequiredFolderModes)
	validateReplacementExternalSoak(report, gates.RequiredExternalSoakScenarios)
	validateReplacementMemoryDiagnosticsCoverage(report, gates.RequiredMemoryDiagnosticFields)
	validateReplacementDurabilityCoverage(report, gates.RequiredDurabilityFields)
	validateGoVsRustRESTSurface(report)
	validateGoVsRustProtocolSurface(report)
	validateGoVsRustFolderModeSurface(report)
}

func validateReplacementExternalSoak(report *guardrailReport, required []string) {
	if len(required) == 0 {
		return
	}

	for _, id := range required {
		scenarioID := strings.TrimSpace(id)
		if scenarioID == "" {
			continue
		}

		outcome, found, err := readScenarioOutcome(scenarioID)
		if err != nil {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-external-soak",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("failed to read latest differential report for %q", scenarioID),
			})
			continue
		}
		if !found {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-external-soak",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("required external soak scenario %q missing from latest differential report", scenarioID),
			})
			continue
		}
		if strings.ToLower(strings.TrimSpace(outcome.Status)) != "pass" {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-external-soak",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("external soak scenario %q must pass for replacement readiness (status=%q)", scenarioID, outcome.Status),
			})
			continue
		}

		goEvidence := normalizeScenarioEvidence(outcome.GoEvidence)
		rustEvidence := normalizeScenarioEvidence(outcome.RustEvidence)
		if goEvidence == "" {
			goEvidence = normalizeScenarioEvidence(outcome.Evidence)
		}
		if rustEvidence == "" {
			rustEvidence = normalizeScenarioEvidence(outcome.Evidence)
		}
		if goEvidence == "" {
			goEvidence = "synthetic"
		}
		if rustEvidence == "" {
			rustEvidence = "synthetic"
		}
		requiredRank := scenarioEvidenceRank("external-soak")
		if scenarioEvidenceRank(goEvidence) < requiredRank || scenarioEvidenceRank(rustEvidence) < requiredRank {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-external-soak",
				Path:    "parity/diff-reports/latest.json",
				Message: fmt.Sprintf("external soak scenario %q requires external-soak evidence for both sides (go=%q rust=%q)", scenarioID, goEvidence, rustEvidence),
			})
		}
	}
}

func validateReplacementAPIEndpointCoverage(report *guardrailReport, required []string) {
	if len(required) == 0 {
		return
	}
	outcome, found, err := readScenarioOutcome("daemon-api-surface")
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    "parity/diff-reports/latest.json",
			Message: "failed to read latest differential report for daemon-api-surface",
		})
		return
	}
	if !found {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    "parity/diff-reports/latest.json",
			Message: "daemon-api-surface scenario result missing from latest differential report",
		})
		return
	}
	if strings.ToLower(strings.TrimSpace(outcome.Status)) != "pass" {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    "parity/diff-reports/latest.json",
			Message: fmt.Sprintf("daemon-api-surface must pass for replacement readiness (status=%q)", outcome.Status),
		})
		return
	}
	if scenarioEvidenceRank(normalizeScenarioEvidence(outcome.RustEvidence)) < scenarioEvidenceRank("daemon") {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    "parity/diff-reports/latest.json",
			Message: fmt.Sprintf("daemon-api-surface rust evidence must be at least daemon (got %q)", normalizeScenarioEvidence(outcome.RustEvidence)),
		})
		return
	}

	snapshot := map[string]any{}
	const path = "parity/harness/snapshots/rust-daemon-api-surface.json"
	if err := readJSON(path, &snapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    path,
			Message: "missing or invalid rust daemon api surface snapshot",
		})
		return
	}
	covered := toStringSet(snapshot["covered_endpoints"])
	missing := missingRequiredStrings(required, covered)
	if len(missing) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    path,
			Message: fmt.Sprintf("rust daemon api surface missing %d required endpoints: %s", len(missing), strings.Join(missing, ", ")),
		})
		return
	}

	assertionsRaw, ok := snapshot["endpoint_assertions"].(map[string]any)
	if !ok || len(assertionsRaw) == 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-api-coverage",
			Path:    path,
			Message: "rust daemon api surface is missing endpoint_assertions",
		})
		return
	}

	requiredKeysByEndpoint := replacementAPIRequiredResponseKeys()
	for _, endpoint := range required {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			continue
		}
		rawAssertion, found := assertionsRaw[endpoint]
		if !found {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-api-coverage",
				Path:    path,
				Message: fmt.Sprintf("rust daemon api surface missing assertion for required endpoint %q", endpoint),
			})
			continue
		}

		assertion, ok := rawAssertion.(map[string]any)
		if !ok {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-api-coverage",
				Path:    path,
				Message: fmt.Sprintf("rust daemon api assertion for %q has invalid shape", endpoint),
			})
			continue
		}

		statusCode, ok := jsonNumberToInt(assertion["status_code"])
		if !ok || statusCode != 200 {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-api-coverage",
				Path:    path,
				Message: fmt.Sprintf("rust daemon api assertion for %q must have status_code=200 (got %v)", endpoint, assertion["status_code"]),
			})
		}

		responseKind, ok := assertion["response_kind"].(string)
		responseKind = strings.TrimSpace(strings.ToLower(responseKind))
		if !ok || (responseKind != "object" && responseKind != "array") {
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-api-coverage",
				Path:    path,
				Message: fmt.Sprintf("rust daemon api assertion for %q must have response_kind object/array (got %v)", endpoint, assertion["response_kind"]),
			})
		}

		presentKeys := toStringSet(assertion["present_keys"])
		requiredKeys := requiredKeysByEndpoint[endpoint]
		if len(requiredKeys) == 0 {
			requiredKeys = stringSliceFromAny(assertion["required_keys"])
		}
		for _, key := range requiredKeys {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			if _, found := presentKeys[key]; found {
				continue
			}
			report.Failures = append(report.Failures, reportFailure{
				Rule:    "replacement-api-coverage",
				Path:    path,
				Message: fmt.Sprintf("rust daemon api assertion for %q missing required response key %q", endpoint, key),
			})
		}
	}
}

func replacementAPIRequiredResponseKeys() map[string][]string {
	return map[string][]string{
		"GET /rest/system/version":           {"version", "longVersion", "os", "arch"},
		"GET /rest/system/status":            {"myID", "uptimeS", "memoryEstimatedBytes", "memoryBudgetBytes"},
		"GET /rest/system/connections":       {"total", "connections"},
		"GET /rest/system/config/folders":    {"count", "folders"},
		"GET /rest/db/browse":                {"folder", "limit", "items"},
		"GET /rest/db/completion":            {"folder", "device", "completionPct", "needBytes"},
		"GET /rest/db/file":                  {"folder", "file", "exists"},
		"GET /rest/db/ignores":               {"folder", "count", "patterns"},
		"GET /rest/db/jobs":                  {"folder", "state", "jobs"},
		"GET /rest/db/localchanged":          {"folder", "count", "files"},
		"GET /rest/db/need":                  {"folder", "limit", "items"},
		"GET /rest/db/remoteneed":            {"folder", "device", "count", "items"},
		"GET /rest/db/status":                {"folder", "state", "localFiles", "needFiles"},
		"POST /rest/db/bringtofront":         {"folder", "action", "ok"},
		"POST /rest/db/override":             {"folder", "action", "ok"},
		"POST /rest/db/pull":                 {"folder", "state", "sequence", "pull"},
		"POST /rest/db/reset":                {"folder", "action", "ok"},
		"POST /rest/db/revert":               {"folder", "action", "ok"},
		"POST /rest/db/scan":                 {"folder", "sequence", "state", "stats"},
		"POST /rest/system/config/folders":   {"added", "folder"},
		"POST /rest/system/config/restart":   {"restarted", "folder"},
		"DELETE /rest/system/config/folders": {"removed", "folder"},
	}
}

func jsonNumberToInt(raw any) (int, bool) {
	switch typed := raw.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case uint:
		return int(typed), true
	case uint32:
		return int(typed), true
	case uint64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func stringSliceFromAny(value any) []string {
	out := make([]string, 0)
	switch typed := value.(type) {
	case []string:
		for _, entry := range typed {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			out = append(out, entry)
		}
	case []any:
		for _, raw := range typed {
			entry, ok := raw.(string)
			if !ok {
				continue
			}
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			out = append(out, entry)
		}
	}
	return out
}

func readScenarioOutcome(id string) (diffScenario, bool, error) {
	latest := differentialReport{}
	if err := readJSON("parity/diff-reports/latest.json", &latest); err != nil {
		return diffScenario{}, false, err
	}
	target := strings.TrimSpace(id)
	for _, sc := range latest.Scenarios {
		if strings.TrimSpace(sc.ID) != target {
			continue
		}
		return sc, true, nil
	}
	return diffScenario{}, false, nil
}

func validateReplacementProtocolCoverage(report *guardrailReport, required []string) {
	if len(required) == 0 {
		return
	}
	snapshot := map[string]any{}
	const path = "parity/harness/snapshots/rust-protocol-state-transition.json"
	if err := readJSON(path, &snapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-protocol-coverage",
			Path:    path,
			Message: "missing or invalid rust protocol state transition snapshot",
		})
		return
	}
	covered := toStringSet(snapshot["message_types"])
	missing := missingRequiredStrings(required, covered)
	if len(missing) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-protocol-coverage",
			Path:    path,
			Message: fmt.Sprintf("rust protocol scenario missing %d required message types: %s", len(missing), strings.Join(missing, ", ")),
		})
	}
}

func validateReplacementFolderModeCoverage(report *guardrailReport, required []string) {
	if len(required) == 0 {
		return
	}
	snapshot := map[string]any{}
	const path = "parity/harness/snapshots/rust-folder-type-behavior.json"
	if err := readJSON(path, &snapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-coverage",
			Path:    path,
			Message: "missing or invalid rust folder type snapshot",
		})
		return
	}
	folderModes, ok := snapshot["folder_modes"].(map[string]any)
	if !ok {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-coverage",
			Path:    path,
			Message: "folder_modes payload missing or invalid",
		})
		return
	}
	covered := make(map[string]struct{}, len(folderModes))
	for key := range folderModes {
		covered[strings.TrimSpace(key)] = struct{}{}
	}
	missing := missingRequiredStrings(required, covered)
	if len(missing) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-coverage",
			Path:    path,
			Message: fmt.Sprintf("rust folder mode scenario missing %d required modes: %s", len(missing), strings.Join(missing, ", ")),
		})
	}
}

func validateReplacementMemoryDiagnosticsCoverage(report *guardrailReport, required []string) {
	if len(required) == 0 {
		return
	}
	snapshot := map[string]any{}
	const path = "parity/harness/snapshots/rust-memory-cap-diagnostics.json"
	if err := readJSON(path, &snapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-memory-diagnostics-coverage",
			Path:    path,
			Message: "missing or invalid rust memory diagnostics snapshot",
		})
		return
	}

	rawReport, ok := snapshot["report"].(map[string]any)
	if !ok {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-memory-diagnostics-coverage",
			Path:    path,
			Message: "memory diagnostics report payload missing or invalid",
		})
		return
	}
	covered := make(map[string]struct{}, len(rawReport))
	for key, value := range rawReport {
		if strings.TrimSpace(key) == "" || value == nil {
			continue
		}
		if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
			continue
		}
		covered[strings.TrimSpace(key)] = struct{}{}
	}
	missing := missingRequiredStrings(required, covered)
	if len(missing) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-memory-diagnostics-coverage",
			Path:    path,
			Message: fmt.Sprintf("rust memory diagnostics missing %d required fields: %s", len(missing), strings.Join(missing, ", ")),
		})
	}
}

func validateReplacementDurabilityCoverage(report *guardrailReport, required []string) {
	if len(required) == 0 {
		return
	}
	snapshot := map[string]any{}
	const path = "parity/harness/snapshots/rust-wal-free-durability.json"
	if err := readJSON(path, &snapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-durability-coverage",
			Path:    path,
			Message: "missing or invalid rust wal-free durability snapshot",
		})
		return
	}
	covered := make(map[string]struct{}, len(snapshot))
	for key, value := range snapshot {
		if strings.TrimSpace(key) == "" || value == nil {
			continue
		}
		if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
			continue
		}
		covered[strings.TrimSpace(key)] = struct{}{}
	}
	missing := missingRequiredStrings(required, covered)
	if len(missing) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-durability-coverage",
			Path:    path,
			Message: fmt.Sprintf("rust durability snapshot missing %d required fields: %s", len(missing), strings.Join(missing, ", ")),
		})
		return
	}

	active, _ := snapshot["active_segment"].(string)
	if active != "" && !strings.HasPrefix(active, "CSEG-") {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-durability-coverage",
			Path:    path,
			Message: fmt.Sprintf("active_segment must use CSEG-* commit segment naming, got %q", active),
		})
	}
}

func toStringSet(value any) map[string]struct{} {
	out := make(map[string]struct{})
	switch typed := value.(type) {
	case []string:
		for _, entry := range typed {
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			out[entry] = struct{}{}
		}
	case []any:
		for _, raw := range typed {
			entry, ok := raw.(string)
			if !ok {
				continue
			}
			entry = strings.TrimSpace(entry)
			if entry == "" {
				continue
			}
			out[entry] = struct{}{}
		}
	}
	return out
}

func missingRequiredStrings(required []string, covered map[string]struct{}) []string {
	missing := make([]string, 0)
	for _, entry := range required {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if _, ok := covered[entry]; ok {
			continue
		}
		missing = append(missing, entry)
	}
	sort.Strings(missing)
	return missing
}

func validateGoVsRustRESTSurface(report *guardrailReport) {
	goSurface, err := extractGoRESTSurface()
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-rest-surface",
			Path:    "lib/api/api.go",
			Message: fmt.Sprintf("failed to extract go rest surface: %v", err),
		})
		return
	}

	rustSnapshot := map[string]any{}
	const rustPath = "parity/harness/snapshots/rust-daemon-api-surface.json"
	if err := readJSON(rustPath, &rustSnapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-rest-surface",
			Path:    rustPath,
			Message: "missing or invalid rust daemon api surface snapshot",
		})
		return
	}
	rustSurface := toStringSet(rustSnapshot["covered_endpoints"])

	missing := make([]string, 0)
	for endpoint := range goSurface {
		if _, ok := rustSurface[endpoint]; ok {
			continue
		}
		missing = append(missing, endpoint)
	}
	sort.Strings(missing)
	const gapPath = "parity/diff-reports/replacement-rest-surface-missing.json"
	if len(missing) == 0 {
		_ = os.Remove(gapPath)
		return
	}

	if err := writeReplacementGapFile(gapPath, "replacement-rest-surface", len(goSurface), len(rustSurface), missing); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-rest-surface",
			Path:    gapPath,
			Message: fmt.Sprintf("failed to write replacement gap report: %v", err),
		})
	}

	preview := missing
	if len(preview) > 25 {
		preview = preview[:25]
	}
	report.Failures = append(report.Failures, reportFailure{
		Rule: "replacement-rest-surface",
		Path: gapPath,
		Message: fmt.Sprintf(
			"go exposes %d REST endpoints while rust daemon evidence covers %d; missing %d endpoint(s), first %d: %s",
			len(goSurface),
			len(rustSurface),
			len(missing),
			len(preview),
			strings.Join(preview, ", "),
		),
	})
}

func extractGoRESTSurface() (map[string]struct{}, error) {
	bs, err := readRepoFile("lib/api/api.go")
	if err != nil {
		return nil, err
	}
	text := string(bs)
	endpoints := make(map[string]struct{})

	directPattern := regexp.MustCompile(`restMux\.(?:HandlerFunc|Handler)\(\s*http\.(Method[A-Za-z]+),\s*"(/rest/[^"]+)"`)
	for _, match := range directPattern.FindAllStringSubmatch(text, -1) {
		if len(match) != 3 {
			continue
		}
		method := normalizeHTTPMethodToken(match[1])
		path := strings.TrimSpace(match[2])
		if method == "" || path == "" {
			continue
		}
		endpoints[method+" "+path] = struct{}{}
	}

	configRegisterMethods := map[string][]string{
		"registerConfig":                {"GET", "PUT"},
		"registerConfigDeprecated":      {"GET", "POST"},
		"registerConfigInsync":          {"GET"},
		"registerConfigRequiresRestart": {"GET"},
		"registerFolders":               {"GET", "PUT", "POST"},
		"registerDevices":               {"GET", "PUT", "POST"},
		"registerFolder":                {"GET", "PUT", "PATCH", "DELETE"},
		"registerDevice":                {"GET", "PUT", "PATCH", "DELETE"},
		"registerDefaultFolder":         {"GET", "PUT", "PATCH"},
		"registerDefaultDevice":         {"GET", "PUT", "PATCH"},
		"registerDefaultIgnores":        {"GET", "PUT"},
		"registerOptions":               {"GET", "PUT", "PATCH"},
		"registerLDAP":                  {"GET", "PUT", "PATCH"},
		"registerGUI":                   {"GET", "PUT", "PATCH"},
	}
	registerPattern := regexp.MustCompile(`configBuilder\.(register[A-Za-z0-9]+)\("(/rest/[^"]+)"\)`)
	for _, match := range registerPattern.FindAllStringSubmatch(text, -1) {
		if len(match) != 3 {
			continue
		}
		methods := configRegisterMethods[match[1]]
		if len(methods) == 0 {
			continue
		}
		path := strings.TrimSpace(match[2])
		if path == "" {
			continue
		}
		for _, method := range methods {
			endpoints[method+" "+path] = struct{}{}
		}
	}

	return endpoints, nil
}

func validateGoVsRustProtocolSurface(report *guardrailReport) {
	goSurface, err := extractGoBEPMessageTypes()
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-protocol-surface",
			Path:    "internal/gen/bep/bep.pb.go",
			Message: fmt.Sprintf("failed to extract go bep message surface: %v", err),
		})
		return
	}

	rustSnapshot := map[string]any{}
	const rustPath = "parity/harness/snapshots/rust-protocol-state-transition.json"
	if err := readJSON(rustPath, &rustSnapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-protocol-surface",
			Path:    rustPath,
			Message: "missing or invalid rust protocol state transition snapshot",
		})
		return
	}

	rawMessageTypes := toStringSet(rustSnapshot["message_types"])
	rustSurface := make(map[string]struct{}, len(rawMessageTypes))
	for raw := range rawMessageTypes {
		normalized := normalizeBEPMessageType(raw)
		if normalized == "" {
			continue
		}
		rustSurface[normalized] = struct{}{}
	}
	rustCoreSurface, err := extractRustBEPMessageTypes()
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-protocol-surface",
			Path:    "syncthing-rs/src/bep_core.rs",
			Message: fmt.Sprintf("failed to extract rust bep message surface: %v", err),
		})
		return
	}
	for messageType := range rustCoreSurface {
		rustSurface[messageType] = struct{}{}
	}

	missing := make([]string, 0)
	for messageType := range goSurface {
		if _, ok := rustSurface[messageType]; ok {
			continue
		}
		missing = append(missing, messageType)
	}
	sort.Strings(missing)
	const gapPath = "parity/diff-reports/replacement-protocol-surface-missing.json"
	if len(missing) == 0 {
		_ = os.Remove(gapPath)
		return
	}

	if err := writeReplacementGapFile(gapPath, "replacement-protocol-surface", len(goSurface), len(rustSurface), missing); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-protocol-surface",
			Path:    gapPath,
			Message: fmt.Sprintf("failed to write replacement gap report: %v", err),
		})
	}

	preview := missing
	if len(preview) > 25 {
		preview = preview[:25]
	}
	report.Failures = append(report.Failures, reportFailure{
		Rule: "replacement-protocol-surface",
		Path: gapPath,
		Message: fmt.Sprintf(
			"go exposes %d BEP message types while rust scenario evidence covers %d; missing %d type(s), first %d: %s",
			len(goSurface),
			len(rustSurface),
			len(missing),
			len(preview),
			strings.Join(preview, ", "),
		),
	})
}

func extractRustBEPMessageTypes() (map[string]struct{}, error) {
	bs, err := readRepoFile("syncthing-rs/src/bep_core.rs")
	if err != nil {
		return nil, err
	}
	text := string(bs)
	messageTypes := make(map[string]struct{})

	structPattern := regexp.MustCompile(`pub\(crate\)\s+struct\s+([A-Za-z][A-Za-z0-9_]*)\s*\{`)
	for _, match := range structPattern.FindAllStringSubmatch(text, -1) {
		if len(match) != 2 {
			continue
		}
		normalized := normalizeBEPMessageType(match[1])
		if normalized == "" {
			continue
		}
		messageTypes[normalized] = struct{}{}
	}

	if len(messageTypes) == 0 {
		return nil, errors.New("no rust bep message structs found")
	}
	return messageTypes, nil
}

func extractGoBEPMessageTypes() (map[string]struct{}, error) {
	bs, err := readRepoFile("internal/gen/bep/bep.pb.go")
	if err != nil {
		return nil, err
	}
	text := string(bs)
	messageTypes := make(map[string]struct{})

	protoReflectPattern := regexp.MustCompile(`func \(x \*([A-Za-z][A-Za-z0-9_]*)\) ProtoReflect\(`)
	for _, match := range protoReflectPattern.FindAllStringSubmatch(text, -1) {
		if len(match) != 2 {
			continue
		}
		normalized := normalizeBEPMessageType(match[1])
		if normalized == "" {
			continue
		}
		messageTypes[normalized] = struct{}{}
	}

	return messageTypes, nil
}

func validateGoVsRustFolderModeSurface(report *guardrailReport) {
	goModes, err := extractGoFolderModes()
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-surface",
			Path:    "lib/config/foldertype.go",
			Message: fmt.Sprintf("failed to extract go folder mode surface: %v", err),
		})
		return
	}

	rustSnapshot := map[string]any{}
	const rustPath = "parity/harness/snapshots/rust-folder-type-behavior.json"
	if err := readJSON(rustPath, &rustSnapshot); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-surface",
			Path:    rustPath,
			Message: "missing or invalid rust folder type behavior snapshot",
		})
		return
	}

	rawModes, ok := rustSnapshot["folder_modes"].(map[string]any)
	if !ok {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-surface",
			Path:    rustPath,
			Message: "folder_modes payload missing or invalid",
		})
		return
	}
	rustModes := make(map[string]struct{}, len(rawModes))
	for mode := range rawModes {
		canonical := canonicalFolderMode(mode)
		if canonical == "" {
			continue
		}
		rustModes[canonical] = struct{}{}
	}

	missing := make([]string, 0)
	for mode := range goModes {
		if _, ok := rustModes[mode]; ok {
			continue
		}
		missing = append(missing, mode)
	}
	sort.Strings(missing)
	const gapPath = "parity/diff-reports/replacement-folder-mode-surface-missing.json"
	if len(missing) == 0 {
		_ = os.Remove(gapPath)
		return
	}

	if err := writeReplacementGapFile(gapPath, "replacement-folder-mode-surface", len(goModes), len(rustModes), missing); err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "replacement-folder-mode-surface",
			Path:    gapPath,
			Message: fmt.Sprintf("failed to write replacement gap report: %v", err),
		})
	}

	report.Failures = append(report.Failures, reportFailure{
		Rule:    "replacement-folder-mode-surface",
		Path:    gapPath,
		Message: fmt.Sprintf("rust folder mode evidence missing %d go mode(s): %s", len(missing), strings.Join(missing, ", ")),
	})
}

func extractGoFolderModes() (map[string]struct{}, error) {
	bs, err := readRepoFile("lib/config/foldertype.go")
	if err != nil {
		return nil, err
	}
	text := string(bs)
	modes := make(map[string]struct{})

	casePattern := regexp.MustCompile(`case\s+([^\n:]+):`)
	modePattern := regexp.MustCompile(`"([^"]+)"`)
	for _, caseMatch := range casePattern.FindAllStringSubmatch(text, -1) {
		if len(caseMatch) != 2 {
			continue
		}
		for _, modeMatch := range modePattern.FindAllStringSubmatch(caseMatch[1], -1) {
			if len(modeMatch) != 2 {
				continue
			}
			mode := canonicalFolderMode(modeMatch[1])
			if mode == "" {
				continue
			}
			modes[mode] = struct{}{}
		}
	}

	if len(modes) == 0 {
		return nil, errors.New("no folder mode literals found")
	}
	return modes, nil
}

func canonicalFolderMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "readwrite", "sendreceive", "sendrecv":
		return "sendrecv"
	case "readonly", "sendonly":
		return "sendonly"
	case "receiveonly", "recvonly":
		return "recvonly"
	case "receiveencrypted", "recvenc", "receive-encrypted":
		return "recvenc"
	default:
		return ""
	}
}

func normalizeBEPMessageType(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, "-", "_")
	reAcronymBoundary := regexp.MustCompile(`([A-Z]+)([A-Z][a-z])`)
	reWordBoundary := regexp.MustCompile(`([a-z0-9])([A-Z])`)
	token = reAcronymBoundary.ReplaceAllString(token, "${1}_${2}")
	token = reWordBoundary.ReplaceAllString(token, "${1}_${2}")
	token = strings.ToLower(token)
	token = strings.Trim(token, "_")
	for strings.Contains(token, "__") {
		token = strings.ReplaceAll(token, "__", "_")
	}
	return token
}

func normalizeHTTPMethodToken(token string) string {
	token = strings.TrimSpace(token)
	if !strings.HasPrefix(token, "Method") {
		return strings.ToUpper(token)
	}
	token = strings.TrimPrefix(token, "Method")
	if token == "" {
		return ""
	}
	return strings.ToUpper(token)
}

func readRepoFile(relPath string) ([]byte, error) {
	relPath = filepath.Clean(relPath)
	candidatePrefixes := []string{
		".",
		"..",
		"../..",
	}
	var lastErr error
	for _, prefix := range candidatePrefixes {
		candidate := filepath.Join(prefix, relPath)
		bs, err := os.ReadFile(candidate)
		if err == nil {
			return bs, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			lastErr = err
		} else if lastErr == nil {
			lastErr = err
		}
	}

	wd, err := os.Getwd()
	if err == nil {
		dir := wd
		for depth := 0; depth < 12; depth++ {
			candidate := filepath.Join(dir, relPath)
			bs, readErr := os.ReadFile(candidate)
			if readErr == nil {
				return bs, nil
			}
			if !errors.Is(readErr, fs.ErrNotExist) {
				lastErr = readErr
			} else if lastErr == nil {
				lastErr = readErr
			}

			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	if lastErr == nil {
		lastErr = fs.ErrNotExist
	}
	return nil, fmt.Errorf("open %s: %w", relPath, lastErr)
}

func validateExceptions(report *guardrailReport, ex exceptionsFile, mode string) {
	now := time.Now().UTC()

	if (mode == "release" || mode == "replacement") && len(ex.Items) > 0 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "release-exceptions",
			Path:    "parity/exceptions.json",
			Message: fmt.Sprintf("%s mode requires parity/exceptions.json to be empty", mode),
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
		{Prefix: "pub const ", IsPublic: true},
		{Prefix: "pub static ", IsPublic: true},
		{Prefix: "pub(crate) fn ", IsPublic: false},
		{Prefix: "pub(crate) struct ", IsPublic: false},
		{Prefix: "pub(crate) enum ", IsPublic: false},
		{Prefix: "pub(crate) trait ", IsPublic: false},
		{Prefix: "pub(crate) const ", IsPublic: false},
		{Prefix: "pub(crate) static ", IsPublic: false},
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
					sym = strings.TrimSuffix(sym, ":")
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
		validateReportFreshness(report, latestPath, "differential report", latest.GeneratedAt, maxDiffReportAge)
		validateDifferentialEvidenceInputs(report, latestPath, latest)
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
		validateReportFreshness(report, testPath, "memory-cap status report", test.GeneratedAt, maxDiffReportAge)
		validateMemoryCapStatus(report, testPath, test)
	}

	if mode != "release" && mode != "replacement" {
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
	} else {
		validateReportFreshness(report, interopPath, "interop report", interop.GeneratedAt, maxDiffReportAge)
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
		validateReportFreshness(report, durPath, "durability report", dur.GeneratedAt, maxDiffReportAge)
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

func validateMemoryCapStatus(report *guardrailReport, path string, test testStatusReport) {
	status := strings.ToLower(strings.TrimSpace(test.MemoryCap.Status))
	if status == "" || status == "skipped" || (status != "pass" && status != "passed") {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "memory-cap-tests",
			Path:    path,
			Message: fmt.Sprintf("memory-cap tests must pass and not be skipped (status=%q)", test.MemoryCap.Status),
		})
	}
	if test.MemoryCap.Flaky {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "memory-cap-tests",
			Path:    path,
			Message: "memory-cap tests must not be marked flaky",
		})
	}
	if test.MemoryCap.ProfileMB != 50 {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "memory-cap-tests",
			Path:    path,
			Message: fmt.Sprintf("memory-cap profile must be 50 MB (profile_mb=%d)", test.MemoryCap.ProfileMB),
		})
	}
}

func validateReportFreshness(report *guardrailReport, path, label, generatedAt string, maxAge time.Duration) {
	ts := strings.TrimSpace(generatedAt)
	if ts == "" {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-report-stale",
			Path:    path,
			Message: fmt.Sprintf("%s is missing generated_at", label),
		})
		return
	}
	parsed, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-report-stale",
			Path:    path,
			Message: fmt.Sprintf("%s has invalid generated_at %q", label, generatedAt),
		})
		return
	}
	age := time.Since(parsed)
	if age < 0 {
		age = -age
	}
	if age > maxAge {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-report-stale",
			Path:    path,
			Message: fmt.Sprintf("%s is stale (%s old, max %s)", label, age.Round(time.Minute), maxAge),
		})
	}
}

func validateDifferentialEvidenceInputs(report *guardrailReport, path string, latest differentialReport) {
	roots := latest.InputRoots
	if len(roots) == 0 {
		roots = parityEvidenceInputRoots
	}
	if strings.TrimSpace(latest.InputsDigest) == "" {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-evidence-stale",
			Path:    path,
			Message: "differential report is missing inputs_digest (rerun parity harness)",
		})
		return
	}
	currentDigest, err := computeInputDigest(roots)
	if err != nil {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-evidence-stale",
			Path:    path,
			Message: fmt.Sprintf("failed to recompute parity evidence digest: %v", err),
		})
		return
	}
	if !strings.EqualFold(strings.TrimSpace(currentDigest), strings.TrimSpace(latest.InputsDigest)) {
		report.Failures = append(report.Failures, reportFailure{
			Rule:    "differential-evidence-stale",
			Path:    path,
			Message: "differential evidence is outdated vs current sources (rerun parity harness)",
		})
	}
}

func computeInputDigest(roots []string) (string, error) {
	files, err := collectInputDigestFiles(roots)
	if err != nil {
		return "", err
	}
	if len(files) == 0 {
		return "", errors.New("no input files found for parity evidence digest")
	}

	h := sha256.New()
	for _, path := range files {
		bs, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		normalized := filepath.ToSlash(path)
		if _, err := h.Write([]byte(normalized)); err != nil {
			return "", err
		}
		if _, err := h.Write([]byte{0}); err != nil {
			return "", err
		}
		if _, err := h.Write(bs); err != nil {
			return "", err
		}
		if _, err := h.Write([]byte{0}); err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func collectInputDigestFiles(roots []string) ([]string, error) {
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, root := range roots {
		root = filepath.Clean(strings.TrimSpace(root))
		if root == "" {
			continue
		}
		info, err := os.Stat(root)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			normalized := filepath.ToSlash(root)
			if _, ok := seen[normalized]; !ok {
				seen[normalized] = struct{}{}
				out = append(out, root)
			}
			continue
		}
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				name := d.Name()
				if name == ".git" || name == "target" || name == ".gocache" {
					return filepath.SkipDir
				}
				return nil
			}
			normalized := filepath.ToSlash(path)
			if _, ok := seen[normalized]; ok {
				return nil
			}
			seen[normalized] = struct{}{}
			out = append(out, path)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	sort.Strings(out)
	return out, nil
}

func normalizeScenarioEvidence(evidence string) string {
	switch strings.ToLower(strings.TrimSpace(evidence)) {
	case "synthetic":
		return "synthetic"
	case "component":
		return "component"
	case "daemon", "runtime":
		return "daemon"
	case "peer-interop", "interop":
		return "peer-interop"
	case "external-soak", "soak":
		return "external-soak"
	default:
		return ""
	}
}

func scenarioEvidenceRank(evidence string) int {
	switch normalizeScenarioEvidence(evidence) {
	case "synthetic":
		return 0
	case "component":
		return 1
	case "daemon":
		return 2
	case "peer-interop":
		return 3
	case "external-soak":
		return 4
	default:
		return 0
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

func writeReplacementGapFile(path, rule string, sourceCount, targetCount int, missing []string) error {
	normalizedMissing := make([]string, len(missing))
	copy(normalizedMissing, missing)
	sort.Strings(normalizedMissing)
	report := replacementGapReport{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Rule:          rule,
		SourceCount:   sourceCount,
		TargetCount:   targetCount,
		Missing:       normalizedMissing,
	}
	return writeJSON(path, report)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
