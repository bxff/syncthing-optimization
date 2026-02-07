# Parity Guardrails

This directory contains the enforceable parity contract for the Rust rewrite.

## Files

- `feature-manifest.json`: machine-generated source-of-truth feature inventory from selected Go surfaces.
- `mapping-rust.json`: per-feature Rust implementation mapping (`rust_component`, `rust_symbol`, required tests, status).
- `exceptions.json`: explicit, approved exceptions for specific guardrail rules.
- `diff-reports/*.json`: differential harness results (Go vs Rust), interop, durability, and memory-cap test status.
- `guardrail-report.json`: CI-mode guardrail result.
- `guardrail-report-release.json`: release-mode guardrail result.
- `dashboard.json` / `dashboard.md`: completeness dashboard by subsystem.

## Commands

Regenerate artifacts:

```bash
go run ./script/parity.go generate
```

Run differential parity harness:

```bash
GOCACHE=/tmp/go-cache go run ./script/parity_harness.go run \
  --config parity/harness/scenarios.json \
  --latest parity/diff-reports/latest.json \
  --test-status parity/diff-reports/test-status.json \
  --interop parity/diff-reports/interop.json \
  --durability parity/diff-reports/durability.json \
  --profile-mb 50
```

Run guardrails in CI mode:

```bash
go run ./script/parity.go check --mode ci --report parity/guardrail-report.json
```

Run guardrails in release mode:

```bash
go run ./script/parity.go check --mode release --report parity/guardrail-report-release.json
```

Generate dashboard:

```bash
go run ./script/parity.go dashboard
```

## Status values

- `missing`: feature not implemented in Rust.
- `implemented`: implemented in Rust, but not parity-verified.
- `parity-verified`: implemented and verified against parity requirements.

## Exception policy

No implicit exceptions are allowed. Any exception must be entered in `exceptions.json` with:

- `rule`
- `reason`
- `approved_by`
- `decision_record`
- optional `expires_at` (RFC3339)

Expired exceptions are ignored by guardrail checks.
