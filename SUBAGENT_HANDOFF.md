# Syncthing-RS Parity Continuation Handoff

## Current State
- Branch: `codex/rust-rewrite-parity-guardrails`
- Uncommitted modified files:
  - `script/parity_pair_review.go`
  - `script/parity_pair_review_test.go`
  - `syncthing-rs/src/db.rs`
  - `syncthing-rs/src/folder_core.rs`
  - `syncthing-rs/src/model_core.rs`
- Function-pair artifact explosion was cleaned up (large generated `function-pair-*` outputs removed).

## What Was Just Implemented
- Hardened parity runner against model outages/rate limits.
- Added deterministic local fallback behavior and a strict local-only execution mode.
- Added stronger deterministic checks in local review path:
  - constant/value drift
  - function parameter drift
  - error-contract drift
  - branch-shape mismatch
  - unwrap/expect in T1 logic paths
- Added tests for these behaviors in `script/parity_pair_review_test.go`.

## Why This Matters
- External model quota/infra failures should no longer block parity passes.
- We can continue parity review work without depending on Opus/Gemini availability.

## Required Guardrails
- Do not generate per-item `.txt` artifacts.
- Prefer JSON report outputs only.
- Use deterministic local checks as baseline gate.
- Use LLM review only as enrichment when quota allows.

## Commands To Continue

### 1) Validate runner changes
```bash
go test ./script/parity_pair_review.go ./script/parity_pair_review_test.go
```

### 2) Run full deterministic function-pair pass (quota-proof)
```bash
go run ./script/parity_pair_review.go run \
  --plan parity/diff-reports/function-pair-review-plan.json \
  --out parity/diff-reports/function-pair-discrepancy-report-local-only-full.json \
  --workers 12 \
  --local-only \
  --checkpoint-every 200 \
  --tier all
```

### 3) Pull top actionable findings
```bash
jq -r '.findings[] | .priority + "\t" + .title + "\t" + .id + "\t" + .location' \
  parity/diff-reports/function-pair-discrepancy-report-local-only-full.json \
  | sort | uniq -c | sort -nr | head -n 100
```

### 4) Fix highest-impact classes first
1. `P0/P1` in `syncthing-rs/src/model_core.rs`
2. `P0/P1` in `syncthing-rs/src/folder_core.rs`
3. `P0/P1` in `syncthing-rs/src/db.rs`

### 5) Re-run pass after each fix batch
```bash
go run ./script/parity_pair_review.go run \
  --plan parity/diff-reports/function-pair-review-plan.json \
  --out parity/diff-reports/function-pair-discrepancy-report-local-only-full.json \
  --workers 12 \
  --local-only \
  --checkpoint-every 200 \
  --tier all
```

### 6) Optional hybrid enrichment when model quota is available
```bash
go run ./script/parity_pair_review.go run \
  --plan parity/diff-reports/function-pair-review-plan.json \
  --out parity/diff-reports/function-pair-discrepancy-report-hybrid.json \
  --workers 8 \
  --tier T1 \
  --limit 200 \
  --model google/antigravity-claude-opus-4-6-thinking \
  --timeout 2m \
  --max-retries 2 \
  --retry-delay 2s \
  --fallback-local-on-infra-error
```

## Acceptance Target For This Phase
- `go test` for parity runner passes.
- Full local-only report runs with:
  - `errors = 0`
  - `agent_unavailable = 0`
- P0/P1 findings trend down each iteration.

## Commit Guidance
- Keep parity-runner changes and Rust runtime fixes in coherent commits.
- Suggested split:
  1. `feat(parity): make function-pair review quota-proof with local-only fallback`
  2. `fix(rust-parity): resolve P0/P1 parity discrepancies in core runtime paths`
