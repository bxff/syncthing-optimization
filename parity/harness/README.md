# Differential Harness

This harness compares deterministic Go and Rust scenario snapshots and emits parity gate reports.

## Scenario Config

- File: `parity/harness/scenarios.json`
- Each scenario has:
  - `id`, `description`, `severity`, `required`, `tags`
  - `comparator` (currently `json-equal`)
  - `go` and `rust` side configs with either:
    - `snapshot_path` (read existing JSON snapshot), or
    - `command` (run command and parse stdout JSON), optionally writing `snapshot_path`

## Run

```bash
GOCACHE=/tmp/go-cache go run ./script/parity_harness.go run \
  --config parity/harness/scenarios.json \
  --latest parity/diff-reports/latest.json \
  --test-status parity/diff-reports/test-status.json \
  --interop parity/diff-reports/interop.json \
  --durability parity/diff-reports/durability.json \
  --profile-mb 50
```

## Output Reports

- `parity/diff-reports/latest.json`
- `parity/diff-reports/test-status.json`
- `parity/diff-reports/interop.json`
- `parity/diff-reports/durability.json`

## Expected Lifecycle

1. Start with placeholder Go snapshots and missing Rust snapshots.
2. Add Rust snapshot producers per scenario.
3. Replace placeholder data with deterministic workload outputs.
4. Gate releases only when all required scenarios pass.
