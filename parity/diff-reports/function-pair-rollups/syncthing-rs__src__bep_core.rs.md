# File Review: `syncthing-rs/src/bep_core.rs`

<!-- parity-rollup-meta: {"rust_path":"syncthing-rs/src/bep_core.rs","findings":3,"p0":0,"p1":3,"p2":0,"p3":0} -->
- Updated: 2026-02-09T05:32:22Z
- Findings: 3 (P0=0, P1=3, P2=0, P3=0)

## Top Findings
1. [P1] Constant/value drift between Go and Rust (`lib/protocol/bep_fileinfo.go:48 | syncthing-rs/src/bep_core.rs:71`) Go assignment value `map[FlagLocal]string{` differs from Rust value `1 << 4`; this can change compatibility/behavior.
2. [P1] Constant/value drift between Go and Rust (`lib/protocol/bep_fileinfo.go:48 | syncthing-rs/src/bep_core.rs:81`) Go assignment value `map[FlagLocal]string{` differs from Rust value `FlagLocalReceiveOnly | FlagLocalRemoteInvalid`; this can change compatibility/behavior.
3. [P1] Constant/value drift between Go and Rust (`lib/protocol/bep_fileinfo.go:48 | syncthing-rs/src/bep_core.rs:81`) Go assignment value `map[FlagLocal]string{` differs from Rust value `FlagLocalReceiveOnly | FlagLocalRemoteInvalid`; this can change compatibility/behavior.
