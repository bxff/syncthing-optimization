# File Review: `syncthing-rs/src/config.rs`

<!-- parity-rollup-meta: {"rust_path":"syncthing-rs/src/config.rs","findings":2,"p0":0,"p1":2,"p2":0,"p3":0} -->
- Updated: 2026-02-09T05:32:22Z
- Findings: 2 (P0=0, P1=2, P2=0, P3=0)

## Top Findings
1. [P1] Function parameter count drift (`lib/config/folderconfiguration.go:125 | syncthing-rs/src/config.rs:365`) Go signature exposes 1 params while Rust exposes 0 params; this often indicates dropped or added behavior.
2. [P1] Function parameter count drift (`lib/config/folderconfiguration.go:415 | syncthing-rs/src/config.rs:568`) Go signature exposes 2 params while Rust exposes 1 params; this often indicates dropped or added behavior.
