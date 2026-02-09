# File Review: `syncthing-rs/src/folder_core.rs`

<!-- parity-rollup-meta: {"rust_path":"syncthing-rs/src/folder_core.rs","findings":112,"p0":0,"p1":112,"p2":0,"p3":0} -->
- Updated: 2026-02-09T05:32:22Z
- Findings: 112 (P0=0, P1=112, P2=0, P3=0)

## Top Findings
1. [P1] Constant/value drift between Go and Rust (`lib/model/folder.go:42 | syncthing-rs/src/folder_core.rs:19`) Go assignment value `10000` differs from Rust value `1000`; this can change compatibility/behavior.
2. [P1] Constant/value drift between Go and Rust (`lib/model/folder_sendrecv.go:65 | syncthing-rs/src/folder_core.rs:35`) Go assignment value `fs.ModeSetgid | fs.ModeSetuid | fs.ModeSticky` differs from Rust value `1.0`; this can change compatibility/behavior.
3. [P1] Constant/value drift between Go and Rust (`lib/model/folder_sendrecv.go:65 | syncthing-rs/src/folder_core.rs:37`) Go assignment value `fs.ModeSetgid | fs.ModeSetuid | fs.ModeSticky` differs from Rust value `4096`; this can change compatibility/behavior.
4. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:1852`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
5. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:1998`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
6. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2006`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
7. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2017`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
8. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2028`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
9. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2072`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
10. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2091`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
11. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2097`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
12. [P1] Error-return contract drift (`syncthing-rs/src/folder_core.rs:2110`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.

_Additional findings omitted: 100_
