# File Review: `syncthing-rs/src/model_core.rs`

<!-- parity-rollup-meta: {"rust_path":"syncthing-rs/src/model_core.rs","findings":125,"p0":0,"p1":125,"p2":0,"p3":0} -->
- Updated: 2026-02-09T05:32:22Z
- Findings: 125 (P0=0, P1=125, P2=0, P3=0)

## Top Findings
1. [P1] Constant/value drift between Go and Rust (`lib/model/model.go:184 | syncthing-rs/src/model_core.rs:39`) Go assignment value `&model{}` differs from Rust value `"no versioner configured"`; this can change compatibility/behavior.
2. [P1] Constant/value drift between Go and Rust (`lib/model/model.go:188 | syncthing-rs/src/model_core.rs:21`) Go assignment value `make(map[config.FolderType]folderFactory)` differs from Rust value `"folder not running"`; this can change compatibility/behavior.
3. [P1] Constant/value drift between Go and Rust (`lib/model/model.go:188 | syncthing-rs/src/model_core.rs:38`) Go assignment value `make(map[config.FolderType]folderFactory)` differs from Rust value `"remote device missing in cluster config"`; this can change compatibility/behavior.
4. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1029`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
5. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1069`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
6. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1091`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
7. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1096`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
8. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1100`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
9. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1107`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
10. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1114`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
11. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1188`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.
12. [P1] Error-return contract drift (`syncthing-rs/src/model_core.rs:1210`) Go signature returns `error` but Rust signature does not return a `Result`, which can hide failure paths.

_Additional findings omitted: 113_
