# File Review: `syncthing-rs/src/bep_compat.rs`

<!-- parity-rollup-meta: {"rust_path":"syncthing-rs/src/bep_compat.rs","findings":41,"p0":0,"p1":41,"p2":0,"p3":0} -->
- Updated: 2026-02-09T05:32:22Z
- Findings: 41 (P0=0, P1=41, P2=0, P3=0)

## Top Findings
1. [P1] Constant/value drift between Go and Rust (`internal/gen/bep/bep.pb.go:2018 | syncthing-rs/src/bep_compat.rs:1114`) Go assignment value `[]byte{` differs from Rust value `"hello version too old"`; this can change compatibility/behavior.
2. [P1] Constant/value drift between Go and Rust (`internal/gen/bep/bep.pb.go:2018 | syncthing-rs/src/bep_compat.rs:1160`) Go assignment value `[]byte{` differs from Rust value `b"syntax = \"proto3\"; package bep;"`; this can change compatibility/behavior.
3. [P1] Constant/value drift between Go and Rust (`internal/gen/bep/bep.pb.go:2322 | syncthing-rs/src/bep_compat.rs:1117`) Go assignment value `make([]protoimpl.EnumInfo, 8)` differs from Rust value `"_"`; this can change compatibility/behavior.
4. [P1] Constant/value drift between Go and Rust (`internal/gen/bep/bep.pb.go:2322 | syncthing-rs/src/bep_compat.rs:1160`) Go assignment value `make([]protoimpl.EnumInfo, 8)` differs from Rust value `b"syntax = \"proto3\"; package bep;"`; this can change compatibility/behavior.
5. [P1] Constant/value drift between Go and Rust (`internal/gen/bep/bep.pb.go:2322 | syncthing-rs/src/bep_compat.rs:1160`) Go assignment value `make([]protoimpl.EnumInfo, 8)` differs from Rust value `b"syntax = \"proto3\"; package bep;"`; this can change compatibility/behavior.
6. [P1] Constant/value drift between Go and Rust (`internal/gen/bep/bep.pb.go:2356 | syncthing-rs/src/bep_compat.rs:1116`) Go assignment value `[]int32{` differs from Rust value `"bep.proto"`; this can change compatibility/behavior.
7. [P1] Constant/value drift between Go and Rust (`lib/protocol/bep_fileinfo.go:48 | syncthing-rs/src/bep_compat.rs:1114`) Go assignment value `map[FlagLocal]string{` differs from Rust value `"hello version too old"`; this can change compatibility/behavior.
8. [P1] Constant/value drift between Go and Rust (`lib/protocol/bep_fileinfo.go:662 | syncthing-rs/src/bep_compat.rs:1161`) Go assignment value `map[int][sha256.Size]byte{` differs from Rust value `OnceLock::new()`; this can change compatibility/behavior.
9. [P1] Function parameter count drift (`lib/protocol/bep_fileinfo.go:156 | syncthing-rs/src/bep_compat.rs:219`) Go signature exposes 1 params while Rust exposes 0 params; this often indicates dropped or added behavior.
10. [P1] Function parameter count drift (`lib/protocol/bep_fileinfo.go:156 | syncthing-rs/src/bep_compat.rs:219`) Go signature exposes 1 params while Rust exposes 0 params; this often indicates dropped or added behavior.
11. [P1] Function parameter count drift (`lib/protocol/bep_fileinfo.go:294 | syncthing-rs/src/bep_compat.rs:1239`) Go signature exposes 2 params while Rust exposes 1 params; this often indicates dropped or added behavior.
12. [P1] Function parameter count drift (`lib/protocol/bep_fileinfo.go:383 | syncthing-rs/src/bep_compat.rs:576`) Go signature exposes 0 params while Rust exposes 1 params; this often indicates dropped or added behavior.

_Additional findings omitted: 29_
