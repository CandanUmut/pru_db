# PRU-DB — Core Workspace Skeleton
This workspace contains multiple Rust crates:
- crates/pru_core      : core engine (segments, filters, postings, resolver keys, manifest, atoms)
- crates/pru_cli       : CLI (init, add-resolver, resolve, info, verify)
- crates/pru_py        : Python bindings via pyo3 (minimal)
- crates/pru_bench     : Criterion benchmarks (skeleton)

NOTE: Cargo.toml files are intentionally omitted (per setup request). Add them later.
