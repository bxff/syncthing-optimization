use crate::store::{FileMetadata, Store, StoreConfig, JOURNAL_FILE_NAME};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn run_scenario_snapshot(id: &str) -> Result<Value, String> {
    match id {
        "index-sequence-behavior" => scenario_index_sequence_behavior(),
        "global-need-decision" => scenario_global_need_decision(),
        "conflict-and-ignore-semantics" => scenario_conflict_and_ignore_semantics(),
        "folder-type-behavior" => scenario_folder_type_behavior(),
        "protocol-state-transition" => scenario_protocol_state_transition(),
        "memory-cap-50mb" => scenario_memory_cap_50mb(),
        "wal-free-durability" => scenario_wal_free_durability(),
        "crash-recovery" => scenario_crash_recovery(),
        _ => Err(format!("unknown scenario id: {id}")),
    }
}

pub(crate) fn scenario_ids() -> &'static [&'static str] {
    &[
        "index-sequence-behavior",
        "global-need-decision",
        "conflict-and-ignore-semantics",
        "folder-type-behavior",
        "protocol-state-transition",
        "memory-cap-50mb",
        "wal-free-durability",
        "crash-recovery",
    ]
}

fn scenario_index_sequence_behavior() -> Result<Value, String> {
    let root = scenario_root("index-sequence-behavior")?;
    let mut store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;

    store
        .upsert_file(meta("default", "c.txt", 3, false, 300, vec!["h3"]))
        .map_err(err_to_string)?;
    store
        .upsert_file(meta("default", "a.txt", 1, false, 100, vec!["h1"]))
        .map_err(err_to_string)?;
    store
        .upsert_file(meta("default", "b.txt", 2, false, 200, vec!["h2"]))
        .map_err(err_to_string)?;
    let prefix_hits = store.all_files_in_folder_prefix("default", "a");

    let ordered: Vec<Value> = store
        .all_files_lexicographic()
        .into_iter()
        .map(|f| json!({"folder": f.folder, "path": f.path, "sequence": f.sequence}))
        .collect();

    let out = json!({
        "scenario": "index-sequence-behavior",
        "source": "rust",
        "status": "prototype",
        "prefix_a_count": prefix_hits.len(),
        "ordered_files": ordered
    });

    cleanup(root);
    Ok(out)
}

fn scenario_global_need_decision() -> Result<Value, String> {
    let local = BTreeMap::from([
        ("a.txt".to_string(), 2_u64),
        ("b.txt".to_string(), 5_u64),
        ("c.txt".to_string(), 1_u64),
    ]);
    let remote = BTreeMap::from([
        ("a.txt".to_string(), 3_u64),
        ("b.txt".to_string(), 5_u64),
        ("d.txt".to_string(), 1_u64),
    ]);

    let mut need = Vec::new();
    for (path, remote_seq) in &remote {
        let local_seq = local.get(path).copied().unwrap_or(0);
        if *remote_seq > local_seq {
            need.push(path.clone());
        }
    }

    Ok(json!({
        "scenario": "global-need-decision",
        "source": "rust",
        "status": "prototype",
        "need_paths": need,
        "need_count": need.len()
    }))
}

fn scenario_conflict_and_ignore_semantics() -> Result<Value, String> {
    let paths = [
        "docs/readme.md",
        "docs/.DS_Store",
        "tmp/build.tmp",
        "docs/readme.sync-conflict-20260207.md",
    ];

    let ignored = paths
        .iter()
        .filter(|p| p.ends_with(".tmp") || p.ends_with(".DS_Store"))
        .count();
    let conflicts = paths.iter().filter(|p| p.contains("sync-conflict")).count();

    Ok(json!({
        "scenario": "conflict-and-ignore-semantics",
        "source": "rust",
        "status": "prototype",
        "ignored_count": ignored,
        "conflict_count": conflicts,
        "considered_count": paths.len() - ignored
    }))
}

fn scenario_folder_type_behavior() -> Result<Value, String> {
    Ok(json!({
        "scenario": "folder-type-behavior",
        "source": "rust",
        "status": "prototype",
        "folder_modes": {
            "sendrecv": ["scan", "index", "pull", "push"],
            "recvonly": ["scan", "index", "pull", "local_revert_required"],
            "recvenc": ["scan", "index_encrypted", "pull_encrypted"]
        }
    }))
}

fn scenario_protocol_state_transition() -> Result<Value, String> {
    Ok(json!({
        "scenario": "protocol-state-transition",
        "source": "rust",
        "status": "prototype",
        "transitions": [
            "dial",
            "hello",
            "cluster_config",
            "index",
            "index_update",
            "ping"
        ]
    }))
}

fn scenario_memory_cap_50mb() -> Result<Value, String> {
    let root = scenario_root("memory-cap-50mb")?;
    let mut store =
        Store::open(StoreConfig::new(&root).with_memory_cap_mb(50)).map_err(err_to_string)?;

    for i in 0_u64..10_000 {
        store
            .upsert_file(meta(
                "default",
                &format!("dir/{i:05}.bin"),
                i + 1,
                false,
                i,
                vec!["a1b2c3d4e5f6"],
            ))
            .map_err(err_to_string)?;
    }

    let stats = store.stats();
    let out = json!({
        "scenario": "memory-cap-50mb",
        "source": "rust",
        "status": "prototype",
        "estimated_memory_bytes": stats.estimated_memory_bytes,
        "memory_budget_bytes": stats.memory_budget_bytes,
        "under_budget": stats.estimated_memory_bytes <= stats.memory_budget_bytes
    });

    cleanup(root);
    Ok(out)
}

fn scenario_wal_free_durability() -> Result<Value, String> {
    let root = scenario_root("wal-free-durability")?;

    {
        let mut store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
        store
            .upsert_file(meta("default", "one.txt", 1, false, 100, vec!["h1"]))
            .map_err(err_to_string)?;
        store
            .upsert_file(meta("default", "two.txt", 2, false, 200, vec!["h2"]))
            .map_err(err_to_string)?;
        store
            .upsert_file(meta("default", "tmp-delete.txt", 3, false, 300, vec!["h3"]))
            .map_err(err_to_string)?;
        store
            .delete_file("default", "tmp-delete.txt")
            .map_err(err_to_string)?;
        store.compact().map_err(err_to_string)?;
    }

    let store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
    let out = json!({
        "scenario": "wal-free-durability",
        "source": "rust",
        "status": "prototype",
        "file_count": store.file_count(),
        "journal_file": JOURNAL_FILE_NAME,
        "paths": store
            .all_files_lexicographic()
            .into_iter()
            .map(|f| f.path)
            .collect::<Vec<_>>()
    });

    cleanup(root);
    Ok(out)
}

fn scenario_crash_recovery() -> Result<Value, String> {
    let root = scenario_root("crash-recovery")?;

    {
        let mut store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
        store
            .upsert_file(meta("default", "a.txt", 1, false, 100, vec!["h1"]))
            .map_err(err_to_string)?;
        store
            .upsert_file(meta("default", "b.txt", 2, false, 200, vec!["h2"]))
            .map_err(err_to_string)?;

        let mut tail = OpenOptions::new()
            .append(true)
            .open(store.journal_path())
            .map_err(err_to_string)?;
        tail.write_all(&512_u32.to_le_bytes())
            .map_err(err_to_string)?;
        tail.write_all(&0_u32.to_le_bytes())
            .map_err(err_to_string)?;
        tail.write_all(b"{").map_err(err_to_string)?;
        tail.sync_all().map_err(err_to_string)?;
    }

    let recovered = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
    let a_present = recovered.get_file("default", "a.txt").is_some();
    let out = json!({
        "scenario": "crash-recovery",
        "source": "rust",
        "status": "prototype",
        "file_count": recovered.file_count(),
        "a_present": a_present,
        "recovered_paths": recovered
            .all_files_lexicographic()
            .into_iter()
            .map(|f| f.path)
            .collect::<Vec<_>>()
    });

    cleanup(root);
    Ok(out)
}

fn scenario_root(id: &str) -> Result<PathBuf, String> {
    let mut root = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .as_nanos();
    root.push(format!(
        "syncthing-rs-scenario-{id}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root).map_err(err_to_string)?;
    Ok(root)
}

fn cleanup(path: PathBuf) {
    let _ = fs::remove_dir_all(path);
}

fn meta(
    folder: &str,
    path: &str,
    sequence: u64,
    deleted: bool,
    modified_ns: u64,
    block_hashes: Vec<&str>,
) -> FileMetadata {
    FileMetadata {
        folder: folder.to_string(),
        path: path.to_string(),
        sequence,
        deleted,
        modified_ns,
        size: 0,
        block_hashes: block_hashes.into_iter().map(str::to_string).collect(),
    }
}

fn err_to_string(err: impl std::fmt::Display) -> String {
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_scenarios_produce_json() {
        for id in scenario_ids() {
            let out = run_scenario_snapshot(id).expect("run scenario");
            assert_eq!(out["scenario"], *id);
            assert_eq!(out["source"], "rust");
        }
    }
}
