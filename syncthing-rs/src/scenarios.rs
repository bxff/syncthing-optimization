use crate::bep::{decode_frame, default_exchange, encode_frame, message_name};
use crate::config::demo_configs;
use crate::folder_modes::all_mode_actions;
use crate::index_engine::{FolderUpdate, IndexEngine};
use crate::model_core::{newFolderConfiguration, NewModel};
use crate::planner::{classify_paths, compute_need, VersionedFile};
use crate::store::{FileMetadata, PageCursor, Store, StoreConfig, JOURNAL_FILE_NAME};
use crate::walker::{walk_deterministic, WalkConfig};
use serde_json::{json, Value};
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
        "path-order-invariant" => scenario_path_order_invariant(),
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
        "path-order-invariant",
        "memory-cap-50mb",
        "wal-free-durability",
        "crash-recovery",
    ]
}

fn scenario_index_sequence_behavior() -> Result<Value, String> {
    let root = scenario_root("index-sequence-behavior")?;
    let mut store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
    let mut index = IndexEngine::new();
    let mut rejected_updates = 0_usize;

    store
        .upsert_file(meta("default", "c.txt", 3, false, 300, vec!["h3"]))
        .map_err(err_to_string)?;
    if let Err(_err) = index.apply_update(
        "default",
        FolderUpdate {
            path: "c.txt".to_string(),
            sequence: 3,
            deleted: false,
            ignored: false,
        },
    ) {
        rejected_updates += 1;
    }
    store
        .upsert_file(meta("default", "a.txt", 1, false, 100, vec!["h1"]))
        .map_err(err_to_string)?;
    if let Err(_err) = index.apply_update(
        "default",
        FolderUpdate {
            path: "a.txt".to_string(),
            sequence: 1,
            deleted: false,
            ignored: false,
        },
    ) {
        rejected_updates += 1;
    }
    store
        .upsert_file(meta("default", "b.txt", 2, false, 200, vec!["h2"]))
        .map_err(err_to_string)?;
    if let Err(_err) = index.apply_update(
        "default",
        FolderUpdate {
            path: "b.txt".to_string(),
            sequence: 2,
            deleted: false,
            ignored: false,
        },
    ) {
        rejected_updates += 1;
    }
    if let Err(_err) = index.apply_update(
        "default",
        FolderUpdate {
            path: "late.txt".to_string(),
            sequence: 2,
            deleted: false,
            ignored: false,
        },
    ) {
        rejected_updates += 1;
    }
    let prefix_hits = store.all_files_in_folder_prefix("default", "a");

    let ordered: Vec<Value> = store
        .all_files_lexicographic()
        .into_iter()
        .map(|f| json!({"folder": f.folder, "path": f.path, "sequence": f.sequence}))
        .collect();
    let mut cursor_path: Option<String> = None;
    let mut paged_paths = Vec::new();
    loop {
        let page = store.files_in_folder_ordered_page("default", cursor_path.as_deref(), 2);
        for f in &page.items {
            paged_paths.push(f.path.clone());
        }
        match page.next_cursor {
            Some(next) => cursor_path = Some(next.path),
            None => break,
        }
    }

    let out = json!({
        "scenario": "index-sequence-behavior",
        "source": "rust",
        "status": "validated",
        "prefix_a_count": prefix_hits.len(),
        "ordered_files": ordered,
        "paged_paths": paged_paths,
        "index_folder_sequence": index.folder_sequence("default"),
        "index_file_count": index.ordered_files(Some("default")).len(),
        "rejected_updates": rejected_updates
    });

    cleanup(root);
    Ok(out)
}

fn scenario_global_need_decision() -> Result<Value, String> {
    let local = vec![
        versioned("a.txt", 2, false, false),
        versioned("b.txt", 5, false, false),
        versioned("c.txt", 1, false, false),
        versioned("e.txt", 10, false, false),
    ];
    let remote = vec![
        versioned("a.txt", 3, false, false),
        versioned("b.txt", 5, false, false),
        versioned("d.txt", 1, false, false),
        versioned("e.txt", 11, true, false),
    ];
    let plan = compute_need(&local, &remote);
    let need_count = plan.need_paths.len();
    let stale_delete_count = plan.stale_deletes.len();

    Ok(json!({
        "scenario": "global-need-decision",
        "source": "rust",
        "status": "validated",
        "need_paths": plan.need_paths,
        "need_count": need_count,
        "stale_deletes": plan.stale_deletes,
        "stale_delete_count": stale_delete_count
    }))
}

fn scenario_conflict_and_ignore_semantics() -> Result<Value, String> {
    let paths = vec![
        "docs/readme.md",
        "docs/.DS_Store",
        "tmp/build.tmp",
        "docs/readme.sync-conflict-20260207.md",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();

    let classification = classify_paths(&paths, &[".tmp", ".DS_Store"]);
    let ignored_count = classification.ignored.len();
    let conflict_count = classification.conflicts.len();
    let considered_count = classification.considered.len();

    Ok(json!({
        "scenario": "conflict-and-ignore-semantics",
        "source": "rust",
        "status": "validated",
        "ignored_count": ignored_count,
        "ignored_paths": classification.ignored,
        "conflict_count": conflict_count,
        "conflict_paths": classification.conflicts,
        "considered_count": considered_count
    }))
}

fn scenario_folder_type_behavior() -> Result<Value, String> {
    let configs = demo_configs();
    for cfg in &configs {
        cfg.validate()?;
    }
    let actions = all_mode_actions();
    let mut mode_json = serde_json::Map::new();
    for action in actions {
        mode_json.insert(
            action.mode.as_str().to_string(),
            json!({
                "pipeline": action.pipeline,
                "may_push": action.may_push,
                "requires_local_revert": action.requires_local_revert,
                "encrypted_index": action.encrypted_index
            }),
        );
    }
    let config_modes = configs
        .iter()
        .map(|cfg| {
            (
                cfg.id.clone(),
                json!({
                    "folder_type": cfg.folder_type.as_str(),
                    "mode": cfg.folder_type.to_mode().as_str(),
                    "fs_watcher_enabled": cfg.fs_watcher_enabled,
                    "rescan_interval_s": cfg.rescan_interval_s,
                    "paused": cfg.paused
                }),
            )
        })
        .collect::<serde_json::Map<String, serde_json::Value>>();

    Ok(json!({
        "scenario": "folder-type-behavior",
        "source": "rust",
        "status": "validated",
        "folder_modes": mode_json,
        "folder_configs": config_modes
    }))
}

fn scenario_protocol_state_transition() -> Result<Value, String> {
    let exchange = default_exchange();
    let mut frame_sizes = Vec::new();
    let mut message_types = Vec::new();
    for message in &exchange {
        let frame = encode_frame(message)?;
        frame_sizes.push(frame.len());
        let decoded = decode_frame(&frame)?;
        message_types.push(message_name(&decoded));
    }

    let req_root = scenario_root("protocol-state-transition")?;
    fs::write(req_root.join("a.txt"), b"hello-world").map_err(err_to_string)?;
    let mut model = NewModel();
    model.newFolder(newFolderConfiguration(
        "default",
        &req_root.to_string_lossy(),
    ));
    let exchange_result = model.RunBepExchange("peer-a", &exchange)?;
    let generated_responses = exchange_result
        .outbound_messages
        .iter()
        .filter_map(|message| match message {
            crate::bep::BepMessage::Response { id, code, data_len } => Some(json!({
                "id": id,
                "code": code,
                "data_len": data_len,
            })),
            _ => None,
        })
        .collect::<Vec<_>>();
    let request_data = model.RequestData("default", "a.txt", 6, 5)?;
    let request_data_hex = bytes_to_hex(&request_data);
    let device_downloads = model.DownloadProgress("peer-a");
    let remote_sequence = model
        .RemoteSequences("default")
        .get("remote")
        .copied()
        .unwrap_or_default();
    cleanup(req_root);

    Ok(json!({
        "scenario": "protocol-state-transition",
        "source": "rust",
        "status": "validated",
        "transitions": exchange_result.transitions,
        "message_types": message_types,
        "frame_sizes": frame_sizes,
        "generated_responses": generated_responses,
        "device_downloads": device_downloads,
        "remote_sequence": remote_sequence,
        "request_data_hex": request_data_hex
    }))
}

fn scenario_path_order_invariant() -> Result<Value, String> {
    let root = scenario_root("path-order-invariant")?;
    let fs_root = root.join("fs");
    let spill_root = root.join("spill");
    let store_root = root.join("store");
    fs::create_dir_all(&fs_root).map_err(err_to_string)?;
    fs::create_dir_all(&spill_root).map_err(err_to_string)?;

    let files = [
        "a/x.txt",
        "a/z.txt",
        "a.d/x.txt",
        "a.d/y.txt",
        "a.d/y/z.txt",
        "aa.bin",
        "b.txt",
    ];
    for file in files {
        let path = fs_root.join(file);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(err_to_string)?;
        }
        fs::write(path, b"v").map_err(err_to_string)?;
    }

    let cfg = WalkConfig::new(&spill_root).with_spill_threshold_entries(2);
    let mut walk_paths = Vec::new();
    let walk_stats = walk_deterministic(&fs_root, &cfg, |path| walk_paths.push(path))
        .map_err(err_to_string)?;

    let mut store = Store::open(StoreConfig::new(&store_root)).map_err(err_to_string)?;
    for (idx, path) in walk_paths.iter().enumerate() {
        store
            .upsert_file(meta(
                "default",
                path,
                (idx as u64) + 1,
                false,
                idx as u64,
                vec!["h"],
            ))
            .map_err(err_to_string)?;
    }

    let mut db_paths = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = store.files_in_folder_ordered_page("default", cursor.as_deref(), 2);
        for file in page.items {
            db_paths.push(file.path);
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next.path),
            None => break,
        }
    }

    let paths_match = walk_paths == db_paths;
    let output = json!({
        "scenario": "path-order-invariant",
        "source": "rust",
        "status": "validated",
        "walk_paths": walk_paths,
        "db_paths": db_paths,
        "match": paths_match,
        "spill_files_created": walk_stats.spill_files_created,
        "files_emitted": walk_stats.files_emitted,
        "directories_seen": walk_stats.directories_seen,
    });

    cleanup(root);
    Ok(output)
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

    let mut page_cursor: Option<PageCursor> = None;
    let mut scanned_entries = 0_usize;
    let mut page_count = 0_usize;
    loop {
        let page = store.all_files_ordered_page(page_cursor.as_ref(), 1000);
        scanned_entries += page.items.len();
        page_count += 1;
        match page.next_cursor {
            Some(next) => page_cursor = Some(next),
            None => break,
        }
    }

    let stats = store.stats();
    let out = json!({
        "scenario": "memory-cap-50mb",
        "source": "rust",
        "status": "validated",
        "estimated_memory_bytes": stats.estimated_memory_bytes,
        "memory_budget_bytes": stats.memory_budget_bytes,
        "file_count": stats.file_count,
        "scanned_entries": scanned_entries,
        "page_count": page_count,
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
            .delete_file("default", "local", "tmp-delete.txt")
            .map_err(err_to_string)?;
        store.compact().map_err(err_to_string)?;
    }

    let store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
    let stats = store.stats();
    let out = json!({
        "scenario": "wal-free-durability",
        "source": "rust",
        "status": "validated",
        "file_count": store.file_count(),
        "deleted_tombstone_count": stats.deleted_tombstone_count,
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
    let a_present = recovered.get_file("default", "local", "a.txt").is_some();
    let out = json!({
        "scenario": "crash-recovery",
        "source": "rust",
        "status": "validated",
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

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect::<String>()
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
        device: "local".to_string(),
        path: path.to_string(),
        sequence,
        deleted,
        ignored: false,
        local_flags: 0,
        file_type: "file".to_string(),
        modified_ns,
        size: 0,
        block_hashes: block_hashes.into_iter().map(str::to_string).collect(),
    }
}

fn err_to_string(err: impl std::fmt::Display) -> String {
    err.to_string()
}

fn versioned(path: &str, sequence: u64, deleted: bool, ignored: bool) -> VersionedFile {
    VersionedFile {
        path: path.to_string(),
        sequence,
        deleted,
        ignored,
    }
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
