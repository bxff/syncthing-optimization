use crate::bep::{decode_frame, default_exchange, encode_frame, message_name};
use crate::folder_modes::all_mode_actions;
use crate::index_engine::{FolderUpdate, IndexEngine};
use crate::planner::{classify_paths, compute_need, VersionedFile};
use crate::protocol::{default_sequence, run_events};
use crate::store::{FileMetadata, PageCursor, Store, StoreConfig, JOURNAL_FILE_NAME};
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
        "status": "prototype",
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
        "status": "prototype",
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
        "status": "prototype",
        "ignored_count": ignored_count,
        "ignored_paths": classification.ignored,
        "conflict_count": conflict_count,
        "conflict_paths": classification.conflicts,
        "considered_count": considered_count
    }))
}

fn scenario_folder_type_behavior() -> Result<Value, String> {
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

    Ok(json!({
        "scenario": "folder-type-behavior",
        "source": "rust",
        "status": "prototype",
        "folder_modes": mode_json
    }))
}

fn scenario_protocol_state_transition() -> Result<Value, String> {
    let trace = run_events(&default_sequence())?;
    let exchange = default_exchange();
    let mut frame_sizes = Vec::new();
    let mut message_types = Vec::new();
    for message in &exchange {
        let frame = encode_frame(message)?;
        frame_sizes.push(frame.len());
        let decoded = decode_frame(&frame)?;
        message_types.push(message_name(&decoded));
    }
    Ok(json!({
        "scenario": "protocol-state-transition",
        "source": "rust",
        "status": "prototype",
        "transitions": trace,
        "message_types": message_types,
        "frame_sizes": frame_sizes
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
        "status": "prototype",
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
            .delete_file("default", "tmp-delete.txt")
            .map_err(err_to_string)?;
        store.compact().map_err(err_to_string)?;
    }

    let store = Store::open(StoreConfig::new(&root)).map_err(err_to_string)?;
    let stats = store.stats();
    let out = json!({
        "scenario": "wal-free-durability",
        "source": "rust",
        "status": "prototype",
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
