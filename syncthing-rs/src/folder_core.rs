#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::config::{FolderConfiguration, MemoryPolicy};
use crate::db::{self, Db};
use crate::folder_modes::{mode_actions, FolderMode};
use crate::planner::{classify_paths, compute_need, VersionedFile};
use crate::store::compare_path_order;
use crate::walker::{walk_deterministic, WalkConfig};
use crc32fast::Hasher;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

pub(crate) const deletedBatchSize: usize = 1000;
pub(crate) const kqueueItemCountThreshold: usize = 100_000;
pub(crate) const maxToRemove: usize = 10_000;
const LOCAL_DEVICE_ID: &str = "local";
const SCAN_DB_BATCH_SIZE: usize = 256;
const BLOCK_CHUNK_SIZE: usize = 128 * 1024;

pub(crate) const dbUpdateDeleteDir: u8 = 1;
pub(crate) const dbUpdateDeleteFile: u8 = 2;
pub(crate) const dbUpdateHandleDir: u8 = 3;
pub(crate) const dbUpdateHandleFile: u8 = 4;
pub(crate) const dbUpdateHandleSymlink: u8 = 5;
pub(crate) const dbUpdateInvalidate: u8 = 6;
pub(crate) const dbUpdateShortcutFile: u8 = 7;

pub(crate) const defaultCopiers: usize = 4;
pub(crate) const defaultPullerPause: f64 = 1.0;
pub(crate) const defaultPullerPendingKiB: usize = 64 * 1024;
pub(crate) const maxPullerIterations: usize = 4096;
pub(crate) const retainBits: u32 = 0xFFFF;

pub(crate) static activity: &str = "folder_activity";
pub(crate) static blockStats: &str = "block_stats";
pub(crate) static blockStatsMut: &str = "block_stats_mut";
pub(crate) static contextRemovingOldItem: &str = "removing_old_item";
pub(crate) static errDirHasIgnored: &str = "directory contains ignored children";
pub(crate) static errDirHasToBeScanned: &str = "directory needs scan before delete";
pub(crate) static errDirNotEmpty: &str = "directory is not empty";
pub(crate) static errDirPrefix: &str = "directory prefix mismatch";
pub(crate) static errIncompatibleSymlink: &str = "incompatible symlink";
pub(crate) static errModified: &str = "item modified during pull";
pub(crate) static errNoDevice: &str = "no remote device available";
pub(crate) static errNotAvailable: &str = "requested data not available";
pub(crate) static errUnexpectedDirOnFileDel: &str = "unexpected directory for file deletion";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FileError {
    pub(crate) Path: String,
    pub(crate) Err: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PullStats {
    pub(crate) needed_files: usize,
    pub(crate) total_blocks: usize,
    pub(crate) reused_same_path_blocks: usize,
    pub(crate) fetched_blocks: usize,
    pub(crate) throttled: bool,
    pub(crate) hard_blocked: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemoryTelemetry {
    pub(crate) estimated_bytes: usize,
    pub(crate) budget_bytes: usize,
    pub(crate) soft_limit_bytes: usize,
    pub(crate) throttle_events: u64,
    pub(crate) hard_block_events: u64,
    pub(crate) last_cap_violation: Option<String>,
    pub(crate) pull_throttled: bool,
    pub(crate) pull_hard_blocked: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct dbUpdateJob {
    pub(crate) jobType: dbUpdateType,
    pub(crate) file: db::FileInfo,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct sharedPullerState {
    pub(crate) folder: String,
    pub(crate) source_device: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct copyBlocksState {
    pub(crate) sharedPullerState: sharedPullerState,
    pub(crate) blocks: Vec<db::BlockMapEntry>,
    pub(crate) have: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct pullBlockState {
    pub(crate) sharedPullerState: sharedPullerState,
    pub(crate) block: db::BlockMapEntry,
}

impl Default for dbUpdateJob {
    fn default() -> Self {
        Self {
            jobType: dbUpdateType::Update,
            file: db::FileInfo {
                folder: String::new(),
                path: String::new(),
                sequence: 0,
                modified_ns: 0,
                size: 0,
                deleted: false,
                ignored: false,
                local_flags: 0,
                file_type: db::FileInfoType::File,
                block_hashes: Vec::new(),
            },
        }
    }
}

impl Default for pullBlockState {
    fn default() -> Self {
        Self {
            sharedPullerState: sharedPullerState::default(),
            block: db::BlockMapEntry {
                blocklist_hash: Vec::new(),
                offset: 0,
                block_index: 0,
                size: 0,
                file_name: String::new(),
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum dbUpdateType {
    #[default]
    Update,
    DeleteFile,
    DeleteDir,
    Invalidate,
    Shortcut,
}

impl dbUpdateType {
    pub(crate) fn String(&self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::DeleteFile => "delete_file",
            Self::DeleteDir => "delete_dir",
            Self::Invalidate => "invalidate",
            Self::Shortcut => "shortcut",
        }
    }
}

pub(crate) trait puller {
    fn pull(&mut self, folder_id: &str) -> Result<usize, String>;
}

#[derive(Clone, Debug, Default)]
pub(crate) struct syncRequest {
    pub(crate) fn_name: String,
    pub(crate) err: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct scanBatch {
    pub(crate) f: String,
    pub(crate) updateBatch: Vec<db::FileInfo>,
    pub(crate) toRemove: BTreeSet<String>,
}

impl scanBatch {
    pub(crate) fn Update(&mut self, file: db::FileInfo) {
        self.updateBatch.push(file);
    }

    pub(crate) fn Remove(&mut self, path: &str) {
        self.toRemove.insert(path.to_string());
    }

    pub(crate) fn Flush(&mut self) -> (Vec<db::FileInfo>, Vec<String>) {
        let mut updates = Vec::new();
        std::mem::swap(&mut updates, &mut self.updateBatch);
        let removes = self.flushToRemove();
        (updates, removes)
    }

    pub(crate) fn FlushIfFull(&mut self, threshold: usize) -> Option<(Vec<db::FileInfo>, Vec<String>)> {
        if self.updateBatch.len() + self.toRemove.len() >= threshold {
            return Some(self.Flush());
        }
        None
    }

    pub(crate) fn flushToRemove(&mut self) -> Vec<String> {
        let removes = self.toRemove.iter().cloned().collect::<Vec<_>>();
        self.toRemove.clear();
        removes
    }
}

#[derive(Clone, Debug)]
pub(crate) struct streamingCFiler {
    pub(crate) iter: VecDeque<db::FileInfo>,
    pub(crate) current: Option<db::FileInfo>,
    pub(crate) deleted: Vec<db::FileInfo>,
    pub(crate) hasMore: bool,
    pub(crate) advanced: bool,
    pub(crate) errFn: Option<String>,
    pub(crate) onDeletedBatch: usize,
}

impl Default for streamingCFiler {
    fn default() -> Self {
        Self {
            iter: VecDeque::new(),
            current: None,
            deleted: Vec::new(),
            hasMore: false,
            advanced: false,
            errFn: None,
            onDeletedBatch: deletedBatchSize,
        }
    }
}

impl streamingCFiler {
    pub(crate) fn CurrentFile(&self) -> Option<db::FileInfo> {
        self.current.clone()
    }

    pub(crate) fn Error(&self) -> Option<String> {
        self.errFn.clone()
    }

    pub(crate) fn addDeleted(&mut self, file: db::FileInfo) {
        self.deleted.push(file);
    }

    pub(crate) fn advance(&mut self) -> Option<db::FileInfo> {
        self.advanced = true;
        let next = self.iter.pop_front();
        self.hasMore = !self.iter.is_empty();
        self.current = next.clone();
        next
    }

    pub(crate) fn Finish(&mut self) -> Vec<db::FileInfo> {
        let mut out = Vec::new();
        std::mem::swap(&mut out, &mut self.deleted);
        out
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct folder {
    pub(crate) config: FolderConfiguration,
    pub(crate) db: Arc<Mutex<db::WalFreeDb>>,
    pub(crate) forcedRescanPaths: BTreeSet<String>,
    pub(crate) scanScheduled: bool,
    pub(crate) pullScheduled: bool,
    pub(crate) errorsMut: Vec<FileError>,
    pub(crate) watchErr: Option<String>,
    pub(crate) watchRunning: bool,
    pub(crate) localFlags: u32,
    pub(crate) pullStats: PullStats,
    pub(crate) memoryThrottleEvents: u64,
    pub(crate) memoryHardBlockEvents: u64,
    pub(crate) lastCapViolation: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ScanExecutionStats {
    files_seen: usize,
    files_updated: usize,
    files_deleted: usize,
}

#[derive(Clone, Debug, Default)]
struct LocalFileCursor {
    page: Vec<db::FileInfo>,
    index: usize,
    next_cursor: Option<String>,
    done: bool,
}

impl LocalFileCursor {
    fn next(
        &mut self,
        db: &db::WalFreeDb,
        folder: &str,
        device: &str,
    ) -> Result<Option<db::FileInfo>, String> {
        loop {
            if let Some(item) = self.page.get(self.index).cloned() {
                self.index += 1;
                return Ok(Some(item));
            }
            if self.done {
                return Ok(None);
            }
            let page = db.all_local_files_ordered_page(
                folder,
                device,
                self.next_cursor.as_deref(),
                1024,
            )?;
            self.page = page.items;
            self.index = 0;
            self.next_cursor = page.next_cursor;
            if self.page.is_empty() {
                if self.next_cursor.is_none() {
                    self.done = true;
                    return Ok(None);
                }
                continue;
            }
            if self.next_cursor.is_none() {
                self.done = true;
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ScanScope {
    prefix: String,
    dir: Option<PathBuf>,
    file: Option<String>,
}

impl folder {
    pub(crate) fn String(&self) -> String {
        format!("folder:{}", self.config.id)
    }

    pub(crate) fn BringToFront(&mut self) {
        self.scanScheduled = true;
        self.pullScheduled = true;
    }

    pub(crate) fn DelayScan(&mut self) {
        self.scanScheduled = false;
    }

    pub(crate) fn Errors(&self) -> Vec<FileError> {
        self.errorsMut.clone()
    }

    pub(crate) fn Jobs(&self) -> BTreeMap<&'static str, bool> {
        BTreeMap::from([
            ("scan", self.scanScheduled),
            ("pull", self.pullScheduled),
            ("watch", self.watchRunning),
        ])
    }

    pub(crate) fn Override(&mut self) {
        self.pullScheduled = true;
    }

    pub(crate) fn Revert(&mut self) {
        self.forcedRescanPaths.clear();
        self.scanScheduled = true;
    }

    pub(crate) fn Reschedule(&mut self) {
        self.ScheduleScan();
        self.SchedulePull();
    }

    pub(crate) fn Scan(&mut self, subdirs: &[String]) {
        self.clearScanErrors();
        self.scanSubdirs(subdirs);
        let normalized_subdirs = normalize_scan_subdirs(subdirs);
        let scan_root = self.config.path.clone();
        match self.scan_filesystem_deterministic(&normalized_subdirs) {
            Ok(stats) => {
                if stats.files_seen > 0 {
                    self.pullScheduled = true;
                }
            }
            Err(err) => self.newScanError(&scan_root, &err),
        }
        self.scanScheduled = false;
    }

    pub(crate) fn ScheduleForceRescan(&mut self, subdirs: &[String]) {
        self.handleForcedRescans(subdirs);
        self.forcedRescanRequested();
    }

    pub(crate) fn SchedulePull(&mut self) {
        self.pullScheduled = true;
    }

    pub(crate) fn ScheduleScan(&mut self) {
        self.scanScheduled = true;
    }

    pub(crate) fn Serve(&mut self) {
        self.startWatch();
        if self.scanScheduled {
            self.scanTimerFired();
        }
        if self.pullScheduled {
            let _ = self.pull();
        }
    }

    pub(crate) fn WatchError(&self) -> Option<String> {
        self.watchErr.clone()
    }

    pub(crate) fn clearScanErrors(&mut self) {
        self.errorsMut.retain(|e| !e.Err.contains("scan"));
    }

    pub(crate) fn doInSync(&self) -> bool {
        self.watchErr.is_none() && self.errorsMut.is_empty() && !self.pullScheduled && !self.scanScheduled
    }

    pub(crate) fn emitDiskChangeEvents(&self, paths: &[String]) -> Vec<String> {
        paths.iter().map(|p| format!("disk_change:{p}")).collect()
    }

    pub(crate) fn findRename(&self, old_name: &str, new_name: &str) -> Option<(String, String)> {
        if old_name == new_name {
            None
        } else {
            Some((old_name.to_string(), new_name.to_string()))
        }
    }

    pub(crate) fn getHealthErrorAndLoadIgnores(&self) -> Option<String> {
        self.getHealthErrorWithoutIgnores().or_else(|| {
            if self.config.paused {
                Some("folder paused".to_string())
            } else {
                None
            }
        })
    }

    pub(crate) fn getHealthErrorWithoutIgnores(&self) -> Option<String> {
        self.watchErr.clone().or_else(|| self.errorsMut.first().map(|e| e.Err.clone()))
    }

    pub(crate) fn handleForcedRescans(&mut self, paths: &[String]) {
        for path in paths {
            self.forcedRescanPaths.insert(path.clone());
        }
    }

    pub(crate) fn forcedRescanRequested(&mut self) {
        self.scanScheduled = true;
    }

    pub(crate) fn ignoresUpdated(&mut self) {
        self.scanScheduled = true;
    }

    pub(crate) fn monitorWatch(&mut self) {
        if self.watchErr.is_some() {
            self.scheduleWatchRestart();
        }
    }

    pub(crate) fn newScanBatch(&self) -> scanBatch {
        scanBatch {
            f: self.config.id.clone(),
            ..Default::default()
        }
    }

    pub(crate) fn newScanError(&mut self, path: &str, err: &str) {
        self.errorsMut.push(FileError {
            Path: path.to_string(),
            Err: format!("scan: {err}"),
        });
    }

    pub(crate) fn pull(&mut self) -> Result<usize, String> {
        self.pullBasePause();
        self.pullScheduled = false;

        let (estimated, budget) = {
            let db = self.db.lock().map_err(|_| "db lock poisoned".to_string())?;
            let estimated = db.estimated_memory_bytes();
            let budget = db.memory_budget_bytes();
            (estimated, budget)
        };

        let soft_limit = if budget == 0 {
            0
        } else {
            budget
                .saturating_mul(self.config.memory_soft_percent.max(1) as usize)
                / 100
        };

        let mut throttled = false;
        let mut query_limit = self.config.memory_pull_page_items.max(1) as usize;
        self.lastCapViolation = None;
        match self.config.memory_policy {
            MemoryPolicy::Throttle => {
                if estimated > soft_limit {
                    throttled = true;
                    self.memoryThrottleEvents = self.memoryThrottleEvents.saturating_add(1);
                    // Apply backpressure by shrinking this pull iteration's batch.
                    query_limit = query_limit.min(256);
                }
            }
            MemoryPolicy::Fail => {
                if budget == 0 || estimated > budget {
                    self.memoryHardBlockEvents = self.memoryHardBlockEvents.saturating_add(1);
                    self.lastCapViolation = Some(format!(
                        "memory cap exceeded: estimated={} budget={} folder={}",
                        estimated, budget, self.config.id
                    ));
                    self.pullStats = PullStats {
                        hard_blocked: true,
                        ..Default::default()
                    };
                    return Err("memory cap exceeded".to_string());
                }
            }
            MemoryPolicy::BestEffort => {}
        }

        let mut pulled_count = 0_usize;
        let mut total_blocks = 0_usize;
        let mut reused_same_path_blocks = 0_usize;
        let mut cursor: Option<String> = None;
        let folder_root = PathBuf::from(&self.config.path);
        loop {
            let (needed_batch, next_cursor, batch_total_blocks, batch_reused_blocks) = {
                let db = self.db.lock().map_err(|_| "db lock poisoned".to_string())?;
                let page = db
                    .all_needed_global_files_ordered_page(
                        &self.config.id,
                        LOCAL_DEVICE_ID,
                        cursor.as_deref(),
                        query_limit,
                    )
                    .map_err(|e| format!("pull query: {e}"))?;
                let mut batch_total = 0_usize;
                let mut batch_reused = 0_usize;
                for global in &page.items {
                    batch_total = batch_total.saturating_add(global.block_hashes.len());
                    if let Ok(Some(local_prev)) =
                        db.get_device_file(&self.config.id, LOCAL_DEVICE_ID, &global.path)
                    {
                        batch_reused = batch_reused
                            .saturating_add(count_same_path_reuse_blocks(&local_prev, global));
                    }
                }
                (page.items, page.next_cursor, batch_total, batch_reused)
            };

            if needed_batch.is_empty() {
                break;
            }

            total_blocks = total_blocks.saturating_add(batch_total_blocks);
            reused_same_path_blocks = reused_same_path_blocks.saturating_add(batch_reused_blocks);

            let mut local_updates = Vec::with_capacity(needed_batch.len());
            for global in &needed_batch {
                if let Err(err) = apply_remote_file_to_disk(&folder_root, global) {
                    self.errorsMut.push(FileError {
                        Path: global.path.clone(),
                        Err: err.clone(),
                    });
                    return Err(format!("pull apply {}: {err}", global.path));
                }
                let mut applied = global.clone();
                applied.folder = self.config.id.clone();
                applied.local_flags = 0;
                if applied.deleted {
                    applied.block_hashes.clear();
                }
                local_updates.push(applied);
            }

            if !local_updates.is_empty() {
                pulled_count = pulled_count.saturating_add(local_updates.len());
                let mut db = self.db.lock().map_err(|_| "db lock poisoned".to_string())?;
                db.update(&self.config.id, LOCAL_DEVICE_ID, local_updates)
                    .map_err(|e| format!("pull apply update: {e}"))?;
            }

            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }
        }

        let fetched_blocks = total_blocks.saturating_sub(reused_same_path_blocks);

        self.pullStats = PullStats {
            needed_files: pulled_count,
            total_blocks,
            reused_same_path_blocks,
            fetched_blocks,
            throttled,
            hard_blocked: false,
        };
        Ok(pulled_count)
    }

    pub(crate) fn PullStats(&self) -> PullStats {
        self.pullStats
    }

    pub(crate) fn MemoryTelemetry(&self) -> MemoryTelemetry {
        let (estimated_bytes, budget_bytes) = self
            .db
            .lock()
            .map(|db| (db.estimated_memory_bytes(), db.memory_budget_bytes()))
            .unwrap_or((0, 0));
        let soft_limit_bytes = if budget_bytes == 0 {
            0
        } else {
            budget_bytes
                .saturating_mul(self.config.memory_soft_percent.max(1) as usize)
                / 100
        };
        MemoryTelemetry {
            estimated_bytes,
            budget_bytes,
            soft_limit_bytes,
            throttle_events: self.memoryThrottleEvents,
            hard_block_events: self.memoryHardBlockEvents,
            last_cap_violation: self.lastCapViolation.clone(),
            pull_throttled: self.pullStats.throttled,
            pull_hard_blocked: self.pullStats.hard_blocked,
        }
    }

    pub(crate) fn pullBasePause(&self) -> f64 {
        defaultPullerPause
    }

    pub(crate) fn restartWatch(&mut self) {
        self.stopWatch();
        self.startWatch();
    }

    pub(crate) fn scanOnWatchErr(&mut self) {
        if self.watchErr.is_some() {
            self.ScheduleForceRescan(&[]);
        }
    }

    pub(crate) fn scanSubdirs(&mut self, subdirs: &[String]) {
        self.scanSubdirsChangedAndNew(subdirs);
        self.scanSubdirsDeletedAndIgnored(subdirs);
    }

    pub(crate) fn scanSubdirsChangedAndNew(&mut self, subdirs: &[String]) {
        for s in subdirs {
            self.forcedRescanPaths.insert(s.clone());
        }
    }

    pub(crate) fn scanSubdirsDeletedAndIgnored(&mut self, subdirs: &[String]) {
        let paths = subdirs.to_vec();
        let classified = classify_paths(&paths, &[".tmp", ".DS_Store"]);
        for ignored in classified.ignored {
            self.forcedRescanPaths.remove(&ignored);
        }
    }

    fn scan_filesystem_deterministic(
        &mut self,
        subdirs: &[String],
    ) -> Result<ScanExecutionStats, String> {
        let root = PathBuf::from(&self.config.path);
        if !root.exists() {
            return Err(format!("scan root missing: {}", root.display()));
        }
        if !root.is_dir() {
            return Err(format!("scan root not a directory: {}", root.display()));
        }

        let mut spill_dir = root.join(".stfolder");
        spill_dir.push("scan-spill");
        fs::create_dir_all(&spill_dir).map_err(|e| format!("create spill dir: {e}"))?;
        let spill_threshold = self.config.memory_scan_spill_threshold_entries.max(1) as usize;
        let walk_cfg = WalkConfig::new(&spill_dir).with_spill_threshold_entries(spill_threshold);
        let scopes = resolve_scan_scopes(&root, subdirs)?;

        let mut db = self
            .db
            .lock()
            .map_err(|_| "db lock poisoned".to_string())?;
        let mut cursor = LocalFileCursor::default();
        let mut current_local = cursor.next(&db, &self.config.id, LOCAL_DEVICE_ID)?;
        let mut next_sequence = db
            .get_device_sequence(&self.config.id, LOCAL_DEVICE_ID)?
            .saturating_add(1);
        let mut stats = ScanExecutionStats::default();
        let mut updates = Vec::new();
        let mut scan_error: Option<String> = None;
        let ignore_delete = self.config.ignore_delete;

        let mut process_path = |relative_path: String| {
            if scan_error.is_some() {
                return;
            }
            if let Err(err) = process_scanned_file(
                &root,
                &self.config.id,
                &relative_path,
                subdirs,
                ignore_delete,
                &mut db,
                &mut cursor,
                &mut current_local,
                &mut updates,
                &mut next_sequence,
                &mut stats,
            ) {
                scan_error = Some(err);
                return;
            }
            if let Err(err) = flush_scan_updates_if_full(&self.config.id, &mut db, &mut updates) {
                scan_error = Some(err);
            }
        };

        for scope in &scopes {
            if let Some(file_path) = &scope.file {
                process_path(file_path.clone());
                continue;
            }
            let abs_dir = match &scope.dir {
                Some(path) => path,
                None => continue,
            };
            walk_deterministic(abs_dir, &walk_cfg, |relative_from_scope| {
                if scope.prefix.is_empty() {
                    process_path(relative_from_scope);
                } else {
                    process_path(format!("{}/{}", scope.prefix, relative_from_scope));
                }
            })
            .map_err(|e| format!("walk {}: {e}", abs_dir.display()))?;
        }
        if let Some(err) = scan_error {
            return Err(err);
        }

        while let Some(existing) = current_local.clone() {
            if !path_in_scopes(&existing.path, subdirs) {
                current_local = cursor.next(&db, &self.config.id, LOCAL_DEVICE_ID)?;
                continue;
            }
            if !ignore_delete {
                queue_deleted_file(&existing, &mut updates, &mut next_sequence);
                stats.files_deleted = stats.files_deleted.saturating_add(1);
            }
            current_local = cursor.next(&db, &self.config.id, LOCAL_DEVICE_ID)?;
            flush_scan_updates_if_full(&self.config.id, &mut db, &mut updates)?;
        }
        flush_scan_updates(&self.config.id, &mut db, &mut updates)?;

        Ok(stats)
    }

    pub(crate) fn scanTimerFired(&mut self) {
        let forced = self.forcedRescanPaths.iter().cloned().collect::<Vec<_>>();
        self.Scan(&forced);
    }

    pub(crate) fn scheduleWatchRestart(&mut self) {
        self.watchRunning = false;
        self.watchErr = Some("watch restart scheduled".to_string());
    }

    pub(crate) fn setError(&mut self, path: &str, err: &str) {
        self.errorsMut.push(FileError {
            Path: path.to_string(),
            Err: err.to_string(),
        });
    }

    pub(crate) fn setWatchError(&mut self, err: &str) {
        self.watchErr = Some(err.to_string());
    }

    pub(crate) fn startWatch(&mut self) {
        self.watchRunning = true;
        self.watchErr = None;
    }

    pub(crate) fn stopWatch(&mut self) {
        self.watchRunning = false;
    }

    pub(crate) fn updateLocals(&mut self, local: &[VersionedFile], remote: &[VersionedFile]) -> Vec<String> {
        let need = compute_need(local, remote);
        for p in &need.need_paths {
            self.forcedRescanPaths.insert(p.clone());
        }
        need.need_paths
    }

    pub(crate) fn updateLocalsFromPulling(&mut self, pulled: &[String]) {
        for p in pulled {
            self.forcedRescanPaths.remove(p);
        }
    }

    pub(crate) fn updateLocalsFromScanning(&mut self, scanned: &[String]) {
        for p in scanned {
            self.forcedRescanPaths.insert(p.clone());
        }
    }

    pub(crate) fn versionCleanupTimerFired(&mut self) -> usize {
        let mut cleaned = 0;
        if self.forcedRescanPaths.len() > maxToRemove {
            self.forcedRescanPaths = self
                .forcedRescanPaths
                .iter()
                .take(maxToRemove)
                .cloned()
                .collect();
            cleaned = maxToRemove;
        }
        cleaned
    }
}

fn normalize_scan_subdirs(subdirs: &[String]) -> Vec<String> {
    if subdirs.is_empty() {
        return Vec::new();
    }
    let mut raw = unifySubs(subdirs);
    if raw.iter().any(|s| s.is_empty() || s == ".") {
        return Vec::new();
    }
    raw.sort_by(|a, b| compare_path_order(a, b));

    let mut collapsed = Vec::new();
    for sub in raw {
        if collapsed
            .iter()
            .any(|base: &String| sub == *base || sub.starts_with(&format!("{base}/")))
        {
            continue;
        }
        collapsed.push(sub);
    }
    collapsed
}

fn resolve_scan_scopes(root: &Path, subdirs: &[String]) -> Result<Vec<ScanScope>, String> {
    if subdirs.is_empty() {
        return Ok(vec![ScanScope {
            prefix: String::new(),
            dir: Some(root.to_path_buf()),
            file: None,
        }]);
    }

    let mut scopes = Vec::new();
    for sub in subdirs {
        let abs = root.join(sub);
        match fs::metadata(&abs) {
            Ok(meta) if meta.is_dir() => scopes.push(ScanScope {
                prefix: sub.clone(),
                dir: Some(abs),
                file: None,
            }),
            Ok(meta) if meta.is_file() => scopes.push(ScanScope {
                prefix: sub.clone(),
                dir: None,
                file: Some(sub.clone()),
            }),
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => scopes.push(ScanScope {
                prefix: sub.clone(),
                dir: None,
                file: None,
            }),
            Err(err) => {
                return Err(format!("stat {}: {err}", abs.display()));
            }
        }
    }
    scopes.sort_by(|a, b| compare_path_order(&a.prefix, &b.prefix));
    Ok(scopes)
}

fn path_in_scopes(path: &str, subdirs: &[String]) -> bool {
    if subdirs.is_empty() {
        return true;
    }
    let normalized = path.trim_matches('/');
    subdirs.iter().any(|sub| {
        let base = sub.trim_matches('/');
        normalized == base || normalized.starts_with(&format!("{base}/"))
    })
}

fn should_skip_scan_path(path: &str) -> bool {
    path.starts_with(".stfolder/")
        || path == ".stfolder"
        || path.starts_with(".syncthing.")
        || path.ends_with("/.DS_Store")
        || path.ends_with(".tmp")
}

fn process_scanned_file(
    root: &Path,
    folder_id: &str,
    relative_path: &str,
    scoped_subdirs: &[String],
    ignore_delete: bool,
    db: &mut db::WalFreeDb,
    cursor: &mut LocalFileCursor,
    current_local: &mut Option<db::FileInfo>,
    updates: &mut Vec<db::FileInfo>,
    next_sequence: &mut i64,
    stats: &mut ScanExecutionStats,
) -> Result<(), String> {
    let relative_path = relative_path.trim_matches('/').to_string();
    if relative_path.is_empty() || should_skip_scan_path(&relative_path) {
        return Ok(());
    }

    while let Some(existing) = current_local.clone() {
        if !path_in_scopes(&existing.path, scoped_subdirs) {
            *current_local = cursor.next(db, folder_id, LOCAL_DEVICE_ID)?;
            continue;
        }
        match compare_path_order(&existing.path, &relative_path) {
            std::cmp::Ordering::Less => {
                if !ignore_delete {
                    queue_deleted_file(&existing, updates, next_sequence);
                    stats.files_deleted = stats.files_deleted.saturating_add(1);
                }
                *current_local = cursor.next(db, folder_id, LOCAL_DEVICE_ID)?;
            }
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => break,
        }
    }

    let abs = root.join(&relative_path);
    let metadata = fs::symlink_metadata(&abs)
        .map_err(|e| format!("stat {}: {e}", abs.display()))?;
    if !metadata.is_file() {
        return Ok(());
    }
    stats.files_seen = stats.files_seen.saturating_add(1);

    let modified_ns = file_modified_ns(&metadata).map_err(|e| format!("mtime {}: {e}", abs.display()))?;
    let size = i64::try_from(metadata.len()).unwrap_or(i64::MAX);

    if let Some(existing) = current_local.clone() {
        if existing.path == relative_path
            && !existing.deleted
            && !existing.ignored
            && existing.file_type == db::FileInfoType::File
            && existing.modified_ns == modified_ns
            && existing.size == size
        {
            *current_local = cursor.next(db, folder_id, LOCAL_DEVICE_ID)?;
            return Ok(());
        }
    }

    let block_hashes = hash_file_blocks(&abs)?;
    let new_path = relative_path.clone();
    let new_file = db::FileInfo {
        folder: folder_id.to_string(),
        path: new_path.clone(),
        sequence: *next_sequence,
        modified_ns,
        size,
        deleted: false,
        ignored: false,
        local_flags: 0,
        file_type: db::FileInfoType::File,
        block_hashes,
    };
    *next_sequence = next_sequence.saturating_add(1);
    stats.files_updated = stats.files_updated.saturating_add(1);
    updates.push(new_file);

    if let Some(existing) = current_local.clone() {
        if existing.path == new_path {
            *current_local = cursor.next(db, folder_id, LOCAL_DEVICE_ID)?;
        }
    }

    Ok(())
}

fn flush_scan_updates_if_full(
    folder_id: &str,
    db: &mut db::WalFreeDb,
    updates: &mut Vec<db::FileInfo>,
) -> Result<(), String> {
    if updates.len() < SCAN_DB_BATCH_SIZE {
        return Ok(());
    }
    flush_scan_updates(folder_id, db, updates)
}

fn flush_scan_updates(
    folder_id: &str,
    db: &mut db::WalFreeDb,
    updates: &mut Vec<db::FileInfo>,
) -> Result<(), String> {
    if updates.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(updates);
    db.update(folder_id, LOCAL_DEVICE_ID, batch)
        .map_err(|e| format!("persist scan batch: {e}"))
}

fn queue_deleted_file(
    existing: &db::FileInfo,
    updates: &mut Vec<db::FileInfo>,
    next_sequence: &mut i64,
) {
    if existing.deleted {
        return;
    }
    let mut deleted = existing.clone();
    deleted.deleted = true;
    deleted.ignored = false;
    deleted.sequence = *next_sequence;
    deleted.block_hashes.clear();
    deleted.local_flags = 0;
    *next_sequence = next_sequence.saturating_add(1);
    updates.push(deleted);
}

fn file_modified_ns(metadata: &fs::Metadata) -> Result<i64, String> {
    let modified = metadata
        .modified()
        .map_err(|e| e.to_string())?
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?;
    i64::try_from(modified.as_nanos()).map_err(|_| "mtime overflow".to_string())
}

fn hash_file_blocks(path: &Path) -> Result<Vec<String>, String> {
    let mut file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut buf = vec![0_u8; BLOCK_CHUNK_SIZE];
    let mut hashes = Vec::new();
    loop {
        let read = file
            .read(&mut buf)
            .map_err(|e| format!("read {}: {e}", path.display()))?;
        if read == 0 {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&buf[..read]);
        hashes.push(format!("{:08x}", hasher.finalize()));
    }
    Ok(hashes)
}

fn count_same_path_reuse_blocks(local_prev: &db::FileInfo, target: &db::FileInfo) -> usize {
    let mut reused = 0_usize;
    for (idx, wanted) in target.block_hashes.iter().enumerate() {
        if let Some(existing) = local_prev.block_hashes.get(idx) {
            if existing == wanted {
                reused += 1;
            }
        }
    }
    reused
}

fn apply_remote_file_to_disk(root: &Path, file: &db::FileInfo) -> Result<(), String> {
    let abs = safe_join_relative_path(root, &file.path)?;
    if file.deleted {
        return delete_path_if_exists(&abs);
    }

    match file.file_type {
        db::FileInfoType::Directory => ensure_directory_path(&abs),
        db::FileInfoType::File | db::FileInfoType::Symlink => write_sparse_file(&abs, file.size),
    }
}

fn safe_join_relative_path(root: &Path, relative: &str) -> Result<PathBuf, String> {
    use std::path::Component;

    let candidate = Path::new(relative);
    let mut clean = PathBuf::new();
    for component in candidate.components() {
        match component {
            Component::Normal(seg) => clean.push(seg),
            Component::CurDir => {}
            _ => {
                return Err(format!("invalid relative path: {relative}"));
            }
        }
    }

    Ok(root.join(clean))
}

fn delete_path_if_exists(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.is_dir() {
                fs::remove_dir_all(path).map_err(|e| format!("remove dir {}: {e}", path.display()))
            } else {
                fs::remove_file(path).map_err(|e| format!("remove file {}: {e}", path.display()))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("stat {}: {err}", path.display())),
    }
}

fn ensure_directory_path(path: &Path) -> Result<(), String> {
    if path.exists() && !path.is_dir() {
        fs::remove_file(path).map_err(|e| format!("remove non-dir {}: {e}", path.display()))?;
    }
    fs::create_dir_all(path).map_err(|e| format!("create dir {}: {e}", path.display()))
}

fn write_sparse_file(path: &Path, size: i64) -> Result<(), String> {
    let size = usize::try_from(size).map_err(|_| format!("invalid file size: {size}"))?;
    if path.exists() && path.is_dir() {
        fs::remove_dir_all(path).map_err(|e| format!("remove dir {}: {e}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| format!("create file {}: {e}", path.display()))?;
    file.set_len(size as u64)
        .map_err(|e| format!("set file size {}: {e}", path.display()))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct sendReceiveFolder {
    pub(crate) folder: folder,
    pub(crate) queue: VecDeque<String>,
    pub(crate) tempPullErrors: Vec<FileError>,
    pub(crate) writeLimiter: usize,
    pub(crate) blockPullReorderer: BTreeMap<String, usize>,
}

impl sendReceiveFolder {
    pub(crate) fn BringToFront(&mut self) { self.folder.BringToFront(); }
    pub(crate) fn Jobs(&self) -> BTreeMap<&'static str, bool> { self.folder.Jobs() }

    pub(crate) fn checkParent(&self, path: &str) -> Result<(), String> {
        if path.contains("../") {
            return Err(errDirPrefix.to_string());
        }
        Ok(())
    }

    pub(crate) fn checkToBeDeleted(&self, path: &str) -> Result<(), String> {
        if path.ends_with('/') {
            Err(errUnexpectedDirOnFileDel.to_string())
        } else {
            Ok(())
        }
    }

    pub(crate) fn copierRoutine(&mut self) -> usize {
        let mut copied = 0;
        while let Some(path) = self.queue.pop_front() {
            if self.copyBlock(&path).is_ok() {
                copied += 1;
            }
        }
        copied
    }

    pub(crate) fn copyBlock(&mut self, path: &str) -> Result<(), String> {
        self.copyBlockFromFile(path)
            .or_else(|_| self.copyBlockFromFolder(path))
    }

    pub(crate) fn copyBlockFromFile(&mut self, path: &str) -> Result<(), String> {
        self.withLimiter(path.len())?;
        self.blockPullReorderer.insert(path.to_string(), path.len());
        Ok(())
    }

    pub(crate) fn copyBlockFromFolder(&mut self, path: &str) -> Result<(), String> {
        self.withLimiter(path.len() / 2 + 1)?;
        self.blockPullReorderer.insert(path.to_string(), path.len() / 2 + 1);
        Ok(())
    }

    pub(crate) fn copyOwnershipFromParent(&self, _path: &str) -> bool { true }

    pub(crate) fn dbUpdaterRoutine(&mut self, jobs: &[dbUpdateJob]) -> usize {
        jobs.len()
    }

    pub(crate) fn deleteDir(&mut self, path: &str) -> Result<(), String> {
        self.deleteDirOnDisk(path)
    }

    pub(crate) fn deleteDirOnDisk(&mut self, path: &str) -> Result<(), String> {
        if path.is_empty() {
            return Err(errDirNotEmpty.to_string());
        }
        Ok(())
    }

    pub(crate) fn deleteDirOnDiskHandleChildren(&mut self, path: &str) -> Result<(), String> {
        self.deleteDirOnDisk(path)
    }

    pub(crate) fn deleteFile(&mut self, path: &str) -> Result<(), String> {
        self.deleteFileWithCurrent(path)
    }

    pub(crate) fn deleteFileWithCurrent(&mut self, path: &str) -> Result<(), String> {
        if path.ends_with('/') {
            return Err(errUnexpectedDirOnFileDel.to_string());
        }
        Ok(())
    }

    pub(crate) fn deleteItemOnDisk(&mut self, path: &str) -> Result<(), String> {
        self.deleteFile(path)
    }

    pub(crate) fn finisherRoutine(&mut self) -> usize {
        self.tempPullErrors.len()
    }

    pub(crate) fn handleDir(&mut self, path: &str) -> Result<(), String> {
        self.checkParent(path)
    }

    pub(crate) fn handleFile(&mut self, path: &str) -> Result<(), String> {
        self.checkParent(path)?;
        self.pullBlock(path)
    }

    pub(crate) fn handleSymlink(&mut self, path: &str) -> Result<(), String> {
        self.handleSymlinkCheckExisting(path)
    }

    pub(crate) fn handleSymlinkCheckExisting(&mut self, path: &str) -> Result<(), String> {
        if path.ends_with('/') {
            return Err(errIncompatibleSymlink.to_string());
        }
        Ok(())
    }

    pub(crate) fn inWritableDir(&self, path: &str) -> bool {
        !path.starts_with('.')
    }

    pub(crate) fn limitedWriteAt(&mut self, bytes: usize) -> Result<(), String> {
        self.withLimiter(bytes)
    }

    pub(crate) fn moveForConflict(&self, path: &str) -> String {
        conflictName(path)
    }

    pub(crate) fn newPullError(&self, path: &str, err: &str) -> FileError {
        FileError {
            Path: path.to_string(),
            Err: err.to_string(),
        }
    }

    pub(crate) fn performFinish(&mut self) -> usize {
        self.finisherRoutine()
    }

    pub(crate) fn processDeletions(&mut self, paths: &[String]) -> usize {
        paths.iter()
            .filter_map(|p| self.deleteItemOnDisk(p).ok())
            .count()
    }

    pub(crate) fn processNeeded(&mut self, paths: &[String]) -> usize {
        let mut processed = 0;
        for p in paths {
            if self.handleFile(p).is_ok() {
                processed += 1;
            }
        }
        processed
    }

    pub(crate) fn pull(&mut self) -> Result<usize, String> {
        self.pullerRoutine()
    }

    pub(crate) fn pullBlock(&mut self, path: &str) -> Result<(), String> {
        self.queue.push_back(path.to_string());
        Ok(())
    }

    pub(crate) fn pullScannerRoutine(&mut self) -> Vec<String> {
        self.queue.iter().cloned().collect()
    }

    pub(crate) fn pullerIteration(&mut self) -> Result<usize, String> {
        let queued = self.queue.len();
        let copied = self.copierRoutine();
        if copied == 0 && queued > 0 {
            return Err(errNotAvailable.to_string());
        }
        Ok(copied)
    }

    pub(crate) fn pullerRoutine(&mut self) -> Result<usize, String> {
        let mut total = 0;
        for _ in 0..maxPullerIterations {
            let copied = self.pullerIteration()?;
            total += copied;
            if copied == 0 {
                break;
            }
        }
        Ok(total)
    }

    pub(crate) fn renameFile(&self, old_name: &str, new_name: &str) -> Option<(String, String)> {
        self.folder.findRename(old_name, new_name)
    }

    pub(crate) fn reuseBlocks(&mut self, candidate: &str) -> bool {
        self.blockPullReorderer.contains_key(candidate)
    }

    pub(crate) fn scanIfItemChanged(&mut self, changed: bool) {
        if changed {
            self.folder.ScheduleScan();
        }
    }

    pub(crate) fn setPlatformData(&self, fi: &mut db::FileInfo) {
        fi.local_flags &= retainBits;
    }

    pub(crate) fn shortcutFile(&mut self, path: &str) -> bool {
        if !self.blockPullReorderer.contains_key(path) {
            self.blockPullReorderer.insert(path.to_string(), 0);
            true
        } else {
            false
        }
    }

    pub(crate) fn updateFileInfoChangeTime(&self, fi: &mut db::FileInfo, ts_ns: i64) {
        fi.modified_ns = ts_ns;
    }

    pub(crate) fn verifyBuffer(&self, buf: &[u8]) -> bool {
        !buf.is_empty()
    }

    pub(crate) fn withLimiter(&mut self, bytes: usize) -> Result<(), String> {
        if self.writeLimiter == 0 {
            self.writeLimiter = defaultPullerPendingKiB;
        }
        if bytes > self.writeLimiter {
            return Err("write limiter exceeded".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct deleteQueue {
    pub(crate) dirs: Vec<String>,
    pub(crate) scanChan: Vec<String>,
    pub(crate) ignores: BTreeSet<String>,
    pub(crate) handler: Arc<Mutex<sendReceiveFolder>>,
}

impl deleteQueue {
    pub(crate) fn handle(&mut self, path: &str) {
        self.dirs.push(path.to_string());
    }

    pub(crate) fn flush(&mut self) -> usize {
        let batch = self.dirs.iter().take(deletedBatchSize).cloned().collect::<Vec<_>>();
        if let Ok(mut h) = self.handler.lock() {
            let _ = h.processDeletions(&batch);
        }
        self.dirs = self.dirs.iter().skip(batch.len()).cloned().collect();
        batch.len()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct receiveOnlyFolder {
    pub(crate) sendReceiveFolder: sendReceiveFolder,
}

impl receiveOnlyFolder {
    pub(crate) fn Revert(&mut self) { self.revert(); }

    pub(crate) fn revert(&mut self) {
        self.sendReceiveFolder.folder.Revert();
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct receiveEncryptedFolder {
    pub(crate) sendReceiveFolder: sendReceiveFolder,
}

impl receiveEncryptedFolder {
    pub(crate) fn Revert(&mut self) { self.revert(); }

    pub(crate) fn revert(&mut self) {
        self.sendReceiveFolder.folder.Revert();
    }

    pub(crate) fn revertHandleDirs(&mut self, dirs: &[String]) -> usize {
        let mut q = deleteQueue {
            handler: Arc::new(Mutex::new(self.sendReceiveFolder.clone())),
            ..Default::default()
        };
        for d in dirs {
            q.handle(d);
        }
        q.flush()
    }
}

pub(crate) fn newFolder(config: FolderConfiguration, db: Arc<Mutex<db::WalFreeDb>>) -> folder {
    folder {
        config,
        db,
        ..Default::default()
    }
}

pub(crate) fn newSendReceiveFolder(config: FolderConfiguration, db: Arc<Mutex<db::WalFreeDb>>) -> sendReceiveFolder {
    sendReceiveFolder {
        folder: newFolder(config, db),
        writeLimiter: defaultPullerPendingKiB,
        ..Default::default()
    }
}

pub(crate) fn newReceiveOnlyFolder(config: FolderConfiguration, db: Arc<Mutex<db::WalFreeDb>>) -> receiveOnlyFolder {
    receiveOnlyFolder {
        sendReceiveFolder: newSendReceiveFolder(config, db),
    }
}

pub(crate) fn newReceiveEncryptedFolder(config: FolderConfiguration, db: Arc<Mutex<db::WalFreeDb>>) -> receiveEncryptedFolder {
    let mut f = newSendReceiveFolder(config, db);
    f.folder.localFlags |= FlagMode::Encrypted as u32;
    receiveEncryptedFolder {
        sendReceiveFolder: f,
    }
}

pub(crate) fn newStreamingCFiler(files: Vec<db::FileInfo>) -> streamingCFiler {
    let mut iter = files;
    iter.sort_by(|a, b| a.path.cmp(&b.path));
    streamingCFiler {
        hasMore: !iter.is_empty(),
        iter: iter.into(),
        ..Default::default()
    }
}

pub(crate) fn unifySubs(subs: &[String]) -> Vec<String> {
    let mut uniq = BTreeSet::new();
    for sub in subs {
        uniq.insert(sub.trim_matches('/').to_string());
    }
    uniq.into_iter().collect()
}

pub(crate) fn conflictName(path: &str) -> String {
    format!("{path}.sync-conflict-rs")
}

pub(crate) fn isConflict(path: &str) -> bool {
    path.contains("sync-conflict")
}

pub(crate) fn existingConflicts(paths: &[String]) -> Vec<String> {
    paths.iter().filter(|p| isConflict(p)).cloned().collect()
}

pub(crate) fn popCandidate(candidates: &mut VecDeque<String>) -> Option<String> {
    candidates.pop_front()
}

pub(crate) fn populateOffsets(entries: &mut [db::BlockMapEntry]) {
    let mut offset = 0_i64;
    for e in entries.iter_mut() {
        e.offset = offset;
        offset += i64::from(e.size);
    }
}

pub(crate) fn blockDiff(local: &[db::BlockMapEntry], remote: &[db::BlockMapEntry]) -> Vec<db::BlockMapEntry> {
    let local_keys = local
        .iter()
        .map(|b| (b.block_index, b.size, b.blocklist_hash.clone()))
        .collect::<BTreeSet<_>>();

    remote
        .iter()
        .filter(|r| !local_keys.contains(&(r.block_index, r.size, r.blocklist_hash.clone())))
        .cloned()
        .collect()
}

pub(crate) fn init() {
    let _ = mode_actions(FolderMode::SendReceive);
}

#[derive(Clone, Copy, Debug)]
enum FlagMode {
    Encrypted = 1,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!(
            "syncthing-rs-folder-core-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp root");
        path
    }

    fn file(path: &str, seq: i64, hashes: &[&str]) -> db::FileInfo {
        db::FileInfo {
            folder: "default".to_string(),
            path: path.to_string(),
            sequence: seq,
            modified_ns: seq,
            size: 1,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: hashes.iter().map(|h| (*h).to_string()).collect(),
        }
    }

    fn scan_cfg(root: &Path) -> FolderConfiguration {
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg
    }

    #[test]
    fn scan_batch_flushes() {
        let mut sb = scanBatch::default();
        sb.Remove("a");
        let (_, removes) = sb.Flush();
        assert_eq!(removes, vec!["a"]);
    }

    #[test]
    fn sendrecv_handles_queue() {
        let cfg = FolderConfiguration::default();
        let db = Arc::new(Mutex::new(db::WalFreeDb::default()));
        let mut f = newSendReceiveFolder(cfg, db);
        f.pullBlock("a.txt").expect("queue");
        let copied = f.pullerRoutine().expect("puller");
        assert_eq!(copied, 1);
    }

    #[test]
    fn conflict_helpers_detect_conflicts() {
        let p = conflictName("x.txt");
        assert!(isConflict(&p));
    }

    #[test]
    fn pull_reuses_blocks_from_same_path_only() {
        let root = temp_root("same-path-reuse");
        let db_root = temp_root("same-path-reuse-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        dbv.update(
            "default",
            "local",
            vec![
                file("same.txt", 1, &["h1", "hx", "h3"]),
                file("other.txt", 1, &["h2"]),
            ],
        )
        .expect("update local");
        dbv.update(
            "default",
            "remote",
            vec![file("same.txt", 2, &["h1", "h2", "h3"])],
        )
        .expect("update remote");

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        let mut f = newFolder(cfg, db);
        let pulled = f.pull().expect("pull");
        let stats = f.PullStats();

        assert_eq!(pulled, 1);
        assert_eq!(stats.needed_files, 1);
        assert_eq!(stats.total_blocks, 3);
        assert_eq!(stats.reused_same_path_blocks, 2);
        assert_eq!(stats.fetched_blocks, 1);
        assert!(!stats.hard_blocked);
        let guard = f.db.lock().expect("db lock");
        let synced = guard
            .get_device_file("default", "local", "same.txt")
            .expect("lookup")
            .expect("local same.txt");
        assert_eq!(synced.sequence, 2);
        assert_eq!(synced.block_hashes, vec!["h1", "h2", "h3"]);
        let need = guard.count_need("default", "local").expect("count need after pull");
        assert_eq!(need.files, 0);
        let disk_file = root.join("same.txt");
        assert!(disk_file.exists());
        assert_eq!(fs::metadata(&disk_file).expect("meta same.txt").len(), 1);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_uses_configured_page_size_without_materializing_all_needs() {
        let root = temp_root("pull-page-size");
        let db_root = temp_root("pull-page-size-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        dbv.update(
            "default",
            "remote",
            vec![
                file("a.txt", 1, &["h1"]),
                file("b.txt", 2, &["h2"]),
                file("c.txt", 3, &["h3"]),
            ],
        )
        .expect("update remote");

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.memory_pull_page_items = 1;
        let mut f = newFolder(cfg, db);

        let pulled = f.pull().expect("pull");
        assert_eq!(pulled, 3);
        assert_eq!(f.PullStats().needed_files, 3);
        assert!(root.join("a.txt").exists());
        assert!(root.join("b.txt").exists());
        assert!(root.join("c.txt").exists());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_hard_blocks_when_fail_policy_exceeds_cap() {
        let root = temp_root("fail-policy");
        let db_root = temp_root("fail-policy-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 0).expect("open runtime db");
        dbv.update(
            "default",
            "local",
            vec![file("same.txt", 1, &["h1"])],
        )
        .expect("update local");
        dbv.update(
            "default",
            "remote",
            vec![file("same.txt", 2, &["h1", "h2"])],
        )
        .expect("update remote");

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.memory_policy = MemoryPolicy::Fail;
        let mut f = newFolder(cfg, db);

        let err = f.pull().expect_err("must block on hard cap");
        assert!(err.contains("memory cap exceeded"));
        assert_eq!(f.memoryHardBlockEvents, 1);
        assert!(f.PullStats().hard_blocked);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_propagates_remote_delete_to_local() {
        let root = temp_root("pull-delete");
        let db_root = temp_root("pull-delete-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        dbv.update(
            "default",
            "local",
            vec![file("gone.txt", 1, &["h1"])],
        )
        .expect("update local");
        let mut deleted = file("gone.txt", 2, &[]);
        deleted.deleted = true;
        dbv.update("default", "remote", vec![deleted])
            .expect("update remote delete");

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        fs::write(root.join("gone.txt"), b"to-delete").expect("create local file");
        let mut f = newFolder(cfg, db);

        let pulled = f.pull().expect("pull");
        assert_eq!(pulled, 1);

        let guard = f.db.lock().expect("db lock");
        let local = guard
            .get_device_file("default", "local", "gone.txt")
            .expect("lookup")
            .expect("local file");
        assert!(local.deleted);
        assert!(local.block_hashes.is_empty());
        let need = guard.count_need("default", "local").expect("need");
        assert_eq!(need.files, 0);
        assert_eq!(need.deleted, 0);
        assert!(!root.join("gone.txt").exists());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn memory_telemetry_reports_cap_violation_state() {
        let root = temp_root("telemetry-cap");
        let db_root = temp_root("telemetry-cap-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 0).expect("open runtime db");
        dbv.update(
            "default",
            "local",
            vec![file("same.txt", 1, &["h1"])],
        )
        .expect("update local");
        dbv.update(
            "default",
            "remote",
            vec![file("same.txt", 2, &["h1", "h2"])],
        )
        .expect("update remote");

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.memory_policy = MemoryPolicy::Fail;
        let mut f = newFolder(cfg, db);

        let _ = f.pull();
        let telemetry = f.MemoryTelemetry();
        assert_eq!(telemetry.hard_block_events, 1);
        assert!(telemetry.pull_hard_blocked);
        assert!(telemetry.last_cap_violation.is_some());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_materializes_remote_file_on_disk() {
        let root = temp_root("pull-materialize");
        let db_root = temp_root("pull-materialize-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        let mut remote = file("dir/new.bin", 1, &["h1", "h2"]);
        remote.size = 4096;
        dbv.update("default", "remote", vec![remote.clone()])
            .expect("update remote");

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        let mut f = newFolder(cfg, db);

        let pulled = f.pull().expect("pull");
        assert_eq!(pulled, 1);

        let on_disk = root.join("dir").join("new.bin");
        assert!(on_disk.exists());
        assert_eq!(fs::metadata(&on_disk).expect("meta").len(), 4096);

        let guard = f.db.lock().expect("db lock");
        let local = guard
            .get_device_file("default", "local", "dir/new.bin")
            .expect("lookup")
            .expect("local file");
        assert_eq!(local.sequence, 1);
        assert!(!local.deleted);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_rejects_parent_path_escape() {
        let root = temp_root("pull-path-escape");
        let db_root = temp_root("pull-path-escape-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        let mut remote = file("../outside.txt", 1, &["h1"]);
        remote.size = 12;
        dbv.update("default", "remote", vec![remote])
            .expect("update remote");

        let outside = root
            .parent()
            .expect("parent")
            .join("outside.txt");
        let _ = fs::remove_file(&outside);

        let db = Arc::new(Mutex::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        let mut f = newFolder(cfg, db);

        let err = f.pull().expect_err("must reject path escape");
        assert!(err.contains("invalid relative path"));
        assert!(!outside.exists());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn scan_persists_files_in_segment_path_order() {
        let root = temp_root("scan-order");
        fs::create_dir_all(root.join("a")).expect("mkdir a");
        fs::create_dir_all(root.join("a.d")).expect("mkdir a.d");
        fs::write(root.join("a").join("x.txt"), b"x").expect("write");
        fs::write(root.join("a").join("z.txt"), b"z").expect("write");
        fs::write(root.join("a.d").join("x.txt"), b"x").expect("write");
        fs::write(root.join("b.txt"), b"b").expect("write");

        let db_root = temp_root("scan-order-db");
        let db = Arc::new(Mutex::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]);

        let ordered = db
            .lock()
            .expect("db lock")
            .all_local_files_ordered("default", "local")
            .expect("all local ordered")
            .into_iter()
            .map(|fi| fi.path)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["a/x.txt", "a/z.txt", "a.d/x.txt", "b.txt"]);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn scan_with_ignore_delete_keeps_missing_files_non_deleted() {
        let root = temp_root("scan-ignore-delete");
        fs::create_dir_all(root.join("foo")).expect("mkdir foo");
        fs::write(root.join("foo").join("a.txt"), b"a").expect("write foo/a");

        let db_root = temp_root("scan-ignore-delete-db");
        let db = Arc::new(Mutex::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut cfg = scan_cfg(&root);
        cfg.ignore_delete = true;
        let mut f = newFolder(cfg, db.clone());
        f.Scan(&[]);

        fs::remove_file(root.join("foo").join("a.txt")).expect("remove foo/a");
        f.Scan(&[String::from("foo")]);

        let fi = db
            .lock()
            .expect("db lock")
            .get_device_file("default", "local", "foo/a.txt")
            .expect("lookup")
            .expect("file record");
        assert!(!fi.deleted);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn scoped_scan_deletes_only_within_target_subdir() {
        let root = temp_root("scan-scope");
        fs::create_dir_all(root.join("foo")).expect("mkdir foo");
        fs::create_dir_all(root.join("bar")).expect("mkdir bar");
        fs::write(root.join("foo").join("a.txt"), b"a").expect("write foo/a");
        fs::write(root.join("bar").join("b.txt"), b"b").expect("write bar/b");

        let db_root = temp_root("scan-scope-db");
        let db = Arc::new(Mutex::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]);

        fs::remove_file(root.join("foo").join("a.txt")).expect("remove foo/a");
        f.Scan(&[String::from("foo")]);

        let guard = db.lock().expect("db lock");
        let foo = guard
            .get_device_file("default", "local", "foo/a.txt")
            .expect("lookup foo")
            .expect("foo record");
        let bar = guard
            .get_device_file("default", "local", "bar/b.txt")
            .expect("lookup bar")
            .expect("bar record");

        assert!(foo.deleted);
        assert!(!bar.deleted);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn single_file_scope_scan_updates_only_target_file() {
        let root = temp_root("scan-file-scope");
        fs::create_dir_all(root.join("foo")).expect("mkdir foo");
        fs::create_dir_all(root.join("bar")).expect("mkdir bar");
        fs::write(root.join("foo").join("a.txt"), b"a-v1").expect("write foo/a");
        fs::write(root.join("bar").join("b.txt"), b"b-v1").expect("write bar/b");

        let db_root = temp_root("scan-file-scope-db");
        let db = Arc::new(Mutex::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]);

        let (foo_seq_before, bar_seq_before) = {
            let guard = db.lock().expect("db lock before");
            let foo = guard
                .get_device_file("default", "local", "foo/a.txt")
                .expect("foo lookup")
                .expect("foo record");
            let bar = guard
                .get_device_file("default", "local", "bar/b.txt")
                .expect("bar lookup")
                .expect("bar record");
            (foo.sequence, bar.sequence)
        };

        fs::write(root.join("bar").join("b.txt"), b"b-v2").expect("rewrite bar/b");
        f.Scan(&[String::from("bar/b.txt")]);

        let guard = db.lock().expect("db lock after");
        let foo = guard
            .get_device_file("default", "local", "foo/a.txt")
            .expect("foo lookup")
            .expect("foo record");
        let bar = guard
            .get_device_file("default", "local", "bar/b.txt")
            .expect("bar lookup")
            .expect("bar record");
        assert_eq!(foo.sequence, foo_seq_before);
        assert!(bar.sequence > bar_seq_before);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }
}
