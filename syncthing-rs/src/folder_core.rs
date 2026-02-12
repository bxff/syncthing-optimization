#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::config::{FolderConfiguration, MemoryPolicy};
use crate::db::{self, Db};
use crate::folder_modes::{mode_actions, FolderMode};
use crate::store::compare_path_order;
use crate::walker::{walk_deterministic, WalkConfig};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant, UNIX_EPOCH};

pub(crate) const deletedBatchSize: usize = 1000;
pub(crate) const kqueueItemCountThreshold: usize = 10_000;
pub(crate) const maxToRemove: usize = 1_000;
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

pub(crate) const defaultCopiers: usize = 2;
pub(crate) const defaultPullerPause: f64 = 60.0;
pub(crate) const defaultPullerPendingKiB: usize = 32 * 1024;
pub(crate) const maxPullerIterations: usize = 3;
pub(crate) const retainBits: u32 = 0o7000;

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
pub(crate) struct CapViolationReport {
    pub(crate) folder: String,
    pub(crate) subsystem: String,
    pub(crate) policy: String,
    pub(crate) requested_bytes: usize,
    pub(crate) reserved_bytes: usize,
    pub(crate) budget_bytes: usize,
    pub(crate) queue_items: usize,
    pub(crate) query_limit: usize,
    pub(crate) runtime_budget_key: String,
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
    pub(crate) runtime_reserved_bytes: usize,
    pub(crate) runtime_budget_bytes: usize,
    pub(crate) runtime_throttle_events: u64,
    pub(crate) runtime_hard_block_events: u64,
    pub(crate) runtime_cap_violation_report: Option<CapViolationReport>,
}

#[derive(Debug)]
pub(crate) struct RuntimeMemoryBudget {
    pub(crate) key: String,
    pub(crate) max_bytes: usize,
    used_bytes: AtomicUsize,
    throttle_events: AtomicU64,
    hard_block_events: AtomicU64,
}

impl Default for RuntimeMemoryBudget {
    fn default() -> Self {
        Self {
            key: "default".to_string(),
            max_bytes: usize::MAX,
            used_bytes: AtomicUsize::new(0),
            throttle_events: AtomicU64::new(0),
            hard_block_events: AtomicU64::new(0),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MemoryPermit {
    budget: Arc<RuntimeMemoryBudget>,
    bytes: usize,
}

impl RuntimeMemoryBudget {
    fn reserved_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Acquire)
    }

    fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<MemoryPermit> {
        if bytes == 0 || self.max_bytes == usize::MAX {
            return Some(MemoryPermit {
                budget: self.clone(),
                bytes: 0,
            });
        }
        loop {
            let current = self.used_bytes.load(Ordering::Acquire);
            let next = current.saturating_add(bytes);
            if next > self.max_bytes {
                return None;
            }
            if self
                .used_bytes
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(MemoryPermit {
                    budget: self.clone(),
                    bytes,
                });
            }
        }
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.budget
                .used_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
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
    pub(crate) db: Option<Arc<RwLock<db::WalFreeDb>>>,
    pub(crate) updateBatch: Vec<db::FileInfo>,
    pub(crate) toRemove: Vec<String>,
}

impl scanBatch {
    pub(crate) fn Update(&mut self, file: db::FileInfo) {
        self.updateBatch.push(file);
    }

    pub(crate) fn Remove(&mut self, path: &str) {
        self.toRemove.push(path.to_string());
    }

    pub(crate) fn Flush(&mut self) -> Result<(Vec<db::FileInfo>, Vec<String>), String> {
        let removes = self.flushToRemove()?;
        if !self.updateBatch.is_empty() {
            if let Some(db) = &self.db {
                let mut guard = db.write().map_err(|_| "db lock poisoned".to_string())?;
                guard
                    .update(&self.f, LOCAL_DEVICE_ID, self.updateBatch.clone())
                    .map_err(|err| format!("scan batch update failed: {err}"))?;
            }
        }
        let updates = std::mem::take(&mut self.updateBatch);
        Ok((updates, removes))
    }

    pub(crate) fn FlushIfFull(
        &mut self,
        threshold: usize,
    ) -> Result<Option<(Vec<db::FileInfo>, Vec<String>)>, String> {
        if self.updateBatch.len() + self.toRemove.len() >= threshold {
            return Ok(Some(self.Flush()?));
        }
        Ok(None)
    }

    pub(crate) fn flushToRemove(&mut self) -> Result<Vec<String>, String> {
        if self.toRemove.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(db) = &self.db {
            let mut guard = db.write().map_err(|_| "db lock poisoned".to_string())?;
            guard.drop_files_named(&self.f, LOCAL_DEVICE_ID, &self.toRemove)?;
        }
        let removes = self.toRemove.clone();
        self.toRemove.clear();
        Ok(removes)
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
    pub(crate) db: Arc<RwLock<db::WalFreeDb>>,
    pub(crate) runtimeMemoryBudget: Arc<RuntimeMemoryBudget>,
    pub(crate) forcedRescanPaths: BTreeSet<String>,
    pub(crate) scanScheduled: bool,
    pub(crate) scanDelayUntil: Option<Instant>,
    pub(crate) pullScheduled: bool,
    pub(crate) errorsMut: Vec<FileError>,
    pub(crate) watchErr: Option<String>,
    pub(crate) watchRunning: bool,
    pub(crate) localFlags: u32,
    pub(crate) initialScanCompleted: bool,
    pub(crate) pullStats: PullStats,
    pub(crate) memoryThrottleEvents: u64,
    pub(crate) memoryHardBlockEvents: u64,
    pub(crate) lastCapViolation: Option<String>,
    pub(crate) lastRuntimeCapViolationReport: Option<CapViolationReport>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScanExecutionStats {
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
            let page =
                db.all_local_files_ordered_page(folder, device, self.next_cursor.as_deref(), 1024)?;
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

fn runtime_memory_budget_registry() -> &'static Mutex<BTreeMap<String, Arc<RuntimeMemoryBudget>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<String, Arc<RuntimeMemoryBudget>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn runtime_budget_key(db: &Arc<RwLock<db::WalFreeDb>>, cfg: &FolderConfiguration) -> String {
    let runtime_root = db
        .read()
        .map(|guard| guard.runtime_root().to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown-runtime".to_string());
    format!("{runtime_root}:{}MB", cfg.memory_max_mb.max(1))
}

fn runtime_budget_for_folder(
    db: &Arc<RwLock<db::WalFreeDb>>,
    cfg: &FolderConfiguration,
) -> Arc<RuntimeMemoryBudget> {
    let key = runtime_budget_key(db, cfg);
    let mut registry = runtime_memory_budget_registry()
        .lock()
        .expect("runtime memory budget registry lock");
    if let Some(existing) = registry.get(&key) {
        return existing.clone();
    }

    let budget = Arc::new(RuntimeMemoryBudget {
        key: key.clone(),
        max_bytes: (cfg.memory_max_mb.max(1) as usize).saturating_mul(1024 * 1024),
        used_bytes: AtomicUsize::new(0),
        throttle_events: AtomicU64::new(0),
        hard_block_events: AtomicU64::new(0),
    });
    registry.insert(key, budget.clone());
    budget
}

impl folder {
    pub(crate) fn String(&self) -> String {
        format!("folder:{}", self.config.id)
    }

    pub(crate) fn BringToFront(&mut self) {
        self.scanScheduled = true;
        self.pullScheduled = true;
    }

    pub(crate) fn DelayScan(&mut self, next: Duration) {
        self.scanScheduled = true;
        self.scanDelayUntil = Instant::now().checked_add(next);
    }

    pub(crate) fn Errors(&self) -> Vec<FileError> {
        let mut errors = self.errorsMut.clone();
        errors.sort_by(|a, b| a.Path.cmp(&b.Path).then_with(|| a.Err.cmp(&b.Err)));
        errors
    }

    pub(crate) fn Jobs(&self) -> BTreeMap<&'static str, bool> {
        BTreeMap::from([
            ("scan", self.scanScheduled),
            ("pull", self.pullScheduled),
            ("watch", self.watchRunning),
        ])
    }

    pub(crate) fn Override(&mut self) {
        // Base folder has no override behavior. Concrete folder types may override.
    }

    pub(crate) fn Revert(&mut self) {
        // Base folder has no revert behavior. Concrete folder types may override.
    }

    pub(crate) fn Reschedule(&mut self) {
        if self.config.rescan_interval_s <= 0 {
            return;
        }
        self.scanScheduled = true;
        self.scanDelayUntil =
            Instant::now().checked_add(Duration::from_secs(self.config.rescan_interval_s as u64));
    }

    pub(crate) fn Scan(&mut self, subdirs: &[String]) -> Result<(), String> {
        self.clearScanErrors();
        self.scanSubdirs(subdirs)?;
        self.scanScheduled = false;
        self.scanDelayUntil = None;
        Ok(())
    }

    pub(crate) fn ScheduleForceRescan(&mut self, subdirs: &[String]) {
        let normalized = normalize_scan_subdirs(subdirs);
        if normalized.is_empty() {
            self.forcedRescanPaths.insert(String::new());
        } else {
            for sub in normalized {
                self.forcedRescanPaths.insert(sub);
            }
        }
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
            if let Some(next_scan_at) = self.scanDelayUntil {
                if Instant::now() >= next_scan_at {
                    if let Err(err) = self.scanTimerFired() {
                        self.newScanError(&self.config.path.clone(), &err);
                    }
                }
            } else if let Err(err) = self.scanTimerFired() {
                self.newScanError(&self.config.path.clone(), &err);
            }
        }
        if self.pullScheduled {
            if let Err(err) = self.pull() {
                self.newScanError(&self.config.path.clone(), &format!("pull: {err}"));
            }
        }
    }

    pub(crate) fn WatchError(&self) -> Option<String> {
        self.watchErr.clone()
    }

    pub(crate) fn clearScanErrors(&mut self) {
        self.errorsMut.clear();
    }

    pub(crate) fn doInSync(&self) -> bool {
        self.watchErr.is_none()
            && self.errorsMut.is_empty()
            && !self.pullScheduled
            && !self.scanScheduled
    }

    pub(crate) fn emitDiskChangeEvents(&self, paths: &[String], event_type: &str) -> Vec<String> {
        paths.iter().map(|p| format!("{event_type}:{p}")).collect()
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
        self.watchErr
            .clone()
            .or_else(|| self.errorsMut.first().map(|e| e.Err.clone()))
    }

    pub(crate) fn handleForcedRescans(&mut self) -> Result<(), String> {
        if self.forcedRescanPaths.is_empty() {
            return Ok(());
        }

        let mut drained = self.forcedRescanPaths.iter().cloned().collect::<Vec<_>>();
        self.forcedRescanPaths.clear();
        drained.sort_by(|a, b| compare_path_order(a, b));

        let mut mark_err: Option<String> = None;
        if let Ok(mut db) = self.db.write() {
            let mut updates = Vec::new();
            let mut next_sequence = db
                .get_device_sequence(&self.config.id, LOCAL_DEVICE_ID)
                .unwrap_or(0)
                .saturating_add(1);
            for path in &drained {
                if path.is_empty() {
                    continue;
                }
                match db.get_device_file(&self.config.id, LOCAL_DEVICE_ID, path) {
                    Ok(Some(mut local)) => {
                        local.local_flags |= db::FLAG_LOCAL_MUST_RESCAN;
                        local.sequence = next_sequence;
                        next_sequence = next_sequence.saturating_add(1);
                        updates.push(local);
                    }
                    Ok(None) => {}
                    Err(err) => {
                        mark_err = Some(format!("force-rescan lookup failed for {path}: {err}"));
                        break;
                    }
                }
            }
            if mark_err.is_none() && !updates.is_empty() {
                if let Err(err) = db.update(&self.config.id, LOCAL_DEVICE_ID, updates) {
                    mark_err = Some(format!("force-rescan mark update failed: {err}"));
                }
            }
        } else {
            mark_err = Some("db lock poisoned".to_string());
        }

        if let Some(err) = mark_err {
            for path in drained {
                self.forcedRescanPaths.insert(path);
            }
            let scan_path = self.config.path.clone();
            self.newScanError(&scan_path, &err);
            return Err(err);
        }

        self.scanSubdirs(&drained)?;
        Ok(())
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
            db: Some(self.db.clone()),
            ..Default::default()
        }
    }

    pub(crate) fn newScanError(&mut self, path: &str, err: &str) {
        self.errorsMut.push(FileError {
            Path: path.to_string(),
            Err: err.to_string(),
        });
    }

    pub(crate) fn pull(&mut self) -> Result<usize, String> {
        self.pullBasePause();
        self.pullScheduled = false;

        let mode = self.config.folder_type.to_mode();
        if mode == FolderMode::SendOnly {
            self.pullStats = PullStats::default();
            return Ok(0);
        }

        let (estimated, budget) = {
            let db = self.db.read().map_err(|_| "db lock poisoned".to_string())?;
            let estimated = db.estimated_memory_bytes();
            let budget = db.memory_budget_bytes();
            (estimated, budget)
        };

        let soft_limit = if budget == 0 {
            0
        } else {
            budget.saturating_mul(self.config.memory_soft_percent.max(1) as usize) / 100
        };

        let mut throttled = false;
        let mut query_limit = self.config.memory_pull_page_items.max(1) as usize;
        self.lastCapViolation = None;
        self.lastRuntimeCapViolationReport = None;
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
                    self.lastRuntimeCapViolationReport = Some(CapViolationReport {
                        folder: self.config.id.clone(),
                        subsystem: "pull-db-budget".to_string(),
                        policy: "fail".to_string(),
                        requested_bytes: estimated,
                        reserved_bytes: estimated,
                        budget_bytes: budget,
                        queue_items: 0,
                        query_limit,
                        runtime_budget_key: self.runtimeMemoryBudget.key.clone(),
                    });
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
                let db = self.db.read().map_err(|_| "db lock poisoned".to_string())?;
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

            let batch_runtime_bytes = estimate_needed_batch_runtime_bytes(&needed_batch);
            let batch_permit: Option<MemoryPermit> = match self.config.memory_policy {
                MemoryPolicy::Fail => {
                    match self.runtimeMemoryBudget.try_acquire(batch_runtime_bytes) {
                        Some(permit) => Some(permit),
                        None => {
                            self.memoryHardBlockEvents =
                                self.memoryHardBlockEvents.saturating_add(1);
                            self.runtimeMemoryBudget
                                .hard_block_events
                                .fetch_add(1, Ordering::AcqRel);
                            self.lastCapViolation = Some(format!(
                            "runtime budget exceeded: reserved={} requested={} budget={} folder={} key={}",
                            self.runtimeMemoryBudget.reserved_bytes(),
                            batch_runtime_bytes,
                            self.runtimeMemoryBudget.max_bytes,
                            self.config.id,
                            self.runtimeMemoryBudget.key
                        ));
                            self.lastRuntimeCapViolationReport = Some(CapViolationReport {
                                folder: self.config.id.clone(),
                                subsystem: "pull-runtime-budget".to_string(),
                                policy: "fail".to_string(),
                                requested_bytes: batch_runtime_bytes,
                                reserved_bytes: self.runtimeMemoryBudget.reserved_bytes(),
                                budget_bytes: self.runtimeMemoryBudget.max_bytes,
                                queue_items: needed_batch.len(),
                                query_limit,
                                runtime_budget_key: self.runtimeMemoryBudget.key.clone(),
                            });
                            self.pullStats = PullStats {
                                hard_blocked: true,
                                ..Default::default()
                            };
                            return Err("memory cap exceeded".to_string());
                        }
                    }
                }
                MemoryPolicy::Throttle => {
                    match self.runtimeMemoryBudget.try_acquire(batch_runtime_bytes) {
                        Some(permit) => Some(permit),
                        None => {
                            throttled = true;
                            self.memoryThrottleEvents = self.memoryThrottleEvents.saturating_add(1);
                            self.runtimeMemoryBudget
                                .throttle_events
                                .fetch_add(1, Ordering::AcqRel);
                            query_limit = query_limit.saturating_div(2).max(1);
                            continue;
                        }
                    }
                }
                MemoryPolicy::BestEffort => {
                    self.runtimeMemoryBudget.try_acquire(batch_runtime_bytes)
                }
            };

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
                let mut db = self
                    .db
                    .write()
                    .map_err(|_| "db lock poisoned".to_string())?;
                db.update(&self.config.id, LOCAL_DEVICE_ID, local_updates)
                    .map_err(|e| format!("pull apply update: {e}"))?;
            }

            cursor = next_cursor;
            if cursor.is_none() {
                break;
            }

            drop(batch_permit);
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
            .read()
            .map(|db| (db.estimated_memory_bytes(), db.memory_budget_bytes()))
            .unwrap_or((0, 0));
        let soft_limit_bytes = if budget_bytes == 0 {
            0
        } else {
            budget_bytes.saturating_mul(self.config.memory_soft_percent.max(1) as usize) / 100
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
            runtime_reserved_bytes: self.runtimeMemoryBudget.reserved_bytes(),
            runtime_budget_bytes: self.runtimeMemoryBudget.max_bytes,
            runtime_throttle_events: self
                .runtimeMemoryBudget
                .throttle_events
                .load(Ordering::Acquire),
            runtime_hard_block_events: self
                .runtimeMemoryBudget
                .hard_block_events
                .load(Ordering::Acquire),
            runtime_cap_violation_report: self.lastRuntimeCapViolationReport.clone(),
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

    pub(crate) fn scanSubdirs(&mut self, subdirs: &[String]) -> Result<(), String> {
        if let Some(err) = self.getHealthErrorAndLoadIgnores() {
            return Err(err);
        }

        let normalized = normalize_scan_subdirs(subdirs);
        let mut changed = 0_usize;
        match self.scanSubdirsChangedAndNew(&normalized) {
            Ok(stats) => {
                changed = changed.saturating_add(stats.files_updated + stats.files_deleted);
            }
            Err(err) => {
                return Err(err);
            }
        }

        match self.scanSubdirsDeletedAndIgnored(&normalized) {
            Ok(extra) => {
                changed = changed.saturating_add(extra);
            }
            Err(err) => {
                return Err(err);
            }
        }

        if changed > 0 {
            self.SchedulePull();
        }
        Ok(())
    }

    pub(crate) fn scanSubdirsChangedAndNew(
        &mut self,
        subdirs: &[String],
    ) -> Result<ScanExecutionStats, String> {
        self.scan_filesystem_deterministic(subdirs)
    }

    pub(crate) fn scanSubdirsDeletedAndIgnored(
        &mut self,
        subdirs: &[String],
    ) -> Result<usize, String> {
        let mut removed_pending = 0_usize;
        let mut keep = BTreeSet::new();
        for pending in &self.forcedRescanPaths {
            let covered = subdirs.iter().any(|sub| {
                if sub.is_empty() {
                    true
                } else {
                    pending == sub || pending.starts_with(&format!("{sub}/"))
                }
            });
            if covered {
                removed_pending = removed_pending.saturating_add(1);
            } else {
                keep.insert(pending.clone());
            }
        }
        self.forcedRescanPaths = keep;

        let mut db = self
            .db
            .write()
            .map_err(|_| "db lock poisoned".to_string())?;
        let mut sequence = db
            .get_device_sequence(&self.config.id, LOCAL_DEVICE_ID)
            .map_err(|e| format!("scan phase2 sequence lookup: {e}"))?
            .saturating_add(1);

        let locals = load_scoped_local_files(&db, &self.config.id, subdirs)?;
        let mut updates = Vec::new();
        for mut local in locals {
            if !path_in_scopes(&local.path, subdirs) {
                continue;
            }
            let mut changed = false;
            if local.local_flags & db::FLAG_LOCAL_MUST_RESCAN != 0 {
                local.local_flags &= !db::FLAG_LOCAL_MUST_RESCAN;
                changed = true;
            }

            if should_skip_scan_path(&local.path) && !local.deleted && !local.ignored {
                // Internal/temporary files are tracked as ignored, not deleted.
                local.ignored = true;
                local.block_hashes.clear();
                changed = true;
            }

            if changed {
                local.sequence = sequence;
                sequence = sequence.saturating_add(1);
                updates.push(local);
            }
        }
        let updates_len = updates.len();
        if updates_len > 0 {
            db.update(&self.config.id, LOCAL_DEVICE_ID, updates)
                .map_err(|e| format!("scan phase2 update: {e}"))?;
        }

        Ok(removed_pending.saturating_add(updates_len))
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

        // `.stfolder` is a marker file in Syncthing folders, so spill files must live elsewhere.
        let mut spill_dir = root.join(".stscan-spill");
        spill_dir.push("walk");
        fs::create_dir_all(&spill_dir).map_err(|e| format!("create spill dir: {e}"))?;
        let spill_threshold = self.config.memory_scan_spill_threshold_entries.max(1) as usize;
        let walk_cfg = WalkConfig::new(&spill_dir).with_spill_threshold_entries(spill_threshold);
        let scopes = resolve_scan_scopes(&root, subdirs)?;

        let mut db = self
            .db
            .write()
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

    pub(crate) fn scanTimerFired(&mut self) -> Result<(), String> {
        self.scanDelayUntil = None;
        let result = if self.forcedRescanPaths.is_empty() {
            self.Scan(&[])
        } else {
            self.handleForcedRescans()
        };
        if !self.initialScanCompleted {
            self.initialScanCompleted = true;
        }
        self.Reschedule();
        result
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

    pub(crate) fn updateLocals(&mut self, files: &[db::FileInfo]) -> Result<(), String> {
        if files.is_empty() {
            return Ok(());
        }
        self.db
            .write()
            .map_err(|_| "db lock poisoned".to_string())?
            .update(&self.config.id, LOCAL_DEVICE_ID, files.to_vec())
            .map_err(|e| format!("update locals: {e}"))?;
        for file in files {
            self.forcedRescanPaths.remove(&file.path);
        }
        Ok(())
    }

    pub(crate) fn updateLocalsFromPulling(
        &mut self,
        pulled: &[db::FileInfo],
    ) -> Result<(), String> {
        self.updateLocals(pulled)?;
        let paths = pulled
            .iter()
            .filter(|fi| fi.local_flags & db::FLAG_LOCAL_INVALID == 0)
            .map(|fi| fi.path.clone())
            .collect::<Vec<_>>();
        let _ = self.emitDiskChangeEvents(&paths, "remote_change");
        Ok(())
    }

    pub(crate) fn updateLocalsFromScanning(
        &mut self,
        scanned: &[db::FileInfo],
    ) -> Result<(), String> {
        self.updateLocals(scanned)?;
        let paths = scanned
            .iter()
            .filter(|fi| fi.local_flags & db::FLAG_LOCAL_INVALID == 0)
            .map(|fi| fi.path.clone())
            .collect::<Vec<_>>();
        let _ = self.emitDiskChangeEvents(&paths, "local_change");
        Ok(())
    }

    pub(crate) fn updateLocalsFromScanningPaths(&mut self, scanned: &[String]) {
        let mut updates = Vec::new();
        let mut next_sequence = self
            .db
            .read()
            .ok()
            .and_then(|db| {
                db.get_device_sequence(&self.config.id, LOCAL_DEVICE_ID)
                    .ok()
            })
            .unwrap_or(0)
            .saturating_add(1);

        for path in scanned {
            if should_skip_scan_path(path) {
                continue;
            }
            let local = self.build_local_file_snapshot(path, next_sequence);
            match local {
                Ok(Some(fi)) => {
                    next_sequence = next_sequence.saturating_add(1);
                    updates.push(fi);
                }
                Ok(None) => {}
                Err(err) => self.newScanError(path, &err),
            }
        }

        if !updates.is_empty() {
            if let Err(err) = self.updateLocalsFromScanning(&updates) {
                let scan_path = self.config.path.clone();
                self.newScanError(&scan_path, &err);
            }
        }
    }

    fn build_local_file_snapshot(
        &self,
        path: &str,
        sequence: i64,
    ) -> Result<Option<db::FileInfo>, String> {
        let abs = safe_join_relative_path(Path::new(&self.config.path), path)?;
        match fs::symlink_metadata(&abs) {
            Ok(meta) => {
                let file_type = if meta.is_dir() {
                    db::FileInfoType::Directory
                } else if meta.file_type().is_symlink() {
                    db::FileInfoType::Symlink
                } else {
                    db::FileInfoType::File
                };
                let block_hashes = if file_type == db::FileInfoType::File {
                    hash_file_blocks(&abs)?
                } else if file_type == db::FileInfoType::Symlink {
                    vec![fs::read_link(&abs)
                        .map_err(|e| format!("read symlink {}: {e}", abs.display()))?
                        .to_string_lossy()
                        .to_string()]
                } else {
                    Vec::new()
                };
                let modified_ns = file_modified_ns(&meta)?;
                let size = if file_type == db::FileInfoType::Directory
                    || file_type == db::FileInfoType::Symlink
                {
                    0
                } else {
                    i64::try_from(meta.len()).unwrap_or(i64::MAX)
                };
                Ok(Some(db::FileInfo {
                    folder: self.config.id.clone(),
                    path: path.trim_matches('/').to_string(),
                    sequence,
                    modified_ns,
                    size,
                    deleted: false,
                    ignored: false,
                    local_flags: 0,
                    file_type,
                    block_hashes,
                }))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let existing = self
                    .db
                    .read()
                    .map_err(|_| "db lock poisoned".to_string())?
                    .get_device_file(&self.config.id, LOCAL_DEVICE_ID, path)
                    .map_err(|e| format!("lookup local file {path}: {e}"))?;
                if let Some(mut fi) = existing {
                    fi.deleted = true;
                    fi.ignored = false;
                    fi.sequence = sequence;
                    fi.block_hashes.clear();
                    fi.local_flags = 0;
                    return Ok(Some(fi));
                }
                Ok(None)
            }
            Err(err) => Err(format!("stat {}: {err}", abs.display())),
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
        let abs = safe_join_relative_path(root, sub)?;
        match fs::symlink_metadata(&abs) {
            Ok(meta) if meta.is_dir() => scopes.push(ScanScope {
                prefix: sub.clone(),
                dir: Some(abs),
                file: None,
            }),
            Ok(meta) if meta.is_file() || meta.file_type().is_symlink() => scopes.push(ScanScope {
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

fn load_scoped_local_files(
    db: &db::WalFreeDb,
    folder_id: &str,
    subdirs: &[String],
) -> Result<Vec<db::FileInfo>, String> {
    if subdirs.is_empty() {
        return db::collect_stream(db.all_local_files_ordered(folder_id, LOCAL_DEVICE_ID)?);
    }

    let mut items = Vec::new();
    let mut seen = BTreeSet::new();
    for sub in subdirs {
        let prefix = sub.trim_matches('/');
        if prefix.is_empty() {
            return db::collect_stream(db.all_local_files_ordered(folder_id, LOCAL_DEVICE_ID)?);
        }
        let matches = db::collect_stream(db.all_local_files_with_prefix(
            folder_id,
            LOCAL_DEVICE_ID,
            prefix,
        )?)?;
        for item in matches {
            if seen.insert(item.path.clone()) {
                items.push(item);
            }
        }
    }
    items.sort_by(|a, b| compare_path_order(&a.path, &b.path));
    Ok(items)
}

fn should_skip_scan_path(path: &str) -> bool {
    path.starts_with(".stfolder/")
        || path == ".stfolder"
        || path.starts_with(".stscan-spill/")
        || path == ".stscan-spill"
        || path.starts_with(".syncthing.")
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
    if relative_path.is_empty() {
        return Ok(());
    }
    let skip_path = should_skip_scan_path(&relative_path);

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

    if skip_path {
        if let Some(existing) = current_local.clone() {
            if existing.path == relative_path {
                let mut tracked = existing;
                let mut changed = false;
                if tracked.local_flags & db::FLAG_LOCAL_MUST_RESCAN != 0 {
                    tracked.local_flags &= !db::FLAG_LOCAL_MUST_RESCAN;
                    changed = true;
                }
                if tracked.deleted {
                    tracked.deleted = false;
                    changed = true;
                }
                if !tracked.ignored {
                    tracked.ignored = true;
                    tracked.block_hashes.clear();
                    changed = true;
                }
                if changed {
                    tracked.sequence = *next_sequence;
                    *next_sequence = next_sequence.saturating_add(1);
                    updates.push(tracked);
                    stats.files_updated = stats.files_updated.saturating_add(1);
                }
                *current_local = cursor.next(db, folder_id, LOCAL_DEVICE_ID)?;
            }
        }
        return Ok(());
    }

    let abs = root.join(&relative_path);
    let metadata =
        fs::symlink_metadata(&abs).map_err(|e| format!("stat {}: {e}", abs.display()))?;
    let is_symlink = metadata.file_type().is_symlink();
    if !metadata.is_file() && !is_symlink {
        return Ok(());
    }
    stats.files_seen = stats.files_seen.saturating_add(1);

    let modified_ns =
        file_modified_ns(&metadata).map_err(|e| format!("mtime {}: {e}", abs.display()))?;
    let size = if is_symlink {
        0
    } else {
        i64::try_from(metadata.len()).unwrap_or(i64::MAX)
    };
    let file_type = if is_symlink {
        db::FileInfoType::Symlink
    } else {
        db::FileInfoType::File
    };

    if let Some(existing) = current_local.clone() {
        if existing.path == relative_path
            && !existing.deleted
            && !existing.ignored
            && existing.local_flags & db::FLAG_LOCAL_MUST_RESCAN == 0
            && existing.file_type == file_type
            && existing.modified_ns == modified_ns
            && existing.size == size
        {
            *current_local = cursor.next(db, folder_id, LOCAL_DEVICE_ID)?;
            return Ok(());
        }
    }

    let block_hashes = if is_symlink {
        Vec::new()
    } else {
        hash_file_blocks(&abs)?
    };
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
        file_type,
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
        let mut hasher = Sha256::new();
        hasher.update(&buf[..read]);
        hashes.push(format!("{:x}", hasher.finalize()));
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

fn estimate_needed_batch_runtime_bytes(files: &[db::FileInfo]) -> usize {
    files
        .iter()
        .map(|file| {
            let hash_bytes = file.block_hashes.iter().map(String::len).sum::<usize>();
            let file_type_len = match file.file_type {
                db::FileInfoType::File => 4,
                db::FileInfoType::Directory => 9,
                db::FileInfoType::Symlink => 7,
            };
            file.folder.len() + file.path.len() + file_type_len + hash_bytes + 128
        })
        .sum()
}

fn apply_remote_file_to_disk(root: &Path, file: &db::FileInfo) -> Result<(), String> {
    let abs = safe_join_relative_path(root, &file.path)?;
    if file.deleted {
        return delete_path_if_exists(&abs);
    }

    match file.file_type {
        db::FileInfoType::Directory => ensure_directory_path(&abs),
        db::FileInfoType::File => write_sparse_file(&abs, file.size),
        db::FileInfoType::Symlink => {
            let target = file
                .block_hashes
                .first()
                .map(String::as_str)
                .ok_or_else(|| errIncompatibleSymlink.to_string())?;
            ensure_symlink_path(&abs, target)
        }
    }
}

#[cfg(unix)]
fn ensure_symlink_path(path: &Path, target: &str) -> Result<(), String> {
    use std::os::unix::fs::symlink;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.is_dir() {
                fs::remove_dir_all(path)
                    .map_err(|e| format!("remove dir {}: {e}", path.display()))?;
            } else {
                fs::remove_file(path)
                    .map_err(|e| format!("remove file {}: {e}", path.display()))?;
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(format!("stat {}: {err}", path.display())),
    }
    symlink(target, path).map_err(|e| format!("create symlink {}: {e}", path.display()))
}

#[cfg(not(unix))]
fn ensure_symlink_path(_path: &Path, _target: &str) -> Result<(), String> {
    Err(errIncompatibleSymlink.to_string())
}

fn safe_join_relative_path(root: &Path, relative: &str) -> Result<PathBuf, String> {
    safe_join_relative_path_impl(root, relative, false)
}

fn safe_join_relative_path_allow_leaf_symlink(
    root: &Path,
    relative: &str,
) -> Result<PathBuf, String> {
    safe_join_relative_path_impl(root, relative, true)
}

fn safe_join_relative_path_impl(
    root: &Path,
    relative: &str,
    allow_leaf_symlink: bool,
) -> Result<PathBuf, String> {
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
    if clean.as_os_str().is_empty() {
        return Err(format!("invalid relative path: {relative}"));
    }
    let mut current = root.to_path_buf();
    let parts = clean
        .components()
        .filter_map(|component| match component {
            Component::Normal(seg) => Some(seg.to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for (idx, seg) in parts.iter().enumerate() {
        current.push(seg);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                if allow_leaf_symlink && idx + 1 == parts.len() {
                    continue;
                }
                return Err(format!("symlink path component is not allowed: {relative}"));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("stat {}: {err}", current.display())),
        }
    }
    Ok(root.join(clean))
}

fn delete_path_if_exists(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.is_dir() {
                match fs::remove_dir(path) {
                    Ok(()) => Ok(()),
                    Err(err) if err.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                        Err(errDirNotEmpty.to_string())
                    }
                    Err(err) => Err(format!("remove dir {}: {err}", path.display())),
                }
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
    fn resolve_abs_path(&self, path: &str) -> Result<Option<PathBuf>, String> {
        if self.folder.config.path.trim().is_empty() {
            return Ok(None);
        }
        let root = PathBuf::from(&self.folder.config.path);
        safe_join_relative_path(&root, path).map(Some)
    }

    fn resolve_abs_path_for_delete(&self, path: &str) -> Result<Option<PathBuf>, String> {
        if self.folder.config.path.trim().is_empty() {
            return Ok(None);
        }
        let root = PathBuf::from(&self.folder.config.path);
        safe_join_relative_path_allow_leaf_symlink(&root, path).map(Some)
    }

    pub(crate) fn BringToFront(&mut self) {
        self.folder.BringToFront();
    }
    pub(crate) fn Jobs(&self) -> BTreeMap<&'static str, bool> {
        self.folder.Jobs()
    }

    pub(crate) fn checkParent(&self, path: &str) -> Result<(), String> {
        if path.contains("../") {
            return Err(errDirPrefix.to_string());
        }
        let _ = self.resolve_abs_path(path)?;
        Ok(())
    }

    pub(crate) fn checkToBeDeleted(&self, path: &str) -> Result<(), String> {
        if path.ends_with('/') {
            return Err(errUnexpectedDirOnFileDel.to_string());
        }
        let Some(abs) = self.resolve_abs_path_for_delete(path)? else {
            return Ok(());
        };
        let meta = match fs::symlink_metadata(&abs) {
            Ok(meta) => Some(meta),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => None,
            Err(err) => return Err(format!("stat {}: {err}", abs.display())),
        };
        let expected = self
            .folder
            .db
            .read()
            .map_err(|_| "db lock poisoned".to_string())?
            .get_device_file(&self.folder.config.id, LOCAL_DEVICE_ID, path)
            .map_err(|e| format!("lookup local file {path}: {e}"))?;
        let Some(expected) = expected else {
            return match meta {
                Some(_) => Err(errModified.to_string()),
                None => Ok(()),
            };
        };
        if expected.deleted {
            return Ok(());
        }
        let meta = match meta {
            Some(meta) => meta,
            None => {
                return Err(errModified.to_string());
            }
        };

        let disk_type = if meta.is_dir() {
            db::FileInfoType::Directory
        } else if meta.file_type().is_symlink() {
            db::FileInfoType::Symlink
        } else {
            db::FileInfoType::File
        };
        let disk_size = if disk_type == db::FileInfoType::File {
            i64::try_from(meta.len()).unwrap_or(i64::MAX)
        } else {
            0
        };
        let disk_mtime = file_modified_ns(&meta)?;
        if expected.file_type != disk_type
            || expected.size != disk_size
            || expected.modified_ns != disk_mtime
        {
            return Err(errModified.to_string());
        }
        Ok(())
    }

    pub(crate) fn copierRoutine(&mut self) -> usize {
        let mut copied = 0;
        while let Some(path) = self.queue.pop_front() {
            match self.copyBlock(&path) {
                Ok(()) => copied += 1,
                Err(err) => {
                    let pull_err = self.newPullError(&path, &err);
                    self.tempPullErrors.push(pull_err.clone());
                    self.folder.errorsMut.push(pull_err);
                }
            }
        }
        copied
    }

    pub(crate) fn copyBlock(&mut self, path: &str) -> Result<(), String> {
        self.copyBlockFromFile(path)
            .or_else(|_| self.copyBlockFromFolder(path))
    }

    pub(crate) fn copyBlockFromFile(&mut self, path: &str) -> Result<(), String> {
        let reservation = path.len().max(1);
        self.withLimiter(reservation)?;
        if let Some(abs) = self.resolve_abs_path(path)? {
            match fs::metadata(&abs) {
                Ok(meta) => {
                    if meta.is_dir() {
                        return Err(errUnexpectedDirOnFileDel.to_string());
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    return Err(errNotAvailable.to_string());
                }
                Err(err) => return Err(format!("stat {}: {err}", abs.display())),
            }
        }
        self.blockPullReorderer
            .insert(path.to_string(), reservation);
        Ok(())
    }

    pub(crate) fn copyBlockFromFolder(&mut self, path: &str) -> Result<(), String> {
        let reservation = path.len() / 2 + 1;
        self.withLimiter(reservation)?;
        if let Some(abs) = self.resolve_abs_path(path)? {
            if abs.exists() && abs.is_dir() {
                return Err(errUnexpectedDirOnFileDel.to_string());
            }
            if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
            }
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&abs)
                .map_err(|e| format!("open {}: {e}", abs.display()))?;
        }
        self.blockPullReorderer
            .insert(path.to_string(), reservation);
        Ok(())
    }

    pub(crate) fn copyOwnershipFromParent(&self, _path: &str) -> bool {
        true
    }

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
        self.checkToBeDeleted(path)?;
        let Some(abs) = self.resolve_abs_path_for_delete(path)? else {
            return Ok(());
        };
        match fs::symlink_metadata(&abs) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(errUnexpectedDirOnFileDel.to_string());
                }
                let mut entries =
                    fs::read_dir(&abs).map_err(|e| format!("read dir {}: {e}", abs.display()))?;
                if entries.next().is_some() {
                    return Err(errDirNotEmpty.to_string());
                }
                fs::remove_dir(&abs).map_err(|e| format!("remove dir {}: {e}", abs.display()))?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("stat {}: {err}", abs.display())),
        }
        Ok(())
    }

    pub(crate) fn deleteDirOnDiskHandleChildren(&mut self, path: &str) -> Result<(), String> {
        if path.is_empty() {
            return Err(errDirNotEmpty.to_string());
        }
        let Some(abs) = self.resolve_abs_path_for_delete(path)? else {
            return Ok(());
        };
        match fs::symlink_metadata(&abs) {
            Ok(meta) => {
                if !meta.is_dir() {
                    return Err(errUnexpectedDirOnFileDel.to_string());
                }
                fs::remove_dir_all(&abs)
                    .map_err(|e| format!("remove dir tree {}: {e}", abs.display()))?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("stat {}: {err}", abs.display())),
        }
        Ok(())
    }

    pub(crate) fn deleteFile(&mut self, path: &str) -> Result<(), String> {
        self.deleteFileWithCurrent(path)
    }

    pub(crate) fn deleteFileWithCurrent(&mut self, path: &str) -> Result<(), String> {
        self.checkToBeDeleted(path)?;
        let Some(abs) = self.resolve_abs_path_for_delete(path)? else {
            return Ok(());
        };
        match fs::symlink_metadata(&abs) {
            Ok(meta) => {
                if meta.is_dir() {
                    return Err(errUnexpectedDirOnFileDel.to_string());
                }
                fs::remove_file(&abs).map_err(|e| format!("remove file {}: {e}", abs.display()))?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("stat {}: {err}", abs.display())),
        }
        Ok(())
    }

    pub(crate) fn deleteItemOnDisk(&mut self, path: &str) -> Result<(), String> {
        if let Some(abs) = self.resolve_abs_path_for_delete(path)? {
            match fs::symlink_metadata(&abs) {
                Ok(meta) => {
                    if meta.is_dir() {
                        return self.deleteDirOnDisk(path);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(err) => return Err(format!("stat {}: {err}", abs.display())),
            }
        }
        self.deleteFile(path)
    }

    pub(crate) fn finisherRoutine(&mut self) -> usize {
        self.tempPullErrors.len()
    }

    pub(crate) fn handleDir(&mut self, path: &str) -> Result<(), String> {
        self.checkParent(path)?;
        if let Some(abs) = self.resolve_abs_path(path)? {
            ensure_directory_path(&abs)?;
        }
        Ok(())
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
        if let Some(abs) = self.resolve_abs_path(path)? {
            if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
            }
            match fs::symlink_metadata(&abs) {
                Ok(meta) => {
                    if meta.is_dir() {
                        fs::remove_dir_all(&abs)
                            .map_err(|e| format!("remove dir {}: {e}", abs.display()))?;
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(format!("stat {}: {err}", abs.display())),
            }
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&abs)
                .map_err(|e| format!("open {}: {e}", abs.display()))?;
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
        let mut deleted = 0;
        for p in paths {
            match self.deleteItemOnDisk(p) {
                Ok(()) => deleted += 1,
                Err(err) => {
                    let pull_err = self.newPullError(p, &format!("delete: {err}"));
                    self.tempPullErrors.push(pull_err.clone());
                    self.folder.errorsMut.push(pull_err);
                }
            }
        }
        deleted
    }

    pub(crate) fn processNeeded(&mut self, paths: &[String]) -> Result<usize, String> {
        let mut processed = 0;
        let mut failures = Vec::new();
        for p in paths {
            match self.handleFile(p) {
                Ok(()) => {
                    processed += 1;
                }
                Err(err) => failures.push(format!("{p}: {err}")),
            }
        }
        if failures.is_empty() {
            Ok(processed)
        } else {
            Err(format!(
                "processNeeded failed for {} path(s): {}",
                failures.len(),
                failures.join("; ")
            ))
        }
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

    pub(crate) fn renameFile(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(String, String), String> {
        let renamed = self
            .folder
            .findRename(old_name, new_name)
            .ok_or_else(|| "rename source not found".to_string())?;
        let old_abs = self
            .resolve_abs_path(&renamed.0)?
            .ok_or_else(|| format!("source path escapes folder root: {}", renamed.0))?;
        let new_abs = self
            .resolve_abs_path(&renamed.1)?
            .ok_or_else(|| format!("target path escapes folder root: {}", renamed.1))?;

        if let Some(parent) = new_abs.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("create rename target dir {}: {e}", parent.display()))?;
        }
        if !old_abs.exists() {
            return Err(format!(
                "rename source does not exist: {}",
                old_abs.display()
            ));
        }
        fs::rename(&old_abs, &new_abs).map_err(|e| {
            format!(
                "rename {} -> {} failed: {e}",
                old_abs.display(),
                new_abs.display()
            )
        })?;
        Ok(renamed)
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
        let batch = self
            .dirs
            .iter()
            .take(deletedBatchSize)
            .cloned()
            .collect::<Vec<_>>();
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
    pub(crate) fn Revert(&mut self) {
        if let Err(err) = self.revert() {
            let scan_path = self.sendReceiveFolder.folder.config.path.clone();
            self.sendReceiveFolder.folder.newScanError(&scan_path, &err);
        }
    }

    pub(crate) fn revert(&mut self) -> Result<(), String> {
        let folder_id = self.sendReceiveFolder.folder.config.id.clone();
        let mut db = self
            .sendReceiveFolder
            .folder
            .db
            .write()
            .map_err(|_| "db lock poisoned".to_string())?;
        let mut sequence = db
            .get_device_sequence(&folder_id, LOCAL_DEVICE_ID)
            .map_err(|e| format!("load local sequence: {e}"))?
            .saturating_add(1);
        let locals = db::collect_stream(
            db.all_local_files_ordered(&folder_id, LOCAL_DEVICE_ID)
                .map_err(|e| format!("load local files: {e}"))?,
        )
        .map_err(|e| format!("load local files: {e}"))?;
        let remote_devices = db
            .list_devices_for_folder(&folder_id)
            .map_err(|e| format!("list remote devices: {e}"))?;

        let mut updates = Vec::new();
        let mut disk_deletes = Vec::new();
        for mut local in locals {
            if local.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY == 0 {
                continue;
            }
            let mut has_remote_entry = false;
            for device in &remote_devices {
                if db
                    .get_device_file(&folder_id, device, &local.path)
                    .map_err(|e| format!("lookup remote file {} on {}: {e}", local.path, device))?
                    .is_some()
                {
                    has_remote_entry = true;
                    break;
                }
            }
            if !has_remote_entry {
                continue;
            }
            match db.get_global_file(&folder_id, &local.path) {
                Ok(Some(global)) => {
                    if global.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY != 0 {
                        local.deleted = true;
                        local.local_flags &= !db::FLAG_LOCAL_RECEIVE_ONLY;
                        local.block_hashes.clear();
                        disk_deletes.push(local.path.clone());
                    } else {
                        let mut merged = global;
                        merged.local_flags &= !db::FLAG_LOCAL_RECEIVE_ONLY;
                        merged.sequence = sequence;
                        sequence = sequence.saturating_add(1);
                        updates.push(merged);
                        continue;
                    }
                }
                Ok(None) => continue,
                Err(err) => return Err(format!("lookup global {}: {err}", local.path)),
            }
            local.sequence = sequence;
            sequence = sequence.saturating_add(1);
            updates.push(local);
        }
        if !updates.is_empty() {
            db.update(&folder_id, LOCAL_DEVICE_ID, updates)
                .map_err(|e| format!("persist receive-only revert: {e}"))?;
        }
        drop(db);
        for path in disk_deletes {
            self.sendReceiveFolder.deleteItemOnDisk(&path)?;
        }
        self.sendReceiveFolder.folder.SchedulePull();
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct receiveEncryptedFolder {
    pub(crate) sendReceiveFolder: sendReceiveFolder,
}

impl receiveEncryptedFolder {
    pub(crate) fn Revert(&mut self) {
        if let Err(err) = self.revert() {
            let scan_path = self.sendReceiveFolder.folder.config.path.clone();
            self.sendReceiveFolder.folder.newScanError(&scan_path, &err);
        }
    }

    pub(crate) fn revert(&mut self) -> Result<(), String> {
        let folder_id = self.sendReceiveFolder.folder.config.id.clone();
        let mut db = self
            .sendReceiveFolder
            .folder
            .db
            .write()
            .map_err(|_| "db lock poisoned".to_string())?;
        let mut sequence = db
            .get_device_sequence(&folder_id, LOCAL_DEVICE_ID)
            .map_err(|e| format!("load local sequence: {e}"))?
            .saturating_add(1);
        let locals = db::collect_stream(
            db.all_local_files_ordered(&folder_id, LOCAL_DEVICE_ID)
                .map_err(|e| format!("load local files: {e}"))?,
        )
        .map_err(|e| format!("load local files: {e}"))?;
        let mut dirs = Vec::new();
        let mut files_to_delete = Vec::new();
        let mut updates = Vec::new();

        for mut local in locals {
            let receive_only_changed = local.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY != 0;
            if !receive_only_changed || local.deleted {
                continue;
            }
            if local.file_type == db::FileInfoType::Directory {
                dirs.push(local.path.clone());
                continue;
            }
            files_to_delete.push(local.path.clone());
            local.deleted = true;
            local.block_hashes.clear();
            local.sequence = sequence;
            sequence = sequence.saturating_add(1);
            updates.push(local);
        }

        if !updates.is_empty() {
            db.update(&folder_id, LOCAL_DEVICE_ID, updates)
                .map_err(|e| format!("persist receive-encrypted revert: {e}"))?;
        }
        drop(db);

        for path in files_to_delete {
            let _ = self.sendReceiveFolder.deleteItemOnDisk(&path);
        }
        let _ = self.revertHandleDirs(&dirs);
        self.sendReceiveFolder.folder.SchedulePull();
        Ok(())
    }

    pub(crate) fn revertHandleDirs(&mut self, dirs: &[String]) -> usize {
        if dirs.is_empty() {
            return 0;
        }
        let mut sorted = dirs.to_vec();
        sorted.sort_by(|a, b| b.cmp(a));
        let mut removed = 0_usize;
        for dir in sorted {
            if self
                .sendReceiveFolder
                .deleteDirOnDiskHandleChildren(&dir)
                .is_ok()
            {
                removed = removed.saturating_add(1);
            }
            self.sendReceiveFolder
                .folder
                .ScheduleForceRescan(&[dir.clone()]);
        }
        removed
    }
}

pub(crate) fn newFolder(config: FolderConfiguration, db: Arc<RwLock<db::WalFreeDb>>) -> folder {
    let runtimeMemoryBudget = runtime_budget_for_folder(&db, &config);
    folder {
        config,
        db,
        runtimeMemoryBudget,
        ..Default::default()
    }
}

pub(crate) fn newSendReceiveFolder(
    config: FolderConfiguration,
    db: Arc<RwLock<db::WalFreeDb>>,
) -> sendReceiveFolder {
    sendReceiveFolder {
        folder: newFolder(config, db),
        writeLimiter: defaultPullerPendingKiB,
        ..Default::default()
    }
}

pub(crate) fn newReceiveOnlyFolder(
    config: FolderConfiguration,
    db: Arc<RwLock<db::WalFreeDb>>,
) -> receiveOnlyFolder {
    receiveOnlyFolder {
        sendReceiveFolder: newSendReceiveFolder(config, db),
    }
}

pub(crate) fn newReceiveEncryptedFolder(
    config: FolderConfiguration,
    db: Arc<RwLock<db::WalFreeDb>>,
) -> receiveEncryptedFolder {
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

pub(crate) fn blockDiff(
    local: &[db::BlockMapEntry],
    remote: &[db::BlockMapEntry],
) -> Vec<db::BlockMapEntry> {
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
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        let (_, removes) = sb.Flush().expect("flush");
        assert_eq!(removes, vec!["a"]);
    }

    #[test]
    fn scan_batch_flush_applies_removes_before_updates() {
        let root = temp_root("scan-batch-order");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        db.write()
            .expect("db lock")
            .update("default", "local", vec![file("same.txt", 1, &["h0"])])
            .expect("seed local");

        let mut sb = scanBatch {
            f: "default".to_string(),
            db: Some(db.clone()),
            ..Default::default()
        };
        sb.Remove("same.txt");
        sb.Update(file("same.txt", 2, &["h1"]));
        let (updates, removes) = sb.Flush().expect("flush");
        assert_eq!(removes, vec!["same.txt".to_string()]);
        assert_eq!(updates.len(), 1);

        let current = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "same.txt")
            .expect("lookup")
            .expect("file must exist after remove+update");
        assert_eq!(current.sequence, 2);
        assert_eq!(current.block_hashes, vec!["h1".to_string()]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn base_folder_override_and_revert_are_noops() {
        let root = temp_root("folder-noop-actions");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newFolder(scan_cfg(&root), db);
        f.scanScheduled = false;
        f.pullScheduled = false;
        f.forcedRescanPaths.insert("a/b.txt".to_string());

        f.Override();
        f.Revert();

        assert!(!f.scanScheduled);
        assert!(!f.pullScheduled);
        assert!(f.forcedRescanPaths.contains("a/b.txt"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn serve_does_not_block_pull_when_scan_is_delayed() {
        let root = temp_root("serve-delay-does-not-block-pull");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newFolder(scan_cfg(&root), db);
        f.scanScheduled = true;
        f.scanDelayUntil = Instant::now().checked_add(Duration::from_secs(60));
        f.pullScheduled = true;

        f.Serve();

        assert!(
            !f.pullScheduled,
            "pull should still run when a scan is delayed into the future"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scan_timer_fired_marks_initial_scan_and_reschedules() {
        let root = temp_root("scan-timer-reschedule");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newFolder(scan_cfg(&root), db);
        f.scanScheduled = true;
        f.pullScheduled = false;
        f.initialScanCompleted = false;

        f.scanTimerFired().expect("scan timer");

        assert!(
            f.initialScanCompleted,
            "first scan should mark initial scan done"
        );
        assert!(
            f.scanScheduled,
            "scan should be rescheduled for next interval"
        );
        assert!(f.scanDelayUntil.is_some(), "next scan delay must be set");
        assert!(
            !f.pullScheduled,
            "rescheduling the scan must not force a pull by default"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scan_rejects_parent_escape_in_subdir_scope() {
        let root = temp_root("scan-parent-escape");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newFolder(scan_cfg(&root), db);

        let err = f
            .Scan(&["../outside".to_string()])
            .expect_err("must reject parent escape");
        assert!(err.contains("invalid relative path"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn forced_rescan_paths_are_not_lost_on_mark_error() {
        let root = temp_root("forced-rescan-retry");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        {
            let db_for_panic = db.clone();
            let _ = std::panic::catch_unwind(move || {
                let _guard = db_for_panic.write().expect("db write lock");
                panic!("poison db lock");
            });
        }

        let mut f = newFolder(scan_cfg(&root), db);
        f.forcedRescanPaths.insert("a.txt".to_string());
        let err = f
            .handleForcedRescans()
            .expect_err("must fail with poisoned db");
        assert!(err.contains("db lock poisoned"));
        assert!(f.forcedRescanPaths.contains("a.txt"));

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn safe_join_rejects_symlink_component_escape() {
        use std::os::unix::fs::symlink;

        let root = temp_root("safe-join-symlink-escape");
        let outside = temp_root("safe-join-symlink-outside");
        symlink(&outside, root.join("escape")).expect("symlink");

        let err =
            safe_join_relative_path(&root, "escape/pwn.txt").expect_err("must reject symlink path");
        assert!(err.contains("symlink"));

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(outside);
    }

    #[cfg(unix)]
    #[test]
    fn scan_records_symlink_entries() {
        use std::os::unix::fs::symlink;

        let root = temp_root("scan-symlink-entry");
        fs::write(root.join("target.txt"), b"ok").expect("write target");
        symlink(root.join("target.txt"), root.join("link.txt")).expect("symlink");

        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("scan");

        let link = db
            .read()
            .expect("db read")
            .get_device_file("default", LOCAL_DEVICE_ID, "link.txt")
            .expect("get file")
            .expect("link entry exists");
        assert_eq!(link.file_type, db::FileInfoType::Symlink);
        assert!(!link.deleted);

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn apply_remote_file_applies_symlink_entries() {
        let root = temp_root("pull-symlink-apply");
        let file = db::FileInfo {
            folder: "default".to_string(),
            path: "link.txt".to_string(),
            sequence: 1,
            modified_ns: 1,
            size: 0,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::Symlink,
            block_hashes: vec!["target.txt".to_string()],
        };

        apply_remote_file_to_disk(&root, &file).expect("apply symlink pull");
        let link_path = root.join("link.txt");
        let meta = fs::symlink_metadata(&link_path).expect("symlink metadata");
        assert!(meta.file_type().is_symlink());
        let target = fs::read_link(&link_path).expect("read symlink target");
        assert_eq!(target.to_string_lossy(), "target.txt");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sendrecv_handles_queue() {
        let cfg = FolderConfiguration::default();
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newSendReceiveFolder(cfg, db);
        f.pullBlock("a.txt").expect("queue");
        let copied = f.pullerRoutine().expect("puller");
        assert_eq!(copied, 1);
    }

    #[test]
    fn sendrecv_applies_disk_mutations_for_file_and_dir_paths() {
        let root = temp_root("sendrecv-disk-mutations");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newSendReceiveFolder(scan_cfg(&root), db);

        f.handleDir("dir/sub").expect("create dir");
        f.handleFile("dir/sub/a.txt").expect("queue file");
        let copied = f.pullerRoutine().expect("apply queued writes");
        assert_eq!(copied, 1);
        assert!(root.join("dir").join("sub").join("a.txt").exists());

        let file_meta = fs::symlink_metadata(root.join("dir").join("sub").join("a.txt"))
            .expect("file metadata");
        let file_mtime = file_modified_ns(&file_meta).expect("file mtime");
        f.folder
            .db
            .write()
            .expect("db write")
            .update(
                "default",
                "local",
                vec![db::FileInfo {
                    folder: "default".to_string(),
                    path: "dir/sub/a.txt".to_string(),
                    sequence: 1,
                    modified_ns: file_mtime,
                    size: 0,
                    deleted: false,
                    ignored: false,
                    local_flags: 0,
                    file_type: db::FileInfoType::File,
                    block_hashes: Vec::new(),
                }],
            )
            .expect("seed local entry");

        f.deleteFile("dir/sub/a.txt").expect("delete file");
        assert!(!root.join("dir").join("sub").join("a.txt").exists());

        fs::create_dir_all(root.join("dir").join("sub").join("nested")).expect("mkdir nested");
        fs::write(
            root.join("dir").join("sub").join("nested").join("x.txt"),
            b"x",
        )
        .expect("write nested");
        let dir_meta = fs::symlink_metadata(root.join("dir").join("sub")).expect("dir metadata");
        let dir_mtime = file_modified_ns(&dir_meta).expect("dir mtime");
        f.folder
            .db
            .write()
            .expect("db write")
            .update(
                "default",
                "local",
                vec![db::FileInfo {
                    folder: "default".to_string(),
                    path: "dir/sub".to_string(),
                    sequence: 2,
                    modified_ns: dir_mtime,
                    size: 0,
                    deleted: false,
                    ignored: false,
                    local_flags: 0,
                    file_type: db::FileInfoType::Directory,
                    block_hashes: Vec::new(),
                }],
            )
            .expect("seed local dir entry");
        let err = f
            .deleteDirOnDisk("dir/sub")
            .expect_err("must reject non-empty dir");
        assert_eq!(err, errDirNotEmpty);
        f.deleteDirOnDiskHandleChildren("dir/sub")
            .expect("recursive delete");
        assert!(!root.join("dir").join("sub").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_path_if_exists_rejects_non_empty_directory() {
        let root = temp_root("delete-path-non-empty");
        let dir = root.join("dir");
        fs::create_dir_all(&dir).expect("mkdir dir");
        fs::write(dir.join("child.txt"), b"x").expect("write child");

        let err = delete_path_if_exists(&dir).expect_err("must reject non-empty dir");
        assert_eq!(err, errDirNotEmpty);
        assert!(dir.join("child.txt").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_file_with_current_rejects_locally_modified_item() {
        let root = temp_root("delete-modified-guard");
        fs::write(root.join("x.txt"), b"v1").expect("seed file");

        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut baseline = newFolder(scan_cfg(&root), db.clone());
        baseline.Scan(&[]).expect("baseline scan");

        std::thread::sleep(Duration::from_millis(2));
        fs::write(root.join("x.txt"), b"v2").expect("mutate file");

        let mut sr = newSendReceiveFolder(scan_cfg(&root), db);
        let err = sr
            .deleteFileWithCurrent("x.txt")
            .expect_err("must reject modified file");
        assert_eq!(err, errModified);
        assert!(root.join("x.txt").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_file_with_current_rejects_missing_disk_item_when_db_has_file() {
        let root = temp_root("delete-missing-guard");
        fs::write(root.join("x.txt"), b"v1").expect("seed file");

        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut baseline = newFolder(scan_cfg(&root), db.clone());
        baseline.Scan(&[]).expect("baseline scan");

        fs::remove_file(root.join("x.txt")).expect("remove file");

        let mut sr = newSendReceiveFolder(scan_cfg(&root), db);
        let err = sr
            .deleteFileWithCurrent("x.txt")
            .expect_err("must reject missing file drift");
        assert_eq!(err, errModified);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn delete_file_with_current_rejects_untracked_existing_item() {
        let root = temp_root("delete-untracked-guard");
        fs::write(root.join("x.txt"), b"v1").expect("seed file");

        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut sr = newSendReceiveFolder(scan_cfg(&root), db);
        let err = sr
            .deleteFileWithCurrent("x.txt")
            .expect_err("must reject untracked delete");
        assert_eq!(err, errModified);
        assert!(root.join("x.txt").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn delete_file_with_current_allows_tracked_symlink_leaf() {
        use std::os::unix::fs::symlink;

        let root = temp_root("delete-symlink-leaf");
        fs::write(root.join("target.txt"), b"target").expect("write target");
        symlink(root.join("target.txt"), root.join("link.txt")).expect("create symlink");

        let meta = fs::symlink_metadata(root.join("link.txt")).expect("symlink metadata");
        let mtime = file_modified_ns(&meta).expect("mtime");

        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        db.write()
            .expect("db lock")
            .update(
                "default",
                "local",
                vec![db::FileInfo {
                    folder: "default".to_string(),
                    path: "link.txt".to_string(),
                    sequence: 1,
                    modified_ns: mtime,
                    size: 0,
                    deleted: false,
                    ignored: false,
                    local_flags: 0,
                    file_type: db::FileInfoType::Symlink,
                    block_hashes: Vec::new(),
                }],
            )
            .expect("seed local");

        let mut sr = newSendReceiveFolder(scan_cfg(&root), db);
        sr.deleteFileWithCurrent("link.txt")
            .expect("delete symlink");
        assert!(!root.join("link.txt").exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn process_deletions_records_errors_instead_of_swallowing() {
        let root = temp_root("process-deletions-errors");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut sr = newSendReceiveFolder(scan_cfg(&root), db);

        let deleted = sr.processDeletions(&["../escape.txt".to_string()]);
        assert_eq!(deleted, 0);
        assert_eq!(sr.tempPullErrors.len(), 1);
        assert_eq!(sr.folder.errorsMut.len(), 1);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receive_only_revert_preserves_flag_when_global_missing() {
        let root = temp_root("recvonly-revert-missing-global");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        db.write()
            .expect("db lock")
            .update(
                "default",
                "local",
                vec![db::FileInfo {
                    folder: "default".to_string(),
                    path: "x.txt".to_string(),
                    sequence: 1,
                    modified_ns: 1,
                    size: 2,
                    deleted: false,
                    ignored: false,
                    local_flags: db::FLAG_LOCAL_RECEIVE_ONLY | db::FLAG_LOCAL_IGNORED,
                    file_type: db::FileInfoType::File,
                    block_hashes: vec!["h1".to_string()],
                }],
            )
            .expect("seed local");

        let mut ro = newReceiveOnlyFolder(scan_cfg(&root), db.clone());
        ro.revert().expect("revert");

        let current = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "x.txt")
            .expect("lookup")
            .expect("local file");
        assert_eq!(
            current.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY,
            db::FLAG_LOCAL_RECEIVE_ONLY
        );
        assert!(!current.deleted);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn receive_only_revert_deletes_disk_when_global_is_receive_only_changed() {
        let root = temp_root("recvonly-revert-delete-disk");
        fs::write(root.join("x.txt"), b"v1").expect("seed file");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        {
            let mut guard = db.write().expect("db lock");
            guard
                .update(
                    "default",
                    "local",
                    vec![db::FileInfo {
                        folder: "default".to_string(),
                        path: "x.txt".to_string(),
                        sequence: 1,
                        modified_ns: 1,
                        size: 2,
                        deleted: false,
                        ignored: false,
                        local_flags: db::FLAG_LOCAL_RECEIVE_ONLY,
                        file_type: db::FileInfoType::File,
                        block_hashes: vec!["h1".to_string()],
                    }],
                )
                .expect("seed local");
            guard
                .update(
                    "default",
                    "peer-a",
                    vec![db::FileInfo {
                        folder: "default".to_string(),
                        path: "x.txt".to_string(),
                        sequence: 2,
                        modified_ns: 2,
                        size: 2,
                        deleted: false,
                        ignored: false,
                        local_flags: db::FLAG_LOCAL_RECEIVE_ONLY,
                        file_type: db::FileInfoType::File,
                        block_hashes: vec!["h2".to_string()],
                    }],
                )
                .expect("seed peer");
        }

        let mut ro = newReceiveOnlyFolder(scan_cfg(&root), db.clone());
        ro.revert().expect("revert");

        assert!(!root.join("x.txt").exists());
        let current = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "x.txt")
            .expect("lookup")
            .expect("local file");
        assert!(current.deleted);
        assert_eq!(current.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY, 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sendrecv_rename_file_updates_disk_state() {
        let root = temp_root("sendrecv-rename");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let f = newSendReceiveFolder(scan_cfg(&root), db);

        let old_path = root.join("old.txt");
        let new_path = root.join("new").join("name.txt");
        fs::write(&old_path, b"data").expect("write old file");
        let renamed = f
            .renameFile("old.txt", "new/name.txt")
            .expect("rename result");
        assert_eq!(renamed, ("old.txt".to_string(), "new/name.txt".to_string()));
        assert!(!old_path.exists());
        assert!(new_path.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sendrecv_records_copy_errors_when_disk_target_is_directory() {
        let root = temp_root("sendrecv-copy-errors");
        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newSendReceiveFolder(scan_cfg(&root), db);

        fs::create_dir_all(root.join("dir")).expect("mkdir dir");
        f.pullBlock("dir").expect("queue target");
        let err = f.pullerRoutine().expect_err("must fail");
        assert_eq!(err, errNotAvailable);
        assert_eq!(f.tempPullErrors.len(), 1);
        assert_eq!(f.tempPullErrors[0].Path, "dir");
        assert!(
            f.tempPullErrors[0].Err.contains(errUnexpectedDirOnFileDel),
            "expected directory-copy failure, got {}",
            f.tempPullErrors[0].Err
        );

        let _ = fs::remove_dir_all(root);
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

        let db = Arc::new(RwLock::new(dbv));
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
        let guard = f.db.read().expect("db lock");
        let synced = guard
            .get_device_file("default", "local", "same.txt")
            .expect("lookup")
            .expect("local same.txt");
        assert_eq!(synced.sequence, 2);
        assert_eq!(synced.block_hashes, vec!["h1", "h2", "h3"]);
        let need = guard
            .count_need("default", "local")
            .expect("count need after pull");
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

        let db = Arc::new(RwLock::new(dbv));
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
    fn send_only_mode_does_not_apply_remote_pull() {
        let root = temp_root("send-only-no-pull");
        let db_root = temp_root("send-only-no-pull-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        dbv.update("default", "local", vec![file("local.txt", 1, &["h1"])])
            .expect("update local");
        dbv.update("default", "remote", vec![file("remote.txt", 2, &["h2"])])
            .expect("update remote");
        let db = Arc::new(RwLock::new(dbv));

        let mut cfg = scan_cfg(&root);
        cfg.folder_type = crate::config::FolderType::SendOnly;
        let mut f = newFolder(cfg, db.clone());
        let pulled = f.pull().expect("pull");
        assert_eq!(pulled, 0);

        let guard = db.read().expect("db lock");
        assert!(
            guard
                .get_device_file("default", "local", "remote.txt")
                .expect("lookup")
                .is_none(),
            "send-only mode must not mirror remote file into local device view"
        );

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn folders_share_runtime_budget_for_same_db_root_and_cap() {
        let root_a = temp_root("shared-budget-a");
        let root_b = temp_root("shared-budget-b");
        let db_root = temp_root("shared-budget-db");
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 64).expect("open runtime db"),
        ));

        let mut cfg_a = scan_cfg(&root_a);
        cfg_a.id = "folder-a".to_string();
        cfg_a.memory_max_mb = 64;
        let mut cfg_b = scan_cfg(&root_b);
        cfg_b.id = "folder-b".to_string();
        cfg_b.memory_max_mb = 64;

        let f_a = newFolder(cfg_a, db.clone());
        let f_b = newFolder(cfg_b, db);
        assert!(
            Arc::ptr_eq(&f_a.runtimeMemoryBudget, &f_b.runtimeMemoryBudget),
            "folders on same runtime DB root and cap should share runtime memory manager"
        );

        let _ = fs::remove_dir_all(root_a);
        let _ = fs::remove_dir_all(root_b);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_hard_blocks_when_fail_policy_exceeds_cap() {
        let root = temp_root("fail-policy");
        let db_root = temp_root("fail-policy-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 0).expect("open runtime db");
        dbv.update("default", "local", vec![file("same.txt", 1, &["h1"])])
            .expect("update local");
        dbv.update(
            "default",
            "remote",
            vec![file("same.txt", 2, &["h1", "h2"])],
        )
        .expect("update remote");

        let db = Arc::new(RwLock::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.memory_policy = MemoryPolicy::Fail;
        let mut f = newFolder(cfg, db);

        let err = f.pull().expect_err("must block on hard cap");
        assert!(err.contains("memory cap exceeded"));
        assert_eq!(f.memoryHardBlockEvents, 1);
        assert!(f.PullStats().hard_blocked);
        let report = f
            .MemoryTelemetry()
            .runtime_cap_violation_report
            .expect("cap report");
        assert_eq!(report.subsystem, "pull-db-budget");
        assert_eq!(report.policy, "fail");
        assert_eq!(report.folder, "default");
        assert_eq!(report.query_limit, 1024);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_hard_blocks_when_runtime_budget_is_exceeded() {
        let root = temp_root("runtime-budget-fail-policy");
        let db_root = temp_root("runtime-budget-fail-policy-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 128).expect("open runtime db");
        let large_hashes = (0..35_000)
            .map(|_| "0123456789abcdef0123456789abcdef".to_string())
            .collect::<Vec<_>>();
        let remote = db::FileInfo {
            folder: "default".to_string(),
            path: "same.txt".to_string(),
            sequence: 2,
            modified_ns: 2,
            size: 1,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: large_hashes,
        };
        dbv.update("default", "remote", vec![remote])
            .expect("update remote");

        let db = Arc::new(RwLock::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.memory_policy = MemoryPolicy::Fail;
        cfg.memory_max_mb = 1;
        let mut f = newFolder(cfg, db);

        let err = f.pull().expect_err("must block on runtime budget");
        assert!(err.contains("memory cap exceeded"));
        assert_eq!(f.memoryHardBlockEvents, 1);
        assert!(f.PullStats().hard_blocked);
        let telemetry = f.MemoryTelemetry();
        let report = telemetry.runtime_cap_violation_report.expect("cap report");
        assert_eq!(report.subsystem, "pull-runtime-budget");
        assert_eq!(report.policy, "fail");
        assert_eq!(report.folder, "default");
        assert_eq!(report.queue_items, 1);
        assert_eq!(report.budget_bytes, 1 * 1024 * 1024);
        assert!(telemetry.runtime_hard_block_events >= 1);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn pull_propagates_remote_delete_to_local() {
        let root = temp_root("pull-delete");
        let db_root = temp_root("pull-delete-db");
        let mut dbv = db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db");
        dbv.update("default", "local", vec![file("gone.txt", 1, &["h1"])])
            .expect("update local");
        let mut deleted = file("gone.txt", 2, &[]);
        deleted.deleted = true;
        dbv.update("default", "remote", vec![deleted])
            .expect("update remote delete");

        let db = Arc::new(RwLock::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        fs::write(root.join("gone.txt"), b"to-delete").expect("create local file");
        let mut f = newFolder(cfg, db);

        let pulled = f.pull().expect("pull");
        assert_eq!(pulled, 1);

        let guard = f.db.read().expect("db lock");
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
        dbv.update("default", "local", vec![file("same.txt", 1, &["h1"])])
            .expect("update local");
        dbv.update(
            "default",
            "remote",
            vec![file("same.txt", 2, &["h1", "h2"])],
        )
        .expect("update remote");

        let db = Arc::new(RwLock::new(dbv));
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
        assert!(telemetry.runtime_cap_violation_report.is_some());

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

        let db = Arc::new(RwLock::new(dbv));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "default".to_string();
        cfg.path = root.to_string_lossy().to_string();
        let mut f = newFolder(cfg, db);

        let pulled = f.pull().expect("pull");
        assert_eq!(pulled, 1);

        let on_disk = root.join("dir").join("new.bin");
        assert!(on_disk.exists());
        assert_eq!(fs::metadata(&on_disk).expect("meta").len(), 4096);

        let guard = f.db.read().expect("db lock");
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

        let outside = root.parent().expect("parent").join("outside.txt");
        let _ = fs::remove_file(&outside);

        let db = Arc::new(RwLock::new(dbv));
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
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("initial scan");

        let ordered = db
            .read()
            .expect("db lock")
            .all_local_files_ordered("default", "local")
            .and_then(db::collect_stream)
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
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut cfg = scan_cfg(&root);
        cfg.ignore_delete = true;
        let mut f = newFolder(cfg, db.clone());
        f.Scan(&[]).expect("initial scan");

        fs::remove_file(root.join("foo").join("a.txt")).expect("remove foo/a");
        f.Scan(&[String::from("foo")]).expect("scoped scan");

        let fi = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "foo/a.txt")
            .expect("lookup")
            .expect("file record");
        assert!(!fi.deleted);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn forced_rescan_reindexes_unchanged_file_and_clears_rescan_flag() {
        let root = temp_root("forced-rescan-unchanged");
        fs::write(root.join("a.txt"), b"same").expect("write a");

        let db_root = temp_root("forced-rescan-unchanged-db");
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("initial scan");

        let before = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "a.txt")
            .expect("lookup before")
            .expect("before file");

        f.ScheduleForceRescan(&[String::from("a.txt")]);
        f.handleForcedRescans().expect("forced rescan");

        let after = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "a.txt")
            .expect("lookup after")
            .expect("after file");

        assert!(
            after.sequence > before.sequence,
            "forced rescan should refresh sequence even when mtime/size is unchanged"
        );
        assert_eq!(
            after.local_flags & db::FLAG_LOCAL_MUST_RESCAN,
            0,
            "forced rescan marker must be cleared after scan"
        );
        assert!(!after.deleted);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn scan_marks_skipped_internal_temp_files_as_ignored_not_deleted() {
        let root = temp_root("scan-skip-temp-file");
        let temp_path = ".syncthing.cache.tmp";
        fs::write(root.join(temp_path), b"x").expect("write temp");

        let db_root = temp_root("scan-skip-temp-file-db");
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        db.write()
            .expect("db lock")
            .update("default", "local", vec![file(temp_path, 1, &["h0"])])
            .expect("seed temp file");

        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("scan");

        let tracked = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", temp_path)
            .expect("lookup")
            .expect("tracked temp file");
        assert!(
            tracked.ignored,
            "internal temp files should be ignored instead of treated as deletes"
        );
        assert!(!tracked.deleted);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn scan_tracks_regular_tmp_and_ds_store_files() {
        let root = temp_root("scan-regular-tmp-and-ds-store");
        fs::write(root.join("report.tmp"), b"tmp").expect("write report.tmp");
        fs::write(root.join(".DS_Store"), b"ds").expect("write .DS_Store");

        let db_root = temp_root("scan-regular-tmp-and-ds-store-db");
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("scan");

        let report = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "report.tmp")
            .expect("lookup report")
            .expect("report tracked");
        assert!(!report.deleted);
        assert!(!report.ignored);

        let ds_store = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", ".DS_Store")
            .expect("lookup ds_store")
            .expect("ds_store tracked");
        assert!(!ds_store.deleted);
        assert!(!ds_store.ignored);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(db_root);
    }

    #[test]
    fn scan_uses_sha256_block_hashes() {
        let root = temp_root("scan-sha256-block-hashes");
        fs::write(root.join("a.txt"), b"hello-world").expect("write a.txt");

        let db = Arc::new(RwLock::new(db::WalFreeDb::default()));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("scan");

        let entry = db
            .read()
            .expect("db lock")
            .get_device_file("default", "local", "a.txt")
            .expect("lookup")
            .expect("entry");
        assert!(!entry.block_hashes.is_empty());
        assert_eq!(entry.block_hashes[0].len(), 64);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scoped_scan_deletes_only_within_target_subdir() {
        let root = temp_root("scan-scope");
        fs::create_dir_all(root.join("foo")).expect("mkdir foo");
        fs::create_dir_all(root.join("bar")).expect("mkdir bar");
        fs::write(root.join("foo").join("a.txt"), b"a").expect("write foo/a");
        fs::write(root.join("bar").join("b.txt"), b"b").expect("write bar/b");

        let db_root = temp_root("scan-scope-db");
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("initial scan");

        fs::remove_file(root.join("foo").join("a.txt")).expect("remove foo/a");
        f.Scan(&[String::from("foo")]).expect("scoped scan");

        let guard = db.read().expect("db lock");
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
        let db = Arc::new(RwLock::new(
            db::WalFreeDb::open_runtime(&db_root, 50).expect("open runtime db"),
        ));
        let mut f = newFolder(scan_cfg(&root), db.clone());
        f.Scan(&[]).expect("initial scan");

        let (foo_seq_before, bar_seq_before) = {
            let guard = db.read().expect("db lock before");
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
        f.Scan(&[String::from("bar/b.txt")])
            .expect("single file scope scan");

        let guard = db.read().expect("db lock after");
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
