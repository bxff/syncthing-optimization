#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::config::FolderConfiguration;
use crate::db::{self, Db};
use crate::folder_modes::{mode_actions, FolderMode};
use crate::planner::{classify_paths, compute_need, VersionedFile};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

pub(crate) const deletedBatchSize: usize = 1000;
pub(crate) const kqueueItemCountThreshold: usize = 100_000;
pub(crate) const maxToRemove: usize = 10_000;

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
        self.scanSubdirs(subdirs);
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
        let db = self.db.lock().map_err(|_| "db lock poisoned".to_string())?;
        let needed = db
            .all_needed_global_files(
                &self.config.id,
                "local",
                db::PullOrder::Alphabetic,
                10_000,
                0,
            )
            .map_err(|e| format!("pull query: {e}"))?;
        Ok(needed.len())
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

    pub(crate) fn scanTimerFired(&mut self) {
        self.handleForcedRescans(&self.forcedRescanPaths.iter().cloned().collect::<Vec<_>>());
        self.scanScheduled = false;
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
}
