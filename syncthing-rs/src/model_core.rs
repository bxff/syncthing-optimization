#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::bep::{BepMessage, IndexEntry};
use crate::config::FolderConfiguration;
use crate::db::{self, Db};
use crate::folder_core;
use crate::folder_modes::FolderMode;
use crate::protocol::run_message_exchange;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

pub(crate) static ErrFolderMissing: &str = "folder missing";
pub(crate) static ErrFolderNotRunning: &str = "folder not running";
pub(crate) static ErrFolderPaused: &str = "folder paused";
pub(crate) static errDevicePaused: &str = "device paused";
pub(crate) static errDeviceUnknown: &str = "device unknown";
pub(crate) static errEncryptionInvConfigLocal: &str = "local encryption config invalid";
pub(crate) static errEncryptionInvConfigRemote: &str = "remote encryption config invalid";
pub(crate) static errEncryptionNotEncryptedLocal: &str = "local folder is not encrypted";
pub(crate) static errEncryptionNotEncryptedUntrusted: &str =
    "untrusted remote folder is not encrypted";
pub(crate) static errEncryptionPassword: &str = "invalid encryption password";
pub(crate) static errEncryptionPlainForReceiveEncrypted: &str =
    "receive encrypted folder has plain data";
pub(crate) static errEncryptionPlainForRemoteEncrypted: &str =
    "remote encrypted folder has plain data";
pub(crate) static errEncryptionTokenRead: &str = "failed reading encryption token";
pub(crate) static errEncryptionTokenWrite: &str = "failed writing encryption token";
pub(crate) static errMissingLocalInClusterConfig: &str = "local device missing in cluster config";
pub(crate) static errMissingRemoteInClusterConfig: &str = "remote device missing in cluster config";
pub(crate) static errNoVersioner: &str = "no versioner configured";
pub(crate) static errStopped: &str = "model stopped";
pub(crate) static folderFactories: &str = "folder_factories";
pub(crate) static underscore_var: &str = "_";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Availability {
    pub(crate) ID: String,
    pub(crate) FromTemporary: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ClusterConfigReceivedEventData {
    pub(crate) Device: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConnectionInfo {
    pub(crate) Address: String,
    pub(crate) Type: String,
    pub(crate) Crypto: String,
    pub(crate) IsLocal: bool,
    pub(crate) protocol_Statistics: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ConnectionStats {
    pub(crate) Address: String,
    pub(crate) Type: String,
    pub(crate) Crypto: String,
    pub(crate) IsLocal: bool,
    pub(crate) Connected: bool,
    pub(crate) Paused: bool,
    pub(crate) Primary: bool,
    pub(crate) Secondary: bool,
    pub(crate) ClientVersion: String,
    pub(crate) protocol_Statistics: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FolderCompletion {
    pub(crate) CompletionPct: i32,
    pub(crate) NeedItems: i32,
    pub(crate) NeedDeletes: i32,
    pub(crate) NeedBytes: i64,
    pub(crate) GlobalItems: i32,
    pub(crate) GlobalBytes: i64,
    pub(crate) Sequence: i64,
    pub(crate) RemoteState: String,
}

impl FolderCompletion {
    pub(crate) fn add(&mut self, other: &FolderCompletion) {
        self.NeedItems += other.NeedItems;
        self.NeedDeletes += other.NeedDeletes;
        self.NeedBytes += other.NeedBytes;
        self.GlobalItems += other.GlobalItems;
        self.GlobalBytes += other.GlobalBytes;
        self.Sequence = self.Sequence.max(other.Sequence);
        self.setCompletionPct();
    }

    pub(crate) fn setCompletionPct(&mut self) {
        if self.GlobalBytes <= 0 {
            self.CompletionPct = 100;
            return;
        }
        let need_ratio = self.NeedBytes as f64 / self.GlobalBytes as f64;
        let pct = (100_f64 * (1_f64 - need_ratio)).clamp(0_f64, 100_f64);
        self.CompletionPct = pct as i32;
        if self.NeedBytes == 0 && self.NeedDeletes > 0 {
            self.CompletionPct = 95;
        }
    }

    pub(crate) fn Map(&self) -> BTreeMap<&'static str, Value> {
        BTreeMap::from([
            ("completionPct", Value::from(self.CompletionPct)),
            ("needItems", Value::from(self.NeedItems)),
            ("needDeletes", Value::from(self.NeedDeletes)),
            ("needBytes", Value::from(self.NeedBytes)),
            ("globalItems", Value::from(self.GlobalItems)),
            ("globalBytes", Value::from(self.GlobalBytes)),
            ("sequence", Value::from(self.Sequence)),
            ("remoteState", Value::from(self.RemoteState.clone())),
        ])
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TreeEntry {
    pub(crate) Name: String,
    pub(crate) Type: String,
    pub(crate) Size: i64,
    pub(crate) ModTime: i64,
    pub(crate) Children: Vec<TreeEntry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct clusterConfigDeviceInfo {
    pub(crate) local: bool,
    pub(crate) remote: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct deviceIDSet {
    set: BTreeSet<String>,
}

impl deviceIDSet {
    pub(crate) fn add(&mut self, id: &str) {
        self.set.insert(id.to_string());
    }

    pub(crate) fn AsSlice(&self) -> Vec<String> {
        self.set.iter().cloned().collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct folderDeviceSet {
    set: BTreeMap<String, BTreeSet<String>>,
}

impl folderDeviceSet {
    pub(crate) fn set(&mut self, folder_id: &str, devices: &[String]) {
        self.set
            .insert(folder_id.to_string(), devices.iter().cloned().collect());
    }

    pub(crate) fn has(&self, folder_id: &str) -> bool {
        self.set.contains_key(folder_id)
    }

    pub(crate) fn hasDevice(&self, folder_id: &str, device_id: &str) -> bool {
        self.set
            .get(folder_id)
            .map(|s| s.contains(device_id))
            .unwrap_or(false)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct folderFactory {
    pub(crate) mode: FolderMode,
}

impl Default for folderFactory {
    fn default() -> Self {
        Self {
            mode: FolderMode::SendReceive,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct pager {
    pub(crate) get: Vec<String>,
    pub(crate) toSkip: usize,
}

impl pager {
    pub(crate) fn skip(&mut self, count: usize) {
        self.toSkip = self.toSkip.saturating_add(count);
    }

    pub(crate) fn done(&self) -> bool {
        self.toSkip >= self.get.len()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct redactedError {
    pub(crate) error: String,
    pub(crate) redacted: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct requestResponse {
    pub(crate) once: bool,
    pub(crate) closed: bool,
    pub(crate) data: Vec<u8>,
}

impl requestResponse {
    pub(crate) fn Data(&self) -> Vec<u8> {
        self.data.clone()
    }

    pub(crate) fn Wait(&mut self) -> Result<Vec<u8>, String> {
        if self.closed {
            return Err("request-response closed".to_string());
        }
        self.once = true;
        Ok(self.Data())
    }

    pub(crate) fn Close(&mut self) {
        self.closed = true;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct storedEncryptionToken {
    pub(crate) FolderID: String,
    pub(crate) Token: String,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct syncMutexMap {
    pub(crate) inner: HashMap<String, Arc<Mutex<()>>>,
}

impl syncMutexMap {
    pub(crate) fn Get(&mut self, key: &str) -> Arc<Mutex<()>> {
        self.inner
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct updatedPendingFolder {
    pub(crate) FolderID: String,
    pub(crate) FolderLabel: String,
    pub(crate) DeviceID: String,
    pub(crate) ReceiveEncrypted: bool,
    pub(crate) RemoteEncrypted: bool,
}

pub(crate) trait Model {
    fn State(&self, folder: &str) -> String;
}

#[derive(Clone, Debug)]
pub(crate) struct model {
    pub(crate) id: String,
    pub(crate) shortID: String,
    pub(crate) started: bool,
    pub(crate) closed: bool,
    pub(crate) cfg: BTreeMap<String, FolderConfiguration>,
    pub(crate) foldersRunning: BTreeMap<String, bool>,
    pub(crate) folderCfgs: BTreeMap<String, FolderConfiguration>,
    pub(crate) folderRunners: BTreeMap<String, folder_core::folder>,
    pub(crate) folderIgnores: BTreeMap<String, Vec<String>>,
    pub(crate) folderEncryptionPasswordTokens: BTreeMap<String, String>,
    pub(crate) folderEncryptionFailures: BTreeMap<String, String>,
    pub(crate) folderVersioners: BTreeMap<String, String>,
    pub(crate) folderRestartMuts: syncMutexMap,
    pub(crate) folderIOLimiter: BTreeMap<String, usize>,
    pub(crate) connections: BTreeMap<String, ConnectionStats>,
    pub(crate) deviceConnIDs: BTreeMap<String, String>,
    pub(crate) connRequestLimiters: BTreeMap<String, usize>,
    pub(crate) deviceDownloads: BTreeMap<String, Vec<String>>,
    pub(crate) deviceStatRefs: BTreeMap<String, BTreeMap<String, i64>>,
    pub(crate) remoteFolderStates: BTreeMap<String, String>,
    pub(crate) pendingFolderOffers: BTreeMap<String, BTreeSet<String>>,
    pub(crate) protectedFiles: BTreeSet<String>,
    pub(crate) observed: deviceIDSet,
    pub(crate) helloMessages: BTreeMap<String, String>,
    pub(crate) progressEmitter: bool,
    pub(crate) promotedConnID: String,
    pub(crate) promotionTimer: i64,
    pub(crate) keyGen: i64,
    pub(crate) indexHandlers: BTreeMap<String, String>,
    pub(crate) globalRequestLimiter: usize,
    pub(crate) mut_count: usize,
    pub(crate) sdb: Arc<RwLock<db::WalFreeDb>>,
    pub(crate) evLogger: Vec<String>,
    pub(crate) fatalChan: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct BepExchangeResult {
    pub(crate) transitions: Vec<String>,
    pub(crate) outbound_messages: Vec<BepMessage>,
}

impl Default for model {
    fn default() -> Self {
        model_with_runtime(runtime_db_root(), runtime_db_memory_cap_mb())
    }
}

fn model_with_runtime(db_root: PathBuf, db_memory_cap_mb: usize) -> model {
    let sdb = open_runtime_db(&db_root, db_memory_cap_mb).unwrap_or_else(|err| panic!("{err}"));
    model {
        id: "local-device".to_string(),
        shortID: "local".to_string(),
        started: true,
        closed: false,
        cfg: BTreeMap::new(),
        foldersRunning: BTreeMap::new(),
        folderCfgs: BTreeMap::new(),
        folderRunners: BTreeMap::new(),
        folderIgnores: BTreeMap::new(),
        folderEncryptionPasswordTokens: BTreeMap::new(),
        folderEncryptionFailures: BTreeMap::new(),
        folderVersioners: BTreeMap::new(),
        folderRestartMuts: syncMutexMap::default(),
        folderIOLimiter: BTreeMap::new(),
        connections: BTreeMap::new(),
        deviceConnIDs: BTreeMap::new(),
        connRequestLimiters: BTreeMap::new(),
        deviceDownloads: BTreeMap::new(),
        deviceStatRefs: BTreeMap::new(),
        remoteFolderStates: BTreeMap::new(),
        pendingFolderOffers: BTreeMap::new(),
        protectedFiles: BTreeSet::new(),
        observed: deviceIDSet::default(),
        helloMessages: BTreeMap::new(),
        progressEmitter: true,
        promotedConnID: String::new(),
        promotionTimer: 0,
        keyGen: 0,
        indexHandlers: BTreeMap::new(),
        globalRequestLimiter: 4,
        mut_count: 0,
        sdb: Arc::new(RwLock::new(sdb)),
        evLogger: Vec::new(),
        fatalChan: None,
    }
}

fn open_runtime_db(db_root: &Path, db_memory_cap_mb: usize) -> Result<db::WalFreeDb, String> {
    fs::create_dir_all(db_root).map_err(|err| {
        format!(
            "failed to create runtime db root {}: {err}",
            db_root.display()
        )
    })?;
    db::WalFreeDb::open_runtime(db_root, db_memory_cap_mb)
        .map_err(|err| format!("failed to open runtime db at {}: {err}", db_root.display()))
}

fn runtime_db_root() -> PathBuf {
    if let Ok(path) = std::env::var("SYNCTHING_RS_DB_ROOT") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut root = std::env::temp_dir();
    root.push(format!("syncthing-rs-db-{}-{suffix}", std::process::id()));
    root
}

fn runtime_db_memory_cap_mb() -> usize {
    const DEFAULT_MB: usize = 50;
    std::env::var("SYNCTHING_RS_MEMORY_MAX_MB")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_MB)
}

impl Model for model {
    fn State(&self, folder: &str) -> String {
        <model>::State(self, folder)
    }
}

impl model {
    pub(crate) fn String(&self) -> String {
        format!("model:{}", self.id)
    }

    pub(crate) fn Closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn AddConnection(&mut self, device: &str, stats: ConnectionStats) {
        self.connections.insert(device.to_string(), stats);
    }

    pub(crate) fn ConnectedTo(&self, device: &str) -> bool {
        self.connections
            .get(device)
            .map(|s| s.Connected)
            .unwrap_or(false)
    }

    pub(crate) fn ConnectionStats(&self) -> BTreeMap<String, Value> {
        let mut out = BTreeMap::new();
        let mut connections = BTreeMap::new();
        for (device, stats) in &self.connections {
            if device == &self.id {
                continue;
            }
            connections.insert(
                device.clone(),
                serde_json::json!({
                    "address": stats.Address,
                    "type": stats.Type,
                    "crypto": stats.Crypto,
                    "isLocal": stats.IsLocal,
                    "connected": stats.Connected,
                    "paused": stats.Paused,
                    "primary": stats.Primary,
                    "secondary": stats.Secondary,
                    "clientVersion": stats.ClientVersion,
                    "statistics": stats.protocol_Statistics,
                }),
            );
        }
        out.insert(
            "connections".to_string(),
            Value::Object(connections.into_iter().collect()),
        );
        let in_total = self
            .connections
            .values()
            .filter_map(|stats| stats.protocol_Statistics.get("inBytesTotal"))
            .copied()
            .sum::<i64>();
        let out_total = self
            .connections
            .values()
            .filter_map(|stats| stats.protocol_Statistics.get("outBytesTotal"))
            .copied()
            .sum::<i64>();
        out.insert(
            "total".to_string(),
            serde_json::json!({
                "inBytesTotal": in_total,
                "outBytesTotal": out_total,
            }),
        );
        out
    }

    pub(crate) fn ConnectionStatsForDevice(&self, device: &str) -> Option<ConnectionStats> {
        self.connections.get(device).cloned()
    }

    pub(crate) fn DeviceStatistics(
        &self,
    ) -> Result<BTreeMap<String, BTreeMap<String, i64>>, String> {
        Ok(self.deviceStatRefs.clone())
    }

    pub(crate) fn DeviceStatisticsForDevice(&self, device: &str) -> BTreeMap<String, i64> {
        self.deviceStatRefs.get(device).cloned().unwrap_or_default()
    }

    pub(crate) fn BringToFront(&mut self, folder_id: &str) -> Result<(), String> {
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .BringToFront();
        Ok(())
    }

    pub(crate) fn DelayScan(&mut self, folder_id: &str, next: Duration) {
        if let Some(folder) = self.folderRunners.get_mut(folder_id) {
            folder.DelayScan(next);
        }
    }

    pub(crate) fn DelayScanImmediate(&mut self, folder_id: &str) {
        self.DelayScan(folder_id, Duration::from_secs(0))
    }

    pub(crate) fn FolderErrors(
        &self,
        folder_id: &str,
    ) -> Result<Vec<folder_core::FileError>, String> {
        Ok(self
            .folderRunners
            .get(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .Errors())
    }

    pub(crate) fn FolderProgressBytesCompleted(&self, folder_id: &str) -> i64 {
        let completion = self
            .Completion("local", folder_id)
            .unwrap_or_else(|_| FolderCompletion::default());
        completion.GlobalBytes - completion.NeedBytes
    }

    pub(crate) fn FolderStatistics(&self, folder_id: &str) -> BTreeMap<String, Value> {
        let c = self
            .Completion("local", folder_id)
            .unwrap_or_else(|_| FolderCompletion::default());
        let mut out = BTreeMap::from([
            ("completionPct".to_string(), Value::from(c.CompletionPct)),
            ("needItems".to_string(), Value::from(c.NeedItems)),
            ("needBytes".to_string(), Value::from(c.NeedBytes)),
        ]);
        if let Some(runner) = self.folderRunners.get(folder_id) {
            let telemetry = runner.MemoryTelemetry();
            out.insert(
                "memoryEstimatedBytes".to_string(),
                Value::from(telemetry.estimated_bytes as u64),
            );
            out.insert(
                "memoryBudgetBytes".to_string(),
                Value::from(telemetry.budget_bytes as u64),
            );
            out.insert(
                "memorySoftLimitBytes".to_string(),
                Value::from(telemetry.soft_limit_bytes as u64),
            );
            out.insert(
                "memoryThrottleEvents".to_string(),
                Value::from(telemetry.throttle_events as u64),
            );
            out.insert(
                "memoryHardBlockEvents".to_string(),
                Value::from(telemetry.hard_block_events as u64),
            );
            out.insert(
                "memoryPullThrottled".to_string(),
                Value::from(telemetry.pull_throttled),
            );
            out.insert(
                "memoryPullHardBlocked".to_string(),
                Value::from(telemetry.pull_hard_blocked),
            );
            out.insert(
                "memoryRuntimeReservedBytes".to_string(),
                Value::from(telemetry.runtime_reserved_bytes as u64),
            );
            out.insert(
                "memoryRuntimeBudgetBytes".to_string(),
                Value::from(telemetry.runtime_budget_bytes as u64),
            );
            out.insert(
                "memoryRuntimeThrottleEvents".to_string(),
                Value::from(telemetry.runtime_throttle_events as u64),
            );
            out.insert(
                "memoryRuntimeHardBlockEvents".to_string(),
                Value::from(telemetry.runtime_hard_block_events as u64),
            );
            if let Some(report) = telemetry.runtime_cap_violation_report {
                out.insert(
                    "memoryCapViolationReport".to_string(),
                    serde_json::json!({
                        "folder": report.folder,
                        "subsystem": report.subsystem,
                        "policy": report.policy,
                        "requestedBytes": report.requested_bytes,
                        "reservedBytes": report.reserved_bytes,
                        "budgetBytes": report.budget_bytes,
                        "queueItems": report.queue_items,
                        "queryLimit": report.query_limit,
                        "runtimeBudgetKey": report.runtime_budget_key,
                    }),
                );
            }
            if let Some(last) = telemetry.last_cap_violation {
                out.insert("memoryLastCapViolation".to_string(), Value::from(last));
            }
        }
        out
    }

    pub(crate) fn Completion(
        &self,
        device: &str,
        folder_id: &str,
    ) -> Result<FolderCompletion, String> {
        let normalized_device = if device == self.id { "local" } else { device };
        if folder_id.is_empty() {
            let mut aggregate = FolderCompletion::default();
            for cfg in self.folderCfgs.values() {
                if cfg.paused {
                    continue;
                }
                if normalized_device != "local" && !cfg.shared_with(normalized_device) {
                    continue;
                }
                match self.folderCompletion(normalized_device, &cfg.id) {
                    Ok(comp) => aggregate.add(&comp),
                    Err(err) if err == ErrFolderPaused => continue,
                    Err(err) => return Err(err),
                }
            }
            return Ok(aggregate);
        }
        self.folderCompletion(normalized_device, folder_id)
    }

    pub(crate) fn folderCompletion(
        &self,
        device: &str,
        folder_id: &str,
    ) -> Result<FolderCompletion, String> {
        self.checkFolderRunningRLocked(folder_id)?;
        let state = self
            .remoteFolderStates
            .get(&format!("{device}:{folder_id}"))
            .or_else(|| self.remoteFolderStates.get(folder_id))
            .cloned()
            .unwrap_or_else(|| "idle".to_string());
        let db = self
            .sdb
            .read()
            .map_err(|_| "db lock poisoned".to_string())?;
        let global = db.count_global(folder_id)?;
        let need = db.count_need(folder_id, device)?;
        let sequence = db.get_device_sequence(folder_id, device)?;
        Ok(newFolderCompletion(global, need, sequence, &state))
    }

    pub(crate) fn CurrentFolderFile(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        self.CurrentFolderFileStatus(folder_id, path).ok().flatten()
    }

    pub(crate) fn CurrentFolderFileStatus(
        &self,
        folder_id: &str,
        path: &str,
    ) -> Result<Option<db::FileInfo>, String> {
        if !self.folderCfgs.contains_key(folder_id) {
            return Err(ErrFolderMissing.to_string());
        }
        self.sdb
            .read()
            .map_err(|_| "db lock poisoned".to_string())?
            .get_device_file(folder_id, "local", path)
            .map_err(|e| format!("current folder file lookup failed: {e}"))
    }

    pub(crate) fn CurrentGlobalFile(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        self.CurrentGlobalFileStatus(folder_id, path).ok().flatten()
    }

    pub(crate) fn CurrentGlobalFileStatus(
        &self,
        folder_id: &str,
        path: &str,
    ) -> Result<Option<db::FileInfo>, String> {
        if !self.folderCfgs.contains_key(folder_id) {
            return Err(ErrFolderMissing.to_string());
        }
        self.sdb
            .read()
            .map_err(|_| "db lock poisoned".to_string())?
            .get_global_file(folder_id, path)
            .map_err(|e| format!("current global file lookup failed: {e}"))
    }

    pub(crate) fn CurrentIgnores(&self, folder_id: &str) -> Vec<String> {
        self.folderIgnores
            .get(folder_id)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn LoadIgnores(&mut self, folder_id: &str, ignores: Vec<String>) {
        self.folderIgnores.insert(folder_id.to_string(), ignores);
    }

    pub(crate) fn SetIgnores(&mut self, folder_id: &str, ignores: Vec<String>) {
        self.setIgnores(folder_id, ignores);
    }

    pub(crate) fn setIgnores(&mut self, folder_id: &str, ignores: Vec<String>) {
        self.folderIgnores.insert(folder_id.to_string(), ignores);
        if let Some(f) = self.folderRunners.get_mut(folder_id) {
            f.ignoresUpdated();
        }
    }

    pub(crate) fn LocalFiles(&self, folder_id: &str) -> Vec<db::FileInfo> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| {
                db.all_local_files(folder_id, "local")
                    .ok()
                    .and_then(|stream| db::collect_stream(stream).ok())
            })
            .unwrap_or_default()
    }

    pub(crate) fn LocalFilesSequenced(
        &self,
        folder_id: &str,
        from: i64,
        limit: usize,
    ) -> Vec<db::FileInfo> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| {
                db.all_local_files_by_sequence(folder_id, "local", from, limit)
                    .ok()
                    .and_then(|stream| db::collect_stream(stream).ok())
            })
            .unwrap_or_default()
    }

    pub(crate) fn LocalChangedFolderFiles(&self, folder_id: &str) -> Vec<String> {
        self.LocalFiles(folder_id)
            .into_iter()
            .filter(|f| !f.deleted)
            .map(|f| f.path)
            .collect()
    }

    pub(crate) fn AllGlobalFiles(&self, folder_id: &str) -> Vec<db::FileMetadata> {
        self.AllGlobalFilesStatus(folder_id).unwrap_or_default()
    }

    pub(crate) fn AllGlobalFilesStatus(
        &self,
        folder_id: &str,
    ) -> Result<Vec<db::FileMetadata>, String> {
        if !self.folderCfgs.contains_key(folder_id) {
            return Err(ErrFolderMissing.to_string());
        }
        self.sdb
            .read()
            .map_err(|_| "db lock poisoned".to_string())?
            .all_global_files(folder_id)
            .and_then(db::collect_stream)
            .map_err(|e| format!("all global files query failed: {e}"))
    }

    pub(crate) fn AllForBlocksHash(&self, folder_id: &str, hash: &[u8]) -> Vec<db::FileMetadata> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| {
                db.all_local_files_with_blocks_hash(folder_id, hash)
                    .ok()
                    .and_then(|stream| db::collect_stream(stream).ok())
            })
            .unwrap_or_default()
    }

    pub(crate) fn Availability(&self, folder_id: &str, path: &str) -> Vec<Availability> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| db.get_global_availability(folder_id, path).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|id| Availability {
                ID: id,
                FromTemporary: false,
            })
            .collect()
    }

    pub(crate) fn GlobalSize(&self, folder_id: &str) -> (usize, i64) {
        let files = self.AllGlobalFiles(folder_id);
        let count = files.len();
        let bytes = files.iter().map(|f| f.size).sum();
        (count, bytes)
    }

    pub(crate) fn LocalSize(&self, folder_id: &str) -> (usize, i64) {
        let files = self.LocalFiles(folder_id);
        let count = files.len();
        let bytes = files.iter().map(|f| f.size).sum();
        (count, bytes)
    }

    pub(crate) fn NeedFolderFiles(&self, folder_id: &str) -> Vec<db::FileInfo> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| {
                db.all_needed_global_files(folder_id, "local", db::PullOrder::Alphabetic, 10_000, 0)
                    .ok()
                    .and_then(|stream| db::collect_stream(stream).ok())
            })
            .unwrap_or_default()
    }

    pub(crate) fn NeedSize(&self, folder_id: &str) -> (usize, i64) {
        let files = self.NeedFolderFiles(folder_id);
        (files.len(), files.iter().map(|f| f.size).sum())
    }

    pub(crate) fn ReceiveOnlySize(&self, folder_id: &str) -> (usize, i64) {
        let mut files = self.LocalFiles(folder_id);
        files.retain(|f| f.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY != 0);
        (files.len(), files.iter().map(|f| f.size).sum())
    }

    pub(crate) fn RemoteNeedFolderFiles(
        &self,
        folder_id: &str,
        _device: &str,
    ) -> Vec<db::FileInfo> {
        self.NeedFolderFiles(folder_id)
    }

    pub(crate) fn RemoteSequences(&self, folder_id: &str) -> BTreeMap<String, i64> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| db.remote_sequences(folder_id).ok())
            .unwrap_or_default()
    }

    pub(crate) fn RequestGlobal(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        self.CurrentGlobalFile(folder_id, path)
    }

    pub(crate) fn Request(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        self.CurrentFolderFile(folder_id, path)
    }

    pub(crate) fn RequestData(
        &self,
        folder_id: &str,
        path: &str,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, String> {
        let cfg = self
            .folderCfgs
            .get(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?;
        let abs = safe_join_folder_relative(Path::new(&cfg.path), path)?;
        readOffsetIntoBuf(&abs, offset, size)
    }

    pub(crate) fn RequestGlobalData(
        &self,
        folder_id: &str,
        path: &str,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, String> {
        self.RequestData(folder_id, path, offset, size)
    }

    pub(crate) fn ApplyBepMessage(
        &mut self,
        device: &str,
        message: &BepMessage,
    ) -> Result<Option<BepMessage>, String> {
        let device = device.trim();
        if device.is_empty() {
            return Err("device id is required".to_string());
        }
        match message {
            BepMessage::Hello {
                device_name: _,
                client_name,
            } => {
                self.deviceWasSeen(device);
                self.OnHello(device, client_name);
                self.connections
                    .entry(device.to_string())
                    .or_insert_with(|| ConnectionStats {
                        Connected: true,
                        ClientVersion: client_name.clone(),
                        ..ConnectionStats::default()
                    });
                Ok(None)
            }
            BepMessage::ClusterConfig { folders } => {
                for folder_id in folders {
                    let folder_id = folder_id.trim();
                    if folder_id.is_empty() {
                        continue;
                    }
                    self.remoteFolderStates
                        .insert(folder_id.to_string(), "cluster_configured".to_string());
                    if !self.folderCfgs.contains_key(folder_id) {
                        self.pendingFolderOffers
                            .entry(folder_id.to_string())
                            .or_default()
                            .insert(device.to_string());
                    }
                }
                Ok(None)
            }
            BepMessage::Index { folder, files } => {
                let converted = files
                    .iter()
                    .map(|file| fileInfoFromIndexEntry(folder, file))
                    .collect::<Vec<_>>();
                self.Index(folder, &converted)?;
                Ok(None)
            }
            BepMessage::IndexUpdate { folder, files } => {
                let converted = files
                    .iter()
                    .map(|file| fileInfoFromIndexEntry(folder, file))
                    .collect::<Vec<_>>();
                self.IndexUpdate(folder, &converted)?;
                Ok(None)
            }
            BepMessage::Request {
                id,
                folder,
                name,
                offset,
                size,
                hash: _,
            } => match self.RequestData(folder, name, *offset, *size as usize) {
                Ok(data) => Ok(Some(BepMessage::Response {
                    id: *id,
                    code: 0,
                    data_len: data.len() as u32,
                    data,
                })),
                Err(_) => Ok(Some(BepMessage::Response {
                    id: *id,
                    code: 1,
                    data_len: 0,
                    data: Vec::new(),
                })),
            },
            BepMessage::Response {
                id: _,
                code,
                data_len,
                data,
            } => {
                let stats = self.deviceStatRefs.entry(device.to_string()).or_default();
                *stats.entry("responses".to_string()).or_insert(0) += 1;
                let observed_len = (*data_len as usize).max(data.len()) as i64;
                *stats.entry("response_bytes".to_string()).or_insert(0) += observed_len;
                *stats.entry("response_errors".to_string()).or_insert(0) +=
                    if *code == 0 { 0 } else { 1 };
                if *data_len > 0 && (*data_len as usize) != data.len() {
                    *stats
                        .entry("response_len_mismatch".to_string())
                        .or_insert(0) += 1;
                }
                Ok(None)
            }
            BepMessage::DownloadProgress { folder, updates } => {
                let entries = updates
                    .iter()
                    .map(|update| format!("{folder}:{}:{}", update.name, update.version))
                    .collect::<Vec<_>>();
                self.deviceDownloads.insert(device.to_string(), entries);
                Ok(None)
            }
            BepMessage::Ping { timestamp_ms } => {
                let stats = self.deviceStatRefs.entry(device.to_string()).or_default();
                stats.insert("last_ping_timestamp_ms".to_string(), *timestamp_ms as i64);
                Ok(None)
            }
            BepMessage::Close { reason: _ } => {
                self.deviceDidCloseRLocked(device);
                Ok(None)
            }
        }
    }

    pub(crate) fn RunBepExchange(
        &mut self,
        device: &str,
        messages: &[BepMessage],
    ) -> Result<BepExchangeResult, String> {
        let transitions = run_message_exchange(messages)?
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();

        let mut outbound_messages = Vec::new();
        for message in messages {
            if let Some(outbound) = self.ApplyBepMessage(device, message)? {
                outbound_messages.push(outbound);
            }
        }

        Ok(BepExchangeResult {
            transitions,
            outbound_messages,
        })
    }

    pub(crate) fn State(&self, folder_id: &str) -> String {
        if self.closed {
            return "stopped".to_string();
        }
        if !self.foldersRunning.get(folder_id).copied().unwrap_or(false) {
            return "idle".to_string();
        }
        "running".to_string()
    }

    pub(crate) fn Sequence(&self, folder_id: &str) -> i64 {
        self.sdb
            .read()
            .ok()
            .and_then(|db| db.get_device_sequence(folder_id, "local").ok())
            .unwrap_or(0)
    }

    pub(crate) fn ScanFolder(&mut self, folder_id: &str) -> Result<(), String> {
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .Scan(&[])?;
        Ok(())
    }

    pub(crate) fn ScanFolderSubdirs(
        &mut self,
        folder_id: &str,
        subs: &[String],
    ) -> Result<(), String> {
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .Scan(subs)?;
        Ok(())
    }

    pub(crate) fn ScheduleForceRescan(
        &mut self,
        folder_id: &str,
        subs: &[String],
    ) -> Result<(), String> {
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .ScheduleForceRescan(subs);
        Ok(())
    }

    pub(crate) fn ScanFolders(&mut self, ids: &[String]) {
        for id in ids {
            let _ = self.ScanFolder(id);
        }
    }

    pub(crate) fn Override(&mut self, folder_id: &str) -> Result<(), String> {
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .Override();
        Ok(())
    }

    pub(crate) fn Revert(&mut self, folder_id: &str) -> Result<(), String> {
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .Revert();
        Ok(())
    }

    pub(crate) fn ResetFolder(&mut self, folder_id: &str) {
        self.cleanPending(folder_id);
        self.folderIgnores.remove(folder_id);
    }

    pub(crate) fn RestoreFolderVersions(&mut self, folder_id: &str) -> bool {
        self.folderVersioners.contains_key(folder_id)
    }

    pub(crate) fn GetFolderVersions(&self, folder_id: &str) -> Vec<String> {
        self.folderVersioners
            .get(folder_id)
            .map(|v| vec![v.clone()])
            .unwrap_or_default()
    }

    pub(crate) fn PendingDevices(&self) -> Vec<String> {
        self.connections
            .iter()
            .filter_map(|(id, st)| (!st.Connected).then_some(id.clone()))
            .collect()
    }

    pub(crate) fn PendingFolders(&self) -> Vec<String> {
        self.pendingFolderOffers.keys().cloned().collect()
    }

    pub(crate) fn PendingFolderOffers(&self) -> BTreeMap<String, Vec<String>> {
        let mut out = BTreeMap::new();
        for (folder, devices) in &self.pendingFolderOffers {
            let mut listed = devices
                .iter()
                .map(|device| device.trim().to_string())
                .filter(|device| !device.is_empty())
                .collect::<Vec<_>>();
            listed.sort();
            listed.dedup();
            if !listed.is_empty() {
                out.insert(folder.clone(), listed);
            }
        }
        out
    }

    pub(crate) fn DismissPendingDevice(&mut self, device: &str) -> Result<(), String> {
        if device.trim().is_empty() {
            return Err("device id is required".to_string());
        }
        self.connections.remove(device);
        Ok(())
    }

    pub(crate) fn DismissPendingFolder(
        &mut self,
        device: &str,
        folder: &str,
    ) -> Result<(), String> {
        if device.trim().is_empty() {
            return Err("device id is required".to_string());
        }
        if folder.trim().is_empty() {
            return Err("folder id is required".to_string());
        }
        if self.folderCfgs.contains_key(folder)
            || self
                .foldersRunning
                .get(folder)
                .copied()
                .unwrap_or(false)
        {
            // Dismissing a configured/running folder is a no-op for compatibility with
            // API callers that treat dismiss as idempotent and only use pending offers.
            return Ok(());
        }
        if let Some(devices) = self.pendingFolderOffers.get_mut(folder) {
            devices.remove(device);
            if devices.is_empty() {
                self.pendingFolderOffers.remove(folder);
            }
        }
        Ok(())
    }

    pub(crate) fn UsageReportingStats(&self) -> BTreeMap<String, Value> {
        BTreeMap::from([
            ("folders".to_string(), Value::from(self.folderCfgs.len())),
            (
                "connections".to_string(),
                Value::from(self.connections.len()),
            ),
            (
                "observedDevices".to_string(),
                Value::from(self.observed.AsSlice().len()),
            ),
        ])
    }

    pub(crate) fn GlobalDirectoryTree(&self, folder_id: &str) -> TreeEntry {
        let mut root = TreeEntry {
            Name: folder_id.to_string(),
            Type: "dir".to_string(),
            ..Default::default()
        };
        for f in self.AllGlobalFiles(folder_id) {
            root.Children.push(TreeEntry {
                Name: f.name,
                Type: if f.deleted {
                    "deleted".to_string()
                } else {
                    "file".to_string()
                },
                Size: f.size,
                ModTime: f.mod_nanos,
                Children: Vec::new(),
            });
        }
        root
    }

    pub(crate) fn WatchError(&self, folder_id: &str) -> Option<String> {
        self.folderRunners
            .get(folder_id)
            .and_then(|f| f.WatchError())
    }

    pub(crate) fn ClusterConfig(&self, device: &str) -> ClusterConfigReceivedEventData {
        ClusterConfigReceivedEventData {
            Device: device.to_string(),
        }
    }

    pub(crate) fn CommitConfiguration(&mut self, cfgs: &[FolderConfiguration]) {
        for cfg in cfgs {
            self.folderCfgs.insert(cfg.id.clone(), cfg.clone());
            self.cfg.insert(cfg.id.clone(), cfg.clone());
        }
    }

    pub(crate) fn VerifyConfiguration(&self, folder_id: &str) -> Result<(), String> {
        if !self.folderCfgs.contains_key(folder_id) {
            return Err(ErrFolderMissing.to_string());
        }
        Ok(())
    }

    pub(crate) fn OnHello(&mut self, device: &str, hello: &str) {
        self.helloMessages
            .insert(device.to_string(), hello.to_string());
    }

    pub(crate) fn DownloadProgress(&self, device: &str) -> Vec<String> {
        self.deviceDownloads
            .get(device)
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn Index(&mut self, folder_id: &str, files: &[db::FileInfo]) -> Result<(), String> {
        let mut db = self
            .sdb
            .write()
            .map_err(|_| "db lock poisoned".to_string())?;
        db.update(folder_id, "remote", files.to_vec())?;
        Ok(())
    }

    pub(crate) fn IndexUpdate(
        &mut self,
        folder_id: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        self.Index(folder_id, files)
    }

    pub(crate) fn addAndStartFolderLocked(&mut self, cfg: FolderConfiguration) {
        self.addAndStartFolderLockedWithIgnores(cfg, Vec::new());
    }

    pub(crate) fn addAndStartFolderLockedWithIgnores(
        &mut self,
        cfg: FolderConfiguration,
        ignores: Vec<String>,
    ) {
        let id = cfg.id.clone();
        self.pendingFolderOffers.remove(&id);
        self.folderCfgs.insert(id.clone(), cfg.clone());
        self.folderIgnores.insert(id.clone(), ignores);
        self.folderRunners
            .insert(id.clone(), folder_core::newFolder(cfg, self.sdb.clone()));
        self.foldersRunning.insert(id, true);
    }

    pub(crate) fn blockAvailability(&self, folder_id: &str, path: &str) -> Vec<Availability> {
        self.blockAvailabilityRLocked(folder_id, path)
    }

    pub(crate) fn blockAvailabilityFromTemporaryRLocked(
        &self,
        folder_id: &str,
        path: &str,
    ) -> Vec<Availability> {
        self.Availability(folder_id, path)
            .into_iter()
            .map(|mut a| {
                a.FromTemporary = true;
                a
            })
            .collect()
    }

    pub(crate) fn blockAvailabilityRLocked(
        &self,
        folder_id: &str,
        path: &str,
    ) -> Vec<Availability> {
        self.Availability(folder_id, path)
    }

    pub(crate) fn ccCheckEncryption(&self, folder_id: &str) -> Result<(), String> {
        if self.folderEncryptionFailures.get(folder_id).is_some() {
            return Err(errEncryptionInvConfigLocal.to_string());
        }
        Ok(())
    }

    pub(crate) fn ccHandleFolders(&mut self, cfgs: &[FolderConfiguration]) {
        self.CommitConfiguration(cfgs);
    }

    pub(crate) fn checkFolderRunningRLocked(&self, folder_id: &str) -> Result<(), String> {
        match self.folderCfgs.get(folder_id) {
            None => return Err(ErrFolderMissing.to_string()),
            Some(cfg) if cfg.paused => return Err(ErrFolderPaused.to_string()),
            Some(_) => {}
        }
        if !self.folderRunners.contains_key(folder_id)
            || !self.foldersRunning.get(folder_id).copied().unwrap_or(false)
        {
            return Err(ErrFolderNotRunning.to_string());
        }
        Ok(())
    }

    pub(crate) fn cleanPending(&mut self, folder_id: &str) {
        self.remoteFolderStates.remove(folder_id);
        self.pendingFolderOffers.remove(folder_id);
    }

    pub(crate) fn cleanupFolderLocked(&mut self, folder_id: &str) {
        self.folderRunners.remove(folder_id);
        self.foldersRunning.remove(folder_id);
    }

    pub(crate) fn closeAllConnectionsAndWait(&mut self) {
        self.connections.clear();
    }

    pub(crate) fn deviceDidCloseRLocked(&mut self, device: &str) {
        self.connections.remove(device);
    }

    pub(crate) fn deviceWasSeen(&mut self, device: &str) {
        self.observed.add(device);
    }

    pub(crate) fn ensureIndexHandler(&mut self, folder_id: &str) {
        self.indexHandlers
            .entry(folder_id.to_string())
            .or_insert_with(|| "default-index-handler".to_string());
    }

    pub(crate) fn getIndexHandlerRLocked(&self, folder_id: &str) -> Option<String> {
        self.indexHandlers.get(folder_id).cloned()
    }

    pub(crate) fn fatal(&mut self, err: &str) {
        self.fatalChan = Some(err.to_string());
        self.closed = true;
    }

    pub(crate) fn fileAvailability(&self, folder_id: &str, path: &str) -> Vec<Availability> {
        self.fileAvailabilityRLocked(folder_id, path)
    }

    pub(crate) fn fileAvailabilityRLocked(&self, folder_id: &str, path: &str) -> Vec<Availability> {
        self.Availability(folder_id, path)
    }

    pub(crate) fn generateClusterConfig(&self) -> BTreeMap<String, FolderConfiguration> {
        self.generateClusterConfigRLocked()
    }

    pub(crate) fn generateClusterConfigRLocked(&self) -> BTreeMap<String, FolderConfiguration> {
        self.folderCfgs.clone()
    }

    pub(crate) fn handleAutoAccepts(&mut self) {
        self.progressEmitter = true;
    }

    pub(crate) fn handleDeintroductions(&mut self, devices: &[String]) {
        for d in devices {
            self.connections.remove(d);
        }
    }

    pub(crate) fn handleIndex(
        &mut self,
        folder_id: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        self.IndexUpdate(folder_id, files)
    }

    pub(crate) fn handleIntroductions(&mut self, devices: &[String]) {
        for d in devices {
            self.observed.add(d);
        }
    }

    pub(crate) fn initFolders(&mut self) {
        for (id, cfg) in self.folderCfgs.clone() {
            self.folderRunners
                .entry(id.clone())
                .or_insert_with(|| folder_core::newFolder(cfg, self.sdb.clone()));
            self.foldersRunning.entry(id).or_insert(true);
        }
    }

    pub(crate) fn introduceDevice(&mut self, device: &str) {
        self.observed.add(device);
    }

    pub(crate) fn newFolder(&mut self, cfg: FolderConfiguration) {
        self.addAndStartFolderLocked(cfg);
    }

    pub(crate) fn numHashers(&self, folder_id: &str) -> i32 {
        self.folderCfgs
            .get(folder_id)
            .map(|c| c.hashers)
            .unwrap_or(0)
    }

    pub(crate) fn promoteConnections(&mut self, device: &str) {
        self.promotedConnID = device.to_string();
    }

    pub(crate) fn recheckFile(&self, folder_id: &str, path: &str) -> bool {
        self.CurrentGlobalFile(folder_id, path).is_some()
    }

    pub(crate) fn removeFolder(&mut self, folder_id: &str) {
        self.cleanupFolderLocked(folder_id);
    }

    pub(crate) fn requestConnectionForDevice(&self, device: &str) -> Option<ConnectionStats> {
        self.connections.get(device).cloned()
    }

    pub(crate) fn restartFolder(&mut self, folder_id: &str) -> Result<(), String> {
        let cfg = self
            .folderCfgs
            .get(folder_id)
            .cloned()
            .ok_or_else(|| ErrFolderMissing.to_string())?;
        self.cleanupFolderLocked(folder_id);
        self.addAndStartFolderLocked(cfg);
        Ok(())
    }

    pub(crate) fn scheduleConnectionPromotion(&mut self, device: &str) {
        self.promotedConnID = device.to_string();
        self.promotionTimer += 1;
    }

    pub(crate) fn sendClusterConfig(&self, device: &str) -> ClusterConfigReceivedEventData {
        self.ClusterConfig(device)
    }

    pub(crate) fn serve(&mut self) {
        self.started = true;
        let keys = self.folderRunners.keys().cloned().collect::<Vec<_>>();
        for folder in keys {
            if let Some(r) = self.folderRunners.get_mut(&folder) {
                r.Serve();
            }
        }
    }

    pub(crate) fn setConnRequestLimitersLocked(&mut self, device: &str, limit: usize) {
        self.connRequestLimiters.insert(device.to_string(), limit);
    }

    pub(crate) fn warnAboutOverwritingProtectedFiles(&self, paths: &[String]) -> Vec<String> {
        paths
            .iter()
            .filter(|p| self.protectedFiles.contains(*p))
            .cloned()
            .collect()
    }

    pub(crate) fn RequestGlobalFile(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        self.RequestGlobal(folder_id, path)
    }

    pub(crate) fn Model(&self) -> String {
        self.String()
    }

    pub(crate) fn Service(&self) -> &'static str {
        "model-service"
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct service {
    pub(crate) model: Arc<Mutex<model>>,
}

impl service {
    pub(crate) fn BringToFront(&self, folder_id: &str) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .BringToFront(folder_id)
    }

    pub(crate) fn DelayScan(&self, folder_id: &str) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .DelayScanImmediate(folder_id);
        Ok(())
    }

    pub(crate) fn DelayScanWithDuration(
        &self,
        folder_id: &str,
        next: Duration,
    ) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .DelayScan(folder_id, next);
        Ok(())
    }

    pub(crate) fn Errors(&self, folder_id: &str) -> Result<Vec<folder_core::FileError>, String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .FolderErrors(folder_id)
    }

    pub(crate) fn GetStatistics(&self, folder_id: &str) -> BTreeMap<String, Value> {
        self.model
            .lock()
            .map(|m| m.FolderStatistics(folder_id))
            .unwrap_or_default()
    }

    pub(crate) fn Jobs(&self, folder_id: &str) -> Result<BTreeMap<&'static str, bool>, String> {
        let guard = self
            .model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?;
        let runner = guard
            .folderRunners
            .get(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?;
        Ok(runner.Jobs())
    }

    pub(crate) fn Override(&self, folder_id: &str) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .Override(folder_id)
    }

    pub(crate) fn Revert(&self, folder_id: &str) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .Revert(folder_id)
    }

    pub(crate) fn Scan(&self, folder_id: &str, subs: &[String]) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .ScanFolderSubdirs(folder_id, subs)
    }

    pub(crate) fn ScheduleForceRescan(
        &self,
        folder_id: &str,
        subs: &[String],
    ) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .ScheduleForceRescan(folder_id, subs)
    }

    pub(crate) fn SchedulePull(&self, folder_id: &str) -> Result<(), String> {
        let mut m = self
            .model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?;
        m.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .SchedulePull();
        Ok(())
    }

    pub(crate) fn ScheduleScan(&self, folder_id: &str) -> Result<(), String> {
        self.model
            .lock()
            .map_err(|_| "service lock poisoned".to_string())?
            .ScanFolder(folder_id)
    }

    pub(crate) fn WatchError(&self, folder_id: &str) -> Option<String> {
        self.model.lock().ok().and_then(|m| m.WatchError(folder_id))
    }

    pub(crate) fn getState(&self, folder_id: &str) -> String {
        self.model
            .lock()
            .map(|m| m.State(folder_id))
            .unwrap_or_else(|_| "error".to_string())
    }

    pub(crate) fn Service(&self) -> &'static str {
        "service"
    }
}

pub(crate) fn NewModel() -> model {
    model::default()
}

pub(crate) fn NewModelWithRuntime(db_root: Option<PathBuf>, memory_cap_mb: Option<usize>) -> model {
    let root = db_root.unwrap_or_else(runtime_db_root);
    let cap_mb = memory_cap_mb.unwrap_or_else(runtime_db_memory_cap_mb);
    model_with_runtime(root, cap_mb)
}

pub(crate) fn newFolderCompletion(
    global: db::Counts,
    need: db::Counts,
    sequence: i64,
    state: &str,
) -> FolderCompletion {
    let mut comp = FolderCompletion {
        GlobalBytes: global.bytes,
        NeedBytes: need.bytes,
        GlobalItems: (global.files + global.directories + global.symlinks) as i32,
        NeedItems: (need.files + need.directories + need.symlinks) as i32,
        NeedDeletes: need.deleted as i32,
        Sequence: sequence,
        RemoteState: state.to_string(),
        ..Default::default()
    };
    comp.setCompletionPct();
    comp
}

pub(crate) fn newFolderConfiguration(id: &str, path: &str) -> FolderConfiguration {
    FolderConfiguration {
        id: id.to_string(),
        path: path.to_string(),
        ..Default::default()
    }
}

pub(crate) fn newRequestResponse() -> requestResponse {
    requestResponse::default()
}

pub(crate) fn newLimitedRequestResponse(limit: usize) -> requestResponse {
    let mut rr = requestResponse::default();
    rr.data = vec![0; limit.min(1024)];
    rr
}

pub(crate) fn newPager(keys: Vec<String>) -> pager {
    pager {
        get: keys,
        toSkip: 0,
    }
}

pub(crate) fn observedDeviceSet(devices: &[String]) -> deviceIDSet {
    let mut out = deviceIDSet::default();
    for d in devices {
        out.add(d);
    }
    out
}

pub(crate) fn findByName(entries: &[TreeEntry], name: &str) -> Option<TreeEntry> {
    entries.iter().find(|e| e.Name == name).cloned()
}

pub(crate) fn mapDevices(devices: &[String]) -> BTreeMap<String, clusterConfigDeviceInfo> {
    devices
        .iter()
        .map(|d| {
            (
                d.clone(),
                clusterConfigDeviceInfo {
                    local: d == "local-device",
                    remote: d != "local-device",
                },
            )
        })
        .collect()
}

pub(crate) fn mapFolders(cfgs: &[FolderConfiguration]) -> BTreeMap<String, FolderConfiguration> {
    cfgs.iter().map(|c| (c.id.clone(), c.clone())).collect()
}

pub(crate) fn encryptionTokenPath(folder_id: &str) -> PathBuf {
    PathBuf::from(format!("/tmp/syncthing-rs-{folder_id}.token"))
}

pub(crate) fn writeEncryptionToken(folder_id: &str, token: &str) -> Result<(), String> {
    let path = encryptionTokenPath(folder_id);
    fs::write(path, token).map_err(|e| format!("{errEncryptionTokenWrite}: {e}"))
}

pub(crate) fn readEncryptionToken(folder_id: &str) -> Result<String, String> {
    let path = encryptionTokenPath(folder_id);
    fs::read_to_string(path).map_err(|e| format!("{errEncryptionTokenRead}: {e}"))
}

pub(crate) fn readOffsetIntoBuf(path: &Path, offset: u64, len: usize) -> Result<Vec<u8>, String> {
    let mut f = fs::File::open(path).map_err(|e| format!("open: {e}"))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|e| format!("seek: {e}"))?;
    let mut buf = vec![0_u8; len];
    let mut read = 0usize;
    while read < len {
        let n = f.read(&mut buf[read..]).map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            break;
        }
        read += n;
    }
    buf.truncate(read);
    Ok(buf)
}

fn fileInfoFromIndexEntry(folder: &str, file: &IndexEntry) -> db::FileInfo {
    let sequence = clamp_u64_to_i64(file.sequence);
    db::FileInfo {
        folder: folder.to_string(),
        path: file.path.clone(),
        sequence,
        modified_ns: sequence,
        size: clamp_u64_to_i64(file.size),
        deleted: file.deleted,
        ignored: false,
        local_flags: 0,
        file_type: db::FileInfoType::File,
        block_hashes: file.block_hashes.clone(),
    }
}

fn clamp_u64_to_i64(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}

fn safe_join_folder_relative(root: &Path, relative: &str) -> Result<PathBuf, String> {
    let mut clean = PathBuf::new();
    for component in Path::new(relative).components() {
        match component {
            Component::Normal(seg) => clean.push(seg),
            Component::CurDir => {}
            _ => return Err(format!("invalid relative path: {relative}")),
        }
    }
    Ok(root.join(clean))
}

pub(crate) fn redactPathError(path: &str, err: &str) -> redactedError {
    let redacted = if path.is_empty() {
        err.to_string()
    } else {
        err.replace(path, "<redacted>")
    };
    redactedError {
        error: err.to_string(),
        redacted,
    }
}

pub(crate) fn without(items: &[String], remove: &[String]) -> Vec<String> {
    let rm: BTreeSet<_> = remove.iter().collect();
    items.iter().filter(|i| !rm.contains(i)).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_map_sets_percentage() {
        let mut c = FolderCompletion {
            NeedBytes: 20,
            GlobalBytes: 100,
            ..Default::default()
        };
        c.setCompletionPct();
        assert_eq!(c.CompletionPct, 80);
    }

    #[test]
    fn model_tracks_folder_lifecycle() {
        let mut m = NewModel();
        let cfg = newFolderConfiguration("default", "/tmp/default");
        m.newFolder(cfg);
        assert_eq!(m.State("default"), "running");
        let stats = m.FolderStatistics("default");
        assert!(stats.contains_key("memoryEstimatedBytes"));
        assert!(stats.contains_key("memoryBudgetBytes"));
        assert!(stats.contains_key("memoryRuntimeReservedBytes"));
        assert!(stats.contains_key("memoryRuntimeBudgetBytes"));
        m.removeFolder("default");
        assert_eq!(m.State("default"), "idle");
    }

    #[test]
    fn dismiss_pending_folder_is_noop_for_running_and_removes_offer_only() {
        let mut m = NewModel();
        let cfg = newFolderConfiguration("running", "/tmp/running");
        m.newFolder(cfg);

        m.DismissPendingFolder("peer-a", "running")
            .expect("dismiss running should be no-op");
        assert!(m.foldersRunning.contains_key("running"));

        m.pendingFolderOffers
            .entry("pending".to_string())
            .or_default()
            .insert("peer-a".to_string());
        m.pendingFolderOffers
            .entry("pending".to_string())
            .or_default()
            .insert("peer-b".to_string());
        m.DismissPendingFolder("peer-a", "pending")
            .expect("dismiss pending folder");
        assert_eq!(m.PendingFolders(), vec!["pending".to_string()]);
        assert_eq!(
            m.PendingFolderOffers()["pending"],
            vec!["peer-b".to_string()]
        );
        m.DismissPendingFolder("peer-b", "pending")
            .expect("dismiss last offer");
        assert!(m.PendingFolders().is_empty());
    }

    #[test]
    fn cluster_config_tracks_pending_folder_offers_for_unknown_folders() {
        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("known", "/tmp/known"));

        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::ClusterConfig {
                folders: vec![
                    "known".to_string(),
                    " ".to_string(),
                    "pending".to_string(),
                ],
            },
        )
        .expect("cluster config");

        assert!(m.PendingFolders().contains(&"pending".to_string()));
        assert_eq!(
            m.PendingFolderOffers()["pending"],
            vec!["peer-a".to_string()]
        );
        assert!(!m.PendingFolders().contains(&"known".to_string()));
    }

    #[test]
    fn folder_statistics_expose_structured_cap_violation_report() {
        let root =
            std::env::temp_dir().join(format!("syncthing-rs-cap-report-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.memory_policy = crate::config::MemoryPolicy::Fail;
        cfg.memory_max_mb = 1;
        let mut m = NewModel();
        m.newFolder(cfg);

        let large_hashes = (0..35_000)
            .map(|_| "0123456789abcdef0123456789abcdef".to_string())
            .collect::<Vec<_>>();
        let remote = db::FileInfo {
            folder: "default".to_string(),
            path: "hot.bin".to_string(),
            sequence: 1,
            modified_ns: 1,
            size: 1,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: large_hashes,
        };
        m.Index("default", &[remote]).expect("index remote");
        let pull_err = m
            .folderRunners
            .get_mut("default")
            .expect("runner")
            .pull()
            .expect_err("must fail under runtime cap");
        assert!(pull_err.contains("memory cap exceeded"));

        let stats = m.FolderStatistics("default");
        let report = stats
            .get("memoryCapViolationReport")
            .expect("cap report in stats");
        assert_eq!(report["folder"], "default");
        assert_eq!(report["policy"], "fail");
        assert_eq!(report["subsystem"], "pull-runtime-budget");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_model_with_runtime_uses_explicit_root_and_memory_cap() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-runtime-config-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let m = NewModelWithRuntime(Some(root.clone()), Some(7));
        let db = m.sdb.read().expect("lock db");
        assert_eq!(db.runtime_root(), &root);
        assert_eq!(db.memory_budget_bytes(), 7 * 1024 * 1024);

        drop(db);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn new_model_with_runtime_fails_when_root_is_not_directory() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-runtime-invalid-root-{}-{nanos}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_file(&root);
        fs::write(&root, b"not-a-directory").expect("create blocking file");

        let panic_result = std::panic::catch_unwind(|| {
            let _ = NewModelWithRuntime(Some(root.clone()), Some(7));
        });
        assert!(panic_result.is_err(), "invalid runtime root must fail fast");

        let _ = fs::remove_file(root);
    }

    #[test]
    fn file_info_from_index_entry_uses_sequence_for_modified_time() {
        let entry = IndexEntry {
            path: "a.txt".to_string(),
            sequence: 42,
            deleted: false,
            size: 7,
            block_hashes: vec!["h1".to_string()],
        };
        let info = fileInfoFromIndexEntry("default", &entry);
        assert_eq!(info.sequence, 42);
        assert_eq!(info.modified_ns, 42);
        assert_eq!(info.size, 7);
    }

    #[test]
    fn file_info_from_index_entry_clamps_large_numbers() {
        let entry = IndexEntry {
            path: "large.bin".to_string(),
            sequence: u64::MAX,
            deleted: false,
            size: u64::MAX,
            block_hashes: Vec::new(),
        };
        let info = fileInfoFromIndexEntry("default", &entry);
        assert_eq!(info.sequence, i64::MAX);
        assert_eq!(info.modified_ns, i64::MAX);
        assert_eq!(info.size, i64::MAX);
    }

    #[test]
    fn token_round_trip() {
        writeEncryptionToken("x", "tok").expect("write token");
        let got = readEncryptionToken("x").expect("read token");
        assert_eq!(got, "tok");
    }

    #[test]
    fn request_data_reads_expected_slice() {
        let root =
            std::env::temp_dir().join(format!("syncthing-rs-request-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");
        fs::write(root.join("a.txt"), b"hello-world").expect("write");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));

        let got = m
            .RequestData("default", "a.txt", 6, 5)
            .expect("request data");
        assert_eq!(got, b"world");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn request_data_rejects_parent_escape() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-request-escape-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        let err = m
            .RequestData("default", "../etc/passwd", 0, 16)
            .expect_err("must reject");
        assert!(err.contains("invalid relative path"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn bep_exchange_applies_remote_index_and_request_flow() {
        let root =
            std::env::temp_dir().join(format!("syncthing-rs-bep-flow-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");
        fs::write(root.join("a.txt"), b"hello-world").expect("write");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        let exchange = crate::bep::default_exchange();

        let result = m.RunBepExchange("peer-a", &exchange).expect("run exchange");

        assert_eq!(
            result.transitions,
            vec![
                "dial",
                "hello",
                "cluster_config",
                "index",
                "index_update",
                "request",
                "response",
                "download_progress",
                "ping",
                "close",
            ]
        );
        assert_eq!(result.outbound_messages.len(), 1);
        assert_eq!(
            result.outbound_messages[0],
            BepMessage::Response {
                id: 1,
                code: 0,
                data_len: 11,
                data: b"hello-world".to_vec(),
            }
        );
        assert_eq!(m.DownloadProgress("peer-a"), vec!["default:a.txt:2"]);
        assert_eq!(m.RemoteSequences("default").get("remote").copied(), Some(2));
        assert_eq!(m.State("default"), "running");
        assert_eq!(m.PendingDevices(), Vec::<String>::new());
        assert_eq!(m.ConnectedTo("peer-a"), false);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_request_returns_error_response_for_invalid_path() {
        let mut m = NewModel();
        let response = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 7,
                    folder: "missing".to_string(),
                    name: "../etc/passwd".to_string(),
                    offset: 0,
                    size: 64,
                    hash: "h".to_string(),
                },
            )
            .expect("response generated");

        assert_eq!(
            response,
            Some(BepMessage::Response {
                id: 7,
                code: 1,
                data_len: 0,
                data: Vec::new(),
            })
        );
    }

    #[test]
    fn apply_bep_message_rejects_empty_device_id() {
        let mut m = NewModel();
        let err = m
            .ApplyBepMessage(
                "   ",
                &BepMessage::Ping {
                    timestamp_ms: 1,
                },
            )
            .expect_err("empty device id must be rejected");
        assert!(err.contains("device id is required"));
    }
}
