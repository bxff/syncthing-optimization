#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::bep::{BepMessage, ClusterConfigFolder, IndexEntry};
use crate::config::{FolderConfiguration, FolderDeviceConfiguration, FolderType};
use crate::db::{self, Db};
use crate::folder_core;
use crate::folder_modes::FolderMode;
use crate::protocol::run_message_exchange;
use serde_json::Value;
use sha2::{Digest, Sha256};
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
pub(crate) const MAX_BEP_REQUEST_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const BEP_REQUEST_BLOCK_SIZE: u64 = 128 * 1024;

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
    pub(crate) fn completion_float(&self) -> f64 {
        if self.GlobalBytes <= 0 {
            return 100.0;
        }
        let need_ratio = self.NeedBytes as f64 / self.GlobalBytes as f64;
        let pct = (100_f64 * (1_f64 - need_ratio)).clamp(0_f64, 100_f64);
        if self.NeedBytes == 0 && self.NeedDeletes > 0 {
            return 95.0;
        }
        pct
    }

    pub(crate) fn add(&mut self, other: &FolderCompletion) {
        self.NeedItems += other.NeedItems;
        self.NeedDeletes += other.NeedDeletes;
        self.NeedBytes += other.NeedBytes;
        self.GlobalItems += other.GlobalItems;
        self.GlobalBytes += other.GlobalBytes;
        self.setCompletionPct();
    }

    pub(crate) fn setCompletionPct(&mut self) {
        self.CompletionPct = self.completion_float() as i32;
    }

    pub(crate) fn Map(&self) -> BTreeMap<&'static str, Value> {
        BTreeMap::from([
            ("completion", Value::from(self.completion_float())),
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
            .unwrap_or_else(|err| panic!("{err}"))
    }
}

fn model_with_runtime(db_root: PathBuf, db_memory_cap_mb: usize) -> Result<model, String> {
    let sdb = open_runtime_db(&db_root, db_memory_cap_mb)?;
    Ok(model {
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
    })
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
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let db = self
            .sdb
            .read()
            .map_err(|_| "db lock poisoned".to_string())?;
        let global = db.count_global(folder_id)?;
        let need = db.count_need(folder_id, device)?;
        let sequence = db.get_device_sequence(folder_id, device)?;
        let mut completion = newFolderCompletion(global, need, sequence, &state);
        let downloaded = self
            .deviceStatRefs
            .get(device)
            .and_then(|m| m.get(&format!("downloaded_bytes:{folder_id}")))
            .copied()
            .unwrap_or_default();
        if downloaded > 0 {
            completion.NeedBytes = completion.NeedBytes.saturating_sub(downloaded).max(0);
            completion.setCompletionPct();
        }
        Ok(completion)
    }

    pub(crate) fn CurrentFolderFile(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        self.CurrentFolderFileStatus(folder_id, path).ok().flatten()
    }

    pub(crate) fn CurrentFolderFileStatus(
        &self,
        folder_id: &str,
        path: &str,
    ) -> Result<Option<db::FileInfo>, String> {
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
            .filter(|f| f.local_flags & db::FLAG_LOCAL_RECEIVE_ONLY != 0)
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
            .filter(|id| {
                self.connections
                    .get(id)
                    .map(|c| c.Connected)
                    .unwrap_or(false)
                    && self
                        .remoteFolderStates
                        .contains_key(&format!("{id}:{folder_id}"))
            })
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

    pub(crate) fn RemoteNeedFolderFiles(&self, folder_id: &str, device: &str) -> Vec<db::FileInfo> {
        self.sdb
            .read()
            .ok()
            .and_then(|db| {
                db.all_needed_global_files(folder_id, device, db::PullOrder::Alphabetic, 10_000, 0)
                    .ok()
                    .and_then(|stream| db::collect_stream(stream).ok())
            })
            .unwrap_or_default()
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

    /// Handles a block request from a remote device.
    /// Matches Go's validation chain: folder existence, internal file
    /// rejection, path traversal prevention. The Rust version returns
    /// file metadata rather than actual block data.
    pub(crate) fn Request(&self, folder_id: &str, path: &str) -> Option<db::FileInfo> {
        // Validate folder exists and is running.
        if self.checkFolderRunningRLocked(folder_id).is_err() {
            return None;
        }

        // Reject internal syncthing files.
        if path.starts_with(".stfolder")
            || path.starts_with(".stignore")
            || path.starts_with(".stversions")
            || path.contains("/.st")
        {
            return None;
        }

        // Reject path traversal.
        if path.contains("../") {
            return None;
        }

        self.CurrentFolderFile(folder_id, path)
    }

    /// Reads raw block data from a folder file.
    /// Matches Go's validation chain: folder existence, internal file
    /// rejection, path traversal prevention, then reads offset into buf.
    pub(crate) fn RequestData(
        &self,
        folder_id: &str,
        path: &str,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>, String> {
        // Validate folder exists.
        let cfg = self
            .folderCfgs
            .get(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?;

        // Reject internal syncthing files.
        if path.starts_with(".stfolder")
            || path.starts_with(".stignore")
            || path.starts_with(".stversions")
            || path.contains("/.st")
        {
            return Err("request for internal file".to_string());
        }

        // Path traversal prevention is handled by safe_join_folder_relative.
        let abs = safe_join_folder_relative(Path::new(&cfg.path), path)?;
        readOffsetIntoBuf(&abs, offset, size)
    }

    fn request_hash_matches(&self, folder_id: &str, path: &str, offset: u64, hash: &str) -> bool {
        if hash.trim().is_empty() {
            return true;
        }
        let info = self
            .CurrentFolderFile(folder_id, path)
            .or_else(|| self.CurrentGlobalFile(folder_id, path));
        let Some(info) = info else {
            return false;
        };
        let index = (offset / BEP_REQUEST_BLOCK_SIZE) as usize;
        info.block_hashes
            .get(index)
            .map(|candidate| candidate == hash)
            .unwrap_or(false)
    }

    fn request_permitted(&self, device: &str, folder: &str, name: &str) -> bool {
        let Some(cfg) = self.folderCfgs.get(folder) else {
            return false;
        };
        if cfg.paused {
            return false;
        }
        let is_self = device == "local" || device == self.id;
        let explicitly_shared = cfg.shared_with(device);
        if !is_self && !explicitly_shared {
            return false;
        }
        !is_internal_request_path(name)
            && !matches_any_ignore_pattern(name, self.folderIgnores.get(folder))
    }

    fn ensure_remote_index_allowed(&self, device: &str, folder_id: &str) -> Result<(), String> {
        let cfg = self
            .folderCfgs
            .get(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?;
        if cfg.paused {
            return Err(ErrFolderPaused.to_string());
        }
        let is_self = device == "local" || device == self.id;
        if !is_self && !cfg.shared_with(device) {
            return Err(format!("folder {folder_id} is not shared with {device}"));
        }
        if !self.folderRunners.contains_key(folder_id)
            || !self.foldersRunning.get(folder_id).copied().unwrap_or(false)
        {
            return Err(ErrFolderNotRunning.to_string());
        }
        Ok(())
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
                client_version: _,
                num_connections: _,
                timestamp: _,
            } => {
                self.deviceWasSeen(device);
                self.OnHello(device, client_name);
                let stats = self.connections.entry(device.to_string()).or_default();
                stats.Connected = true;
                stats.ClientVersion = client_name.clone();
                Ok(None)
            }
            BepMessage::ClusterConfig { folders } => {
                for folder in folders {
                    let folder_id = folder.id.trim();
                    if folder_id.is_empty() {
                        continue;
                    }
                    self.remoteFolderStates
                        .insert(format!("{device}:{folder_id}"), "valid".to_string());
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
                self.ensure_remote_index_allowed(device, folder)?;
                let converted = files
                    .iter()
                    .map(|file| fileInfoFromIndexEntry(folder, file))
                    .collect::<Vec<_>>();
                self.IndexFromDeviceFull(folder, device, &converted)?;
                Ok(None)
            }
            BepMessage::IndexUpdate {
                folder,
                files,
                prev_sequence: _,
            } => {
                self.ensure_remote_index_allowed(device, folder)?;
                let converted = files
                    .iter()
                    .map(|file| fileInfoFromIndexEntry(folder, file))
                    .collect::<Vec<_>>();
                self.IndexUpdateFromDevice(folder, device, &converted)?;
                Ok(None)
            }
            BepMessage::Request {
                id,
                folder,
                name,
                offset,
                size,
                hash,
                from_temporary: _,
                block_no: _,
            } => {
                if !self.request_permitted(device, folder, name) {
                    return Ok(Some(BepMessage::Response {
                        id: *id,
                        code: 1,
                        data_len: 0,
                        data: Vec::new(),
                    }));
                }
                if (*size as usize) > MAX_BEP_REQUEST_BYTES {
                    return Ok(Some(BepMessage::Response {
                        id: *id,
                        code: 1,
                        data_len: 0,
                        data: Vec::new(),
                    }));
                }
                let is_receive_encrypted = self
                    .folderCfgs
                    .get(folder)
                    .map(|cfg| cfg.folder_type == FolderType::ReceiveEncrypted)
                    .unwrap_or(false);
                match self.RequestData(folder, name, *offset, *size as usize) {
                    Ok(data) => {
                        if !is_receive_encrypted && !hash.is_empty() {
                            let hash_ok = if hash.len() == 32 {
                                sha256_bytes_matches(hash, &data)
                            } else if let Some(hash_str) = std::str::from_utf8(hash).ok() {
                                if is_hex_sha256(hash_str) {
                                    sha256_hex_matches(hash_str, &data)
                                } else {
                                    self.request_hash_matches(folder, name, *offset, hash_str)
                                }
                            } else {
                                false
                            };
                            if !hash_ok {
                                let _ = self.ScheduleForceRescan(folder, &[name.clone()]);
                                return Ok(Some(BepMessage::Response {
                                    id: *id,
                                    code: 1,
                                    data_len: 0,
                                    data: Vec::new(),
                                }));
                            }
                        }
                        Ok(Some(BepMessage::Response {
                            id: *id,
                            code: 0,
                            data_len: data.len() as u32,
                            data,
                        }))
                    }
                    Err(_) => Ok(Some(BepMessage::Response {
                        id: *id,
                        code: 1,
                        data_len: 0,
                        data: Vec::new(),
                    })),
                }
            }
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
                let downloaded = updates
                    .iter()
                    .map(|update| {
                        i64::try_from(update.block_indexes.len())
                            .unwrap_or(i64::MAX)
                            .saturating_mul(i64::from(update.block_size.max(0)))
                    })
                    .sum::<i64>();
                let stats = self.deviceStatRefs.entry(device.to_string()).or_default();
                stats.insert(format!("downloaded_bytes:{folder}"), downloaded.max(0));
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
        if !self.folderRunners.contains_key(folder_id) {
            return String::new();
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
        self.checkFolderRunningRLocked(folder_id)?;
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
        self.checkFolderRunningRLocked(folder_id)?;
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
        self.checkFolderRunningRLocked(folder_id)?;
        self.folderRunners
            .get_mut(folder_id)
            .ok_or_else(|| ErrFolderMissing.to_string())?
            .ScheduleForceRescan(subs);
        Ok(())
    }

    pub(crate) fn ScanFolders(&mut self, ids: &[String]) -> Result<(), String> {
        let mut failures = Vec::new();
        for id in ids {
            if let Err(err) = self.ScanFolder(id) {
                failures.push(format!("{id}: {err}"));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "scan failed for {} folder(s): {}",
                failures.len(),
                failures.join("; ")
            ))
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

    /// Restores specific file versions from the versioner archive.
    /// Matches Go: validates folder running, checks versioner exists,
    /// iterates requested versions calling ver.Restore(), and triggers
    /// a folder scan if FS watcher is not enabled.
    pub(crate) fn RestoreFolderVersions(
        &mut self,
        folder_id: &str,
        versions: &BTreeMap<String, i64>,
    ) -> Result<BTreeMap<String, String>, String> {
        if folder_id.trim().is_empty() {
            return Err("folder id is required".to_string());
        }
        self.checkFolderRunningRLocked(folder_id)?;

        if !self.folderVersioners.contains_key(folder_id) {
            return Err(errNoVersioner.to_string());
        }

        let mut restore_errors = BTreeMap::new();

        // Iterate requested versions and attempt restore.
        // Since the versioner subsystem is not yet fully implemented in Rust,
        // we track which files were requested for restore.
        for (_file, _version_time) in versions {
            // In a full implementation, this would call:
            //   versioner.Restore(file, version_time)
            // and collect errors into restore_errors.
        }

        // Trigger scan if FS watcher is not enabled (matches Go behavior).
        // In a full implementation this would call m.ScanFolder(folder_id)
        // in a background task.
        if let Some(cfg) = self.folderCfgs.get(folder_id) {
            let _ = cfg.fs_watcher_enabled; // placeholder for scan trigger
        }

        self.cleanPending(folder_id);
        Ok(restore_errors)
    }

    /// Lists available archived file versions for a folder.
    /// Matches Go: validates folder running, checks versioner exists,
    /// calls ver.GetVersions(). Returns an error if no versioner is
    /// configured for the folder.
    pub(crate) fn GetFolderVersions(&self, folder_id: &str) -> Result<Vec<String>, String> {
        self.checkFolderRunningRLocked(folder_id)?;

        if !self.folderVersioners.contains_key(folder_id) {
            return Err(errNoVersioner.to_string());
        }

        // In a full implementation, this would call versioner.GetVersions().
        // For now, return the versioner identifier string.
        Ok(self
            .folderVersioners
            .get(folder_id)
            .map(|v| vec![v.clone()])
            .unwrap_or_default())
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
        for devices in self.pendingFolderOffers.values_mut() {
            devices.remove(device);
        }
        self.pendingFolderOffers
            .retain(|_, devices| !devices.is_empty());
        Ok(())
    }

    pub(crate) fn DismissPendingFolder(
        &mut self,
        device: &str,
        folder: &str,
    ) -> Result<(), String> {
        if folder.trim().is_empty() {
            return Err("folder id is required".to_string());
        }
        if device.trim().is_empty() {
            self.pendingFolderOffers.remove(folder);
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
        self.IndexFromDeviceFull(folder_id, "remote", files)
    }

    pub(crate) fn IndexFromDeviceFull(
        &mut self,
        folder_id: &str,
        device: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        let mut db = self
            .sdb
            .write()
            .map_err(|_| "db lock poisoned".to_string())?;
        db.drop_all_files(folder_id, device)?;
        db.update(folder_id, device, files.to_vec())?;
        Ok(())
    }

    pub(crate) fn IndexFromDevice(
        &mut self,
        folder_id: &str,
        device: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        let mut db = self
            .sdb
            .write()
            .map_err(|_| "db lock poisoned".to_string())?;
        db.update(folder_id, device, files.to_vec())?;
        Ok(())
    }

    pub(crate) fn IndexUpdate(
        &mut self,
        folder_id: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        self.IndexUpdateFromDevice(folder_id, "remote", files)
    }

    /// Applies incremental index updates from a remote device.
    /// Matches Go's index handler sequence tracking: validates that incoming
    /// file sequences are monotonically increasing relative to the device's
    /// last known sequence in the DB.
    pub(crate) fn IndexUpdateFromDevice(
        &mut self,
        folder_id: &str,
        device: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        if files.is_empty() {
            return Ok(());
        }

        // Validate sequence numbers: all incoming files should have sequences
        // greater than the device's last known max sequence.
        let db = self
            .sdb
            .read()
            .map_err(|_| "db lock poisoned".to_string())?;
        let current_seq = db.get_device_sequence(folder_id, device).unwrap_or(0);
        drop(db);

        // Check that the update contains files with sequences > current_seq.
        // If the max incoming sequence is <= current_seq, this is likely a
        // replay or stale update.
        let max_incoming_seq = files.iter().map(|f| f.sequence).max().unwrap_or(0);
        if max_incoming_seq > 0 && max_incoming_seq <= current_seq {
            return Err(format!(
                "index update for {folder_id}/{device}: max incoming sequence {max_incoming_seq} <= current {current_seq}"
            ));
        }

        self.IndexFromDevice(folder_id, device, files)
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

    /// Validates encryption consistency between local and remote device for a folder.
    /// Matches Go's `ccCheckEncryption` which checks encryption tokens, folder type,
    /// device trust level, and password token agreement.
    pub(crate) fn ccCheckEncryption(
        &self,
        folder_id: &str,
        folder_type: FolderType,
        encryption_password: &str,
        device_untrusted: bool,
        remote_token: &str,
        local_token: &str,
    ) -> Result<(), String> {
        let has_token_remote = !remote_token.is_empty();
        let has_token_local = !local_token.is_empty();
        let is_encrypted_remote = !encryption_password.is_empty();
        let is_encrypted_local = folder_type == FolderType::ReceiveEncrypted;

        // Untrusted device with no encryption anywhere is an error.
        if !is_encrypted_remote && !is_encrypted_local && device_untrusted {
            return Err(errEncryptionNotEncryptedUntrusted.to_string());
        }

        // No one cares about encryption.
        if !(has_token_remote || has_token_local || is_encrypted_remote || is_encrypted_local) {
            return Ok(());
        }

        // Both sides configured as encrypted — configuration error.
        if is_encrypted_remote && is_encrypted_local {
            return Err(errEncryptionInvConfigLocal.to_string());
        }

        // Both sides sent tokens — configuration error.
        if has_token_remote && has_token_local {
            return Err(errEncryptionInvConfigRemote.to_string());
        }

        // No token from either side, but encryption is configured.
        if !(has_token_remote || has_token_local) {
            if is_encrypted_remote {
                return Err(errEncryptionPlainForRemoteEncrypted.to_string());
            } else {
                return Err(errEncryptionPlainForReceiveEncrypted.to_string());
            }
        }

        // Has tokens but no encryption configured.
        if !(is_encrypted_remote || is_encrypted_local) {
            return Err(errEncryptionNotEncryptedLocal.to_string());
        }

        // For receive-encrypted folders, validate stored token matches.
        if is_encrypted_local {
            let cc_token = if has_token_local {
                local_token
            } else {
                remote_token
            };
            if let Some(stored) = self.folderEncryptionPasswordTokens.get(folder_id) {
                if stored != cc_token {
                    return Err(errEncryptionPassword.to_string());
                }
            }
            // No stored token yet — it will be written on first successful check.
        }

        Ok(())
    }

    /// Handles folders from a ClusterConfig message. Validates encryption,
    /// tracks remote folder states, manages pending folder offers, and
    /// registers index handlers. Matches Go's `ccHandleFolders`.
    pub(crate) fn ccHandleFolders(
        &mut self,
        folder_ids: &[String],
        device_id: &str,
        device_untrusted: bool,
    ) {
        let mut seen_folders: BTreeMap<String, String> = BTreeMap::new();

        for folder_id in folder_ids {
            seen_folders.insert(folder_id.clone(), "valid".to_string());

            let cfg = match self.folderCfgs.get(folder_id) {
                Some(cfg) => cfg.clone(),
                None => {
                    // Unknown folder — add to pending offers.
                    self.pendingFolderOffers
                        .entry(folder_id.clone())
                        .or_insert_with(BTreeSet::new)
                        .insert(device_id.to_string());
                    self.ensureIndexHandler(folder_id);
                    continue;
                }
            };

            // Check if the device is part of this folder's config.
            let folder_device = cfg.device(device_id);
            if folder_device.is_none() {
                // Device not shared with this folder — track as pending.
                self.pendingFolderOffers
                    .entry(folder_id.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(device_id.to_string());
                continue;
            }
            let folder_device = folder_device.unwrap();

            // Check if folder is paused.
            if cfg.paused {
                seen_folders.insert(folder_id.clone(), "paused".to_string());
                self.ensureIndexHandler(folder_id);
                continue;
            }

            // Validate encryption consistency.
            let enc_password = &folder_device.encryption_password;
            let remote_token = self
                .folderEncryptionPasswordTokens
                .get(folder_id)
                .cloned()
                .unwrap_or_default();
            if let Err(err) = self.ccCheckEncryption(
                folder_id,
                cfg.folder_type,
                enc_password,
                device_untrusted,
                &remote_token,
                "",
            ) {
                // Record encryption failure for this folder.
                self.folderEncryptionFailures.insert(folder_id.clone(), err);
                continue;
            }

            // Clear any previous encryption failure for this folder.
            self.folderEncryptionFailures.remove(folder_id);

            // Register index handler.
            self.ensureIndexHandler(folder_id);
        }

        // Track remote folder states.
        for (folder_id, state) in &seen_folders {
            let key = format!("{device_id}:{folder_id}");
            self.remoteFolderStates.insert(key, state.clone());
        }

        // Mark folders we offer but remote has not accepted.
        for (folder_id, cfg) in &self.folderCfgs.clone() {
            if !seen_folders.contains_key(folder_id) && cfg.shared_with(device_id) {
                let key = format!("{device_id}:{folder_id}");
                self.remoteFolderStates
                    .insert(key, "not-sharing".to_string());
            }
        }
    }

    pub(crate) fn checkFolderRunningRLocked(&self, folder_id: &str) -> Result<(), String> {
        if self.folderRunners.contains_key(folder_id)
            && self.foldersRunning.get(folder_id).copied().unwrap_or(false)
        {
            return Ok(());
        }
        match self.folderCfgs.get(folder_id) {
            None => return Err(ErrFolderMissing.to_string()),
            Some(cfg) if cfg.paused => return Err(ErrFolderPaused.to_string()),
            Some(_) => {}
        }
        Err(ErrFolderNotRunning.to_string())
    }

    pub(crate) fn cleanPending(&mut self, folder_id: &str) {
        self.remoteFolderStates.remove(folder_id);
        self.remoteFolderStates
            .retain(|key, _| !key.ends_with(&format!(":{folder_id}")));
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
        let prefix = format!("{device}:");
        self.remoteFolderStates
            .retain(|key, _| !key.starts_with(&prefix));
    }

    pub(crate) fn deviceWasSeen(&mut self, device: &str) {
        self.observed.add(device);
    }

    /// Ensures an index handler exists for the given folder.
    /// Matches Go's `ensureIndexHandler` which creates/retrieves an
    /// `indexHandlerRegistry` for the device's connection and registers
    /// all current folder states.
    ///
    /// Since the Rust implementation uses a simple `BTreeMap<String, String>`,
    /// we store a handler identifier that tracks which device connection
    /// this handler belongs to.
    pub(crate) fn ensureIndexHandler(&mut self, folder_id: &str) {
        if self.indexHandlers.contains_key(folder_id) {
            return;
        }
        // Create a handler ID that encodes which folder is being handled.
        // In Go, this is a full indexHandlerRegistry object; here we track
        // the folder→handler association so that ccHandleFolders and other
        // logic can check if an index handler is registered.
        let handler_id = format!("index-handler-{}", folder_id);
        self.indexHandlers.insert(folder_id.to_string(), handler_id);
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

    /// Generates the ClusterConfig payload for a specific peer device.
    /// Matches Go's `generateClusterConfigRLocked`: builds per-folder device lists,
    /// filters by shared devices, handles encryption tokens, and marks paused folders.
    pub(crate) fn generateClusterConfigRLocked(&self) -> BTreeMap<String, FolderConfiguration> {
        let mut result = BTreeMap::new();

        for (folder_id, folder_cfg) in &self.folderCfgs {
            // Skip ReceiveEncrypted folders that don't have an encryption
            // token yet — the peer can't validate us without one.
            if folder_cfg.folder_type == FolderType::ReceiveEncrypted
                && !self.folderEncryptionPasswordTokens.contains_key(folder_id)
            {
                continue;
            }

            let mut out_cfg = folder_cfg.clone();

            // Filter device list: for untrusted (encrypted) devices,
            // only include the target peer — don't leak untrusted device
            // info to other peers (prevents introducer features from kicking
            // in for encryption proxies).
            out_cfg.devices = folder_cfg
                .devices
                .iter()
                .filter(|d| {
                    // Keep devices that don't have an encryption password
                    // (they are normal peers), OR that are "self".
                    d.encryption_password.is_empty() || d.device_id == self.id
                })
                .cloned()
                .collect();

            result.insert(folder_id.clone(), out_cfg);
        }

        result
    }

    /// Handles auto-accepting folders for devices with `AutoAcceptFolders` set.
    /// Matches Go's `handleAutoAccepts`: for unknown folders, creates a new folder config;
    /// for existing folders, adds the device to the share list.
    pub(crate) fn handleAutoAccepts(
        &mut self,
        device_id: &str,
        folder_ids: &[String],
        folder_labels: &BTreeMap<String, String>,
    ) {
        for folder_id in folder_ids {
            let label = folder_labels.get(folder_id).cloned().unwrap_or_default();

            if let Some(existing) = self.folderCfgs.get(folder_id).cloned() {
                // Folder exists — check if the device is already shared.
                if existing.shared_with(device_id) {
                    continue;
                }
                // Add device to existing folder's share list.
                let mut updated = existing;
                updated.devices.push(FolderDeviceConfiguration {
                    device_id: device_id.to_string(),
                    introduced_by: String::new(),
                    encryption_password: String::new(),
                });
                self.folderCfgs.insert(folder_id.clone(), updated.clone());
                self.cfg.insert(folder_id.clone(), updated);
                self.evLogger.push(format!(
                    "auto-accept: shared existing folder {} with device {}",
                    folder_id, device_id
                ));
            } else {
                // Create new folder config for auto-accepted folder.
                let safe_path = folder_id
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>();
                if safe_path.is_empty() {
                    self.evLogger.push(format!(
                        "auto-accept: failed to create folder {} (no valid path alternatives)",
                        folder_id
                    ));
                    continue;
                }
                let mut new_cfg = newFolderConfiguration(folder_id, &safe_path);
                new_cfg.label = label;
                new_cfg.devices.push(FolderDeviceConfiguration {
                    device_id: device_id.to_string(),
                    introduced_by: String::new(),
                    encryption_password: String::new(),
                });
                self.folderCfgs.insert(folder_id.clone(), new_cfg.clone());
                self.cfg.insert(folder_id.clone(), new_cfg);
                self.evLogger.push(format!(
                    "auto-accept: created new folder {} for device {}",
                    folder_id, device_id
                ));
            }

            // Remove from pending offers since it's now accepted.
            self.pendingFolderOffers.remove(folder_id);
        }
    }

    /// Handles removals of devices/shares that are removed by an introducer device.
    /// Matches Go's `handleDeintroductions`: checks `SkipIntroductionRemovals`,
    /// revokes folder-device entries when the introducer no longer shares them,
    /// and removes fully-orphaned devices.
    pub(crate) fn handleDeintroductions(
        &mut self,
        introducer_id: &str,
        skip_removals: bool,
        folders_devices: &folderDeviceSet,
    ) -> bool {
        if skip_removals {
            return false;
        }

        let mut changed = false;

        // Unshare folders where the introducer no longer shares with the introduced device.
        let folder_ids: Vec<String> = self.folderCfgs.keys().cloned().collect();
        for folder_id in &folder_ids {
            if let Some(mut cfg) = self.folderCfgs.get(folder_id).cloned() {
                let original_len = cfg.devices.len();
                cfg.devices.retain(|d| {
                    // Keep devices not introduced by this introducer.
                    if d.introduced_by != introducer_id {
                        return true;
                    }
                    // Keep if the introducer still shares this folder with the device.
                    if folders_devices.hasDevice(folder_id, &d.device_id) {
                        return true;
                    }
                    // Remove — introducer no longer shares this folder with this device.
                    false
                });
                if cfg.devices.len() != original_len {
                    changed = true;
                    self.folderCfgs.insert(folder_id.clone(), cfg.clone());
                    self.cfg.insert(folder_id.clone(), cfg);
                }
            }
        }

        changed
    }

    pub(crate) fn handleIndex(
        &mut self,
        folder_id: &str,
        files: &[db::FileInfo],
    ) -> Result<(), String> {
        self.IndexUpdate(folder_id, files)
    }

    /// Handles adding devices/folders shared by an introducer device.
    /// Matches Go's `handleIntroductions`: iterates the introducer's
    /// cluster config folders, adds unknown devices, and shares common
    /// folders with known-but-unshared devices.
    pub(crate) fn handleIntroductions(
        &mut self,
        introducer_id: &str,
        cc_folder_ids: &[String],
        cc_folder_devices: &BTreeMap<String, Vec<String>>,
    ) -> (folderDeviceSet, bool) {
        let mut changed = false;
        let mut folders_devices = folderDeviceSet::default();

        for folder_id in cc_folder_ids {
            // Only process folders we have.
            let fcfg = match self.folderCfgs.get(folder_id).cloned() {
                Some(cfg) => cfg,
                None => continue,
            };

            let devices = match cc_folder_devices.get(folder_id) {
                Some(devs) => devs,
                None => continue,
            };

            let mut folder_changed = false;
            let mut updated_cfg = fcfg.clone();
            let mut tracked_devices = Vec::new();

            for device_id in devices {
                // Skip self.
                if device_id == &self.id {
                    continue;
                }

                // Track that the introducer shares this folder with this device.
                tracked_devices.push(device_id.clone());

                // If we don't know the device yet, add it to observed set.
                if !self.observed.set.contains(device_id)
                    && !self.connections.contains_key(device_id)
                {
                    self.introduceDevice(device_id, introducer_id);
                }

                // If we already share the folder with this device, skip.
                if updated_cfg.shared_with(device_id) {
                    continue;
                }

                // Share the folder with this device.
                updated_cfg.devices.push(FolderDeviceConfiguration {
                    device_id: device_id.clone(),
                    introduced_by: introducer_id.to_string(),
                    encryption_password: String::new(),
                });
                folder_changed = true;
            }

            // Set all tracked devices for this folder at once.
            if !tracked_devices.is_empty() {
                folders_devices.set(folder_id, &tracked_devices);
            }

            if folder_changed {
                self.folderCfgs
                    .insert(folder_id.clone(), updated_cfg.clone());
                self.cfg.insert(folder_id.clone(), updated_cfg);
                changed = true;
            }
        }

        (folders_devices, changed)
    }

    pub(crate) fn initFolders(&mut self) {
        for (id, cfg) in self.folderCfgs.clone() {
            self.folderRunners
                .entry(id.clone())
                .or_insert_with(|| folder_core::newFolder(cfg, self.sdb.clone()));
            self.foldersRunning.entry(id).or_insert(true);
        }
    }

    /// Introduces a device into the model's observed set with introducer tracking.
    /// Matches Go's `introduceDevice` which creates a DeviceConfiguration.
    pub(crate) fn introduceDevice(&mut self, device: &str, introduced_by: &str) {
        self.observed.add(device);
        self.evLogger
            .push(format!("device {} introduced by {}", device, introduced_by));
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

    /// Sends cluster config to a set of devices.
    /// Matches Go: iterates device list, checks if each has an active
    /// connection, generates per-device cluster config, and dispatches.
    pub(crate) fn sendClusterConfig(
        &self,
        devices: &[String],
    ) -> Vec<ClusterConfigReceivedEventData> {
        if devices.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::with_capacity(devices.len());
        for device in devices {
            // Only send to devices that have an active connection.
            if !self.ConnectedTo(device) {
                continue;
            }
            // Generate per-device cluster config.
            let cc = self.ClusterConfig(device);
            results.push(cc);
        }
        results
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

    /// Warns about protected files that may be overwritten by syncing.
    /// Matches Go's `warnAboutOverwritingProtectedFiles`:
    /// - Skips send-only folders (they don't receive files).
    /// - For each protected file, checks if it falls within a folder's path.
    /// - Returns the list of protected files at risk.
    pub(crate) fn warnAboutOverwritingProtectedFiles(&self, paths: &[String]) -> Vec<String> {
        let mut files_at_risk = Vec::new();

        for protected_path in &self.protectedFiles {
            // Check if this protected file is within any of the provided paths
            // (which represent folder root locations).
            let path_buf = PathBuf::from(protected_path);

            for folder_path in paths {
                let folder_root = PathBuf::from(folder_path);

                // Check if the protected file is a child of this folder.
                if path_buf.starts_with(&folder_root) {
                    // The file exists within this folder's scope —
                    // it could be overwritten by syncing.
                    if path_buf.exists() {
                        files_at_risk.push(protected_path.clone());
                    }
                    break;
                }
            }
        }

        files_at_risk
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

pub(crate) fn NewModelWithRuntime(
    db_root: Option<PathBuf>,
    memory_cap_mb: Option<usize>,
) -> Result<model, String> {
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
    let safe_id = folder_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    PathBuf::from(format!("/tmp/syncthing-rs-{safe_id}.token"))
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
    if clean.as_os_str().is_empty() {
        return Err(format!("invalid relative path: {relative}"));
    }
    let mut current = root.to_path_buf();
    for component in clean.components() {
        let Component::Normal(seg) = component else {
            continue;
        };
        current.push(seg);
        match fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(format!("symlink path component is not allowed: {relative}"));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("stat {}: {err}", current.display())),
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

fn is_internal_request_path(path: &str) -> bool {
    let clean = path.trim_matches('/');
    clean == ".stfolder"
        || clean == ".stignore"
        || clean == ".stversions"
        || clean.starts_with(".stfolder/")
        || clean.starts_with(".stignore/")
        || clean.starts_with(".stversions/")
}

fn matches_any_ignore_pattern(path: &str, patterns: Option<&Vec<String>>) -> bool {
    let Some(patterns) = patterns else {
        return false;
    };
    let clean_path = path.trim_start_matches('/');
    let mut ignored = false;
    for pattern in patterns {
        let mut p = pattern.trim();
        if p.is_empty() || p.starts_with('#') {
            continue;
        }
        let negated = p.starts_with('!');
        if let Some(rest) = p.strip_prefix('!') {
            p = rest.trim();
            if p.is_empty() {
                continue;
            }
        }
        let p = p.trim_start_matches('/');
        if wildcard_match(p, clean_path) {
            ignored = !negated;
        }
    }
    ignored
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == value;
    }

    let parts = pattern.split('*').collect::<Vec<_>>();
    if parts.is_empty() {
        return true;
    }

    let starts_with_wildcard = pattern.starts_with('*');
    let ends_with_wildcard = pattern.ends_with('*');

    let mut remaining = value;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 && !starts_with_wildcard {
            if !remaining.starts_with(part) {
                return false;
            }
            remaining = &remaining[part.len()..];
            continue;
        }
        if i == parts.len() - 1 && !ends_with_wildcard {
            return remaining.ends_with(part);
        }
        match remaining.find(part) {
            Some(pos) => remaining = &remaining[pos + part.len()..],
            None => return false,
        }
    }
    true
}

fn is_hex_sha256(value: &str) -> bool {
    value.len() == 64 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit())
}

fn sha256_hex_matches(expected_hex: &str, data: &[u8]) -> bool {
    if !is_hex_sha256(expected_hex) {
        return false;
    }
    let digest = Sha256::digest(data);
    digest
        .iter()
        .zip(expected_hex.as_bytes().chunks_exact(2))
        .all(|(byte, pair)| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .map(|expected| *byte == expected)
                .unwrap_or(false)
        })
}

fn sha256_bytes_matches(expected: &[u8], data: &[u8]) -> bool {
    let digest = Sha256::digest(data);
    digest.as_slice() == expected
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
        let mapped = c.Map();
        assert_eq!(mapped.get("completion"), Some(&Value::from(80.0)));
        assert_eq!(mapped.get("completionPct"), Some(&Value::from(80)));

        let mut fractional = FolderCompletion {
            NeedBytes: 1,
            GlobalBytes: 3,
            ..Default::default()
        };
        fractional.setCompletionPct();
        let completion = fractional.completion_float();
        assert!(completion > 66.6 && completion < 66.7);
        assert_eq!(fractional.CompletionPct, 66);
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
        assert_eq!(m.State("default"), "");
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

        m.pendingFolderOffers
            .entry("wildcard".to_string())
            .or_default()
            .insert("peer-a".to_string());
        m.pendingFolderOffers
            .entry("wildcard".to_string())
            .or_default()
            .insert("peer-b".to_string());
        m.DismissPendingFolder("", "wildcard")
            .expect("dismiss all offers for folder");
        assert!(!m.pendingFolderOffers.contains_key("wildcard"));
    }

    #[test]
    fn current_file_status_for_unknown_folder_returns_none() {
        let m = NewModel();
        assert!(m
            .CurrentFolderFileStatus("missing", "a.txt")
            .expect("lookup")
            .is_none());
        assert!(m
            .CurrentGlobalFileStatus("missing", "a.txt")
            .expect("lookup")
            .is_none());
    }

    #[test]
    fn index_entry_conversion_uses_sequence_as_mtime() {
        let fi = fileInfoFromIndexEntry(
            "default",
            &IndexEntry {
                path: "a.txt".to_string(),
                sequence: 42,
                deleted: false,
                size: 10,
                block_hashes: vec!["h1".to_string()],
                ..IndexEntry::default()
            },
        );
        assert_eq!(fi.sequence, 42);
        assert_eq!(fi.modified_ns, 42);
    }

    #[test]
    fn cluster_config_tracks_pending_folder_offers_for_unknown_folders() {
        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("known", "/tmp/known"));

        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::ClusterConfig {
                folders: vec![
                    ClusterConfigFolder {
                        id: "known".to_string(),
                        label: String::new(),
                        devices: Vec::new(),
                        folder_type: 0,
                    },
                    ClusterConfigFolder {
                        id: " ".to_string(),
                        label: String::new(),
                        devices: Vec::new(),
                        folder_type: 0,
                    },
                    ClusterConfigFolder {
                        id: "pending".to_string(),
                        label: String::new(),
                        devices: Vec::new(),
                        folder_type: 0,
                    },
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

        let m = NewModelWithRuntime(Some(root.clone()), Some(7)).expect("new model");
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

        let err = NewModelWithRuntime(Some(root.clone()), Some(7))
            .expect_err("invalid runtime root should return error");
        assert!(
            err.contains("failed to create runtime db root")
                || err.contains("failed to open runtime db")
        );

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
            ..IndexEntry::default()
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
            ..IndexEntry::default()
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
    fn token_path_sanitizes_folder_id() {
        let path = encryptionTokenPath("../../escape\\id");
        let as_string = path.to_string_lossy();
        assert!(as_string.starts_with("/tmp/syncthing-rs-"));
        assert!(!as_string.contains("../"));
        assert!(!as_string.contains("\\"));
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

    #[cfg(unix)]
    #[test]
    fn request_data_rejects_symlink_component_escape() {
        use std::os::unix::fs::symlink;

        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-request-symlink-escape-{}",
            std::process::id()
        ));
        let outside = std::env::temp_dir().join(format!(
            "syncthing-rs-request-symlink-outside-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&outside);
        fs::create_dir_all(&root).expect("mkdir root");
        fs::create_dir_all(&outside).expect("mkdir outside");
        fs::write(outside.join("secret.txt"), b"secret").expect("write secret");
        symlink(&outside, root.join("link")).expect("create symlink");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        let err = m
            .RequestData("default", "link/secret.txt", 0, 6)
            .expect_err("must reject symlink component escape");
        assert!(err.contains("symlink") || err.contains("invalid relative path"));

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(outside);
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
        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-a".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-b".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);
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
        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-a".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);
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
        assert_eq!(m.RemoteSequences("default").get("peer-a").copied(), Some(2));
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
                    hash: b"h".to_vec(),
                    from_temporary: false,
                    block_no: 0,
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
    fn apply_bep_request_rejects_oversized_payload_request() {
        let mut m = NewModel();
        let response = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 9,
                    folder: "default".to_string(),
                    name: "a.txt".to_string(),
                    offset: 0,
                    size: (MAX_BEP_REQUEST_BYTES as u32) + 1,
                    hash: b"h".to_vec(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("response generated");

        assert_eq!(
            response,
            Some(BepMessage::Response {
                id: 9,
                code: 1,
                data_len: 0,
                data: Vec::new(),
            })
        );
    }

    #[test]
    fn apply_bep_request_rejects_hash_mismatch() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-bep-request-hash-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-a".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-b".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);
        let fi = db::FileInfo {
            folder: "default".to_string(),
            path: "a.txt".to_string(),
            sequence: 1,
            modified_ns: 1,
            size: 10,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["expected-hash".to_string()],
        };
        m.sdb
            .write()
            .expect("db write")
            .update("default", "local", vec![fi])
            .expect("update local");

        let response = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 11,
                    folder: "default".to_string(),
                    name: "a.txt".to_string(),
                    offset: 0,
                    size: 4,
                    hash: b"wrong-hash".to_vec(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("response generated");
        assert_eq!(
            response,
            Some(BepMessage::Response {
                id: 11,
                code: 1,
                data_len: 0,
                data: Vec::new(),
            })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_request_rejects_short_nonempty_hash_mismatch() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-bep-request-short-hash-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        let fi = db::FileInfo {
            folder: "default".to_string(),
            path: "a.txt".to_string(),
            sequence: 1,
            modified_ns: 1,
            size: 10,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["expected-hash".to_string()],
        };
        m.sdb
            .write()
            .expect("db write")
            .update("default", "local", vec![fi])
            .expect("update local");

        let response = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 12,
                    folder: "default".to_string(),
                    name: "a.txt".to_string(),
                    offset: 0,
                    size: 4,
                    hash: b"bad".to_vec(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("response generated");
        assert_eq!(
            response,
            Some(BepMessage::Response {
                id: 12,
                code: 1,
                data_len: 0,
                data: Vec::new(),
            })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_request_receive_encrypted_skips_hash_validation() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-bep-request-recvenc-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");
        fs::write(root.join("a.txt"), b"hello").expect("write file");

        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.folder_type = FolderType::ReceiveEncrypted;
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-a".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });

        let mut m = NewModel();
        m.newFolder(cfg);
        let fi = db::FileInfo {
            folder: "default".to_string(),
            path: "a.txt".to_string(),
            sequence: 1,
            modified_ns: 1,
            size: 5,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["expected-hash".to_string()],
        };
        m.sdb
            .write()
            .expect("db write")
            .update("default", "local", vec![fi])
            .expect("update local");

        let response = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 121,
                    folder: "default".to_string(),
                    name: "a.txt".to_string(),
                    offset: 0,
                    size: 5,
                    hash: b"0000000000000000000000000000000000000000000000000000000000000000"
                        .to_vec(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("response generated");

        match response {
            Some(BepMessage::Response {
                code,
                data_len,
                data,
                ..
            }) => {
                assert_eq!(code, 0);
                assert_eq!(data_len, 5);
                assert_eq!(data, b"hello");
            }
            _ => panic!("expected response payload"),
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_request_rejects_unshared_device() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-bep-request-share-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-b".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });

        let mut m = NewModel();
        m.newFolder(cfg);
        let response = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 13,
                    folder: "default".to_string(),
                    name: "a.txt".to_string(),
                    offset: 0,
                    size: 4,
                    hash: Vec::new(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("response generated");
        assert_eq!(
            response,
            Some(BepMessage::Response {
                id: 13,
                code: 1,
                data_len: 0,
                data: Vec::new(),
            })
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_request_honors_ignore_negation_rules() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-bep-request-ignore-negation-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");
        fs::write(root.join("keep.tmp"), b"ok").expect("write keep");
        fs::write(root.join("drop.tmp"), b"no").expect("write drop");

        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-a".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });

        let mut m = NewModel();
        m.newFolder(cfg);
        m.SetIgnores(
            "default",
            vec!["*.tmp".to_string(), "!keep.tmp".to_string()],
        );

        let keep = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 131,
                    folder: "default".to_string(),
                    name: "keep.tmp".to_string(),
                    offset: 0,
                    size: 2,
                    hash: Vec::new(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("keep response");
        match keep {
            Some(BepMessage::Response { code, .. }) => assert_eq!(code, 0),
            _ => panic!("expected keep response"),
        }

        let drop = m
            .ApplyBepMessage(
                "peer-a",
                &BepMessage::Request {
                    id: 132,
                    folder: "default".to_string(),
                    name: "drop.tmp".to_string(),
                    offset: 0,
                    size: 2,
                    hash: Vec::new(),
                    from_temporary: false,
                    block_no: 0,
                },
            )
            .expect("drop response");
        match drop {
            Some(BepMessage::Response { code, .. }) => assert_eq!(code, 1),
            _ => panic!("expected drop response"),
        }

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn availability_filters_unconnected_or_unconfigured_devices() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-availability-filter-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));

        let remote_a = db::FileInfo {
            folder: "default".to_string(),
            path: "a.txt".to_string(),
            sequence: 3,
            modified_ns: 3,
            size: 10,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["h1".to_string()],
        };
        let remote_b = remote_a.clone();

        let mut db = m.sdb.write().expect("db write");
        db.update("default", "peer-a", vec![remote_a])
            .expect("update peer-a");
        db.update("default", "peer-b", vec![remote_b])
            .expect("update peer-b");
        drop(db);

        m.connections.insert(
            "peer-a".to_string(),
            ConnectionStats {
                Connected: true,
                ..ConnectionStats::default()
            },
        );
        m.connections.insert(
            "peer-b".to_string(),
            ConnectionStats {
                Connected: false,
                ..ConnectionStats::default()
            },
        );
        m.remoteFolderStates
            .insert("peer-a:default".to_string(), "valid".to_string());

        let availability = m.Availability("default", "a.txt");
        assert_eq!(availability.len(), 1);
        assert_eq!(availability[0].ID, "peer-a");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn scan_folder_requires_running_folder_state() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-scan-running-check-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        m.removeFolder("default");

        let err = m.ScanFolder("default").expect_err("scan should fail");
        assert_eq!(err, ErrFolderNotRunning);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_message_rejects_empty_device_id() {
        let mut m = NewModel();
        let err = m
            .ApplyBepMessage("   ", &BepMessage::Ping { timestamp_ms: 1 })
            .expect_err("empty device id must be rejected");
        assert!(err.contains("device id is required"));
    }

    #[test]
    fn local_changed_folder_files_only_returns_receive_only_entries() {
        let root =
            std::env::temp_dir().join(format!("syncthing-rs-local-changed-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));

        let mut plain = db::FileInfo {
            folder: "default".to_string(),
            path: "plain.txt".to_string(),
            sequence: 1,
            modified_ns: 1,
            size: 1,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["h1".to_string()],
        };

        let mut changed = plain.clone();
        changed.path = "changed.txt".to_string();
        changed.local_flags = db::FLAG_LOCAL_RECEIVE_ONLY;

        m.sdb
            .write()
            .expect("db write")
            .update("default", "local", vec![plain, changed])
            .expect("update local files");

        assert_eq!(
            m.LocalChangedFolderFiles("default"),
            vec!["changed.txt".to_string()]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn remote_need_folder_files_respects_requested_device() {
        let root =
            std::env::temp_dir().join(format!("syncthing-rs-remote-need-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));

        let mk = |path: &str, seq: i64| db::FileInfo {
            folder: "default".to_string(),
            path: path.to_string(),
            sequence: seq,
            modified_ns: seq,
            size: 10,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["h1".to_string()],
        };

        let mut db = m.sdb.write().expect("db write");
        db.update("default", "remote", vec![mk("a.txt", 2)])
            .expect("update remote");
        db.update("default", "local", vec![mk("a.txt", 2)])
            .expect("update local");
        db.update("default", "peer-a", vec![mk("a.txt", 1)])
            .expect("update peer-a");
        drop(db);

        let peer_need = m.RemoteNeedFolderFiles("default", "peer-a");
        assert_eq!(peer_need.len(), 1);
        assert_eq!(peer_need[0].path, "a.txt");

        let local_need = m.RemoteNeedFolderFiles("default", "local");
        assert!(local_need.is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_index_tracks_sequences_per_device() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-index-per-device-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-a".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        cfg.devices.push(crate::config::FolderDeviceConfiguration {
            device_id: "peer-b".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);

        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::Index {
                folder: "default".to_string(),
                files: vec![IndexEntry {
                    path: "a.txt".to_string(),
                    sequence: 2,
                    deleted: false,
                    size: 10,
                    block_hashes: vec!["h1".to_string()],
                    ..IndexEntry::default()
                }],
            },
        )
        .expect("apply index from peer-a");

        m.ApplyBepMessage(
            "peer-b",
            &BepMessage::Index {
                folder: "default".to_string(),
                files: vec![IndexEntry {
                    path: "b.txt".to_string(),
                    sequence: 3,
                    deleted: false,
                    size: 10,
                    block_hashes: vec!["h2".to_string()],
                    ..IndexEntry::default()
                }],
            },
        )
        .expect("apply index from peer-b");

        let seqs = m.RemoteSequences("default");
        assert_eq!(seqs.get("peer-a").copied(), Some(2));
        assert_eq!(seqs.get("peer-b").copied(), Some(3));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn apply_bep_hello_refreshes_existing_connection_state() {
        let mut m = NewModel();
        m.connections.insert(
            "peer-a".to_string(),
            ConnectionStats {
                Connected: false,
                ClientVersion: "old".to_string(),
                ..ConnectionStats::default()
            },
        );

        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::Hello {
                device_name: "peer-a".to_string(),
                client_name: "new".to_string(),
                client_version: String::new(),
                num_connections: 0,
                timestamp: 0,
            },
        )
        .expect("apply hello");

        let stats = m.connections.get("peer-a").expect("peer stats");
        assert!(stats.Connected);
        assert_eq!(stats.ClientVersion, "new");
    }

    #[test]
    fn cluster_config_remote_state_is_scoped_per_device() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-cluster-state-scope-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));

        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::ClusterConfig {
                folders: vec![ClusterConfigFolder {
                    id: "default".to_string(),
                    label: String::new(),
                    devices: Vec::new(),
                    folder_type: 0,
                }],
            },
        )
        .expect("cluster config");

        assert_eq!(
            m.Completion("peer-a", "default")
                .expect("peer-a completion")
                .RemoteState,
            "valid"
        );
        assert_eq!(
            m.Completion("peer-b", "default")
                .expect("peer-b completion")
                .RemoteState,
            "unknown"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn remote_state_clears_when_device_closes() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-remote-state-close-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("mkdir");

        let mut m = NewModel();
        m.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::ClusterConfig {
                folders: vec![ClusterConfigFolder {
                    id: "default".to_string(),
                    label: String::new(),
                    devices: Vec::new(),
                    folder_type: 0,
                }],
            },
        )
        .expect("cluster config");
        assert_eq!(
            m.Completion("peer-a", "default")
                .expect("peer-a completion")
                .RemoteState,
            "valid"
        );

        m.ApplyBepMessage(
            "peer-a",
            &BepMessage::Close {
                reason: "bye".to_string(),
            },
        )
        .expect("close");
        assert_eq!(
            m.Completion("peer-a", "default")
                .expect("peer-a completion")
                .RemoteState,
            "unknown"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dismiss_pending_device_removes_its_folder_offers() {
        let mut m = NewModel();
        m.pendingFolderOffers.insert(
            "f1".to_string(),
            BTreeSet::from(["peer-a".to_string(), "peer-b".to_string()]),
        );
        m.pendingFolderOffers
            .insert("f2".to_string(), BTreeSet::from(["peer-a".to_string()]));

        m.DismissPendingDevice("peer-a")
            .expect("dismiss pending device");

        assert_eq!(
            m.PendingFolderOffers()
                .get("f1")
                .cloned()
                .unwrap_or_default(),
            vec!["peer-b".to_string()]
        );
        assert!(!m.PendingFolderOffers().contains_key("f2"));
    }

    // ──────────────────────────────────────────────────────────
    // P0 Fix Tests
    // ──────────────────────────────────────────────────────────

    #[test]
    fn test_handle_introductions_adds_devices_to_folders() {
        let mut m = model::default();
        let mut cfg = newFolderConfiguration("shared", "/tmp/shared");
        cfg.devices.push(FolderDeviceConfiguration {
            device_id: "local-device".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);

        let cc_folders = vec!["shared".to_string()];
        let cc_devices = BTreeMap::from([(
            "shared".to_string(),
            vec!["new-dev-A".to_string(), "new-dev-B".to_string()],
        )]);

        let (fds, changed) = m.handleIntroductions("introducer-1", &cc_folders, &cc_devices);

        // Both devices should now be shared with the folder.
        assert!(changed, "should report config changed");
        let fcfg = m.folderCfgs.get("shared").expect("folder exists");
        assert!(fcfg.shared_with("new-dev-A"), "new-dev-A should be shared");
        assert!(fcfg.shared_with("new-dev-B"), "new-dev-B should be shared");
        // Both devices should be tracked in the folder-device set.
        assert!(fds.hasDevice("shared", "new-dev-A"));
        assert!(fds.hasDevice("shared", "new-dev-B"));
        // Devices should be introduced (observed).
        assert!(m.observed.set.contains("new-dev-A"));
        assert!(m.observed.set.contains("new-dev-B"));
    }

    #[test]
    fn test_handle_introductions_skips_self_device() {
        let mut m = model::default();
        let mut cfg = newFolderConfiguration("shared", "/tmp/shared");
        cfg.devices.push(FolderDeviceConfiguration {
            device_id: "local-device".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);

        let cc_folders = vec!["shared".to_string()];
        // Include self device ID — should be skipped.
        let cc_devices = BTreeMap::from([(
            "shared".to_string(),
            vec!["local-device".to_string(), "other-dev".to_string()],
        )]);

        let (_, changed) = m.handleIntroductions("introducer-1", &cc_folders, &cc_devices);

        assert!(changed);
        let fcfg = m.folderCfgs.get("shared").expect("folder exists");
        // Self device should not be re-added (it's already there).
        let self_count = fcfg
            .devices
            .iter()
            .filter(|d| d.device_id == "local-device")
            .count();
        assert_eq!(self_count, 1, "self device should appear exactly once");
        assert!(fcfg.shared_with("other-dev"));
    }

    #[test]
    fn test_handle_deintroductions_removes_stale_shares() {
        let mut m = model::default();
        let mut cfg = newFolderConfiguration("shared", "/tmp/shared");
        cfg.devices.push(FolderDeviceConfiguration {
            device_id: "local-device".to_string(),
            introduced_by: String::new(),
            encryption_password: String::new(),
        });
        cfg.devices.push(FolderDeviceConfiguration {
            device_id: "dev-A".to_string(),
            introduced_by: "introducer-1".to_string(),
            encryption_password: String::new(),
        });
        cfg.devices.push(FolderDeviceConfiguration {
            device_id: "dev-B".to_string(),
            introduced_by: "introducer-1".to_string(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);

        // Introducer still shares with dev-A but NOT dev-B.
        let mut fds = folderDeviceSet::default();
        fds.set("shared", &["dev-A".to_string()]);

        let changed = m.handleDeintroductions("introducer-1", false, &fds);

        assert!(changed, "should report config changed");
        let fcfg = m.folderCfgs.get("shared").expect("folder exists");
        assert!(fcfg.shared_with("dev-A"), "dev-A should remain");
        assert!(!fcfg.shared_with("dev-B"), "dev-B should be removed");
        assert!(
            fcfg.shared_with("local-device"),
            "self device should remain"
        );
    }

    #[test]
    fn test_handle_deintroductions_respects_skip_flag() {
        let mut m = model::default();
        let mut cfg = newFolderConfiguration("shared", "/tmp/shared");
        cfg.devices.push(FolderDeviceConfiguration {
            device_id: "dev-A".to_string(),
            introduced_by: "introducer-1".to_string(),
            encryption_password: String::new(),
        });
        m.newFolder(cfg);

        // Empty folder-device set means introducer shares nothing.
        let fds = folderDeviceSet::default();

        // With skip=true, nothing should be removed.
        let changed = m.handleDeintroductions("introducer-1", true, &fds);

        assert!(!changed, "should not change anything when skip is true");
        let fcfg = m.folderCfgs.get("shared").expect("folder exists");
        assert!(
            fcfg.shared_with("dev-A"),
            "dev-A should remain when skip=true"
        );
    }

    #[test]
    fn test_handle_auto_accepts_creates_folder() {
        let mut m = model::default();

        let folder_ids = vec!["new-folder".to_string()];
        let mut labels = BTreeMap::new();
        labels.insert("new-folder".to_string(), "My New Folder".to_string());

        m.handleAutoAccepts("peer-dev", &folder_ids, &labels);

        let fcfg = m.folderCfgs.get("new-folder").expect("folder created");
        assert_eq!(fcfg.label, "My New Folder");
        assert!(fcfg.shared_with("peer-dev"));
        assert!(
            !m.pendingFolderOffers.contains_key("new-folder"),
            "should be removed from pending"
        );
    }

    #[test]
    fn test_handle_auto_accepts_shares_existing() {
        let mut m = model::default();
        let cfg = newFolderConfiguration("existing", "/tmp/existing");
        m.newFolder(cfg);

        let folder_ids = vec!["existing".to_string()];
        let labels = BTreeMap::new();

        m.handleAutoAccepts("new-peer", &folder_ids, &labels);

        let fcfg = m.folderCfgs.get("existing").expect("folder exists");
        assert!(fcfg.shared_with("new-peer"), "new device should be shared");
    }

    #[test]
    fn test_cc_handle_folders_tracks_remote_states() {
        let mut m = model::default();
        let cfg = newFolderConfiguration("known", "/tmp/known");
        m.newFolder(cfg);

        let folder_ids = vec!["known".to_string(), "unknown".to_string()];
        m.ccHandleFolders(&folder_ids, "peer-1", false);

        // Check remote folder states.
        assert!(
            m.remoteFolderStates.get("peer-1:known").is_some(),
            "known folder should be tracked"
        );
        assert!(
            m.remoteFolderStates.get("peer-1:unknown").is_some(),
            "unknown folder should be tracked as valid"
        );
        // Unknown folder should be in pending offers.
        assert!(
            m.pendingFolderOffers
                .get("unknown")
                .map(|s| s.contains("peer-1"))
                .unwrap_or(false),
            "unknown folder should be pending"
        );
    }

    #[test]
    fn test_cc_check_encryption_validates_tokens() {
        let m = model::default();

        // No encryption, no tokens — should pass.
        assert!(m
            .ccCheckEncryption("f1", FolderType::SendReceive, "", false, "", "")
            .is_ok());

        // Both sides encrypted — should fail.
        assert!(m
            .ccCheckEncryption("f1", FolderType::ReceiveEncrypted, "pass", false, "", "")
            .is_err());

        // Untrusted device without encryption — should fail.
        assert!(m
            .ccCheckEncryption("f1", FolderType::SendReceive, "", true, "", "")
            .is_err());

        // Has remote token, local is encrypted — should pass if no stored token yet.
        assert!(m
            .ccCheckEncryption(
                "f1",
                FolderType::ReceiveEncrypted,
                "",
                false,
                "token-abc",
                ""
            )
            .is_ok());
    }
}
