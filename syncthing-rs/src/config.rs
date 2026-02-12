use crate::folder_modes::FolderMode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) const DEFAULT_MARKER_NAME: &str = ".stfolder";
pub(crate) const ENCRYPTION_TOKEN_NAME: &str = "syncthing-encryption_password_token";
pub(crate) const MAX_CONCURRENT_WRITES_DEFAULT: usize = 16;
pub(crate) const MAX_CONCURRENT_WRITES_LIMIT: usize = 256;

pub(crate) const ERR_PATH_NOT_DIRECTORY: &str = "folder path not a directory";
pub(crate) const ERR_PATH_MISSING: &str = "folder path missing";
pub(crate) const ERR_MARKER_MISSING: &str =
    "folder marker missing (this indicates potential data loss, search docs/forum to get information about how to proceed)";

// Compatibility aliases matching Go surface names used by parity guardrails.
pub(crate) const DefaultMarkerName: &str = DEFAULT_MARKER_NAME;
pub(crate) const EncryptionTokenName: &str = ENCRYPTION_TOKEN_NAME;
pub(crate) const maxConcurrentWritesDefault: usize = MAX_CONCURRENT_WRITES_DEFAULT;
pub(crate) const maxConcurrentWritesLimit: usize = MAX_CONCURRENT_WRITES_LIMIT;
pub(crate) const ErrPathNotDirectory: &str = ERR_PATH_NOT_DIRECTORY;
pub(crate) const ErrPathMissing: &str = ERR_PATH_MISSING;
pub(crate) const ErrMarkerMissing: &str = ERR_MARKER_MISSING;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum FolderType {
    #[serde(rename = "sendrecv", alias = "sendreceive", alias = "readwrite")]
    SendReceive,
    #[serde(rename = "sendonly", alias = "readonly")]
    SendOnly,
    #[serde(rename = "recvonly", alias = "receiveonly")]
    ReceiveOnly,
    #[serde(rename = "recvenc", alias = "receiveencrypted")]
    ReceiveEncrypted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PullOrder {
    #[serde(rename = "random")]
    Random,
    #[serde(rename = "alphabetic")]
    Alphabetic,
    #[serde(rename = "smallestFirst", alias = "smallestfirst")]
    SmallestFirst,
    #[serde(rename = "largestFirst", alias = "largestfirst")]
    LargestFirst,
    #[serde(rename = "oldestFirst", alias = "oldestfirst")]
    OldestFirst,
    #[serde(rename = "newestFirst", alias = "newestfirst")]
    NewestFirst,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum BlockPullOrder {
    #[serde(rename = "standard")]
    Standard,
    #[serde(rename = "random")]
    Random,
    #[serde(rename = "inOrder", alias = "inorder")]
    InOrder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CopyRangeMethod {
    #[serde(
        rename = "all",
        alias = "standard",
        alias = "allwithfallback",
        alias = "all_with_fallback",
        alias = "allWithFallback"
    )]
    Standard,
    #[serde(rename = "ioctl")]
    Ioctl,
    #[serde(rename = "copy_file_range", alias = "copyfilerange")]
    CopyFileRange,
    #[serde(rename = "sendfile")]
    Sendfile,
    #[serde(rename = "duplicate_extents", alias = "duplicateextents")]
    DuplicateExtents,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryPolicy {
    Throttle,
    Fail,
    BestEffort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FilesystemType {
    Basic,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VersioningConfiguration {
    #[serde(default, rename = "type")]
    pub(crate) versioning_type: String,
    #[serde(default)]
    pub(crate) params: BTreeMap<String, String>,
    #[serde(default = "default_versioning_cleanup_interval_s")]
    pub(crate) cleanup_interval_s: i32,
    #[serde(default)]
    pub(crate) fs_path: String,
    #[serde(default = "default_filesystem_type")]
    pub(crate) fs_type: FilesystemType,
}

impl Default for VersioningConfiguration {
    fn default() -> Self {
        Self {
            versioning_type: String::new(),
            params: BTreeMap::new(),
            cleanup_interval_s: default_versioning_cleanup_interval_s(),
            fs_path: String::new(),
            fs_type: FilesystemType::Basic,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Size {
    pub(crate) bytes: i64,
}

impl Size {
    pub(crate) fn base_value(self) -> i64 {
        self.bytes.max(0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FolderDeviceConfiguration {
    pub(crate) device_id: String,
    pub(crate) introduced_by: String,
    pub(crate) encryption_password: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct XattrFilterEntry {
    pub(crate) match_pattern: String,
    pub(crate) permit: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct XattrFilter {
    pub(crate) entries: Vec<XattrFilterEntry>,
    pub(crate) max_single_entry_size: usize,
    pub(crate) max_total_size: usize,
}

impl Default for XattrFilter {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            max_single_entry_size: 1024,
            max_total_size: 4096,
        }
    }
}

impl XattrFilter {
    pub(crate) fn permit(&self, value: &str) -> bool {
        if self.entries.is_empty() {
            return true;
        }
        for entry in &self.entries {
            if wildcard_match(&entry.match_pattern, value) {
                return entry.permit;
            }
        }
        false
    }

    pub(crate) fn get_max_single_entry_size(&self) -> usize {
        self.max_single_entry_size
    }

    pub(crate) fn get_max_total_size(&self) -> usize {
        self.max_total_size
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct FolderConfiguration {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) filesystem_type: FilesystemType,
    pub(crate) path: String,
    pub(crate) folder_type: FolderType,
    #[serde(default)]
    pub(crate) versioning: VersioningConfiguration,
    pub(crate) devices: Vec<FolderDeviceConfiguration>,
    pub(crate) rescan_interval_s: i32,
    pub(crate) fs_watcher_enabled: bool,
    pub(crate) fs_watcher_delay_s: f64,
    pub(crate) fs_watcher_timeout_s: f64,
    pub(crate) ignore_perms: bool,
    pub(crate) auto_normalize: bool,
    pub(crate) min_disk_free: Size,
    pub(crate) copiers: i32,
    pub(crate) puller_max_pending_kib: i32,
    pub(crate) hashers: i32,
    pub(crate) order: PullOrder,
    pub(crate) ignore_delete: bool,
    pub(crate) scan_progress_interval_s: i32,
    pub(crate) puller_pause_s: i32,
    pub(crate) puller_delay_s: f64,
    #[serde(default = "default_memory_max_mb")]
    pub(crate) memory_max_mb: i32,
    #[serde(default = "default_memory_policy")]
    pub(crate) memory_policy: MemoryPolicy,
    #[serde(default = "default_memory_soft_percent")]
    pub(crate) memory_soft_percent: i32,
    #[serde(default = "default_memory_telemetry_interval_s")]
    pub(crate) memory_telemetry_interval_s: i32,
    #[serde(default = "default_memory_pull_page_items")]
    pub(crate) memory_pull_page_items: i32,
    #[serde(default = "default_memory_scan_spill_threshold_entries")]
    pub(crate) memory_scan_spill_threshold_entries: i32,
    pub(crate) max_conflicts: i32,
    pub(crate) disable_sparse_files: bool,
    pub(crate) paused: bool,
    pub(crate) marker_name: String,
    pub(crate) copy_ownership_from_parent: bool,
    pub(crate) raw_mod_time_window_s: i32,
    pub(crate) max_concurrent_writes: i32,
    pub(crate) disable_fsync: bool,
    pub(crate) block_pull_order: BlockPullOrder,
    pub(crate) copy_range_method: CopyRangeMethod,
    pub(crate) case_sensitive_fs: bool,
    pub(crate) junctions_as_dirs: bool,
    pub(crate) sync_ownership: bool,
    pub(crate) send_ownership: bool,
    pub(crate) sync_xattrs: bool,
    pub(crate) send_xattrs: bool,
    pub(crate) xattr_filter: XattrFilter,
    pub(crate) deprecated_read_only: bool,
    pub(crate) deprecated_min_disk_free_pct: f64,
    pub(crate) deprecated_pullers: i32,
    pub(crate) deprecated_scan_ownership: bool,
}

impl Default for FolderConfiguration {
    fn default() -> Self {
        Self {
            id: String::new(),
            label: String::new(),
            filesystem_type: FilesystemType::Basic,
            path: String::new(),
            folder_type: FolderType::SendReceive,
            versioning: VersioningConfiguration::default(),
            devices: Vec::new(),
            rescan_interval_s: 3600,
            fs_watcher_enabled: true,
            fs_watcher_delay_s: 10.0,
            fs_watcher_timeout_s: 0.0,
            ignore_perms: false,
            auto_normalize: true,
            min_disk_free: Size { bytes: 0 },
            copiers: 0,
            puller_max_pending_kib: 0,
            hashers: 0,
            order: PullOrder::Random,
            ignore_delete: false,
            scan_progress_interval_s: 0,
            puller_pause_s: 0,
            puller_delay_s: 1.0,
            memory_max_mb: default_memory_max_mb(),
            memory_policy: default_memory_policy(),
            memory_soft_percent: default_memory_soft_percent(),
            memory_telemetry_interval_s: default_memory_telemetry_interval_s(),
            memory_pull_page_items: default_memory_pull_page_items(),
            memory_scan_spill_threshold_entries: default_memory_scan_spill_threshold_entries(),
            max_conflicts: 10,
            disable_sparse_files: false,
            paused: false,
            marker_name: DEFAULT_MARKER_NAME.to_string(),
            copy_ownership_from_parent: false,
            raw_mod_time_window_s: 0,
            max_concurrent_writes: 0,
            disable_fsync: false,
            block_pull_order: BlockPullOrder::Standard,
            copy_range_method: CopyRangeMethod::Standard,
            case_sensitive_fs: false,
            junctions_as_dirs: false,
            sync_ownership: false,
            send_ownership: false,
            sync_xattrs: false,
            send_xattrs: false,
            xattr_filter: XattrFilter::default(),
            deprecated_read_only: false,
            deprecated_min_disk_free_pct: 0.0,
            deprecated_pullers: 0,
            deprecated_scan_ownership: false,
        }
    }
}

impl FolderType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SendReceive => "sendrecv",
            Self::SendOnly => "sendonly",
            Self::ReceiveOnly => "recvonly",
            Self::ReceiveEncrypted => "recvenc",
        }
    }

    pub(crate) fn to_mode(self) -> FolderMode {
        match self {
            Self::SendReceive => FolderMode::SendReceive,
            Self::SendOnly => FolderMode::SendOnly,
            Self::ReceiveOnly => FolderMode::ReceiveOnly,
            Self::ReceiveEncrypted => FolderMode::ReceiveEncrypted,
        }
    }
}

impl FolderConfiguration {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("folder id must not be empty".to_string());
        }
        if self.path.trim().is_empty() {
            return Err(format!("folder {} path must not be empty", self.id));
        }
        if self.rescan_interval_s == 0 && !self.fs_watcher_enabled {
            return Err(format!(
                "folder {} must have either fs watcher enabled or non-zero rescan interval",
                self.id
            ));
        }
        if self.max_conflicts < -1 {
            return Err(format!(
                "folder {} max_conflicts must be >= -1 (got {})",
                self.id, self.max_conflicts
            ));
        }
        if self.memory_max_mb <= 0 {
            return Err(format!(
                "folder {} memory_max_mb must be > 0 (got {})",
                self.id, self.memory_max_mb
            ));
        }
        if !(1..=99).contains(&self.memory_soft_percent) {
            return Err(format!(
                "folder {} memory_soft_percent must be between 1 and 99 (got {})",
                self.id, self.memory_soft_percent
            ));
        }
        if self.memory_telemetry_interval_s <= 0 {
            return Err(format!(
                "folder {} memory_telemetry_interval_s must be > 0 (got {})",
                self.id, self.memory_telemetry_interval_s
            ));
        }
        if self.memory_pull_page_items <= 0 {
            return Err(format!(
                "folder {} memory_pull_page_items must be > 0 (got {})",
                self.id, self.memory_pull_page_items
            ));
        }
        if self.memory_scan_spill_threshold_entries <= 0 {
            return Err(format!(
                "folder {} memory_scan_spill_threshold_entries must be > 0 (got {})",
                self.id, self.memory_scan_spill_threshold_entries
            ));
        }
        if self.folder_type == FolderType::ReceiveEncrypted && !self.ignore_perms {
            return Err(format!(
                "folder {} receive-encrypted mode requires ignore_perms=true",
                self.id
            ));
        }
        Ok(())
    }

    pub(crate) fn copy(&self) -> Self {
        self.clone()
    }

    pub(crate) fn filesystem(&self) -> String {
        PathBuf::from(&self.path).to_string_lossy().to_string()
    }

    pub(crate) fn mod_time_window(&self) -> Duration {
        if self.raw_mod_time_window_s <= 0 {
            Duration::ZERO
        } else {
            Duration::from_secs(self.raw_mod_time_window_s as u64)
        }
    }

    pub(crate) fn marker_filename(&self) -> String {
        let digest = Sha256::digest(self.id.as_bytes());
        format!(
            "syncthing-folder-{:02x}{:02x}{:02x}.txt",
            digest[0], digest[1], digest[2]
        )
    }

    pub(crate) fn marker_contents(&self) -> Vec<u8> {
        let created = humantime::format_rfc3339_seconds(SystemTime::now()).to_string();
        format!(
            "# This directory is a Syncthing folder marker.\n# Do not delete.\n\nfolderID: {}\ncreated: {}\n",
            self.id, created
        )
        .into_bytes()
    }

    pub(crate) fn create_root(&self) -> Result<(), String> {
        fs::create_dir_all(&self.path).map_err(|err| format!("create root: {err}"))
    }

    pub(crate) fn check_path(&self) -> Result<(), String> {
        self.check_filesystem_path(Path::new(&self.path), Path::new("."))
    }

    pub(crate) fn check_filesystem_path(&self, root: &Path, relative: &Path) -> Result<(), String> {
        let target = root.join(relative);
        let metadata = match fs::metadata(&target) {
            Ok(md) => md,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(ERR_PATH_MISSING.to_string())
            }
            Err(err) => return Err(err.to_string()),
        };

        if !metadata.is_dir() {
            return Err(ERR_PATH_NOT_DIRECTORY.to_string());
        }

        let marker = target.join(&self.marker_name);
        if !marker.exists() {
            return Err(ERR_MARKER_MISSING.to_string());
        }
        Ok(())
    }

    pub(crate) fn create_marker(&self) -> Result<(), String> {
        match self.check_path() {
            Ok(()) => return Ok(()),
            Err(err) if err == ERR_MARKER_MISSING => {}
            Err(err) => return Err(err),
        }
        if self.marker_name != DEFAULT_MARKER_NAME {
            return Ok(());
        }
        let marker_dir = Path::new(&self.path).join(DEFAULT_MARKER_NAME);
        match fs::create_dir(&marker_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(format!("create marker dir: {err}")),
        }
        let marker_file = marker_dir.join(self.marker_filename());
        fs::write(marker_file, self.marker_contents()).map_err(|err| err.to_string())
    }

    pub(crate) fn remove_marker(&self) -> Result<(), String> {
        let marker_dir = Path::new(&self.path).join(DEFAULT_MARKER_NAME);
        let marker_file = marker_dir.join(self.marker_filename());
        let _ = fs::remove_file(marker_file);
        fs::remove_dir(marker_dir).map_err(|err| err.to_string())
    }

    pub(crate) fn description(&self) -> String {
        if self.label.is_empty() {
            self.id.clone()
        } else {
            format!("\"{}\" ({})", self.label, self.id)
        }
    }

    pub(crate) fn log_attr(&self) -> serde_json::Value {
        json!({
            "folder": {
                "id": self.id,
                "label": self.label,
                "type": self.folder_type.as_str(),
                "memory": {
                    "max_mb": self.memory_max_mb,
                    "policy": format!("{:?}", self.memory_policy).to_lowercase(),
                    "soft_percent": self.memory_soft_percent,
                    "telemetry_interval_s": self.memory_telemetry_interval_s,
                    "pull_page_items": self.memory_pull_page_items,
                    "scan_spill_threshold_entries": self.memory_scan_spill_threshold_entries
                }
            }
        })
    }

    pub(crate) fn device_ids(&self) -> Vec<String> {
        self.devices.iter().map(|d| d.device_id.clone()).collect()
    }

    pub(crate) fn device(&self, device_id: &str) -> Option<FolderDeviceConfiguration> {
        self.devices
            .iter()
            .find(|d| d.device_id == device_id)
            .cloned()
    }

    pub(crate) fn shared_with(&self, device_id: &str) -> bool {
        self.device(device_id).is_some()
    }

    pub(crate) fn prepare(&mut self, my_id: &str, existing_devices: &[String]) {
        self.devices
            .retain(|d| existing_devices.contains(&d.device_id) || d.device_id == my_id);
        if !self.shared_with(my_id) {
            self.devices.push(FolderDeviceConfiguration {
                device_id: my_id.to_string(),
                introduced_by: String::new(),
                encryption_password: String::new(),
            });
        }
        self.devices.sort_by(|a, b| a.device_id.cmp(&b.device_id));
        self.devices.dedup_by(|a, b| a.device_id == b.device_id);

        if self.rescan_interval_s < 0 {
            self.rescan_interval_s = 0;
        }
        if self.fs_watcher_delay_s <= 0.0 {
            self.fs_watcher_enabled = false;
            self.fs_watcher_delay_s = 10.0;
        } else if self.fs_watcher_delay_s < 0.01 {
            self.fs_watcher_delay_s = 0.01;
        }
        if self.marker_name.trim().is_empty() {
            self.marker_name = DEFAULT_MARKER_NAME.to_string();
        }
        if self.max_concurrent_writes <= 0 {
            self.max_concurrent_writes = MAX_CONCURRENT_WRITES_DEFAULT as i32;
        } else if self.max_concurrent_writes > MAX_CONCURRENT_WRITES_LIMIT as i32 {
            self.max_concurrent_writes = MAX_CONCURRENT_WRITES_LIMIT as i32;
        }
        if self.memory_max_mb <= 0 {
            self.memory_max_mb = default_memory_max_mb();
        }
        if !(1..=99).contains(&self.memory_soft_percent) {
            self.memory_soft_percent = default_memory_soft_percent();
        }
        if self.memory_telemetry_interval_s <= 0 {
            self.memory_telemetry_interval_s = default_memory_telemetry_interval_s();
        }
        if self.memory_pull_page_items <= 0 {
            self.memory_pull_page_items = default_memory_pull_page_items();
        }
        if self.memory_scan_spill_threshold_entries <= 0 {
            self.memory_scan_spill_threshold_entries =
                default_memory_scan_spill_threshold_entries();
        }
        if self.folder_type == FolderType::ReceiveEncrypted {
            self.ignore_perms = true;
        }
    }

    pub(crate) fn requires_restart_only(&self) -> Self {
        let mut copy = self.copy();
        copy.label.clear();
        copy.fs_watcher_enabled = false;
        copy.rescan_interval_s = 0;
        copy
    }

    pub(crate) fn check_available_space(
        &self,
        required: u64,
        available: u64,
    ) -> Result<(), String> {
        let min = self.min_disk_free.base_value().max(0) as u64;
        if min == 0 {
            return Ok(());
        }
        if available < required.saturating_add(min) {
            return Err(format!(
                "insufficient space in folder {}: required={} min_free={} available={}",
                self.description(),
                required,
                min,
                available
            ));
        }
        Ok(())
    }

    pub(crate) fn unmarshal_json(data: &str) -> Result<Self, String> {
        serde_json::from_str(data).map_err(|err| format!("unmarshal json: {err}"))
    }

    pub(crate) fn unmarshal_xml(data: &str) -> Result<Self, String> {
        // Minimal XML parsing for parity harness purposes.
        let source = extract_xml_opening_tag(data, "folder").unwrap_or(data);
        let mut cfg = FolderConfiguration::default();
        cfg.id = extract_xml_attr(source, "id").unwrap_or_default();
        cfg.label = extract_xml_attr(source, "label").unwrap_or_default();
        cfg.path = extract_xml_attr(source, "path").unwrap_or_default();
        cfg.folder_type = match extract_xml_attr(source, "type").as_deref() {
            Some("sendonly") => FolderType::SendOnly,
            Some("receiveonly") | Some("recvonly") => FolderType::ReceiveOnly,
            Some("receiveencrypted") | Some("recvenc") => FolderType::ReceiveEncrypted,
            _ => FolderType::SendReceive,
        };
        Ok(cfg)
    }
}

pub(crate) fn demo_configs() -> Vec<FolderConfiguration> {
    let mut base = FolderConfiguration::default();
    base.id = "default".to_string();
    base.label = "Default".to_string();
    base.path = "/data/default".to_string();
    base.folder_type = FolderType::SendReceive;
    base.devices = vec![FolderDeviceConfiguration {
        device_id: "dev-self".to_string(),
        introduced_by: String::new(),
        encryption_password: String::new(),
    }];

    let mut recv_only = base.clone();
    recv_only.id = "readonly".to_string();
    recv_only.label = "ReadOnly".to_string();
    recv_only.path = "/data/readonly".to_string();
    recv_only.folder_type = FolderType::ReceiveOnly;

    let mut send_only = base.clone();
    send_only.id = "sendonly".to_string();
    send_only.label = "SendOnly".to_string();
    send_only.path = "/data/sendonly".to_string();
    send_only.folder_type = FolderType::SendOnly;

    let mut recv_enc = base.clone();
    recv_enc.id = "encrypted".to_string();
    recv_enc.label = "Encrypted".to_string();
    recv_enc.path = "/data/encrypted".to_string();
    recv_enc.folder_type = FolderType::ReceiveEncrypted;
    recv_enc.ignore_perms = true;

    vec![base, send_only, recv_only, recv_enc]
}

fn extract_xml_attr(xml: &str, attr: &str) -> Option<String> {
    let dq = format!("{attr}=\"");
    if let Some(start) = xml.find(&dq) {
        let rest = &xml[start + dq.len()..];
        let end = rest.find('"')?;
        return Some(rest[..end].to_string());
    }
    let sq = format!("{attr}='");
    let start = xml.find(&sq)? + sq.len();
    let rest = &xml[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn extract_xml_opening_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let needle = format!("<{tag}");
    let start = xml.find(&needle)?;
    let rest = &xml[start..];
    let end = rest.find('>')?;
    Some(&rest[..=end])
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

fn default_memory_max_mb() -> i32 {
    50
}

fn default_memory_policy() -> MemoryPolicy {
    MemoryPolicy::Throttle
}

fn default_memory_soft_percent() -> i32 {
    85
}

fn default_memory_telemetry_interval_s() -> i32 {
    5
}

fn default_memory_pull_page_items() -> i32 {
    1024
}

fn default_memory_scan_spill_threshold_entries() -> i32 {
    10_000
}

fn default_versioning_cleanup_interval_s() -> i32 {
    3600
}

fn default_filesystem_type() -> FilesystemType {
    FilesystemType::Basic
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn validates_basic_behavior() {
        let cfg = demo_configs().into_iter().next().expect("default config");
        assert_eq!(cfg.description(), "\"Default\" (default)");
        assert!(cfg.shared_with("dev-self"));
        assert_eq!(cfg.mod_time_window(), Duration::ZERO);
        assert_eq!(cfg.filesystem(), "/data/default");
    }

    #[test]
    fn prepare_normalizes_limits() {
        let mut cfg = FolderConfiguration::default();
        cfg.id = "f".to_string();
        cfg.path = "/tmp/f".to_string();
        cfg.marker_name = String::new();
        cfg.max_concurrent_writes = 9999;
        cfg.fs_watcher_delay_s = 0.0;
        cfg.folder_type = FolderType::ReceiveEncrypted;
        cfg.prepare("self", &["self".to_string(), "peer".to_string()]);

        assert_eq!(cfg.marker_name, DEFAULT_MARKER_NAME);
        assert_eq!(
            cfg.max_concurrent_writes,
            MAX_CONCURRENT_WRITES_LIMIT as i32
        );
        assert!(!cfg.fs_watcher_enabled);
        assert!(cfg.ignore_perms);
    }

    #[test]
    fn xattr_filter_permit_matches() {
        let filter = XattrFilter {
            entries: vec![
                XattrFilterEntry {
                    match_pattern: "user.*".to_string(),
                    permit: true,
                },
                XattrFilterEntry {
                    match_pattern: "*".to_string(),
                    permit: false,
                },
            ],
            max_single_entry_size: 100,
            max_total_size: 200,
        };

        assert!(filter.permit("user.comment"));
        assert!(!filter.permit("security.selinux"));
        assert_eq!(filter.get_max_single_entry_size(), 100);
        assert_eq!(filter.get_max_total_size(), 200);
    }

    #[test]
    fn marker_ops_and_path_checks() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-config-marker-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create root");

        let mut cfg = FolderConfiguration::default();
        cfg.id = "abc".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.marker_name = DEFAULT_MARKER_NAME.to_string();

        assert_eq!(
            cfg.check_path().expect_err("missing marker"),
            ERR_MARKER_MISSING
        );
        cfg.create_marker().expect("create marker");
        cfg.check_path().expect("path ok");
        cfg.remove_marker().expect("remove marker");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn marker_filename_uses_sha256_prefix() {
        let mut cfg = FolderConfiguration::default();
        cfg.id = "abc".to_string();
        assert_eq!(cfg.marker_filename(), "syncthing-folder-ba7816.txt");
    }

    #[test]
    fn create_marker_requires_existing_root_path() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-config-missing-root-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let mut cfg = FolderConfiguration::default();
        cfg.id = "abc".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.marker_name = DEFAULT_MARKER_NAME.to_string();

        let err = cfg.create_marker().expect_err("missing root should fail");
        assert_eq!(err, ERR_PATH_MISSING);
    }

    #[test]
    fn marker_contents_uses_rfc3339_created_field() {
        let mut cfg = FolderConfiguration::default();
        cfg.id = "abc".to_string();
        let text = String::from_utf8(cfg.marker_contents()).expect("utf8 marker");
        assert!(text.contains("folderID: abc"));
        assert!(text.contains("created: "));
        assert!(!text.contains("created_unix"));
    }

    #[test]
    fn remove_marker_returns_error_when_marker_dir_missing() {
        let root = std::env::temp_dir().join(format!(
            "syncthing-rs-config-remove-marker-missing-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create root");

        let mut cfg = FolderConfiguration::default();
        cfg.id = "abc".to_string();
        cfg.path = root.to_string_lossy().to_string();
        cfg.marker_name = DEFAULT_MARKER_NAME.to_string();

        let err = cfg
            .remove_marker()
            .expect_err("missing marker dir should return error");
        assert!(!err.is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unmarshal_and_space_checks() {
        let data = r#"{
            "id":"f1",
            "label":"F1",
            "filesystem_type":"basic",
            "path":"/tmp/f1",
            "folder_type":"sendreceive",
            "devices":[],
            "rescan_interval_s":10,
            "fs_watcher_enabled":true,
            "fs_watcher_delay_s":1.0,
            "fs_watcher_timeout_s":0.0,
            "ignore_perms":false,
            "auto_normalize":true,
            "min_disk_free":{"bytes":100},
            "copiers":1,
            "puller_max_pending_kib":1,
            "hashers":1,
            "order":"alphabetic",
            "ignore_delete":false,
            "scan_progress_interval_s":1,
            "puller_pause_s":1,
            "puller_delay_s":1.0,
            "max_conflicts":10,
            "disable_sparse_files":false,
            "paused":false,
            "marker_name":".stfolder",
            "copy_ownership_from_parent":false,
            "raw_mod_time_window_s":0,
            "max_concurrent_writes":1,
            "disable_fsync":false,
            "block_pull_order":"standard",
            "copy_range_method":"standard",
            "case_sensitive_fs":false,
            "junctions_as_dirs":false,
            "sync_ownership":false,
            "send_ownership":false,
            "sync_xattrs":false,
            "send_xattrs":false,
            "xattr_filter":{"entries":[],"max_single_entry_size":1024,"max_total_size":4096},
            "deprecated_read_only":false,
            "deprecated_min_disk_free_pct":0.0,
            "deprecated_pullers":0,
            "deprecated_scan_ownership":false
        }"#;
        let cfg = FolderConfiguration::unmarshal_json(data).expect("json parse");
        assert_eq!(cfg.id, "f1");
        assert!(cfg.check_available_space(200, 500).is_ok());
        assert!(cfg.check_available_space(450, 500).is_err());

        let xml_cfg = FolderConfiguration::unmarshal_xml(
            r#"<folder id="fx" label="FolderX" path="/tmp/fx" type="recvonly"></folder>"#,
        )
        .expect("xml parse");
        assert_eq!(xml_cfg.id, "fx");
        assert_eq!(xml_cfg.folder_type, FolderType::ReceiveOnly);

        let xml_send_only = FolderConfiguration::unmarshal_xml(
            r#"<folder id="so" label="SendOnly" path="/tmp/so" type="sendonly"></folder>"#,
        )
        .expect("xml sendonly parse");
        assert_eq!(xml_send_only.folder_type, FolderType::SendOnly);

        let xml_nested = FolderConfiguration::unmarshal_xml(
            r#"<config id="outer" path="/tmp/outer"><folder id="inner" label="Inner" path="/tmp/inner" type="receiveonly"/></config>"#,
        )
        .expect("xml nested parse");
        assert_eq!(xml_nested.id, "inner");
        assert_eq!(xml_nested.path, "/tmp/inner");
        assert_eq!(xml_nested.folder_type, FolderType::ReceiveOnly);

        let xml_single_quoted = FolderConfiguration::unmarshal_xml(
            r#"<folder id='sq' label='Single' path='/tmp/sq' type='sendonly'></folder>"#,
        )
        .expect("xml single quoted parse");
        assert_eq!(xml_single_quoted.id, "sq");
        assert_eq!(xml_single_quoted.path, "/tmp/sq");
        assert_eq!(xml_single_quoted.folder_type, FolderType::SendOnly);
    }

    #[test]
    fn memory_profile_defaults_and_prepare_clamps_invalid_values() {
        let mut cfg = FolderConfiguration::default();
        cfg.id = "m1".to_string();
        cfg.path = "/tmp/m1".to_string();
        cfg.memory_max_mb = -10;
        cfg.memory_soft_percent = 0;
        cfg.memory_telemetry_interval_s = -1;
        cfg.memory_pull_page_items = 0;
        cfg.memory_scan_spill_threshold_entries = 0;
        cfg.max_conflicts = 1;

        cfg.prepare("local-device", &[]);

        assert_eq!(cfg.memory_max_mb, 50);
        assert_eq!(cfg.memory_policy, MemoryPolicy::Throttle);
        assert_eq!(cfg.memory_soft_percent, 85);
        assert_eq!(cfg.memory_telemetry_interval_s, 5);
        assert_eq!(cfg.memory_pull_page_items, 1024);
        assert_eq!(cfg.memory_scan_spill_threshold_entries, 10_000);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn folder_type_and_orders_accept_go_tokens_and_aliases() {
        let canonical: FolderType = serde_json::from_str("\"sendrecv\"").expect("canonical");
        let alias: FolderType = serde_json::from_str("\"sendrecv\"").expect("alias");
        let alias_long: FolderType = serde_json::from_str("\"sendreceive\"").expect("long alias");
        let legacy: FolderType = serde_json::from_str("\"readwrite\"").expect("legacy");
        assert_eq!(canonical, FolderType::SendReceive);
        assert_eq!(alias, FolderType::SendReceive);
        assert_eq!(alias_long, FolderType::SendReceive);
        assert_eq!(legacy, FolderType::SendReceive);
        assert_eq!(FolderType::ReceiveOnly.as_str(), "recvonly");

        let pull: PullOrder = serde_json::from_str("\"smallestFirst\"").expect("pull order");
        assert_eq!(pull, PullOrder::SmallestFirst);

        let block_random: BlockPullOrder =
            serde_json::from_str("\"random\"").expect("block pull random");
        assert_eq!(block_random, BlockPullOrder::Random);

        let block: BlockPullOrder = serde_json::from_str("\"inOrder\"").expect("block pull order");
        assert_eq!(block, BlockPullOrder::InOrder);

        let method: CopyRangeMethod =
            serde_json::from_str("\"copy_file_range\"").expect("copy range method");
        assert_eq!(method, CopyRangeMethod::CopyFileRange);

        let method_all: CopyRangeMethod = serde_json::from_str("\"all\"").expect("copy range all");
        assert_eq!(method_all, CopyRangeMethod::Standard);
    }
}
