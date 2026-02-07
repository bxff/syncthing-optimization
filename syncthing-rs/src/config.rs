use crate::folder_modes::FolderMode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) const DEFAULT_MARKER_NAME: &str = ".stfolder";
pub(crate) const ENCRYPTION_TOKEN_NAME: &str = "syncthing-encryption_password_token";
pub(crate) const MAX_CONCURRENT_WRITES_DEFAULT: usize = 16;
pub(crate) const MAX_CONCURRENT_WRITES_LIMIT: usize = 256;

pub(crate) const ERR_PATH_NOT_DIRECTORY: &str = "folder path not a directory";
pub(crate) const ERR_PATH_MISSING: &str = "folder path missing";
pub(crate) const ERR_MARKER_MISSING: &str = "folder marker missing";

// Compatibility aliases matching Go surface names used by parity guardrails.
pub(crate) const DefaultMarkerName: &str = DEFAULT_MARKER_NAME;
pub(crate) const EncryptionTokenName: &str = ENCRYPTION_TOKEN_NAME;
pub(crate) const maxConcurrentWritesDefault: usize = MAX_CONCURRENT_WRITES_DEFAULT;
pub(crate) const maxConcurrentWritesLimit: usize = MAX_CONCURRENT_WRITES_LIMIT;
pub(crate) const ErrPathNotDirectory: &str = ERR_PATH_NOT_DIRECTORY;
pub(crate) const ErrPathMissing: &str = ERR_PATH_MISSING;
pub(crate) const ErrMarkerMissing: &str = ERR_MARKER_MISSING;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FolderType {
    SendReceive,
    ReceiveOnly,
    ReceiveEncrypted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PullOrder {
    Random,
    Alphabetic,
    SmallestFirst,
    LargestFirst,
    OldestFirst,
    NewestFirst,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum BlockPullOrder {
    Standard,
    InOrder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CopyRangeMethod {
    Standard,
    AllWithFallback,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FilesystemType {
    Basic,
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
            Self::ReceiveOnly => "recvonly",
            Self::ReceiveEncrypted => "recvenc",
        }
    }

    pub(crate) fn to_mode(self) -> FolderMode {
        match self {
            Self::SendReceive => FolderMode::SendReceive,
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
        let mut hasher = DefaultHasher::new();
        self.id.hash(&mut hasher);
        let h = hasher.finish();
        format!("syncthing-folder-{h:06x}.txt")
    }

    pub(crate) fn marker_contents(&self) -> Vec<u8> {
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_secs();
        format!(
            "# This directory is a Syncthing folder marker.\n# Do not delete.\n\nfolderID: {}\ncreated_unix: {}\n",
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
        if self.marker_name != DEFAULT_MARKER_NAME {
            return Ok(());
        }
        let marker_dir = Path::new(&self.path).join(DEFAULT_MARKER_NAME);
        fs::create_dir_all(&marker_dir).map_err(|err| format!("create marker dir: {err}"))?;
        let marker_file = marker_dir.join(self.marker_filename());
        fs::write(marker_file, self.marker_contents()).map_err(|err| err.to_string())
    }

    pub(crate) fn remove_marker(&self) -> Result<(), String> {
        let marker_dir = Path::new(&self.path).join(DEFAULT_MARKER_NAME);
        let marker_file = marker_dir.join(self.marker_filename());
        let _ = fs::remove_file(marker_file);
        let _ = fs::remove_dir(marker_dir);
        Ok(())
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
                "type": self.folder_type.as_str()
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
        let mut cfg = FolderConfiguration::default();
        cfg.id = extract_xml_attr(data, "id").unwrap_or_default();
        cfg.label = extract_xml_attr(data, "label").unwrap_or_default();
        cfg.path = extract_xml_attr(data, "path").unwrap_or_default();
        cfg.folder_type = match extract_xml_attr(data, "type").as_deref() {
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

    let mut recv_enc = base.clone();
    recv_enc.id = "encrypted".to_string();
    recv_enc.label = "Encrypted".to_string();
    recv_enc.path = "/data/encrypted".to_string();
    recv_enc.folder_type = FolderType::ReceiveEncrypted;
    recv_enc.ignore_perms = true;

    vec![base, recv_only, recv_enc]
}

fn extract_xml_attr(xml: &str, attr: &str) -> Option<String> {
    let pattern = format!("{attr}=\"");
    let start = xml.find(&pattern)? + pattern.len();
    let rest = &xml[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
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
    }
}
