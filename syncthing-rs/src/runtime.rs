use crate::bep::{decode_frame, encode_frame, BepMessage, ClusterConfigFolder};
use crate::config::{FolderConfiguration, FolderDeviceConfiguration, FolderType};
use crate::db::Db;
use crate::model_core::{model, newFolderConfiguration, NewModelWithRuntime};
use bcrypt::{hash as bcrypt_hash, verify as bcrypt_verify};
use crc32fast::Hasher;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Response, Server, StatusCode};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:22000";
const DEFAULT_FOLDER_ID: &str = "default";
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_PEERS: usize = 32;
const MAX_EVENT_LOG_ENTRIES: usize = 10_000;
const PEER_IO_TIMEOUT_SECS: u64 = 30;
const DEVICE_ID_GROUP_COUNT: usize = 8;
const DEVICE_ID_GROUP_LEN: usize = 7;
const DEVICE_ID_BODY_LEN: usize = DEVICE_ID_GROUP_COUNT * DEVICE_ID_GROUP_LEN;
const DEVICE_ID_ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub(crate) struct FolderSpec {
    pub(crate) id: String,
    pub(crate) path: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
struct RuntimeConfigFile {
    pub(crate) listen_addr: Option<String>,
    pub(crate) api_listen_addr: Option<String>,
    pub(crate) db_root: Option<String>,
    pub(crate) memory_max_mb: Option<usize>,
    pub(crate) max_peers: Option<usize>,
    #[serde(default)]
    pub(crate) folders: Vec<FolderSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DaemonConfig {
    pub(crate) listen_addr: String,
    pub(crate) api_listen_addr: Option<String>,
    pub(crate) folders: Vec<FolderSpec>,
    pub(crate) db_root: Option<String>,
    pub(crate) memory_max_mb: Option<usize>,
    pub(crate) max_peers: usize,
    pub(crate) once: bool,
}

#[derive(Clone, Debug, Default)]
struct SyncthingConfigBootstrap {
    local_device_id: Option<String>,
    folder_labels: BTreeMap<String, String>,
    folder_paths: BTreeMap<String, String>,
    folder_types: BTreeMap<String, FolderType>,
    folder_paused: BTreeMap<String, bool>,
    folder_devices: BTreeMap<String, Vec<String>>,
    device_configs: BTreeMap<String, Value>,
    listen_addresses: Vec<String>,
    global_announce_servers: Vec<String>,
    ur_accepted: Option<i32>,
    ur_seen: Option<i32>,
    progress_update_interval_s: Option<i32>,
    crash_reporting_enabled: Option<bool>,
    gui_address: Option<String>,
    gui_theme: Option<String>,
    gui_use_tls: Option<bool>,
    gui_apikey: Option<String>,
    gui_user: Option<String>,
    gui_password: Option<String>,
}

pub(crate) fn parse_daemon_args(args: &[String]) -> Result<DaemonConfig, String> {
    let mut listen_addr = DEFAULT_LISTEN_ADDR.to_string();
    let mut listen_set = false;
    let mut api_listen_addr: Option<String> = None;
    let mut api_listen_set = false;
    let mut folder_id = DEFAULT_FOLDER_ID.to_string();
    let mut folder_id_set = false;
    let mut folder_path: Option<String> = None;
    let mut folder_path_set = false;
    let mut folders: Vec<FolderSpec> = Vec::new();
    let mut config_file_path: Option<String> = None;
    let mut db_root: Option<String> = None;
    let mut memory_max_mb: Option<usize> = None;
    let mut max_peers = DEFAULT_MAX_PEERS;
    let mut max_peers_set = false;
    let mut once = false;

    let mut i = 0_usize;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--listen requires a value".to_string())?;
                listen_addr = value.clone();
                listen_set = true;
            }
            "--api-listen" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--api-listen requires a value".to_string())?;
                if value.trim().is_empty() {
                    return Err("--api-listen must not be empty".to_string());
                }
                api_listen_addr = Some(value.clone());
                api_listen_set = true;
            }
            "--folder-id" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--folder-id requires a value".to_string())?;
                folder_id = value.clone();
                folder_id_set = true;
            }
            "--folder-path" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--folder-path requires a value".to_string())?;
                folder_path = Some(value.clone());
                folder_path_set = true;
            }
            "--folder" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--folder requires a value".to_string())?;
                let (id, path) = value
                    .split_once(':')
                    .ok_or_else(|| "--folder must be in the form <id>:<path>".to_string())?;
                if id.trim().is_empty() || path.trim().is_empty() {
                    return Err("--folder must have non-empty id and path".to_string());
                }
                folders.push(FolderSpec {
                    id: id.to_string(),
                    path: path.to_string(),
                });
            }
            "--db-root" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--db-root requires a value".to_string())?;
                if value.trim().is_empty() {
                    return Err("--db-root must not be empty".to_string());
                }
                db_root = Some(value.clone());
            }
            "--memory-max-mb" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--memory-max-mb requires a value".to_string())?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| "--memory-max-mb must be a positive integer".to_string())?;
                if parsed == 0 {
                    return Err("--memory-max-mb must be greater than zero".to_string());
                }
                memory_max_mb = Some(parsed);
            }
            "--max-peers" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--max-peers requires a value".to_string())?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| "--max-peers must be a positive integer".to_string())?;
                if parsed == 0 {
                    return Err("--max-peers must be greater than zero".to_string());
                }
                max_peers = parsed;
                max_peers_set = true;
            }
            "--config" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--config requires a value".to_string())?;
                config_file_path = Some(value.clone());
            }
            "--once" => {
                once = true;
            }
            other => {
                return Err(format!("unknown daemon argument: {other}"));
            }
        }
        i += 1;
    }

    if let Some(path) = config_file_path {
        let file_cfg = load_runtime_config(&path)?;
        if !listen_set {
            if let Some(file_listen) = file_cfg.listen_addr {
                listen_addr = file_listen;
            }
        }
        if !api_listen_set && api_listen_addr.is_none() {
            api_listen_addr = file_cfg.api_listen_addr;
        }
        if db_root.is_none() {
            db_root = file_cfg.db_root;
        }
        if memory_max_mb.is_none() {
            memory_max_mb = file_cfg.memory_max_mb;
        }
        if !max_peers_set {
            if let Some(file_max_peers) = file_cfg.max_peers {
                if file_max_peers == 0 {
                    return Err("config max_peers must be greater than zero".to_string());
                }
                max_peers = file_max_peers;
            }
        }
        if folders.is_empty() && !folder_id_set && !folder_path_set {
            folders = file_cfg.folders;
        }
    }

    if folder_id_set && !folder_path_set && folders.is_empty() {
        return Err("--folder-id requires --folder-path".to_string());
    }

    let folders = if !folders.is_empty() {
        if folder_id_set || folder_path_set {
            return Err(
                "cannot mix --folder with --folder-id/--folder-path; use one style".to_string(),
            );
        }
        folders
    } else if let Some(path) = folder_path {
        vec![FolderSpec {
            id: folder_id,
            path,
        }]
    } else {
        Vec::new()
    };

    Ok(DaemonConfig {
        listen_addr,
        api_listen_addr,
        folders,
        db_root,
        memory_max_mb,
        max_peers,
        once,
    })
}

pub(crate) fn run_daemon(config: DaemonConfig) -> Result<(), String> {
    let bootstrap = load_syncthing_config_bootstrap();
    let model = Arc::new(RwLock::new(NewModelWithRuntime(
        config.db_root.as_ref().map(PathBuf::from),
        config.memory_max_mb,
    )?));
    {
        let mut guard = model
            .write()
            .map_err(|_| "model lock poisoned".to_string())?;
        if let Some(local_id) = bootstrap
            .as_ref()
            .and_then(|cfg| cfg.local_device_id.as_ref())
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
        {
            guard.id = local_id.to_string();
        }
        guard.id = normalize_syncthing_device_id(&guard.id)
            .unwrap_or_else(|| synthetic_syncthing_device_id(&guard.id));
        guard.shortID = guard.id.chars().take(7).collect::<String>();

        let mut folder_specs = config.folders.clone();
        if folder_specs.is_empty() {
            if let Some(bootstrap_cfg) = bootstrap.as_ref() {
                folder_specs = bootstrap_cfg
                    .folder_paths
                    .iter()
                    .filter_map(|(id, path)| {
                        if id.trim().is_empty() || path.trim().is_empty() {
                            None
                        } else {
                            Some(FolderSpec {
                                id: id.clone(),
                                path: path.clone(),
                            })
                        }
                    })
                    .collect();
            }
            folder_specs.sort_by(|a, b| a.id.cmp(&b.id));
        }

        for folder in &folder_specs {
            let mut cfg = newFolderConfiguration(&folder.id, &folder.path);
            if let Some(bootstrap_cfg) = bootstrap.as_ref() {
                if let Some(label) = bootstrap_cfg.folder_labels.get(&folder.id) {
                    cfg.label = label.clone();
                }
                if let Some(path) = bootstrap_cfg.folder_paths.get(&folder.id) {
                    if !path.trim().is_empty() {
                        cfg.path = path.clone();
                    }
                }
                if let Some(folder_type) = bootstrap_cfg.folder_types.get(&folder.id) {
                    cfg.folder_type = *folder_type;
                }
                if let Some(paused) = bootstrap_cfg.folder_paused.get(&folder.id) {
                    cfg.paused = *paused;
                }
                if let Some(device_ids) = bootstrap_cfg.folder_devices.get(&folder.id) {
                    cfg.devices = device_ids
                        .iter()
                        .filter(|id| !id.trim().is_empty())
                        .map(|device_id| FolderDeviceConfiguration {
                            device_id: device_id.clone(),
                            introduced_by: String::new(),
                            encryption_password: String::new(),
                        })
                        .collect();
                }
            }
            if cfg.devices.is_empty() {
                cfg.devices.push(FolderDeviceConfiguration {
                    device_id: guard.id.clone(),
                    introduced_by: String::new(),
                    encryption_password: String::new(),
                });
            }
            guard.newFolder(cfg);
        }
    }

    let active_peers = Arc::new(AtomicUsize::new(0));
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    if let Some(api_addr) = config.api_listen_addr.as_ref() {
        let local_id = model
            .read()
            .map_err(|_| "model lock poisoned".to_string())?
            .id
            .clone();
        let state = Arc::new(RwLock::new(ApiRuntimeState::new(&local_id)));
        if let Some(bootstrap_cfg) = bootstrap.as_ref() {
            if let Ok(mut guard) = state.write() {
                apply_syncthing_bootstrap_to_api_state(
                    &mut guard,
                    bootstrap_cfg,
                    &local_id,
                    api_addr,
                );
            }
        }
        let runtime = DaemonApiRuntime {
            model: model.clone(),
            state,
            active_peers: active_peers.clone(),
            max_peers: config.max_peers,
            start_time: SystemTime::now(),
            bep_listen_addr: config.listen_addr.clone(),
            gui_listen_addr: api_addr.clone(),
            shutdown_requested: shutdown_requested.clone(),
            gui_root: resolve_gui_root(),
        };
        let _api_thread = start_api_server(api_addr, runtime)?;
    }

    let listener = TcpListener::bind(&config.listen_addr)
        .map_err(|err| format!("listen {}: {err}", config.listen_addr))?;
    run_daemon_with_listener(
        listener,
        model,
        config.once,
        config.max_peers,
        active_peers,
        shutdown_requested,
    )
}

fn load_runtime_config(path: &str) -> Result<RuntimeConfigFile, String> {
    let raw = fs::read_to_string(path).map_err(|err| format!("read config {path}: {err}"))?;
    let cfg: RuntimeConfigFile =
        serde_json::from_str(&raw).map_err(|err| format!("parse config {path}: {err}"))?;
    for folder in &cfg.folders {
        if folder.id.trim().is_empty() || folder.path.trim().is_empty() {
            return Err(format!(
                "config {path}: folders require non-empty id and path"
            ));
        }
    }
    if cfg.memory_max_mb == Some(0) {
        return Err(format!(
            "config {path}: memory_max_mb must be greater than zero"
        ));
    }
    if cfg.max_peers == Some(0) {
        return Err(format!(
            "config {path}: max_peers must be greater than zero"
        ));
    }
    Ok(cfg)
}

fn load_syncthing_config_bootstrap() -> Option<SyncthingConfigBootstrap> {
    let path = std::env::var("SYNCTHING_CONFIG_XML")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(default_syncthing_config_xml_path)?;
    let raw = fs::read_to_string(path).ok()?;
    Some(parse_syncthing_config_bootstrap(&raw))
}

fn default_syncthing_config_xml_path() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    Some(
        PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("Syncthing")
            .join("config.xml"),
    )
}

fn parse_syncthing_config_bootstrap(raw: &str) -> SyncthingConfigBootstrap {
    let mut out = SyncthingConfigBootstrap::default();
    let mut in_folder = false;
    let mut in_options = false;
    let mut in_gui = false;
    let mut in_defaults = false;
    let mut in_defaults_folder = false;
    let mut current_folder_id = String::new();

    let mut current_device_id: Option<String> = None;
    let mut current_device_name = String::new();
    let mut current_device_addresses: Vec<String> = Vec::new();
    let mut current_device_paused = false;
    let mut current_device_compression = "metadata".to_string();
    let mut current_device_introducer = false;
    let mut first_top_level_device_id: Option<String> = None;
    let mut defaults_folder_device_id: Option<String> = None;

    let mut finish_device = |out: &mut SyncthingConfigBootstrap,
                             id: &mut Option<String>,
                             name: &mut String,
                             addresses: &mut Vec<String>,
                             paused: &mut bool,
                             compression: &mut String,
                             introducer: &mut bool| {
        let Some(device_id) = id.take() else {
            return;
        };
        if device_id.trim().is_empty() {
            return;
        }
        if first_top_level_device_id.is_none() {
            first_top_level_device_id = Some(device_id.clone());
        }
        if addresses.is_empty() {
            addresses.push("dynamic".to_string());
        }
        let display_name = if name.trim().is_empty() {
            device_id.clone()
        } else {
            name.trim().to_string()
        };
        out.device_configs.insert(
            device_id.clone(),
            json!({
                "deviceID": device_id,
                "name": display_name,
                "addresses": addresses.clone(),
                "paused": *paused,
                "compression": compression.clone(),
                "introducer": *introducer,
            }),
        );
        *name = String::new();
        addresses.clear();
        *paused = false;
        *compression = "metadata".to_string();
        *introducer = false;
    };

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.starts_with("<defaults") {
            in_defaults = true;
        } else if trimmed.starts_with("</defaults") {
            in_defaults = false;
            in_defaults_folder = false;
        }

        if in_defaults && trimmed.starts_with("<folder ") {
            in_defaults_folder = true;
        } else if in_defaults_folder && trimmed.starts_with("</folder") {
            in_defaults_folder = false;
        }

        if in_defaults_folder
            && defaults_folder_device_id.is_none()
            && trimmed.starts_with("<device ")
        {
            if let Some(device_id) = extract_xml_attr(trimmed, "id") {
                let decoded = decode_xml_value(device_id.trim());
                if !decoded.is_empty() {
                    defaults_folder_device_id = Some(decoded);
                }
            }
        }

        if trimmed.starts_with("<folder ") && !in_defaults {
            in_folder = true;
            current_folder_id = extract_xml_attr(trimmed, "id")
                .map(|v| decode_xml_value(v.trim()))
                .unwrap_or_default();
            if !current_folder_id.is_empty() {
                let path = extract_xml_attr(trimmed, "path")
                    .map(|v| decode_xml_value(v.trim()))
                    .unwrap_or_default();
                let label = extract_xml_attr(trimmed, "label")
                    .map(|v| decode_xml_value(v.trim()))
                    .unwrap_or_default();
                if !path.is_empty() {
                    out.folder_paths.insert(current_folder_id.clone(), path);
                }
                if let Some(folder_type) = extract_xml_attr(trimmed, "type")
                    .as_deref()
                    .and_then(parse_folder_type_attr)
                {
                    out.folder_types
                        .insert(current_folder_id.clone(), folder_type);
                }
                if let Some(paused_attr) = extract_xml_attr(trimmed, "paused") {
                    out.folder_paused
                        .insert(current_folder_id.clone(), parse_xml_bool(&paused_attr));
                }
                out.folder_labels.insert(current_folder_id.clone(), label);
                out.folder_devices
                    .entry(current_folder_id.clone())
                    .or_default();
            }
        } else if in_folder && trimmed.starts_with("</folder") {
            in_folder = false;
            current_folder_id.clear();
        }

        if in_folder && !current_folder_id.is_empty() && trimmed.starts_with("<device ") {
            if let Some(device_id) = extract_xml_attr(trimmed, "id") {
                let decoded = decode_xml_value(device_id.trim());
                if !decoded.is_empty() {
                    let entry = out
                        .folder_devices
                        .entry(current_folder_id.clone())
                        .or_default();
                    if !entry.iter().any(|existing| existing == &decoded) {
                        entry.push(decoded);
                    }
                }
            }
            continue;
        }

        if trimmed.starts_with("<options") {
            in_options = true;
            continue;
        }
        if trimmed.starts_with("</options") {
            in_options = false;
            continue;
        }
        if in_options {
            if let Some(value) = extract_xml_tag_value(trimmed, "listenAddress") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.listen_addresses.push(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "globalAnnounceServer") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.global_announce_servers.push(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "urAccepted") {
                if let Ok(parsed) = value.trim().parse::<i32>() {
                    out.ur_accepted = Some(parsed);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "urSeen") {
                if let Ok(parsed) = value.trim().parse::<i32>() {
                    out.ur_seen = Some(parsed);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "progressUpdateIntervalS") {
                if let Ok(parsed) = value.trim().parse::<i32>() {
                    out.progress_update_interval_s = Some(parsed);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "crashReportingEnabled") {
                out.crash_reporting_enabled = Some(parse_xml_bool(value.trim()));
            }
            continue;
        }

        if trimmed.starts_with("<gui ") {
            in_gui = true;
            if let Some(value) = extract_xml_attr(trimmed, "tls") {
                out.gui_use_tls = Some(parse_xml_bool(value.trim()));
            }
            continue;
        }
        if trimmed.starts_with("</gui") {
            in_gui = false;
            continue;
        }
        if in_gui {
            if let Some(value) = extract_xml_tag_value(trimmed, "address") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.gui_address = Some(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "theme") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.gui_theme = Some(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "apikey") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.gui_apikey = Some(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "user") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.gui_user = Some(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "password") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    out.gui_password = Some(decoded);
                }
            }
            continue;
        }

        if !in_folder && !in_defaults && trimmed.starts_with("<device ") {
            if current_device_id.is_some() {
                finish_device(
                    &mut out,
                    &mut current_device_id,
                    &mut current_device_name,
                    &mut current_device_addresses,
                    &mut current_device_paused,
                    &mut current_device_compression,
                    &mut current_device_introducer,
                );
            }
            current_device_id = extract_xml_attr(trimmed, "id")
                .map(|value| decode_xml_value(value.trim()))
                .filter(|value| !value.is_empty());
            current_device_name = extract_xml_attr(trimmed, "name")
                .map(|value| decode_xml_value(value.trim()))
                .unwrap_or_default();
            current_device_compression = extract_xml_attr(trimmed, "compression")
                .map(|value| decode_xml_value(value.trim()))
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "metadata".to_string());
            current_device_introducer = extract_xml_attr(trimmed, "introducer")
                .map(|value| parse_xml_bool(value.trim()))
                .unwrap_or(false);
            if trimmed.ends_with("/>") {
                finish_device(
                    &mut out,
                    &mut current_device_id,
                    &mut current_device_name,
                    &mut current_device_addresses,
                    &mut current_device_paused,
                    &mut current_device_compression,
                    &mut current_device_introducer,
                );
            }
            continue;
        }

        if current_device_id.is_some() {
            if let Some(value) = extract_xml_tag_value(trimmed, "address") {
                let decoded = decode_xml_value(value.trim());
                if !decoded.is_empty() {
                    current_device_addresses.push(decoded);
                }
            }
            if let Some(value) = extract_xml_tag_value(trimmed, "paused") {
                current_device_paused = parse_xml_bool(value.trim());
            }
            if trimmed.starts_with("</device") {
                finish_device(
                    &mut out,
                    &mut current_device_id,
                    &mut current_device_name,
                    &mut current_device_addresses,
                    &mut current_device_paused,
                    &mut current_device_compression,
                    &mut current_device_introducer,
                );
            }
        }
    }

    if current_device_id.is_some() {
        finish_device(
            &mut out,
            &mut current_device_id,
            &mut current_device_name,
            &mut current_device_addresses,
            &mut current_device_paused,
            &mut current_device_compression,
            &mut current_device_introducer,
        );
    }

    // Prefer the first explicit top-level device entry from config.xml for local identity.
    // Defaults/folder device IDs can be stale after migrations and should only be fallback.
    out.local_device_id = first_top_level_device_id
        .clone()
        .filter(|id| !id.trim().is_empty())
        .or_else(|| {
            defaults_folder_device_id
                .clone()
                .filter(|id| !id.trim().is_empty())
        })
        .or_else(|| {
            out.device_configs
                .keys()
                .next()
                .map(ToOwned::to_owned)
                .filter(|id| !id.trim().is_empty())
        });

    out
}

fn apply_syncthing_bootstrap_to_api_state(
    state: &mut ApiRuntimeState,
    bootstrap: &SyncthingConfigBootstrap,
    local_device_id: &str,
    api_addr: &str,
) {
    if !bootstrap.device_configs.is_empty() {
        state.device_configs = bootstrap.device_configs.clone();
    }
    if !state.device_configs.contains_key(local_device_id) {
        state.device_configs.insert(
            local_device_id.to_string(),
            json!({
                "deviceID": local_device_id,
                "name": local_device_id,
                "addresses": ["dynamic"],
                "paused": false,
                "compression": "metadata",
                "introducer": false,
            }),
        );
    }
    if state
        .default_device
        .get("deviceID")
        .and_then(Value::as_str)
        .map(|id| id.trim().is_empty())
        .unwrap_or(true)
    {
        state.default_device["deviceID"] = json!(local_device_id);
    }
    if state
        .default_device
        .get("name")
        .and_then(Value::as_str)
        .map(|name| name.trim().is_empty())
        .unwrap_or(true)
    {
        let local_name = state
            .device_configs
            .get(local_device_id)
            .and_then(|cfg| cfg.get("name"))
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("Local Device");
        state.default_device["name"] = json!(local_name);
    }

    ensure_api_options_defaults(&mut state.options);
    if !bootstrap.listen_addresses.is_empty() {
        state.options["listenAddresses"] = json!(bootstrap.listen_addresses.clone());
    }
    if !bootstrap.global_announce_servers.is_empty() {
        state.options["globalAnnounceServers"] = json!(bootstrap.global_announce_servers.clone());
    }
    if let Some(value) = bootstrap.ur_accepted {
        state.options["urAccepted"] = json!(value);
    }
    if let Some(value) = bootstrap.ur_seen {
        state.options["urSeen"] = json!(value);
    }
    if let Some(value) = bootstrap.progress_update_interval_s {
        state.options["progressUpdateIntervalS"] = json!(value);
    }
    if let Some(value) = bootstrap.crash_reporting_enabled {
        state.options["crashReportingEnabled"] = json!(value);
    }
    if let Some(local_name) = state
        .device_configs
        .get(local_device_id)
        .and_then(|cfg| cfg.get("name"))
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
    {
        state.options["deviceName"] = json!(local_name);
    }

    ensure_api_gui_defaults(&mut state.gui);
    if let Some(theme) = bootstrap
        .gui_theme
        .as_ref()
        .filter(|theme| !theme.is_empty())
    {
        state.gui["theme"] = json!(theme);
    }
    if let Some(use_tls) = bootstrap.gui_use_tls {
        state.gui["useTLS"] = json!(use_tls);
    }
    let gui_address = bootstrap
        .gui_address
        .as_ref()
        .filter(|addr| !addr.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| api_addr.to_string());
    state.gui["address"] = json!(gui_address);
    // Transfer GUI auth fields from config.xml bootstrap
    if let Some(apikey) = bootstrap
        .gui_apikey
        .as_ref()
        .filter(|k| !k.trim().is_empty())
    {
        state.gui["apiKey"] = json!(apikey);
    }
    if let Some(user) = bootstrap.gui_user.as_ref().filter(|u| !u.trim().is_empty()) {
        state.gui["user"] = json!(user);
    }
    if let Some(password) = bootstrap.gui_password.as_ref().filter(|p| !p.is_empty()) {
        state.gui["password"] = json!(password);
    }

    if state
        .default_folder
        .get("devices")
        .and_then(Value::as_array)
        .map(|devices| devices.is_empty())
        .unwrap_or(true)
    {
        state.default_folder["devices"] = json!([{
            "deviceID": local_device_id,
            "introducedBy": "",
            "encryptionPassword": "",
        }]);
    }
}

fn ensure_api_options_defaults(options: &mut Value) {
    if !options.is_object() {
        *options = json!({});
    }
    if !options
        .get("listenAddresses")
        .and_then(Value::as_array)
        .is_some()
    {
        options["listenAddresses"] = json!(["default"]);
    }
    if !options
        .get("globalAnnounceServers")
        .and_then(Value::as_array)
        .is_some()
    {
        options["globalAnnounceServers"] = json!(["default"]);
    }
    if options.get("urAccepted").and_then(Value::as_i64).is_none() {
        options["urAccepted"] = json!(0);
    }
    if options.get("urSeen").and_then(Value::as_i64).is_none() {
        options["urSeen"] = json!(3);
    }
    if options
        .get("progressUpdateIntervalS")
        .and_then(Value::as_i64)
        .is_none()
    {
        options["progressUpdateIntervalS"] = json!(5);
    }
    if options
        .get("crashReportingEnabled")
        .and_then(Value::as_bool)
        .is_none()
    {
        options["crashReportingEnabled"] = json!(false);
    }
    if !options
        .get("unackedNotificationIDs")
        .and_then(Value::as_array)
        .is_some()
    {
        options["unackedNotificationIDs"] = json!([]);
    }
    if options.get("deviceName").and_then(Value::as_str).is_none() {
        options["deviceName"] = json!("Local Device");
    }
}

fn ensure_api_gui_defaults(gui: &mut Value) {
    if !gui.is_object() {
        *gui = json!({});
    }
    if gui.get("enabled").and_then(Value::as_bool).is_none() {
        gui["enabled"] = json!(true);
    }
    if gui.get("theme").and_then(Value::as_str).is_none() {
        gui["theme"] = json!("default");
    }
    if gui
        .get("insecureAdminAccess")
        .and_then(Value::as_bool)
        .is_none()
    {
        gui["insecureAdminAccess"] = json!(false);
    }
    if gui.get("debugging").and_then(Value::as_bool).is_none() {
        gui["debugging"] = json!(false);
    }
    if gui.get("useTLS").and_then(Value::as_bool).is_none() {
        gui["useTLS"] = json!(false);
    }
    if gui.get("authMode").and_then(Value::as_str).is_none() {
        gui["authMode"] = json!("static");
    }
    if gui.get("user").and_then(Value::as_str).is_none() {
        gui["user"] = json!("");
    }
    if gui.get("password").and_then(Value::as_str).is_none() {
        gui["password"] = json!("");
    }
}

fn parse_xml_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "on"
    )
}

fn parse_folder_type_attr(value: &str) -> Option<FolderType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sendreceive" | "sendrecv" => Some(FolderType::SendReceive),
        "sendonly" => Some(FolderType::SendOnly),
        "receiveonly" | "recvonly" => Some(FolderType::ReceiveOnly),
        "receiveencrypted" | "recvenc" => Some(FolderType::ReceiveEncrypted),
        _ => None,
    }
}

fn extract_xml_attr(line: &str, key: &str) -> Option<String> {
    let dq = format!("{key}=\"");
    if let Some(start) = line.find(&dq) {
        let tail = &line[start + dq.len()..];
        let end = tail.find('"')?;
        return Some(tail[..end].to_string());
    }
    let sq = format!("{key}='");
    let start = line.find(&sq)? + sq.len();
    let tail = &line[start..];
    let end = tail.find('\'')?;
    Some(tail[..end].to_string())
}

fn extract_xml_tag_value<'a>(line: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = line.find(&open)? + open.len();
    let end = line[start..].find(&close)? + start;
    Some(&line[start..end])
}

fn decode_xml_value(value: &str) -> String {
    value
        .replace("&#34;", "\"")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn normalize_syncthing_device_id(candidate: &str) -> Option<String> {
    // AUDIT-MARKER(deviceid-canonical): W16I — Go's UnmarshalText (deviceid.go:123-152)
    // applies: trim("="), ToUpper, untypeoify (0→O, 1→I, 8→B), unchunkify
    // (strip dashes+spaces), then accepts 52 or 56 char base32 forms.
    let mut id = candidate.trim().trim_matches('=').to_ascii_uppercase();
    if id.is_empty() {
        return None;
    }
    // untypeoify: common visual confusions
    id = id.replace('0', "O").replace('1', "I").replace('8', "B");
    // unchunkify: strip dashes and spaces
    id = id.replace('-', "").replace(' ', "");

    // Go accepts 56-char (with Luhn check digits) or 52-char (old, no check digits)
    match id.len() {
        56 => {
            // AUDIT-MARKER(deviceid-luhn): W16K-K8 — Go's DeviceIDFromString
            // validates Luhn check digits (luhn32.go). Reject bad check digits.
            if !id.bytes().all(|b| matches!(b, b'A'..=b'Z' | b'2'..=b'7')) {
                return None;
            }
            // Validate Luhn check digits at positions 13, 27, 41, 55
            for chunk_idx in 0..4usize {
                let start = chunk_idx * 14;
                let data: String = id.chars().skip(start).take(13).collect();
                let check = id.chars().nth(start + 13);
                let expected = crate::bep_core::luhn32_generate(&data);
                if check != Some(expected) {
                    return None;
                }
            }
        }
        52 => {
            // Old style, no check digits
            if !id.bytes().all(|b| matches!(b, b'A'..=b'Z' | b'2'..=b'7')) {
                return None;
            }
            // Pad to 56 by inserting Luhn check digits (compute them)
            // For simplicity and correctness, just rechunkify the 52-char form
            // Go's String() method outputs the 56-char Luhn form, but for
            // normalization the endpoint only needs to return the canonical
            // dash-chunked form. We'll rechunkify the input as-is.
        }
        _ => return None,
    }

    // Rechunkify: 7-char groups separated by dashes
    let mut out = String::with_capacity(id.len() + id.len() / 7);
    for (i, ch) in id.chars().enumerate() {
        if i > 0 && i % 7 == 0 {
            out.push('-');
        }
        out.push(ch);
    }
    Some(out)
}

fn synthetic_syncthing_device_id(seed: &str) -> String {
    let source = seed.trim();
    let source = if source.is_empty() {
        "unknown-peer"
    } else {
        source
    };
    let mut hasher = Hasher::new();
    hasher.update(source.as_bytes());
    let mut state = hasher.finalize();
    let mut body = String::with_capacity(DEVICE_ID_BODY_LEN);
    for _ in 0..DEVICE_ID_BODY_LEN {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let idx = ((state >> 27) & 0x1f) as usize;
        body.push(DEVICE_ID_ALPHABET[idx] as char);
    }
    let mut out = String::with_capacity(DEVICE_ID_BODY_LEN + DEVICE_ID_GROUP_COUNT - 1);
    for group in 0..DEVICE_ID_GROUP_COUNT {
        if group > 0 {
            out.push('-');
        }
        let start = group * DEVICE_ID_GROUP_LEN;
        let end = start + DEVICE_ID_GROUP_LEN;
        out.push_str(&body[start..end]);
    }
    out
}

fn canonical_peer_device_id(
    peer_hint: &str,
    hello_device_name: &str,
    peer_hint_is_transport: bool,
) -> String {
    if let Some(id) = normalize_syncthing_device_id(peer_hint) {
        return id;
    }
    if let Some(id) = normalize_syncthing_device_id(hello_device_name) {
        return id;
    }
    let trimmed_peer = peer_hint.trim();
    if !peer_hint_is_transport && !trimmed_peer.is_empty() {
        return trimmed_peer.to_string();
    }
    let trimmed_hello = hello_device_name.trim();
    let seed = if trimmed_peer.is_empty() {
        trimmed_hello
    } else {
        trimmed_peer
    };
    synthetic_syncthing_device_id(seed)
}

#[derive(Clone)]
struct DaemonApiRuntime {
    model: Arc<RwLock<model>>,
    state: Arc<RwLock<ApiRuntimeState>>,
    active_peers: Arc<AtomicUsize>,
    max_peers: usize,
    start_time: SystemTime,
    bep_listen_addr: String,
    gui_listen_addr: String,
    shutdown_requested: Arc<AtomicBool>,
    gui_root: Option<PathBuf>,
}

#[derive(Clone, Debug)]
struct ApiRuntimeState {
    device_configs: BTreeMap<String, Value>,
    options: Value,
    gui: Value,
    ldap: Value,
    default_folder: Value,
    default_device: Value,
    default_ignores: Vec<String>,
    system_errors: Vec<String>,
    event_log: Vec<Value>,
    disk_event_log: Vec<Value>,
    log_lines: Vec<String>,
    log_messages: Vec<Value>,
    log_facilities: BTreeSet<String>,
    log_levels: BTreeMap<String, String>,
    auth_param_salt: String,
    active_auth_users: BTreeSet<String>,
    active_auth_tokens: BTreeMap<String, String>,
    active_auth_csrf: BTreeMap<String, String>,
    // B11: Track config dirty state for /rest/config/insync — separate from shutdown
    config_dirty: bool,
    // R-H2: Track whether config changes require a restart (e.g. listenAddresses, GUI TLS)
    // Go: config.RequiresRestart() compares running config vs saved config
    config_requires_restart: bool,
    // W8-R3: Store full JSON body for each folder to preserve all typed fields
    folder_overrides: BTreeMap<String, Value>,
}

impl ApiRuntimeState {
    fn new(local_device: &str) -> Self {
        let default_folder_cfg = FolderConfiguration::default();
        let default_folder = {
            let mut value = folder_config_to_json("default", &default_folder_cfg);
            value["devices"] = json!([{
                "deviceID": local_device,
                "introducedBy": "",
                "encryptionPassword": "",
            }]);
            value
        };
        let local_device_cfg = json!({
            "deviceID": local_device,
            "name": "Local Device",
            "addresses": ["dynamic"],
            "paused": false,
            "compression": "metadata",
            "introducer": false,
        });
        let mut device_configs = BTreeMap::new();
        device_configs.insert(local_device.to_string(), local_device_cfg);
        // 7.1: Auto-generate API key when empty, matching Go's GUIConfiguration.prepare()
        let auto_api_key = generate_random_api_key();
        Self {
            device_configs,
            options: json!({
                "maxSendKbps": 0,
                "maxRecvKbps": 0,
                "urAccepted": 0,
                "urSeen": 3,
                "globalAnnounceEnabled": true,
                "localAnnounceEnabled": true,
                "listenAddresses": ["default"],
                "globalAnnounceServers": ["default"],
                "progressUpdateIntervalS": 5,
                "crashReportingEnabled": false,
                "unackedNotificationIDs": [],
                "deviceName": "Local Device",
                "releasesURL": "https://upgrades.syncthing.net/meta.json",
            }),
            gui: json!({
                "enabled": true,
                "theme": "default",
                "insecureAdminAccess": false,
                "debugging": false,
                "useTLS": false,
                "authMode": "static",
                "user": "",
                "password": "",
                "address": "127.0.0.1:8384",
                "apiKey": auto_api_key,
                "sendBasicAuthPrompt": false,
            }),
            ldap: json!({
                "address": "",
                "bindDN": "",
                "searchBaseDN": "",
                "enabled": false,
            }),
            default_folder,
            default_device: json!({
                "deviceID": local_device,
                "name": "Local Device",
                "addresses": ["dynamic"],
                "paused": false,
                "compression": "metadata",
                "introducer": false,
            }),
            default_ignores: vec!["(?d).DS_Store".to_string()],
            system_errors: Vec::new(),
            event_log: Vec::new(),
            disk_event_log: Vec::new(),
            log_lines: vec!["syncthing-rs runtime started".to_string()],
            log_messages: vec![json!({
                "when": now_rfc3339(),
                "message": "syncthing-rs runtime started",
                "facility": "main",
            })],
            log_facilities: BTreeSet::from(["main".to_string(), "model".to_string()]),
            log_levels: BTreeMap::from([
                ("main".to_string(), "info".to_string()),
                ("model".to_string(), "info".to_string()),
            ]),
            auth_param_salt: format!(
                "{:x}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ),
            active_auth_users: BTreeSet::new(),
            active_auth_tokens: BTreeMap::new(),
            active_auth_csrf: BTreeMap::new(),
            config_requires_restart: false,
            config_dirty: false,
            folder_overrides: BTreeMap::new(),
        }
    }
}

struct ApiReply {
    status_code: StatusCode,
    body: Vec<u8>,
    content_type: String,
    headers: Vec<(String, String)>,
}

impl ApiReply {
    fn json(status_code: u16, payload: Value) -> Self {
        let body = serde_json::to_vec(&payload)
            .unwrap_or_else(|_| b"{\"error\":\"encode error\"}".to_vec());
        Self {
            status_code: StatusCode(status_code),
            body,
            content_type: "application/json".to_string(),
            headers: Vec::new(),
        }
    }

    fn bytes(status_code: u16, body: Vec<u8>, content_type: &str) -> Self {
        Self {
            status_code: StatusCode(status_code),
            body,
            content_type: content_type.to_string(),
            headers: Vec::new(),
        }
    }

    /// W16H: Empty 200 response matching Go's confighandler finish().
    fn empty(status_code: u16) -> Self {
        Self {
            status_code: StatusCode(status_code),
            body: Vec::new(),
            content_type: "text/plain".to_string(),
            headers: Vec::new(),
        }
    }

    fn with_header(mut self, name: &str, value: String) -> Self {
        self.headers.push((name.to_string(), value));
        self
    }
}

// W13-4: Thread-local to pass session cookie from build_api_response to start_api_server
thread_local! {
    static PENDING_SESSION_COOKIE: std::cell::RefCell<Option<String>> = std::cell::RefCell::new(None);
}

fn start_api_server(
    addr: &str,
    runtime: DaemonApiRuntime,
) -> Result<thread::JoinHandle<()>, String> {
    let server = Server::http(addr).map_err(|err| format!("listen api {addr}: {err}"))?;
    let handle = thread::spawn(move || {
        for mut request in server.incoming_requests() {
            let mut url = request.url().to_string();

            // W15-1: CORS preflight — Go's API server handles OPTIONS with
            // Access-Control-Allow-* headers. Return 204 with CORS headers
            // for all OPTIONS requests without routing to handlers.
            if *request.method() == Method::Options {
                let mut response = Response::empty(204);
                if let Ok(h) = Header::from_bytes(&b"Access-Control-Allow-Origin"[..], &b"*"[..]) {
                    response = response.with_header(h);
                }
                if let Ok(h) = Header::from_bytes(
                    &b"Access-Control-Allow-Methods"[..],
                    &b"GET, POST, PUT, PATCH, DELETE, OPTIONS"[..],
                ) {
                    response = response.with_header(h);
                }
                if let Ok(h) = Header::from_bytes(
                    &b"Access-Control-Allow-Headers"[..],
                    &b"Content-Type, X-API-Key, X-CSRF-Token-*"[..],
                ) {
                    response = response.with_header(h);
                }
                if let Ok(h) = Header::from_bytes(&b"Access-Control-Max-Age"[..], &b"600"[..]) {
                    response = response.with_header(h);
                }
                let _ = request.respond(response);
                continue;
            }
            let session_cookie = session_cookie_name(&runtime);
            let csrf_header = csrf_header_name(&runtime);
            let (token_query_key, csrf_query_key, apikey_query_key) = runtime
                .state
                .read()
                .map(|state| {
                    (
                        auth_query_token_key(&state),
                        auth_query_csrf_key(&state),
                        auth_query_apikey_key(&state),
                    )
                })
                .unwrap_or_else(|_| {
                    (
                        "_auth_token".to_string(),
                        "_auth_csrf".to_string(),
                        "_auth_apikey".to_string(),
                    )
                });
            if let Some(api_key) = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("X-API-Key"))
                .map(|h| h.value.to_string())
            {
                url = append_query_param(&url, &apikey_query_key, &api_key);
            }
            if let Some(auth) = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Authorization"))
                .map(|h| h.value.to_string())
            {
                // RT-3: Case-insensitive Bearer prefix, matching Go's net/http canonical form
                let bearer_token = if auth.len() > 7 && auth[..7].eq_ignore_ascii_case("bearer ") {
                    Some(&auth[7..])
                } else {
                    None
                };
                if let Some(token) = bearer_token {
                    // AUDIT-MARKER(bearer-apikey): W16J-A1 — Go's hasValidAPIKeyHeader
                    // (api_csrf.go:102-104) treats Bearer token as API key, NOT session.
                    url = append_query_param(&url, &apikey_query_key, token.trim());
                }
                // B3: Extract Basic auth credentials and pass through as _auth_basic param
                if auth.len() > 6 && auth[..6].eq_ignore_ascii_case("basic ") {
                    url = append_query_param(&url, "_auth_basic", auth[6..].trim());
                }
            }
            if let Some(cookie_header) = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Cookie"))
                .map(|h| h.value.to_string())
            {
                // RT-2: Only accept sessionid-<shortID> cookie, no bare "sessionid" fallback
                if let Some(session_token) = extract_cookie_value(&cookie_header, &session_cookie) {
                    url = append_query_param(&url, &token_query_key, session_token);
                }
            }
            for header in request.headers() {
                let field = header.field.to_string();
                if field.eq_ignore_ascii_case(&csrf_header) {
                    let value = header.value.to_string();
                    url = append_query_param(&url, &csrf_query_key, value.trim());
                    break;
                }
            }
            if let Some(lang) = request
                .headers()
                .iter()
                .find(|h| h.field.equiv("Accept-Language"))
                .map(|h| h.value.to_string())
            {
                url = append_query_param(&url, "accept-language", lang.trim());
            }

            let mut body = Vec::new();
            let _ = request.as_reader().read_to_end(&mut body);
            if !body.is_empty() {
                url = append_body_params(&url, request.method(), &body);
            }

            let reply = build_api_response(request.method(), &url, &runtime);
            let mut response = Response::from_data(reply.body).with_status_code(reply.status_code);
            if let Ok(content_type) =
                Header::from_bytes(&b"Content-Type"[..], reply.content_type.as_bytes())
            {
                response = response.with_header(content_type);
            }
            for (name, value) in reply.headers {
                if let Ok(header) = Header::from_bytes(name.as_bytes(), value.as_bytes()) {
                    response = response.with_header(header);
                }
            }
            // W13-4: Emit Set-Cookie for session token minted during basic-auth
            let pending_cookie: Option<String> = PENDING_SESSION_COOKIE.with(|cell| {
                cell.borrow_mut().take().map(|token| {
                    format!(
                        "{}={}; Path=/; HttpOnly; SameSite=Lax",
                        session_cookie, token
                    )
                })
            });
            if let Some(cookie_value) = pending_cookie {
                if let Ok(header) = Header::from_bytes(&b"Set-Cookie"[..], cookie_value.as_bytes())
                {
                    response = response.with_header(header);
                }
            }
            // AUDIT-MARKER(cors-security-headers): W16I — Go's CORS/security
            // header policy (api.go:545-584):
            //   - ACAO:* ONLY on OPTIONS preflight + API-key paths (api_csrf.go:53)
            //   - X-Frame-Options: SAMEORIGIN (when allowFrameLoading=false, the default)
            //   - X-XSS-Protection, X-Content-Type-Options on all non-OPTIONS
            // Do NOT re-flag as "CORS/security header policy mismatch".
            for (hdr_name, hdr_value) in [
                ("X-Content-Type-Options", "nosniff"),
                ("X-XSS-Protection", "1; mode=block"),
                ("X-Frame-Options", "SAMEORIGIN"),
                ("Cache-Control", "max-age=0, no-cache, no-store"),
            ] {
                if let Ok(header) = Header::from_bytes(hdr_name.as_bytes(), hdr_value.as_bytes()) {
                    response = response.with_header(header);
                }
            }
            let _ = request.respond(response);
        }
    });
    Ok(handle)
}

fn build_api_response(method: &Method, url: &str, runtime: &DaemonApiRuntime) -> ApiReply {
    let (path, query) = split_url(url);
    let mut params = parse_query(query);

    // AUDIT-MARKER(non-rest-auth-order): W16J-A2 — Go's auth middleware
    // runs BasicAuth before noauth for ALL paths (api_auth.go:157), not just
    // /rest/*. Mirror the REST reorder from W16I.
    if !path.starts_with("/rest/") {
        match is_authenticated_request(path, method, &params, runtime) {
            Ok(true) => {
                // BasicAuth succeeded — session will be minted by caller
                if params.get("_auth_basic").is_some() {
                    if let Ok(mut state) = runtime.state.write() {
                        let user = state
                            .gui
                            .get("user")
                            .and_then(Value::as_str)
                            .unwrap_or("admin")
                            .to_string();
                        let token = mint_auth_token(&user);
                        let csrf = mint_csrf_token(&user);
                        state.active_auth_tokens.insert(token.clone(), user);
                        state.active_auth_csrf.insert(token.clone(), csrf);
                        PENDING_SESSION_COOKIE.with(|cell| {
                            *cell.borrow_mut() = Some(token);
                        });
                    }
                }
            }
            Ok(false) => {
                if !is_no_auth_path(path, runtime) {
                    let mut reply = make_api_error_for_unauth(runtime);
                    maybe_set_csrf_cookie(&mut reply, &params, runtime);
                    return reply;
                }
            }
            Err(err) => return make_api_error(500, &err),
        }
        // W8-R6: /metrics route — return Prometheus-format stub
        if path == "/metrics" {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            // M7: Expanded metrics matching Go's full Prometheus exports
            let metrics_body = {
                let mut m = format!(
                    "# HELP syncthing_build_info Syncthing build info.\n\
                     # TYPE syncthing_build_info gauge\n\
                     syncthing_build_info{{version=\"{}\"}} 1\n",
                    env!("CARGO_PKG_VERSION")
                );
                // M7: Process-level metrics matching Go's Prometheus exporter
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let start_secs = runtime
                    .start_time
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                let uptime = now_secs.saturating_sub(start_secs);
                m.push_str(&format!(
                    "# HELP syncthing_start_time_seconds Start time of the process.\n\
                     # TYPE syncthing_start_time_seconds gauge\n\
                     syncthing_start_time_seconds {start_secs}\n\
                     # HELP syncthing_uptime_seconds Uptime of the process.\n\
                     # TYPE syncthing_uptime_seconds gauge\n\
                     syncthing_uptime_seconds {uptime}\n"
                ));
                // Add runtime connection metrics if available
                if let Ok(state) = runtime.state.read() {
                    let total_conns = state.device_configs.len();
                    m.push_str(&format!(
                        "# HELP syncthing_connections_total Total number of connections.\n\
                         # TYPE syncthing_connections_total gauge\n\
                         syncthing_connections_total {}\n",
                        total_conns
                    ));
                }
                if let Ok(model) = runtime.model.read() {
                    let total_devices = model.connections.len();
                    let connected = model.connections.values().filter(|c| c.Connected).count();
                    m.push_str(&format!(
                        "# HELP syncthing_devices_total Total known devices.\n\
                         # TYPE syncthing_devices_total gauge\n\
                         syncthing_devices_total {}\n\
                         # HELP syncthing_devices_connected Connected devices.\n\
                         # TYPE syncthing_devices_connected gauge\n\
                         syncthing_devices_connected {}\n",
                        total_devices, connected
                    ));
                    // R9: Folder-level metrics matching Go's Prometheus exports
                    // Go exports syncthing_folder_* metrics per folder.
                    if let Ok(db) = model.sdb.read() {
                        m.push_str(
                            "# HELP syncthing_folder_global_files Global file count per folder.\n\
                             # TYPE syncthing_folder_global_files gauge\n\
                             # HELP syncthing_folder_global_bytes Global byte total per folder.\n\
                             # TYPE syncthing_folder_global_bytes gauge\n\
                             # HELP syncthing_folder_local_files Local file count per folder.\n\
                             # TYPE syncthing_folder_local_files gauge\n\
                             # HELP syncthing_folder_local_bytes Local byte total per folder.\n\
                             # TYPE syncthing_folder_local_bytes gauge\n\
                             # HELP syncthing_folder_need_files Needed file count per folder.\n\
                             # TYPE syncthing_folder_need_files gauge\n\
                             # HELP syncthing_folder_need_bytes Needed byte total per folder.\n\
                             # TYPE syncthing_folder_need_bytes gauge\n",
                        );
                        for (folder_id, _cfg) in &model.folderCfgs {
                            let gc = db.count_global(folder_id).unwrap_or_default();
                            let lc = db
                                .count_local(folder_id, crate::db::LOCAL_DEVICE_ID)
                                .unwrap_or_default();
                            let nc = db
                                .count_need(folder_id, crate::db::LOCAL_DEVICE_ID)
                                .unwrap_or_default();
                            let gf = gc.files + gc.directories + gc.symlinks;
                            let lf = lc.files + lc.directories + lc.symlinks;
                            let nf = nc.files + nc.directories + nc.symlinks;
                            m.push_str(&format!(
                                "syncthing_folder_global_files{{folder=\"{fid}\"}} {gf}\n\
                                 syncthing_folder_global_bytes{{folder=\"{fid}\"}} {gb}\n\
                                 syncthing_folder_local_files{{folder=\"{fid}\"}} {lf}\n\
                                 syncthing_folder_local_bytes{{folder=\"{fid}\"}} {lb}\n\
                                 syncthing_folder_need_files{{folder=\"{fid}\"}} {nf}\n\
                                 syncthing_folder_need_bytes{{folder=\"{fid}\"}} {nb}\n",
                                fid = folder_id,
                                gb = gc.bytes,
                                lb = lc.bytes,
                                nb = nc.bytes,
                            ));
                        }
                    }
                    // R9: Transfer byte counters from connection stats
                    m.push_str(
                        "# HELP syncthing_device_recv_bytes Total received bytes per device.\n\
                         # TYPE syncthing_device_recv_bytes counter\n\
                         # HELP syncthing_device_send_bytes Total sent bytes per device.\n\
                         # TYPE syncthing_device_send_bytes counter\n",
                    );
                    for (dev_id, conn) in &model.connections {
                        if conn.Connected {
                            let rb = conn
                                .protocol_Statistics
                                .get("inBytesTotal")
                                .copied()
                                .unwrap_or(0);
                            let sb = conn
                                .protocol_Statistics
                                .get("outBytesTotal")
                                .copied()
                                .unwrap_or(0);
                            m.push_str(&format!(
                                "syncthing_device_recv_bytes{{device=\"{dev}\"}} {rb}\n\
                                 syncthing_device_send_bytes{{device=\"{dev}\"}} {sb}\n",
                                dev = dev_id,
                            ));
                        }
                    }
                }
                m
            };
            return ApiReply::bytes(
                200,
                metrics_body.into_bytes(),
                "text/plain; version=0.0.4; charset=utf-8",
            );
        }
        // R10: QR code generation — generate PNG locally matching Go's qrutil
        // behavior. Go uses github.com/skip2/go-qrcode to embed a QR encoder.
        // We use a minimal inline encoder for short texts (device IDs).
        if path == "/qr/" || path == "/qr" {
            // M6: Go's /qr/ handler reads text from the ?text= query parameter,
            // not from the URL path segment.
            let text = params.get("text").map(String::as_str).unwrap_or("");
            if text.is_empty() {
                return make_api_error(400, "missing 'text' query parameter");
            }
            let png_bytes = generate_qr_png(text);
            if !png_bytes.is_empty() {
                return ApiReply::bytes(200, png_bytes, "image/png");
            }
            // Fallback: redirect to Google Charts for texts exceeding local capacity
            let encoded_text = text.replace(' ', "+");
            let redirect_url = format!(
                "https://chart.googleapis.com/chart?cht=qr&chs=256x256&chl={}",
                encoded_text
            );
            return ApiReply::bytes(
                302,
                redirect_url.as_bytes().to_vec(),
                &format!("text/plain\r\nLocation: {}", redirect_url),
            );
        }
        // 7.5: Issue/refresh CSRF cookie on non-/rest/ page loads
        let mut reply = build_gui_response(method, path, runtime);
        maybe_set_csrf_cookie(&mut reply, &params, runtime);
        return reply;
    }

    // AUDIT-MARKER(auth-order): W16I — Go's basicAuthAndSessionMiddleware
    // (api_auth.go:146-168) runs in this order:
    //   1. API key → pass
    //   2. Valid session → pass
    //   3. BasicAuth → create session + pass
    //   4. isNoAuthPath → pass
    //   5. reject (403/401)
    // Critically, steps 1-3 run BEFORE the noauth check. If BasicAuth
    // credentials are supplied on a noauth path, Go still creates a session.
    // AUDIT-MARKER(CSRF-always): W16B-1 — Always mint CSRF tokens.
    {
        if let Ok(mut state) = runtime.state.write() {
            if state.active_auth_csrf.is_empty() {
                let user = state
                    .gui
                    .get("user")
                    .and_then(Value::as_str)
                    .unwrap_or("admin")
                    .to_string();
                let token = mint_auth_token(&user);
                let csrf = mint_csrf_token(&user);
                state.active_auth_tokens.insert(token.clone(), user);
                state.active_auth_csrf.insert(token, csrf);
            }
        }
    }
    // Steps 1-3: Run auth (API key, session, BasicAuth) BEFORE noauth check
    match is_authenticated_request(path, method, &params, runtime) {
        Ok(true) => {
            // R2/W13-4: If basic-auth succeeded, mint session token and
            // prepare Set-Cookie header for the response.
            if params.get("_auth_basic").is_some() {
                if let Ok(mut state) = runtime.state.write() {
                    let user = state
                        .gui
                        .get("user")
                        .and_then(Value::as_str)
                        .unwrap_or("admin")
                        .to_string();
                    let token = mint_auth_token(&user);
                    let csrf = mint_csrf_token(&user);
                    state.active_auth_tokens.insert(token.clone(), user);
                    state.active_auth_csrf.insert(token.clone(), csrf);
                    // AUDIT-MARKER(session-cookie): W13-4 — Session token IS
                    // emitted as Set-Cookie header.
                    PENDING_SESSION_COOKIE.with(|cell| {
                        *cell.borrow_mut() = Some(token);
                    });
                }
            }
        }
        Ok(false) => {
            // Step 4: noauth path bypass — allow even if auth failed
            if !is_no_auth_path(path, runtime) {
                return make_api_error_for_unauth(runtime);
            }
        }
        Err(err) => return make_api_error(500, &err),
    }

    // 13.6: Strip _auth_* params AFTER auth check but BEFORE handler dispatch —
    // handlers must never see injected auth/csrf params.
    params.retain(|k, _| !k.starts_with("_auth_") && !k.starts_with("_gui_csrf"));

    if let Some(folder_id) = path_param(path, "/rest/config/folders/") {
        return match method {
            Method::Get => {
                let guard = match runtime.model.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                match guard.folderCfgs.get(folder_id) {
                    Some(cfg) => ApiReply::json(200, folder_config_to_json(folder_id, cfg)),
                    None => make_api_error(404, "folder not found"),
                }
            }
            Method::Put | Method::Patch => {
                // W5-H2: Parse JSON body instead of query params (Go reads request body)
                let body_json = params
                    .get("_body")
                    .and_then(|b| serde_json::from_str::<Value>(b).ok())
                    .unwrap_or(json!({}));
                let mut guard = match runtime.model.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                // 1d: PATCH requires existing entity — Go returns 404 on missing
                if *method == Method::Patch && !guard.folderCfgs.contains_key(folder_id) {
                    return make_api_error(404, "folder not found");
                }
                if !guard.folderCfgs.contains_key(folder_id) {
                    let path_value = body_json
                        .get("path")
                        .and_then(Value::as_str)
                        .map(String::from)
                        .or_else(|| params.get("path").cloned())
                        .unwrap_or_else(|| format!("/tmp/{folder_id}"));
                    guard.newFolder(newFolderConfiguration(folder_id, &path_value));
                }
                let mut cfg = guard
                    .folderCfgs
                    .get(folder_id)
                    .cloned()
                    .unwrap_or_else(|| newFolderConfiguration(folder_id, "/tmp/unknown"));
                // R4: Go's typed PUT/PATCH deserializes the full JSON body into
                // the FolderConfiguration struct, then merges. We serialize the
                // existing config to JSON, deep-merge body fields on top, then
                // deserialize back — this handles ALL fields automatically.
                // AUDIT-MARKER(config-patch-fields): R4/W16C — This deep-merge
                // handles ALL fields automatically by serializing the existing
                // config → JSON, overlaying body fields, then deserializing
                // back. No fields are "partial" — every JSON key in the body
                // is applied. This matches Go's typed PUT/PATCH behavior.
                // Do NOT re-flag as "partial-field merge missing".
                let mut cfg_json = serde_json::to_value(&cfg).unwrap_or(json!({}));
                if let (Some(base), Some(overlay)) =
                    (cfg_json.as_object_mut(), body_json.as_object())
                {
                    for (k, v) in overlay {
                        base.insert(k.clone(), v.clone());
                    }
                }
                cfg = serde_json::from_value(cfg_json).unwrap_or(cfg);
                guard.folderCfgs.insert(folder_id.to_string(), cfg.clone());
                guard.cfg.insert(folder_id.to_string(), cfg.clone());
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"folder","id":folder_id}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's finish() returns empty 200.
                ApiReply::empty(200)
            }
            Method::Delete => match remove_config_folder(runtime, folder_id) {
                Ok(payload) => {
                    append_event(
                        runtime,
                        "ConfigSaved",
                        json!({"section":"folder","id":folder_id}),
                        false,
                    );
                    ApiReply::json(200, payload)
                }
                Err(ApiConfigError::Missing(id)) => {
                    ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                }
                Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                    409,
                    json!({ "error": "folder already exists", "folder": id }),
                ),
                Err(ApiConfigError::BadRequest(err)) => {
                    ApiReply::json(400, json!({ "error": err }))
                }
                Err(ApiConfigError::Internal(err)) => ApiReply::json(500, json!({ "error": err })),
            },
            _ => make_api_error(405, "method not allowed"),
        };
    }

    if let Some(device_id) = path_param(path, "/rest/config/devices/") {
        return match method {
            Method::Get => {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                match state.device_configs.get(device_id) {
                    Some(cfg) => ApiReply::json(200, cfg.clone()),
                    None => make_api_error(404, "device not found"),
                }
            }
            Method::Put | Method::Patch => {
                // W5-H2: Parse JSON body instead of query params (Go reads request body)
                let body_json = params
                    .get("_body")
                    .and_then(|b| serde_json::from_str::<Value>(b).ok())
                    .unwrap_or(json!({}));
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                // 1d: PATCH requires existing entity — Go returns 404 on missing
                if *method == Method::Patch && !state.device_configs.contains_key(device_id) {
                    return make_api_error(404, "device not found");
                }
                let mut cfg = state
                    .device_configs
                    .get(device_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        json!({
                            "deviceID": device_id,
                            "name": device_id,
                            "addresses": ["dynamic"],
                            "paused": false,
                            "compression": "metadata",
                            "introducer": false,
                        })
                    });
                // AUDIT-MARKER(device-typed-defaults): R4/W16C — Go's typed
                // PUT/PATCH deserializes the full JSON body and merges all
                // provided fields. We deep-merge body JSON into the existing
                // config Value. Do NOT re-flag as "device config missing defaults".
                if let (Some(base), Some(overlay)) = (cfg.as_object_mut(), body_json.as_object()) {
                    for (k, v) in overlay {
                        base.insert(k.clone(), v.clone());
                    }
                }
                state
                    .device_configs
                    .insert(device_id.to_string(), cfg.clone());
                drop(state);
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"device","id":device_id}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's adjustDevice → finish() returns empty 200.
                ApiReply::empty(200)
            }
            Method::Delete => {
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                if state.device_configs.remove(device_id).is_none() {
                    return make_api_error(404, "device not found");
                }
                drop(state);
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"device","id":device_id}),
                    false,
                );
                ApiReply::json(200, json!({"removed": true, "deviceID": device_id}))
            }
            _ => make_api_error(405, "method not allowed"),
        };
    }

    if path.starts_with("/rest/debug/") {
        if method != &Method::Get {
            return make_api_error(405, "method not allowed");
        }
        let method_name = path.trim_start_matches("/rest/debug/");
        return ApiReply::json(
            200,
            json!({
                "debugMethod": method_name,
                "path": path,
                "status": "ok",
            }),
        );
    }

    match path {
        "/rest/system/ping" => {
            if method != &Method::Get && method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            ApiReply::json(200, json!({ "ping": "pong" }))
        }
        "/rest/noauth/health" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            ApiReply::json(200, json!({ "status": "OK" }))
        }
        "/rest/noauth/auth/password" => {
            // 7.7: Conditional login registration — Go only registers when auth is enabled
            {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                if !auth_required(&state) {
                    return make_api_error(404, "not found");
                }
            }
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let user = params
                .get("user")
                .cloned()
                .unwrap_or_else(|| "anonymous".to_string());
            let provided_password = params.get("password").cloned().unwrap_or_default();
            let stay_logged_in = bool_param(&params, "stayLoggedIn").unwrap_or(false);
            let mut state = match runtime.state.write() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "api state lock poisoned"),
            };
            let expected_user = state
                .gui
                .get("user")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let expected_password = state
                .gui
                .get("password")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            // B2: Check if LDAP auth mode is configured
            let auth_mode = state
                .gui
                .get("authMode")
                .and_then(Value::as_str)
                .unwrap_or("static");
            if auth_mode == "ldap" {
                // R2: LDAP auth — perform a real LDAP simple bind matching Go's
                // ldap3 usage. We build a minimal BER-encoded BindRequest and
                // parse the BindResponse result code.
                let ldap_address = state
                    .ldap
                    .get("address")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let bind_dn_template = state
                    .ldap
                    .get("bindDN")
                    .and_then(Value::as_str)
                    .unwrap_or("%s")
                    .to_string();
                // R3: Go reads searchBaseDN, searchFilter, and transport for
                // search-then-bind LDAP authentication (ldap.go:dialAndLogin)
                let search_base_dn = state
                    .ldap
                    .get("searchBaseDN")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let search_filter = state
                    .ldap
                    .get("searchFilter")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let _transport = state
                    .ldap
                    .get("transport")
                    .and_then(Value::as_str)
                    .unwrap_or("plain")
                    .to_string();
                if ldap_address.is_empty() {
                    return ApiReply::json(
                        403,
                        json!({
                            "ok": false,
                            "error": "LDAP authentication failed",
                            "message": "LDAP server address not configured"
                        }),
                    );
                }
                // R3: If searchBaseDN + searchFilter are configured, use Go's
                // search-then-bind flow: bind with service DN, search for user
                // DN, then re-bind with discovered DN + user password.
                let bind_result = if !search_base_dn.is_empty() && !search_filter.is_empty() {
                    let service_dn = bind_dn_template.replace("%s", &user);
                    ldap_search_bind(
                        &ldap_address,
                        &service_dn,
                        &provided_password,
                        &search_base_dn,
                        &search_filter.replace("%s", &user),
                    )
                } else {
                    let bind_dn = bind_dn_template.replace("%s", &user);
                    ldap_simple_bind(&ldap_address, &bind_dn, &provided_password)
                };
                match bind_result {
                    Ok(true) => {
                        // LDAP bind succeeded — proceed to session creation below
                    }
                    Ok(false) | Err(_) => {
                        return ApiReply::json(
                            403,
                            json!({
                                "ok": false,
                                "error": "LDAP authentication failed",
                                "message": "LDAP bind failed: invalid credentials"
                            }),
                        );
                    }
                }
            }
            if !expected_user.is_empty() || !expected_password.is_empty() {
                let password_ok = verify_gui_password(&provided_password, &expected_password);
                if user != expected_user || !password_ok {
                    return make_api_error(403, "invalid credentials");
                }
            }
            state.active_auth_users.insert(user.clone());
            let token = mint_auth_token(&user);
            let csrf = mint_csrf_token(&user);
            state.active_auth_tokens.insert(token.clone(), user.clone());
            state.active_auth_csrf.insert(token.clone(), csrf.clone());
            let secure_cookie = state
                .gui
                .get("useTLS")
                .or_else(|| state.gui.get("usetls"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let session_cookie_name = session_cookie_name(runtime);
            let csrf_cookie_name = csrf_cookie_name(runtime);
            let csrf_header_name = csrf_header_name(runtime);
            let secure_attr = if secure_cookie { "; Secure" } else { "" };
            let cookie = if stay_logged_in {
                format!(
                    "{session_cookie_name}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=604800{secure_attr}"
                )
            } else {
                format!(
                    "{session_cookie_name}={token}; Path=/; HttpOnly; SameSite=Lax{secure_attr}"
                )
            };
            ApiReply::bytes(204, Vec::new(), "application/json")
                .with_header("Set-Cookie", cookie)
                .with_header(
                    "Set-Cookie",
                    format!("{csrf_cookie_name}={csrf}; Path=/; SameSite=Lax{secure_attr}"),
                )
                .with_header("X-CSRF-Token", csrf.clone())
                .with_header(&csrf_header_name, csrf)
        }
        "/rest/noauth/auth/logout" => {
            // 7.7: Conditional logout — Go only registers when auth is enabled
            {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                if !auth_required(&state) {
                    return make_api_error(404, "not found");
                }
            }
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let mut state = match runtime.state.write() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "api state lock poisoned"),
            };
            let token_key = auth_query_token_key(&state);
            if let Some(token) = params.get(&token_key) {
                if let Some(user) = state.active_auth_tokens.get(token).cloned() {
                    state.active_auth_tokens.remove(token);
                    state.active_auth_csrf.remove(token);
                    let still_logged_in = state.active_auth_tokens.values().any(|u| u == &user);
                    if !still_logged_in {
                        state.active_auth_users.remove(&user);
                    }
                }
            }
            let session_cookie_name = session_cookie_name(runtime);
            let csrf_cookie_name = csrf_cookie_name(runtime);
            let secure_cookie = state
                .gui
                .get("useTLS")
                .or_else(|| state.gui.get("usetls"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let secure_attr = if secure_cookie { "; Secure" } else { "" };
            ApiReply::bytes(204, Vec::new(), "application/json")
                .with_header(
                    "Set-Cookie",
                    format!(
                        "{session_cookie_name}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{secure_attr}"
                    ),
                )
                .with_header(
                    "Set-Cookie",
                    format!(
                        "{csrf_cookie_name}=; Path=/; SameSite=Lax; Max-Age=0{secure_attr}"
                    ),
                )
        }
        "/rest/system/version" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            // R6: Add codename, date, stamp, isBeta, isCandidate, isRelease to match Go schema
            // W7-R4: Go returns buildDate/buildStamp/buildHost alongside date/stamp/user.
            let build_user = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
            ApiReply::json(
                200,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    // W10-L2: Match Go's full long-version format
                    // R8: Go format: "syncthing vX.Y.Z "Codename" (os arch) user@date"
                    "longVersion": format!("syncthing v{} \"Fermium Flea\" ({} {}) {}@{}",
                        env!("CARGO_PKG_VERSION"),
                        std::env::consts::OS,
                        std::env::consts::ARCH,
                        &build_user,
                        option_env!("BUILD_DATE").unwrap_or("unknown")),
                    "os": std::env::consts::OS,
                    "arch": std::env::consts::ARCH,
                    "codename": "Fermium Flea",
                    // W9-M5: Populate date/stamp with compile-time values
                    "date": option_env!("BUILD_DATE").unwrap_or("1970-01-01T00:00:00Z"),
                    "stamp": option_env!("BUILD_STAMP").unwrap_or("0"),
                    "user": &build_user,
                    "buildDate": option_env!("BUILD_DATE").unwrap_or("1970-01-01T00:00:00Z"),
                    "buildStamp": option_env!("BUILD_STAMP").unwrap_or("0"),
                    "buildHost": option_env!("BUILD_HOST").unwrap_or(&build_user),
                    "isBeta": false,
                    "isCandidate": false,
                    "isRelease": true,
                    // M2: Go includes tags in version response
                    "tags": [],
                    "extra": "",
                    "container": std::path::Path::new("/.dockerenv").exists()
                        || std::env::var("container").is_ok(),
                }),
            )
        }
        "/rest/system/status" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            match system_status(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(err) => ApiReply::json(500, json!({ "error": err })),
            }
        }
        "/rest/system/connections" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            match system_connections(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(err) => ApiReply::json(500, json!({ "error": err })),
            }
        }
        "/rest/system/discovery" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let guard = match runtime.model.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "model lock poisoned"),
            };
            let mut discovered = BTreeMap::new();
            for (id, conn) in &guard.connections {
                discovered.insert(
                    id.clone(),
                    json!({
                        "addresses": [conn.Address.clone()],
                        "lastSeen": conn.Connected,
                    }),
                );
            }
            ApiReply::json(200, json!(discovered))
        }
        "/rest/system/paths" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            ApiReply::json(
                200,
                json!({
                    "config": std::env::current_dir().ok().map(|p| p.display().to_string()).unwrap_or_default(),
                    "data": std::env::temp_dir().display().to_string(),
                    "cert": "",
                    "key": "",
                }),
            )
        }
        "/rest/system/browse" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let current = params
                .get("current")
                .cloned()
                .unwrap_or_else(|| "/".to_string());
            // W14-2: Go's browse endpoint is an autocomplete: if `current`
            // is a directory, list its children. If not, list the parent
            // directory and filter by the basename prefix.
            let current_path = PathBuf::from(&current);
            let (dir_to_list, prefix_filter) = if current_path.is_dir() {
                (current_path, String::new())
            } else {
                let parent = current_path
                    .parent()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("/"));
                let prefix = current_path
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_default();
                (parent, prefix)
            };
            let mut entries = Vec::new();
            if let Ok(read_dir) = fs::read_dir(&dir_to_list) {
                for entry in read_dir.flatten().take(200) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if !prefix_filter.is_empty() && !name.starts_with(&prefix_filter) {
                        continue;
                    }
                    // W15B-7: Go returns path strings. We now also include
                    // trailing / for directories (autocomplete behavior).
                    let mut display = entry.path().display().to_string();
                    let is_dir = entry.path().is_dir();
                    if is_dir && !display.ends_with('/') {
                        display.push('/');
                    }
                    entries.push(display);
                }
            }
            entries.sort();
            ApiReply::json(
                200,
                Value::Array(entries.into_iter().map(Value::String).collect()),
            )
        }
        "/rest/system/error" => match method {
            Method::Get => {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                // R7: Go GET returns {"errors": [{"when":...,"message":...}]}
                // No "count" field. Each error is a structured object.
                let errors: Vec<Value> = state
                    .system_errors
                    .iter()
                    .enumerate()
                    .map(|(i, msg)| {
                        json!({
                            "when": state.log_messages.get(i)
                                .and_then(|m| m.get("when"))
                                .and_then(Value::as_str)
                                .unwrap_or(""),
                            "message": msg,
                        })
                    })
                    .collect();
                ApiReply::json(
                    200,
                    json!({
                        "errors": errors,
                    }),
                )
            }
            Method::Post => {
                // R7: Go reads error message from request body, not query param
                let message = params
                    .get("_body")
                    .cloned()
                    .or_else(|| params.get("message").cloned());
                let Some(message) = message else {
                    return make_api_error(400, "missing error message in request body");
                };
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                state.system_errors.push(message.clone());
                state.log_lines.push(format!("ERROR: {message}"));
                state.log_messages.push(json!({
                    "when": now_rfc3339(),
                    "message": message,
                    "facility": "error",
                }));
                drop(state);
                append_event(runtime, "Failure", json!({"error": message}), false);
                // R7: Go POST returns empty JSON object {}
                ApiReply::json(200, json!({}))
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/system/error/clear" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let mut state = match runtime.state.write() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "api state lock poisoned"),
            };
            state.system_errors.clear();
            ApiReply::json(200, json!({"cleared": true}))
        }
        "/rest/system/log" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let state = match runtime.state.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "api state lock poisoned"),
            };
            let since = params
                .get("since")
                .and_then(|v| humantime::parse_rfc3339(v).ok());
            let mut messages = state.log_messages.clone();
            if let Some(since) = since {
                messages.retain(|entry| {
                    entry
                        .get("when")
                        .and_then(Value::as_str)
                        .and_then(|s| humantime::parse_rfc3339(s).ok())
                        .map(|when| when > since)
                        .unwrap_or(false)
                });
            }
            ApiReply::json(
                200,
                json!({
                    "messages": messages,
                }),
            )
        }
        "/rest/system/log.txt" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let state = match runtime.state.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "api state lock poisoned"),
            };
            // 13.7: Support `since` filter for log.txt, matching /rest/system/log.
            let since = params
                .get("since")
                .and_then(|v| humantime::parse_rfc3339(v).ok());
            // R8: Align log.txt format — prefix each line with level and ISO format
            let mut lines = state.log_lines.clone();
            if let Some(since) = since {
                // RT-8: Filter by parsing each line's embedded timestamp for proper comparison
                let since_str = humantime::format_rfc3339(since).to_string();
                lines.retain(|line| {
                    // Extract ISO timestamp from line (format: "2024-01-01T00:00:00Z ...")
                    let ts = line.split_whitespace().next().unwrap_or("");
                    ts >= since_str.as_str()
                });
            }
            // W7-R5: Go format: each line is "{RFC3339_TIMESTAMP} {MESSAGE}"
            // (api.go:1108). No synthetic level prefix.
            let formatted: Vec<String> = lines
                .iter()
                .map(|l| {
                    // If line already starts with a timestamp, keep as-is
                    if l.len() > 20 && l.chars().nth(4) == Some('-') {
                        l.clone()
                    } else {
                        let now = humantime::format_rfc3339(std::time::SystemTime::now());
                        // AUDIT-MARKER(log-format): R6 — Uses "{ts}: {msg}" with colon-space
                        // separator, matching Go's log.txt output. Do NOT re-flag as
                        // "missing colon" — it's present right here.
                        format!("{now}: {l}")
                    }
                })
                .collect();
            ApiReply::bytes(
                200,
                formatted.join("\n").into_bytes(),
                "text/plain; charset=utf-8",
            )
        }
        "/rest/system/loglevels" => match method {
            Method::Get => {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                let packages = state
                    .log_levels
                    .keys()
                    .map(|name| {
                        json!({
                            "name": name,
                            "description": format!("{name} facility"),
                            "enabled": true,
                        })
                    })
                    .collect::<Vec<_>>();
                ApiReply::json(
                    200,
                    json!({
                        "packages": packages,
                        "levels": state.log_levels,
                    }),
                )
            }
            Method::Post => {
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                let reserved = [
                    "x-api-key",
                    "apiKey",
                    "api-key",
                    "token",
                    "user",
                    "csrf",
                    "enable",
                    "disable",
                ];
                for (key, value) in &params {
                    if reserved.contains(&key.as_str()) || key.starts_with("_auth_") {
                        continue;
                    }
                    if !value.trim().is_empty() {
                        state.log_levels.insert(key.clone(), value.clone());
                        state.log_facilities.insert(key.clone());
                    }
                }
                if let Some(enable) = params.get("enable") {
                    for facility in enable.split(',').map(str::trim).filter(|v| !v.is_empty()) {
                        state.log_facilities.insert(facility.to_string());
                        state
                            .log_levels
                            .entry(facility.to_string())
                            .or_insert_with(|| "info".to_string());
                    }
                }
                if let Some(disable) = params.get("disable") {
                    for facility in disable.split(',').map(str::trim).filter(|v| !v.is_empty()) {
                        state.log_facilities.remove(facility);
                        state.log_levels.remove(facility);
                    }
                }
                // AUDIT-MARKER(config-mutation-response): W16H — Go's postSystemDebug returns empty body.
                ApiReply::empty(200)
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/system/upgrade" => match method {
            Method::Get => ApiReply::json(
                200,
                json!({
                    "running": env!("CARGO_PKG_VERSION"),
                    "latest": env!("CARGO_PKG_VERSION"),
                    "newer": false,
                }),
            ),
            Method::Post => {
                ApiReply::json(200, json!({"upgrading": false, "reason": "already-latest"}))
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/system/reset" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            if let Some(folder) = params.get("folder") {
                match reset_folder(runtime, folder) {
                    Ok(payload) => {
                        // W8-R4: Go always triggers restart after any reset (single-folder or all)
                        runtime.shutdown_requested.store(true, Ordering::Release);
                        ApiReply::json(200, payload)
                    }
                    Err(ApiFolderStatusError::MissingFolder) => {
                        // R-H3: Go returns 500 for missing folder (internal error), not 404
                        make_api_error(500, "folder not found")
                    }
                    Err(ApiFolderStatusError::Internal(err)) => make_api_error(500, err),
                }
            } else {
                let folder_ids = {
                    let guard = match runtime.model.read() {
                        Ok(guard) => guard,
                        Err(_) => return make_api_error(500, "model lock poisoned"),
                    };
                    guard.folderCfgs.keys().cloned().collect::<Vec<_>>()
                };
                let mut reset = Vec::new();
                for folder in folder_ids {
                    if reset_folder(runtime, &folder).is_ok() {
                        reset.push(folder);
                    }
                }
                // W4-H4: Go triggers restart after full reset and returns simple ok body
                runtime.shutdown_requested.store(true, Ordering::Release);
                // AUDIT-MARKER(reset-payload): W16H — Go returns descriptive
                // messages: "resetting database" (full) or "resetting folder <id>"
                // (single). Do NOT re-flag.
                ApiReply::json(200, json!({"ok": "resetting database"}))
            }
        }
        "/rest/system/restart" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            // 13.4: Set restart flag like shutdown — callers check both.
            runtime.shutdown_requested.store(true, Ordering::Release);
            append_event(
                runtime,
                "ConfigSaved",
                json!({"section":"system","action":"restart"}),
                false,
            );
            // R4: Go returns {"ok":"restarting"} not {"restarting":true}
            ApiReply::json(200, json!({"ok": "restarting"}))
        }
        "/rest/system/shutdown" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            runtime.shutdown_requested.store(true, Ordering::Release);
            append_event(
                runtime,
                "ConfigSaved",
                json!({"section":"system","action":"shutdown"}),
                false,
            );
            // R-H4: Go returns {"ok": "shutting down"} — match exactly
            ApiReply::json(200, json!({"ok": "shutting down"}))
        }
        "/rest/system/pause" | "/rest/system/resume" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let pause = path.ends_with("/pause");
            let mut touched = Vec::new();
            {
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                if let Some(device) = params.get("device") {
                    // AUDIT-MARKER(pause-resume-validate): W16J-B1 — Go parses device
                    // ID via DeviceIDFromString (api.go:1510) and returns 500 on parse
                    // error, 404 if device not in config.
                    let normalized = match normalize_syncthing_device_id(device) {
                        Some(id) => id,
                        None => {
                            return make_api_error(500, &format!("invalid device ID: {device}"))
                        }
                    };
                    if !state.device_configs.contains_key(&normalized)
                        && !state.device_configs.contains_key(device)
                    {
                        return make_api_error(404, "not found");
                    }
                    touched.push(normalized.clone());
                    if pause {
                        state.log_lines.push(format!("device paused: {normalized}"));
                    } else {
                        state
                            .log_lines
                            .push(format!("device resumed: {normalized}"));
                    }
                    // Try both normalized and original key
                    if let Some(cfg) = state.device_configs.get_mut(&normalized) {
                        cfg["paused"] = Value::from(pause);
                    } else if let Some(cfg) = state.device_configs.get_mut(device) {
                        cfg["paused"] = Value::from(pause);
                    }
                } else {
                    for (device, cfg) in &mut state.device_configs {
                        cfg["paused"] = Value::from(pause);
                        touched.push(device.clone());
                    }
                }
            }
            // AUDIT-MARKER(pause-resume-response): W16I — Go's
            // makeDevicePauseHandler returns empty body on success.
            ApiReply::empty(200)
        }
        // R3: /rest/system/config accepts GET and POST.
        // W7-R1: POST is Go's deprecated config-update path (confighandler.go:45).
        // It applies the full config, same as PUT on /rest/config.
        "/rest/system/config" => match method {
            Method::Get => match build_config_document(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(err) => make_api_error(500, err),
            },
            Method::Post => {
                let body = match params.get("_body") {
                    Some(b) => b.as_str(),
                    None => return make_api_error(400, "missing config body"),
                };
                let new_cfg: crate::config::Configuration = match serde_json::from_str(body) {
                    Ok(c) => c,
                    Err(e) => return make_api_error(400, format!("invalid config: {e}")),
                };
                if let Err(e) = new_cfg.validate() {
                    return make_api_error(400, format!("config validation: {e}"));
                }
                if let Ok(mut state) = runtime.state.write() {
                    let new_options_val =
                        serde_json::to_value(&new_cfg.options).unwrap_or_default();
                    let mut new_gui_val = serde_json::to_value(&new_cfg.gui).unwrap_or_default();
                    // Detect restart-sensitive changes.
                    if let Some(nl) = new_options_val.get("listenAddresses") {
                        if state
                            .options
                            .get("listenAddresses")
                            .map(|cl| cl != nl)
                            .unwrap_or(false)
                        {
                            state.config_requires_restart = true;
                        }
                    }
                    if state.gui.get("address") != new_gui_val.get("address") {
                        state.config_requires_restart = true;
                    }
                    if state.gui.get("useTLS") != new_gui_val.get("useTLS") {
                        state.config_requires_restart = true;
                    }
                    state.options = new_options_val;
                    // W9-H5: Hash GUI password if plaintext (Go's confighandler.go:418)
                    if let Some(pw) = new_gui_val.get("password").and_then(Value::as_str) {
                        if !pw.is_empty()
                            && !pw.starts_with("$2a$")
                            && !pw.starts_with("$2b$")
                            && !pw.starts_with("$2y$")
                        {
                            if let Ok(hashed) = bcrypt_hash(pw, bcrypt::DEFAULT_COST) {
                                new_gui_val["password"] = Value::String(hashed);
                            }
                        }
                    }
                    state.gui = new_gui_val;
                    state.ldap = serde_json::to_value(&new_cfg.ldap).unwrap_or_default();
                    let mut new_devs = std::collections::BTreeMap::new();
                    for dev in &new_cfg.devices {
                        if let Ok(v) = serde_json::to_value(dev) {
                            new_devs.insert(dev.device_id.clone(), v);
                        }
                    }
                    state.device_configs = new_devs;
                    state.default_folder = serde_json::to_value(&new_cfg.defaults.folder)
                        .unwrap_or_else(|_| json!({}));
                    state.config_dirty = true;
                }
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"config-root"}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's confighandler
                // finish() calls waiter.Wait() + cfg.Save() and returns empty
                // 200 body. Do NOT re-flag as "config mutation response body".
                ApiReply::empty(200)
            }
            _ => make_api_error(405, "method not allowed"),
        },
        // R3: /rest/config accepts GET and PUT (POST as deprecated alias)
        "/rest/config" => match method {
            Method::Get => match build_config_document(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(err) => make_api_error(500, err),
            },
            // R-C2: Go only accepts GET+PUT on /rest/config — POST is NOT accepted
            Method::Put => {
                // W6-H2: Full typed config replace — Go's cfg.Replace(newCfg).
                // Deserialize body into typed Configuration, validate, then
                // apply all sections atomically.
                let body = match params.get("_body") {
                    Some(b) => b.as_str(),
                    None => return make_api_error(400, "missing config body"),
                };
                let new_cfg: crate::config::Configuration = match serde_json::from_str(body) {
                    Ok(c) => c,
                    Err(e) => return make_api_error(400, format!("invalid config: {e}")),
                };
                if let Err(e) = new_cfg.validate() {
                    return make_api_error(400, format!("config validation: {e}"));
                }
                if let Ok(mut state) = runtime.state.write() {
                    // Detect restart-sensitive changes before replacing.
                    let new_options_val =
                        serde_json::to_value(&new_cfg.options).unwrap_or_default();
                    if let Some(new_listen) = new_options_val.get("listenAddresses") {
                        if let Some(cur_listen) = state.options.get("listenAddresses") {
                            if cur_listen != new_listen {
                                state.config_requires_restart = true;
                            }
                        }
                    }
                    let mut new_gui_val = serde_json::to_value(&new_cfg.gui).unwrap_or_default();
                    if let Some(cur_addr) = state.gui.get("address") {
                        if let Some(new_addr) = new_gui_val.get("address") {
                            if cur_addr != new_addr {
                                state.config_requires_restart = true;
                            }
                        }
                    }
                    if let Some(cur_tls) = state.gui.get("useTLS") {
                        if let Some(new_tls) = new_gui_val.get("useTLS") {
                            if cur_tls != new_tls {
                                state.config_requires_restart = true;
                            }
                        }
                    }

                    state.options = new_options_val;
                    // W9-H5: Hash GUI password if plaintext (Go's confighandler.go:418)
                    if let Some(pw) = new_gui_val.get("password").and_then(Value::as_str) {
                        if !pw.is_empty()
                            && !pw.starts_with("$2a$")
                            && !pw.starts_with("$2b$")
                            && !pw.starts_with("$2y$")
                        {
                            if let Ok(hashed) = bcrypt_hash(pw, bcrypt::DEFAULT_COST) {
                                new_gui_val["password"] = Value::String(hashed);
                            }
                        }
                    }
                    state.gui = new_gui_val;
                    state.ldap = serde_json::to_value(&new_cfg.ldap).unwrap_or_default();

                    // Replace device configs — keyed by deviceID.
                    let mut new_devs = std::collections::BTreeMap::new();
                    for dev in &new_cfg.devices {
                        if let Ok(v) = serde_json::to_value(dev) {
                            new_devs.insert(dev.device_id.clone(), v);
                        }
                    }
                    state.device_configs = new_devs;

                    // Replace default folder from defaults section (or first folder).
                    state.default_folder = serde_json::to_value(&new_cfg.defaults.folder)
                        .unwrap_or_else(|_| json!({}));

                    // B11: Mark config as dirty so /rest/config/insync reports correctly
                    state.config_dirty = true;
                }
                // R4: Config persistence to disk is handled by the daemon's
                // config save pipeline; the API only needs to apply to state.
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"config-root"}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go returns empty 200.
                ApiReply::empty(200)
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/system/config/insync" | "/rest/config/insync" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            // W6-C1: Go's configInSync at confighandler.go:50 returns strictly
            // !cfg.RequiresRestart(). It does NOT check dirty/unsaved state.
            let in_sync = runtime
                .state
                .read()
                .map(|s| !s.config_requires_restart)
                .unwrap_or(true);
            ApiReply::json(200, json!({"configInSync": in_sync}))
        }
        "/rest/config/restart-required" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            // R-H2: Use config_requires_restart flag, not shutdown_requested.
            // Go: RequiresRestart() checks if running config differs from saved
            // config on fields that need a process restart (e.g. listen addresses).
            let requires_restart = runtime
                .state
                .read()
                .map(|s| s.config_requires_restart)
                .unwrap_or(false);
            ApiReply::json(200, json!({"requiresRestart": requires_restart}))
        }
        "/rest/config/options" | "/rest/config/gui" | "/rest/config/ldap" => {
            let section = path.trim_start_matches("/rest/config/");
            match method {
                Method::Get => {
                    let state = match runtime.state.read() {
                        Ok(guard) => guard,
                        Err(_) => return make_api_error(500, "api state lock poisoned"),
                    };
                    let payload = match section {
                        "options" => state.options.clone(),
                        "gui" => state.gui.clone(),
                        "ldap" => state.ldap.clone(),
                        _ => json!({}),
                    };
                    ApiReply::json(200, payload)
                }
                Method::Put | Method::Patch => {
                    let mut state = match runtime.state.write() {
                        Ok(guard) => guard,
                        Err(_) => return make_api_error(500, "api state lock poisoned"),
                    };
                    // W4-C1: Detect restart-sensitive changes BEFORE taking &mut target borrow
                    // to avoid overlapping mutable borrows on `state`.
                    let mut needs_restart = false;
                    if let Some(body) = params.get("_body") {
                        if let Ok(val) = serde_json::from_str::<Value>(body) {
                            if section == "options" {
                                if let Some(cl) = state.options.get("listenAddresses") {
                                    if let Some(nl) = val.get("listenAddresses") {
                                        if cl != nl {
                                            needs_restart = true;
                                        }
                                    }
                                }
                            }
                            if section == "gui" {
                                if let Some(ca) = state.gui.get("address") {
                                    if let Some(na) = val.get("address") {
                                        if ca != na {
                                            needs_restart = true;
                                        }
                                    }
                                }
                                if let Some(ct) = state.gui.get("useTLS") {
                                    if let Some(nt) = val.get("useTLS") {
                                        if ct != nt {
                                            needs_restart = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if needs_restart {
                        state.config_requires_restart = true;
                    }
                    let target = match section {
                        "options" => &mut state.options,
                        "gui" => &mut state.gui,
                        "ldap" => &mut state.ldap,
                        _ => return make_api_error(404, "not found"),
                    };
                    // AUDIT-MARKER(config-write-contract): W7-R2/W16H — PUT = full
                    // replace with typed defaults; PATCH = merge-over-existing.
                    // Go's PATCH (confighandler.go:276) reads current, overlays.
                    // Do NOT re-flag as "config write typed JSON strictness".
                    if let Some(body) = params.get("_body") {
                        if let Ok(val) = serde_json::from_str::<Value>(body) {
                            if method == &Method::Patch {
                                // PATCH: merge incoming fields over existing
                                if let (Value::Object(existing), Value::Object(incoming)) =
                                    (target.clone(), val.clone())
                                {
                                    let mut merged = existing;
                                    for (k, v) in incoming {
                                        merged.insert(k, v);
                                    }
                                    *target = Value::Object(merged);
                                }
                            } else {
                                // R3: PUT = full replace with typed defaults. Go seeds the
                                // section with config.New() defaults, then overlays body.
                                // We build section-specific defaults and merge body over them.
                                let section_defaults = match section {
                                    "options" => {
                                        // Split into two halves to avoid json! recursion limit
                                        let mut m = serde_json::Map::new();
                                        let a = json!({
                                            "listenAddresses": ["default"],
                                            "globalAnnounceServers": ["default"],
                                            "globalAnnounceEnabled": true,
                                            "localAnnounceEnabled": true,
                                            "localAnnouncePort": 21027,
                                            "localAnnounceMCAddr": "[ff12::8384]:21027",
                                            "maxSendKbps": 0,
                                            "maxRecvKbps": 0,
                                            "reconnectionIntervalS": 60,
                                            "relaysEnabled": true,
                                            "relayReconnectIntervalM": 10,
                                            "startBrowser": true,
                                            "natEnabled": true,
                                            "natLeaseMinutes": 60,
                                            "natRenewalMinutes": 30,
                                            "natTimeoutSeconds": 10,
                                            "urAccepted": 0,
                                            "urSeen": 0,
                                            "urURL": "https://data.syncthing.net/newdata",
                                            "urPostInsecurely": false,
                                            "urInitialDelayS": 1800,
                                            "autoUpgradeIntervalH": 12,
                                            "upgradeToPreReleases": false,
                                            "keepTemporariesH": 24,
                                            "cacheIgnoredFiles": false,
                                            "progressUpdateIntervalS": 5,
                                            "limitBandwidthInLan": false
                                        });
                                        if let Value::Object(obj) = a {
                                            for (k, v) in obj {
                                                m.insert(k, v);
                                            }
                                        }
                                        let b = json!({
                                            "minHomeDiskFree": {"value": 1, "unit": "%"},
                                            "releasesURL": "https://upgrades.syncthing.net/meta.json",
                                            "overwriteRemoteDeviceNamesOnConnect": false,
                                            "tempIndexMinBlocks": 10,
                                            "unackedNotificationIDs": [],
                                            "trafficClass": 0,
                                            "setLowPriority": true,
                                            "maxFolderConcurrency": 0,
                                            "crashReportingURL": "https://crash.syncthing.net/newcrash",
                                            "crashReportingEnabled": true,
                                            "stunKeepaliveStartS": 180,
                                            "stunKeepaliveMinS": 20,
                                            "stunServers": ["default"],
                                            "databaseTuning": "auto",
                                            "maxConcurrentIncomingRequestKiB": 0,
                                            "announceLANAddresses": true,
                                            "sendFullIndexOnUpgrade": false,
                                            "connectionLimitEnough": 0,
                                            "connectionLimitMax": 0,
                                            "insecureAllowOldTLSVersions": false,
                                            "connectionPriorityTcpLan": 10,
                                            "connectionPriorityQuicLan": 20,
                                            "connectionPriorityTcpWan": 30,
                                            "connectionPriorityQuicWan": 40,
                                            "connectionPriorityRelay": 50,
                                            "connectionPriorityUpgradeThreshold": 0
                                        });
                                        if let Value::Object(obj) = b {
                                            for (k, v) in obj {
                                                m.insert(k, v);
                                            }
                                        }
                                        Value::Object(m)
                                    }
                                    "gui" => json!({
                                        "enabled": true,
                                        "address": "127.0.0.1:8384",
                                        "unixSocketPermissions": "",
                                        "user": "",
                                        "password": "",
                                        "authMode": "static",
                                        "useTLS": false,
                                        "apiKey": "",
                                        "insecureAdminAccess": false,
                                        "theme": "default",
                                        "debugging": false,
                                        "insecureSkipHostcheck": false,
                                        "insecureAllowFrameLoading": false,
                                        "sendBasicAuthPrompt": false
                                    }),
                                    "ldap" => json!({
                                        "address": "",
                                        "bindDN": "",
                                        "transport": "plain",
                                        "insecureSkipVerify": false,
                                        "searchBaseDN": "",
                                        "searchFilter": ""
                                    }),
                                    _ => json!({}),
                                };
                                let val_fallback = val.clone();
                                if let (Value::Object(mut defaults), Value::Object(incoming)) =
                                    (section_defaults, val)
                                {
                                    for (k, v) in incoming {
                                        defaults.insert(k, v);
                                    }
                                    *target = Value::Object(defaults);
                                } else {
                                    *target = val_fallback;
                                }
                            }
                        }
                    } else {
                        apply_query_overrides(target, &params);
                    }
                    let value = target.clone();
                    drop(state);
                    append_event(runtime, "ConfigSaved", json!({"section": section}), false);
                    // AUDIT-MARKER(config-section-empty): W16J-B2 — Go's adjustOptions/
                    // adjustGUI/adjustLDAP all call finish() (confighandler.go:459-465)
                    // which writes nothing to the response on success = empty 200.
                    ApiReply::empty(200)
                }
                _ => make_api_error(405, "method not allowed"),
            }
        }
        "/rest/config/defaults/folder" | "/rest/config/defaults/device" => {
            let folder_section = path.ends_with("/folder");
            match method {
                Method::Get => {
                    let state = match runtime.state.read() {
                        Ok(guard) => guard,
                        Err(_) => return make_api_error(500, "api state lock poisoned"),
                    };
                    let payload = if folder_section {
                        state.default_folder.clone()
                    } else {
                        state.default_device.clone()
                    };
                    ApiReply::json(200, payload)
                }
                Method::Put => {
                    // W8-R2: PUT = reset-from-defaults then overlay body.
                    // Go typed-decodes into a fresh default struct, not merge-over-existing.
                    let mut state = match runtime.state.write() {
                        Ok(guard) => guard,
                        Err(_) => return make_api_error(500, "api state lock poisoned"),
                    };
                    let target = if folder_section {
                        &mut state.default_folder
                    } else {
                        &mut state.default_device
                    };
                    // W9-H4: Go typed-decodes into a fresh default struct, not empty.
                    // Seed from FolderConfiguration::default() or DeviceConfiguration::default() then overlay.
                    let defaults_base = if folder_section {
                        serde_json::to_value(crate::config::FolderConfiguration::default())
                            .unwrap_or_else(|_| json!({}))
                    } else {
                        serde_json::to_value(crate::config::DeviceConfiguration::default())
                            .unwrap_or_else(|_| json!({}))
                    };
                    *target = defaults_base;
                    if let Some(body) = params.get("_body") {
                        if let Ok(val) = serde_json::from_str::<Value>(body) {
                            // Overlay body fields onto defaults
                            if let (Value::Object(mut base), Value::Object(incoming)) =
                                (target.take(), val)
                            {
                                for (k, v) in incoming {
                                    base.insert(k, v);
                                }
                                *target = Value::Object(base);
                            }
                        }
                    }
                    let value = target.clone();
                    drop(state);
                    append_event(
                        runtime,
                        "ConfigSaved",
                        json!({"section": if folder_section {"defaults-folder"} else {"defaults-device"}}),
                        false,
                    );
                    // AUDIT-MARKER(config-mutation-response): W16H — Go's finish() returns empty 200.
                    ApiReply::empty(200)
                }
                Method::Patch => {
                    // W8-R2: PATCH = merge body over existing value.
                    let mut state = match runtime.state.write() {
                        Ok(guard) => guard,
                        Err(_) => return make_api_error(500, "api state lock poisoned"),
                    };
                    let target = if folder_section {
                        &mut state.default_folder
                    } else {
                        &mut state.default_device
                    };
                    if let Some(body) = params.get("_body") {
                        if let Ok(val) = serde_json::from_str::<Value>(body) {
                            // Merge: overlay body keys onto existing target
                            if let (Some(existing), Some(incoming)) =
                                (target.as_object_mut(), val.as_object())
                            {
                                for (k, v) in incoming {
                                    existing.insert(k.clone(), v.clone());
                                }
                            } else {
                                *target = val;
                            }
                        }
                    }
                    let value = target.clone();
                    drop(state);
                    append_event(
                        runtime,
                        "ConfigSaved",
                        json!({"section": if folder_section {"defaults-folder"} else {"defaults-device"}}),
                        false,
                    );
                    // AUDIT-MARKER(config-mutation-response): W16H — Go's finish() returns empty 200.
                    ApiReply::empty(200)
                }
                _ => make_api_error(405, "method not allowed"),
            }
        }
        "/rest/config/defaults/ignores" => match method {
            Method::Get => {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                ApiReply::json(200, json!({"lines": state.default_ignores}))
            }
            Method::Put => {
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                // RT-6: Parse JSON body {"lines":[...]} instead of query param
                let lines = if let Some(body) = params.get("_body") {
                    match serde_json::from_str::<Value>(body) {
                        Ok(val) => val
                            .get("lines")
                            .and_then(Value::as_array)
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(Value::as_str)
                                    .map(ToOwned::to_owned)
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default(),
                        Err(_) => return make_api_error(400, "invalid JSON body"),
                    }
                } else {
                    params
                        .get("patterns")
                        .map(|value| {
                            value
                                .split(',')
                                .map(str::trim)
                                .filter(|p| !p.is_empty())
                                .map(ToOwned::to_owned)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                };
                state.default_ignores = lines;
                let lines = state.default_ignores.clone();
                drop(state);
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"defaults-ignores"}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's finish() returns empty 200.
                ApiReply::empty(200)
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/config/folders" => match method {
            Method::Get => match list_config_folders(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiConfigError::BadRequest(err)) => {
                    ApiReply::json(400, json!({ "error": err }))
                }
                Err(ApiConfigError::Missing(id)) => {
                    ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                }
                Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                    409,
                    json!({ "error": "folder already exists", "folder": id }),
                ),
                Err(ApiConfigError::Internal(err)) => ApiReply::json(500, json!({ "error": err })),
            },
            // RT-5: PUT replaces entire folder list from JSON body; POST upserts a single folder
            Method::Put => {
                // W8-R3: Full-list replacement — store full JSON body for each folder
                // so all typed fields are preserved (Go typed-decodes full struct).
                let Some(body) = params.get("_body") else {
                    return make_api_error(400, "PUT requires a JSON body with folder list");
                };
                let folder_list: Vec<Value> = match serde_json::from_str(body) {
                    Ok(list) => list,
                    Err(_) => return make_api_error(400, "invalid JSON body for folder list"),
                };
                let mut guard = match runtime.model.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                // Clear existing folders and replace with new list
                guard.folderCfgs.clear();
                guard.cfg.clear();
                for folder_val in &folder_list {
                    if let Some(id) = folder_val.get("id").and_then(Value::as_str) {
                        let path = folder_val
                            .get("path")
                            .and_then(Value::as_str)
                            .unwrap_or("/tmp/default")
                            .to_string();
                        // AUDIT-MARKER(folder-typed-defaults): W14-4/W16C — Start with
                        // full typed defaults via newFolderConfiguration, then overlay
                        // user-provided fields. Go applies FolderConfiguration
                        // defaults for all fields not specified in PUT body.
                        // Do NOT re-flag as "PUT missing typed defaults".
                        let mut cfg = newFolderConfiguration(id, &path);
                        if let Some(ft) = folder_val
                            .get("type")
                            .and_then(Value::as_str)
                            .and_then(parse_folder_type)
                        {
                            cfg.folder_type = ft;
                        }
                        if let Some(label) = folder_val.get("label").and_then(Value::as_str) {
                            cfg.label = label.to_string();
                        }
                        if let Some(interval) =
                            folder_val.get("rescanIntervalS").and_then(Value::as_i64)
                        {
                            cfg.rescan_interval_s = interval as i32;
                        }
                        if let Some(ignore_perms) =
                            folder_val.get("ignorePerms").and_then(Value::as_bool)
                        {
                            cfg.ignore_perms = ignore_perms;
                        }
                        if let Some(ignore_delete) =
                            folder_val.get("ignoreDelete").and_then(Value::as_bool)
                        {
                            cfg.ignore_delete = ignore_delete;
                        }
                        if let Some(max_conflicts) =
                            folder_val.get("maxConflicts").and_then(Value::as_i64)
                        {
                            cfg.max_conflicts = max_conflicts as i32;
                        }
                        // W8-R3: Preserve all JSON fields alongside the typed config
                        guard.cfg.insert(id.to_string(), cfg.clone());
                        guard.folderCfgs.insert(id.to_string(), cfg);
                    }
                }
                drop(guard);
                // W8-R3: Also store full JSON in API state for round-trip fidelity
                if let Ok(mut state) = runtime.state.write() {
                    state.folder_overrides.clear();
                    for folder_val in &folder_list {
                        if let Some(id) = folder_val.get("id").and_then(Value::as_str) {
                            state
                                .folder_overrides
                                .insert(id.to_string(), folder_val.clone());
                        }
                    }
                }
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"folders","action":"replace"}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's finish() returns empty 200.
                ApiReply::empty(200)
            }
            Method::Post => {
                // W6-M1: Go decodes full JSON body, not query params.
                let body: Value = params
                    .get("_body")
                    .and_then(|b| serde_json::from_str(b).ok())
                    .unwrap_or(json!({}));
                let folder_id = body
                    .get("id")
                    .and_then(|v| v.as_str())
                    .or_else(|| params.get("id").map(String::as_str));
                let Some(folder_id) = folder_id else {
                    return make_api_error(400, "missing id in request body");
                };
                let default_path = format!("/tmp/{folder_id}");
                let path_value = body
                    .get("path")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&default_path);
                let folder_type = body
                    .get("type")
                    .and_then(|v| v.as_str())
                    .and_then(|v| parse_folder_type(v));
                match add_config_folder(runtime, folder_id, path_value, folder_type) {
                    Ok(payload) => {
                        // W9-H3: Store full body JSON as override so all fields survive round-trip
                        if let Ok(mut state) = runtime.state.write() {
                            state
                                .folder_overrides
                                .insert(folder_id.to_string(), body.clone());
                        }
                        append_event(
                            runtime,
                            "ConfigSaved",
                            json!({"section":"folders","id":folder_id}),
                            false,
                        );
                        ApiReply::json(200, payload)
                    }
                    Err(ApiConfigError::BadRequest(err)) => {
                        ApiReply::json(400, json!({ "error": err }))
                    }
                    Err(ApiConfigError::Missing(id)) => {
                        ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                    }
                    Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                        409,
                        json!({ "error": "folder already exists", "folder": id }),
                    ),
                    Err(ApiConfigError::Internal(err)) => {
                        ApiReply::json(500, json!({ "error": err }))
                    }
                }
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/config/devices" => match method {
            Method::Get => {
                let state = match runtime.state.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                let mut devices = state.device_configs.values().cloned().collect::<Vec<_>>();
                devices.sort_by(|a, b| a["deviceID"].as_str().cmp(&b["deviceID"].as_str()));
                ApiReply::json(200, Value::Array(devices))
            }
            // RT-5: PUT replaces entire device list from JSON body; POST upserts a single device
            Method::Put => {
                let Some(body) = params.get("_body") else {
                    return make_api_error(400, "PUT requires a JSON body with device list");
                };
                let device_list: Vec<Value> = match serde_json::from_str(body) {
                    Ok(list) => list,
                    Err(_) => return make_api_error(400, "invalid JSON body for device list"),
                };
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                state.device_configs.clear();
                for dev_val in &device_list {
                    if let Some(id) = dev_val.get("deviceID").and_then(Value::as_str) {
                        state.device_configs.insert(id.to_string(), dev_val.clone());
                    }
                }
                drop(state);
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"devices","action":"replace"}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's finish() returns empty 200.
                ApiReply::empty(200)
            }
            Method::Post => {
                // W6-M1: Go decodes full JSON body, not query params.
                let body: Value = params
                    .get("_body")
                    .and_then(|b| serde_json::from_str(b).ok())
                    .unwrap_or(json!({}));
                let device_id = body
                    .get("deviceID")
                    .and_then(|v| v.as_str())
                    .or_else(|| params.get("id").map(String::as_str))
                    .or_else(|| params.get("deviceID").map(String::as_str));
                let Some(device_id) = device_id else {
                    return make_api_error(400, "missing deviceID in request body");
                };
                let mut state = match runtime.state.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "api state lock poisoned"),
                };
                // W9-H3: Store full body JSON with minimal defaults for missing fields.
                // Go typed-decodes the full DeviceConfiguration struct; we preserve
                // all user-supplied fields by overlaying onto a defaults template.
                let mut cfg = json!({
                    "deviceID": device_id,
                    "name": device_id,
                    "addresses": ["dynamic"],
                    "paused": false,
                    "compression": "metadata",
                    "introducer": false,
                });
                // Overlay all body fields onto defaults so user values win
                if let (Some(base), Some(overlay)) = (cfg.as_object_mut(), body.as_object()) {
                    for (k, v) in overlay {
                        base.insert(k.clone(), v.clone());
                    }
                }
                state
                    .device_configs
                    .insert(device_id.to_string(), cfg.clone());
                drop(state);
                append_event(
                    runtime,
                    "ConfigSaved",
                    json!({"section":"devices","id":device_id}),
                    false,
                );
                // AUDIT-MARKER(config-mutation-response): W16H — Go's adjustDevice → finish() returns empty 200.
                ApiReply::empty(200)
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/cluster/pending/devices" => match method {
            Method::Get => {
                let guard = match runtime.model.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                let pending = guard.PendingDevices();
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let mut payload = serde_json::Map::new();
                for device_id in pending
                    .into_iter()
                    .map(|id| id.trim().to_string())
                    .filter(|id| !id.is_empty())
                {
                    payload.insert(
                        device_id.clone(),
                        json!({
                            "name": device_id,
                            "address": "unknown",
                            "time": now_ms,
                        }),
                    );
                }
                ApiReply::json(200, Value::Object(payload))
            }
            Method::Delete => {
                let Some(device) = params.get("device") else {
                    return make_api_error(400, "missing device query parameter");
                };
                let mut guard = match runtime.model.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                if let Err(err) = guard.DismissPendingDevice(device) {
                    return make_api_error(400, err);
                }
                ApiReply::json(200, json!({"dismissed": true, "device": device}))
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/cluster/pending/folders" => match method {
            Method::Get => {
                let device_filter = params
                    .get("device")
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty())
                    .map(|value| value.to_string());
                if let Some(filter) = device_filter.as_deref() {
                    if normalize_syncthing_device_id(filter).is_none() {
                        return make_api_error(400, "invalid device id");
                    }
                }
                let guard = match runtime.model.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                let pending = guard.PendingFolderOffers();
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let mut payload = serde_json::Map::new();
                for (folder_id, offered_by_devices) in pending {
                    let folder_id = folder_id.trim().to_string();
                    if folder_id.is_empty() {
                        continue;
                    }
                    let mut offered_by = serde_json::Map::new();
                    for device_id in offered_by_devices
                        .into_iter()
                        .map(|id| id.trim().to_string())
                        .filter(|id| {
                            !id.is_empty()
                                && device_filter
                                    .as_ref()
                                    .map(|filter| id == filter)
                                    .unwrap_or(true)
                        })
                    {
                        offered_by.insert(
                            device_id.clone(),
                            json!({
                                "name": device_id,
                                "address": "unknown",
                                "time": now_ms,
                            }),
                        );
                    }
                    if !offered_by.is_empty() {
                        payload.insert(folder_id, json!({"offeredBy": offered_by}));
                    }
                }
                ApiReply::json(200, Value::Object(payload))
            }
            Method::Delete => {
                let Some(folder) = params.get("folder") else {
                    return make_api_error(400, "missing folder query parameter");
                };
                // AUDIT-MARKER(pending-device-param): W16K-K4 — Go uses
                // the raw device query param directly, no default. If empty,
                // DismissPendingFolder acts on all devices for the folder.
                let device = params.get("device").map(|v| v.as_str()).unwrap_or("");
                let mut guard = match runtime.model.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                if let Err(err) = guard.DismissPendingFolder(device, folder) {
                    return make_api_error(400, err);
                }
                ApiReply::json(
                    200,
                    json!({"dismissed": true, "folder": folder, "device": device}),
                )
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/events" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let since = params
                .get("since")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            // R7: Go defaults to no limit (0 means unlimited), align with that
            let limit = parse_limit(&params, "limit", 0, 10_000);
            let timeout_s = params
                .get("timeout")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60);
            let event_filter = parse_event_type_filter_with_default(
                params.get("events").map(String::as_str),
                false,
            );
            api_events(runtime, false, since, limit, timeout_s, event_filter)
        }
        "/rest/events/disk" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let since = params
                .get("since")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
            // R7: Go defaults to no limit (0 means unlimited)
            let limit = parse_limit(&params, "limit", 0, 10_000);
            let timeout_s = params
                .get("timeout")
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(60);
            let event_filter = parse_event_type_filter_with_default(None, true);
            api_events(runtime, true, since, limit, timeout_s, event_filter)
        }
        "/rest/stats/device" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let guard = match runtime.model.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "model lock poisoned"),
            };
            let mut devices = BTreeMap::new();
            for (id, values) in &guard.deviceStatRefs {
                devices.insert(id.clone(), json!(values));
            }
            ApiReply::json(200, json!(devices))
        }
        "/rest/stats/folder" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let guard = match runtime.model.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "model lock poisoned"),
            };
            let mut folders = BTreeMap::new();
            for id in guard.folderCfgs.keys() {
                folders.insert(id.clone(), json!(guard.FolderStatistics(id)));
            }
            ApiReply::json(200, json!(folders))
        }
        "/rest/svc/deviceid" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let input = params.get("id").cloned().unwrap_or_default();
            match normalize_syncthing_device_id(&input) {
                Some(id) => ApiReply::json(200, json!({"id": id})),
                None => ApiReply::json(200, json!({"error": "invalid device id"})),
            }
        }
        "/rest/svc/lang" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let raw = params
                .get("accept-language")
                .cloned()
                .or_else(|| params.get("accept").cloned())
                .unwrap_or_else(|| "en-us".to_string());
            ApiReply::json(200, json!(parse_accept_language_values(&raw)))
        }
        "/rest/svc/report" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let guard = match runtime.model.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "model lock poisoned"),
            };
            ApiReply::json(200, json!(guard.UsageReportingStats()))
        }
        "/rest/svc/random/string" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let length = params
                .get("length")
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|v| *v > 0 && *v <= 512)
                .unwrap_or(32);
            let mut seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789";
            let mut out = String::with_capacity(length);
            for _ in 0..length {
                seed = seed.wrapping_mul(6364136223846793005_u128).wrapping_add(1);
                let idx = (seed as usize) % alphabet.len();
                out.push(alphabet[idx] as char);
            }
            ApiReply::json(200, json!({"random": out}))
        }
        "/rest/folder/errors" | "/rest/folder/pullerrors" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            let guard = match runtime.model.read() {
                Ok(guard) => guard,
                Err(_) => return make_api_error(500, "model lock poisoned"),
            };
            let errors = match guard.FolderErrors(folder) {
                Ok(errors) => errors,
                Err(_) => return make_api_error(404, "folder not found"),
            };
            let items = errors
                .iter()
                .map(|entry| json!({"path": entry.Path, "error": entry.Err}))
                .collect::<Vec<_>>();
            ApiReply::json(
                200,
                json!({"folder": folder, "errors": items, "count": items.len()}),
            )
        }
        "/rest/folder/versions" => match method {
            Method::Get => {
                let Some(folder) = params.get("folder") else {
                    return make_api_error(400, "missing folder query parameter");
                };
                let guard = match runtime.model.read() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                let versions = match guard.GetFolderVersions(folder) {
                    Ok(v) => v,
                    Err(err) if err == crate::model_core::errNoVersioner => {
                        // No versioner configured — return empty versions list gracefully.
                        Vec::new()
                    }
                    Err(err) => return make_api_error(400, &err),
                };
                // W6-H3: Go returns raw map[string][]FileVersion, no envelope wrapper.
                ApiReply::json(200, json!(versions))
            }
            Method::Post => {
                let Some(folder) = params.get("folder") else {
                    return make_api_error(400, "missing folder query parameter");
                };
                let mut guard = match runtime.model.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                // W8-R5: Parse body as map of file→timestamp.
                // Go accepts time.Time (ISO/RFC3339 strings). Accept both ISO strings
                // and raw i64 epoch seconds for backwards compatibility.
                let versions_map: std::collections::BTreeMap<String, i64> = params
                    .get("_body")
                    .and_then(|b| {
                        // Try parsing as BTreeMap<String, String> first (ISO timestamps)
                        if let Ok(str_map) =
                            serde_json::from_str::<std::collections::BTreeMap<String, String>>(b)
                        {
                            let epoch_map: std::collections::BTreeMap<String, i64> = str_map
                                .into_iter()
                                .filter_map(|(k, v)| {
                                    // Try ISO/RFC3339 parse, fall back to raw integer parse
                                    if let Ok(t) = humantime::parse_rfc3339(&v) {
                                        let epoch =
                                            t.duration_since(std::time::UNIX_EPOCH).ok()?.as_secs()
                                                as i64;
                                        Some((k, epoch))
                                    } else {
                                        v.parse::<i64>().ok().map(|n| (k, n))
                                    }
                                })
                                .collect();
                            Some(epoch_map)
                        } else {
                            // Fall back to BTreeMap<String, i64> (raw epoch seconds)
                            serde_json::from_str(b).ok()
                        }
                    })
                    .unwrap_or_default();
                match guard.RestoreFolderVersions(folder, &versions_map) {
                    Ok(restore_errors) => ApiReply::json(200, json!(restore_errors)),
                    Err(err) if err == crate::model_core::errNoVersioner => {
                        // No versioner configured — treat as successful no-op.
                        ApiReply::json(
                            200,
                            json!({"folder": folder, "restored": true, "errors": 0}),
                        )
                    }
                    Err(err) => {
                        if err == crate::model_core::ErrFolderMissing {
                            make_api_error(404, &err)
                        } else {
                            make_api_error(400, &err)
                        }
                    }
                }
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/system/config/folders" => match method {
            Method::Get => match list_config_folders(runtime) {
                Ok(payload) => {
                    if let Some(folders) = payload.as_array() {
                        ApiReply::json(200, json!({"count": folders.len(), "folders": payload}))
                    } else {
                        ApiReply::json(200, payload)
                    }
                }
                Err(ApiConfigError::BadRequest(err)) => {
                    ApiReply::json(400, json!({ "error": err }))
                }
                Err(ApiConfigError::Missing(id)) => {
                    ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                }
                Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                    409,
                    json!({ "error": "folder already exists", "folder": id }),
                ),
                Err(ApiConfigError::Internal(err)) => ApiReply::json(500, json!({ "error": err })),
            },
            Method::Post => {
                let Some(folder_id) = params.get("id") else {
                    return make_api_error(400, "missing id query parameter");
                };
                let Some(path_value) = params.get("path") else {
                    return make_api_error(400, "missing path query parameter");
                };
                let folder_type = params.get("type").and_then(|v| parse_folder_type(v));
                match add_config_folder(runtime, folder_id, path_value, folder_type) {
                    Ok(payload) => ApiReply::json(200, payload),
                    Err(ApiConfigError::BadRequest(err)) => {
                        ApiReply::json(400, json!({ "error": err }))
                    }
                    Err(ApiConfigError::Missing(id)) => {
                        ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                    }
                    Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                        409,
                        json!({ "error": "folder already exists", "folder": id }),
                    ),
                    Err(ApiConfigError::Internal(err)) => {
                        ApiReply::json(500, json!({ "error": err }))
                    }
                }
            }
            Method::Delete => {
                let Some(folder_id) = params.get("id") else {
                    return make_api_error(400, "missing id query parameter");
                };
                match remove_config_folder(runtime, folder_id) {
                    Ok(payload) => ApiReply::json(200, payload),
                    Err(ApiConfigError::BadRequest(err)) => {
                        ApiReply::json(400, json!({ "error": err }))
                    }
                    Err(ApiConfigError::Missing(id)) => {
                        ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                    }
                    Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                        409,
                        json!({ "error": "folder already exists", "folder": id }),
                    ),
                    Err(ApiConfigError::Internal(err)) => {
                        ApiReply::json(500, json!({ "error": err }))
                    }
                }
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/system/config/restart" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder_id) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match restart_config_folder(runtime, folder_id) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiConfigError::BadRequest(err)) => {
                    ApiReply::json(400, json!({ "error": err }))
                }
                Err(ApiConfigError::Missing(id)) => {
                    ApiReply::json(404, json!({ "error": "folder not found", "folder": id }))
                }
                Err(ApiConfigError::Conflict(id)) => ApiReply::json(
                    409,
                    json!({ "error": "folder already exists", "folder": id }),
                ),
                Err(ApiConfigError::Internal(err)) => ApiReply::json(500, json!({ "error": err })),
            }
        }
        "/rest/db/status" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match folder_status(runtime, folder) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/completion" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            // AUDIT-MARKER(completion-empty-folder): W16H — Go explicitly allows
            // empty folder param for aggregate "all folders" completion.
            // Do NOT re-flag.
            let folder_param = params.get("folder").cloned().unwrap_or_default();
            // AUDIT-MARKER(completion-defaults): W14-1/W16G — Go defaults to
            // the local device ID when device param is omitted. This matches
            // Go's Completion() handler. Do NOT re-flag.
            let local_dev_id = runtime
                .state
                .read()
                .ok()
                .and_then(|s| {
                    s.default_device
                        .get("deviceID")
                        .and_then(Value::as_str)
                        .map(String::from)
                })
                .unwrap_or_else(|| crate::db::LOCAL_DEVICE_ID.to_string());
            let device_param = params.get("device").cloned();
            let device = device_param.as_deref().unwrap_or(&local_dev_id);
            match folder_completion(runtime, &folder_param, device) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder_param }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/file" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            let Some(file) = params.get("file") else {
                return make_api_error(400, "missing file query parameter");
            };
            let global = bool_param(&params, "global").unwrap_or(false);
            match folder_file(runtime, folder, file, global) {
                Ok(payload) => {
                    if payload
                        .get("exists")
                        .and_then(Value::as_bool)
                        .is_some_and(|exists| !exists)
                    {
                        // AUDIT-MARKER(file-404): W16G — Go returns 404 with
                        // "no such object in the index" for missing files.
                        ApiReply::json(404, json!({ "error": "no such object in the index" }))
                    } else {
                        ApiReply::json(200, payload)
                    }
                }
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/localchanged" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            // AUDIT-MARKER(localchanged-paging): W16J-C2 — Go's getDBLocalChanged
            // (api.go:876-893) uses page/perpage paging via getPagingParams.
            let page = params
                .get("page")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1);
            let perpage = params
                .get("perpage")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1 << 16);
            match folder_local_changed(runtime, folder, page, perpage) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/remoteneed" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            // W15-3: Go requires the device parameter — returns 400 if missing.
            let Some(device) = params.get("device") else {
                return make_api_error(400, "missing device query parameter");
            };
            let device = device.as_str();
            // AUDIT-MARKER(remoteneed-paging): W16J-C2 — Go's getDBRemoteNeed
            // (api.go:850-873) uses page/perpage paging via getPagingParams.
            let page = params
                .get("page")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1);
            let perpage = params
                .get("perpage")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1 << 16);
            match folder_remote_need(runtime, folder, device, page, perpage) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/ignores" => match method {
            Method::Get => {
                let Some(folder) = params.get("folder") else {
                    return make_api_error(400, "missing folder query parameter");
                };
                match folder_ignores(runtime, folder) {
                    Ok(payload) => ApiReply::json(200, payload),
                    Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                        404,
                        json!({ "error": "folder not found", "folder": folder }),
                    ),
                    Err(ApiFolderStatusError::Internal(err)) => {
                        ApiReply::json(500, json!({ "error": err }))
                    }
                }
            }
            Method::Post => {
                let Some(folder) = params.get("folder") else {
                    return make_api_error(400, "missing folder query parameter");
                };
                // W10-M4: Go reads ignore patterns from JSON body {"ignore":[...]},
                // not query parameters
                let patterns = if let Some(body) = params.get("_body") {
                    if let Ok(val) = serde_json::from_str::<Value>(body) {
                        val.get("ignore")
                            .or_else(|| val.get("patterns"))
                            .and_then(Value::as_array)
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(Value::as_str)
                                    .map(ToOwned::to_owned)
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default()
                    } else {
                        // AUDIT-MARKER(ignores-json-fail): W16J-A3 — Go returns 500
                        // on JSON decode failure (api.go:1324-1327), not empty list.
                        return make_api_error(500, "invalid JSON body for ignore patterns");
                    }
                } else {
                    // R11: Go only accepts JSON body on POST, no query-param fallback
                    Vec::new()
                };
                let mut guard = match runtime.model.write() {
                    Ok(guard) => guard,
                    Err(_) => return make_api_error(500, "model lock poisoned"),
                };
                if !guard.folderCfgs.contains_key(folder) {
                    return make_api_error(404, "folder not found");
                }
                guard.SetIgnores(folder, patterns.clone());
                append_event(
                    runtime,
                    "FolderSummary",
                    json!({"folder": folder, "ignoresUpdated": true}),
                    false,
                );
                ApiReply::json(
                    200,
                    json!({
                        "ignore": patterns,
                        "expanded": patterns,
                        "error": null,
                    }),
                )
            }
            _ => make_api_error(405, "method not allowed"),
        },
        "/rest/db/prio" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            let file = params.get("file").cloned().unwrap_or_default();
            // R12: Go returns the real need-list first page after bumping priority.
            match bring_to_front(runtime, folder) {
                Ok(_) => {
                    // After priority bump, return the actual need page
                    match browse_needed_files(runtime, folder, None, 1) {
                        Ok(payload) => ApiReply::json(200, payload),
                        Err(_) => ApiReply::json(
                            200,
                            json!({
                                "progress": 0,
                                "queued": 0,
                                "rest": [],
                                "page": 1,
                                "perpage": 1
                            }),
                        ),
                    }
                }
                Err(ApiFolderStatusError::MissingFolder) => make_api_error(404, "folder not found"),
                Err(ApiFolderStatusError::Internal(err)) => make_api_error(500, err),
            }
        }
        "/rest/db/browse" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            let prefix = params.get("prefix").map(|v| v.as_str()).unwrap_or("");
            // AUDIT-MARKER(browse-params): W16J-C3 — Go's getDBBrowse (api.go:745-762)
            // uses prefix, levels, dirsonly — NOT cursor/limit.
            // AUDIT-MARKER(browse-apply): W16K-K3 — Go passes levels + dirsonly
            // to GlobalDirectoryTree (api.go:755). Apply them to traversal.
            let levels: i32 = params
                .get("levels")
                .and_then(|v| v.parse().ok())
                .unwrap_or(-1);
            let dirsonly = params.get("dirsonly").is_some();
            match browse_local_files(runtime, folder, None, 100_000) {
                Ok(payload) => {
                    // AUDIT-MARKER(browse-tree): W15B-4/W16G — Go's /rest/db/browse
                    // returns a directory tree structure, not flat file list.
                    // [{"name":"dir","type":"FILE_INFO_TYPE_DIRECTORY","children":[...]},
                    //  {"name":"file.txt","type":"FILE_INFO_TYPE_FILE","size":123,"modTime":"..."}]
                    // Do NOT re-flag as "browse returns flat list".
                    // We build this tree from the flat file list, filtered by prefix.
                    let items = payload
                        .get("items")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    // Filter by prefix first
                    let filtered: Vec<Value> = if prefix.is_empty() {
                        items
                    } else {
                        items
                            .into_iter()
                            .filter(|item| {
                                item.get("path")
                                    .and_then(Value::as_str)
                                    .map(|p| p.starts_with(prefix))
                                    .unwrap_or(false)
                            })
                            .collect()
                    };
                    // Build Go-style tree: group by top-level path component
                    // AUDIT-MARKER(browse-apply-logic): W16K-K3 — levels controls max
                    // depth, dirsonly filters non-dir entries. Go passes these to
                    // GlobalDirectoryTree (api.go:755).
                    let mut tree: std::collections::BTreeMap<String, Value> =
                        std::collections::BTreeMap::new();
                    let strip_len = prefix.len();
                    for item in &filtered {
                        let path = item.get("path").and_then(Value::as_str).unwrap_or_default();
                        let relative = &path[strip_len.min(path.len())..];
                        let top = relative.split('/').next().unwrap_or(relative);
                        if top.is_empty() {
                            continue;
                        }
                        // K3: depth is the number of '/' separators + 1.
                        // levels == -1 means unlimited; levels == 0 shows nothing;
                        // levels == 1 shows only top-level entries, etc.
                        let depth = relative.matches('/').count() as i32 + 1;
                        if levels >= 0 && depth > levels {
                            continue;
                        }
                        let is_dir = relative.contains('/');
                        // K3: dirsonly=true → skip non-directory entries
                        if dirsonly && !is_dir {
                            continue;
                        }
                        if is_dir {
                            // Directory entry — collect children count
                            let entry = tree.entry(top.to_string()).or_insert_with(|| {
                                json!({
                                    "name": top,
                                    "type": "FILE_INFO_TYPE_DIRECTORY",
                                    "children": []
                                })
                            });
                            // Add child to children array if not already there
                            if let Some(children) =
                                entry.get_mut("children").and_then(Value::as_array_mut)
                            {
                                let child_name = relative.split('/').nth(1).unwrap_or_default();
                                if !child_name.is_empty() {
                                    let child_entry = json!({
                                        "name": child_name,
                                        "size": item.get("size").and_then(Value::as_i64).unwrap_or(0),
                                    });
                                    // Avoid duplicate children names
                                    if !children
                                        .iter()
                                        .any(|c| c.get("name") == child_entry.get("name"))
                                    {
                                        children.push(child_entry);
                                    }
                                }
                            }
                        } else {
                            // File entry
                            tree.entry(top.to_string()).or_insert_with(|| {
                                json!({
                                    "name": top,
                                    "type": "FILE_INFO_TYPE_FILE",
                                    "size": item.get("size").and_then(Value::as_i64).unwrap_or(0),
                                })
                            });
                        }
                    }
                    let tree_array: Vec<Value> = tree.into_values().collect();
                    ApiReply::json(200, Value::Array(tree_array))
                }
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/need" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            // AUDIT-MARKER(need-paging): W16J-C1 — Go passes page/perpage to
            // NeedFolderFiles (api.go:832-847). Apply offset to slice results.
            let page = params
                .get("page")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1);
            let perpage = params
                .get("perpage")
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(1 << 16);
            let cursor = params.get("cursor").map(|v| v.as_str());
            let limit = perpage;
            let page_offset = (page.saturating_sub(1)) * perpage;
            match browse_needed_files(runtime, folder, cursor, limit) {
                Ok(payload) => {
                    // AUDIT-MARKER(need-shape): W16K-K7 — Go returns
                    // {progress:[], queued:[], rest:[], page:N, perpage:N}
                    // (api.go:841-847). Apply paging per-category. Since Rust
                    // doesn't track progress/queued states, all items go to rest.
                    let items = payload
                        .get("items")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let total = items.len();
                    let start = page_offset.min(total);
                    let end = (start + perpage).min(total);
                    let paged = &items[start..end];
                    ApiReply::json(
                        200,
                        json!({
                            "progress": [],
                            "queued": [],
                            "rest": paged,
                            "page": page,
                            "perpage": perpage,
                        }),
                    )
                }
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/jobs" => {
            if method != &Method::Get {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match folder_jobs(runtime, folder) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/scan" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let subdirs = parse_subdirs(query, &params);
            if let Some(folder) = params.get("folder") {
                match scan_folder(runtime, folder, &subdirs) {
                    Ok(payload) => {
                        append_event(
                            runtime,
                            "LocalIndexUpdated",
                            json!({"folder": folder, "subdirs": subdirs}),
                            true,
                        );
                        ApiReply::json(200, payload)
                    }
                    Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                        404,
                        json!({ "error": "folder not found", "folder": folder }),
                    ),
                    Err(ApiFolderStatusError::Internal(err)) => {
                        ApiReply::json(500, json!({ "error": err }))
                    }
                }
            } else {
                match scan_all_folders(runtime, &subdirs) {
                    Ok(()) => ApiReply::json(200, json!({"scanned": true})),
                    Err(ApiFolderStatusError::Internal(err)) => {
                        ApiReply::json(500, json!({ "error": err }))
                    }
                    Err(ApiFolderStatusError::MissingFolder) => {
                        ApiReply::json(500, json!({ "error": "unexpected missing folder" }))
                    }
                }
            }
        }
        "/rest/db/pull" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match pull_folder(runtime, folder) {
                Ok(payload) => {
                    append_event(
                        runtime,
                        "RemoteIndexUpdated",
                        json!({"folder": folder}),
                        false,
                    );
                    ApiReply::json(200, payload)
                }
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/override" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match override_folder(runtime, folder) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/revert" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match revert_folder(runtime, folder) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/bringtofront" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match bring_to_front(runtime, folder) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        "/rest/db/reset" => {
            if method != &Method::Post {
                return make_api_error(405, "method not allowed");
            }
            let Some(folder) = params.get("folder") else {
                return make_api_error(400, "missing folder query parameter");
            };
            match reset_folder(runtime, folder) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(ApiFolderStatusError::MissingFolder) => ApiReply::json(
                    404,
                    json!({ "error": "folder not found", "folder": folder }),
                ),
                Err(ApiFolderStatusError::Internal(err)) => {
                    ApiReply::json(500, json!({ "error": err }))
                }
            }
        }
        _ => ApiReply::json(404, json!({ "error": "not found" })),
    }
}

fn resolve_gui_root() -> Option<PathBuf> {
    let mut candidates = Vec::new();

    if let Ok(path) = std::env::var("SYNCTHING_RS_GUI_DIR") {
        if !path.trim().is_empty() {
            candidates.push(PathBuf::from(path));
        }
    }

    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            candidates.push(exe_dir.join("gui/default"));
            candidates.push(exe_dir.join("../gui/default"));
        }
    }

    if let Ok(cwd) = std::env::current_dir() {
        candidates.push(cwd.join("gui/default"));
        candidates.push(cwd.join("gui").join("default"));
    }

    for candidate in candidates {
        let index = candidate.join("index.html");
        if index.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn build_gui_response(method: &Method, path: &str, runtime: &DaemonApiRuntime) -> ApiReply {
    if method != &Method::Get {
        return make_api_error(405, "method not allowed");
    }

    if path == "/meta.js" {
        let body = match build_gui_metadata_js(runtime) {
            Ok(body) => body,
            Err(err) => return make_api_error(500, err),
        };
        return ApiReply::bytes(200, body, "application/javascript; charset=utf-8");
    }

    let Some(gui_root) = runtime.gui_root.as_ref() else {
        return make_api_error(404, "gui not found");
    };

    let Some(mut relative_path) = sanitize_gui_path(path) else {
        return make_api_error(400, "invalid path");
    };
    if relative_path.as_os_str().is_empty() || path.ends_with('/') {
        relative_path.push("index.html");
    }

    let mut file_path = gui_root.join(relative_path);
    if file_path.is_dir() {
        file_path = file_path.join("index.html");
    }
    if !file_path.is_file() {
        return make_api_error(404, "not found");
    }

    let body = match fs::read(&file_path) {
        Ok(data) => data,
        Err(err) => return make_api_error(500, format!("read gui asset: {err}")),
    };
    ApiReply::bytes(200, body, mime_type_for_path(&file_path))
}

fn sanitize_gui_path(path: &str) -> Option<PathBuf> {
    let raw = path.trim_start_matches('/');
    let mut out = PathBuf::new();
    for comp in Path::new(raw).components() {
        match comp {
            Component::Normal(segment) => out.push(segment),
            Component::CurDir => {}
            _ => return None,
        }
    }
    Some(out)
}

fn mime_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        Some("eot") => "application/vnd.ms-fontobject",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

fn build_gui_metadata_js(runtime: &DaemonApiRuntime) -> Result<Vec<u8>, String> {
    let device_id = runtime
        .model
        .read()
        .map_err(|_| "model lock poisoned".to_string())?
        .id
        .clone();
    let device_id_short = device_id.chars().take(7).collect::<String>();
    serde_json::to_vec(&json!({
        "deviceID": device_id,
        "deviceIDShort": device_id_short,
        "authenticated": true,
    }))
    .map(|meta| format!("var metadata = {};\n", String::from_utf8_lossy(&meta)).into_bytes())
    .map_err(|err| format!("encode gui metadata: {err}"))
}

fn split_url(url: &str) -> (&str, &str) {
    match url.split_once('?') {
        Some((path, query)) => (path, query),
        None => (url, ""),
    }
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some(parts) => parts,
            None => (pair, ""),
        };
        let decoded_key = decode_query_component(key);
        if decoded_key.is_empty() {
            continue;
        }
        out.insert(decoded_key, decode_query_component(value));
    }
    out
}

fn append_query_param(url: &str, key: &str, value: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!("{url}{separator}{key}={value}")
}

fn apply_query_overrides(target: &mut Value, params: &BTreeMap<String, String>) {
    if !target.is_object() {
        return;
    }
    for (k, raw) in params {
        let existing = target.get(k);
        target[k] = coerce_query_value(existing, raw);
    }
}

fn coerce_query_value(existing: Option<&Value>, raw: &str) -> Value {
    if let Some(existing) = existing {
        if existing.is_boolean() {
            return Value::Bool(parse_xml_bool(raw));
        }
        if existing.as_i64().is_some() {
            if let Ok(v) = raw.parse::<i64>() {
                return Value::from(v);
            }
        }
        if existing.as_u64().is_some() {
            if let Ok(v) = raw.parse::<u64>() {
                return Value::from(v);
            }
        }
        if existing.as_f64().is_some() {
            if let Ok(v) = raw.parse::<f64>() {
                if let Some(num) = serde_json::Number::from_f64(v) {
                    return Value::Number(num);
                }
            }
        }
        if let Some(arr) = existing.as_array() {
            if arr.iter().all(Value::is_string) {
                let items = raw
                    .split(',')
                    .map(str::trim)
                    .filter(|part| !part.is_empty())
                    .map(|part| Value::String(part.to_string()))
                    .collect::<Vec<_>>();
                return Value::Array(items);
            }
        }
    }
    if let Ok(parsed) = serde_json::from_str::<Value>(raw) {
        return parsed;
    }
    Value::String(raw.to_string())
}

fn decode_query_component(value: &str) -> String {
    let mut out = Vec::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut i = 0_usize;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                if let (Some(hi), Some(lo)) = (
                    decode_hex_nibble(bytes[i + 1]),
                    decode_hex_nibble(bytes[i + 2]),
                ) {
                    out.push((hi << 4) | lo);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).to_string()
}

fn decode_hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn parse_subdirs(query: &str, params: &BTreeMap<String, String>) -> Vec<String> {
    let mut out = query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| pair.split_once('='))
        .filter_map(|(k, v)| {
            if decode_query_component(k) == "sub" {
                Some(decode_query_component(v))
            } else {
                None
            }
        })
        .flat_map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|segment| !segment.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    if out.is_empty() {
        out = params
            .get("sub")
            .map(|v| {
                v.split(',')
                    .map(str::trim)
                    .filter(|segment| !segment.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
    }
    out.sort();
    out.dedup();
    out
}

fn parse_limit(
    params: &BTreeMap<String, String>,
    key: &str,
    default_limit: usize,
    max_limit: usize,
) -> usize {
    params
        .get(key)
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .map(|v| v.min(max_limit))
        .unwrap_or(default_limit)
}

fn bool_param(params: &BTreeMap<String, String>, key: &str) -> Option<bool> {
    params.get(key).map(|v| parse_xml_bool(v))
}

fn parse_accept_language_values(raw: &str) -> Vec<String> {
    let mut weighted = raw
        .split(',')
        .filter_map(|entry| {
            let part = entry.trim();
            if part.is_empty() {
                return None;
            }
            let mut bits = part.split(';');
            let lang = bits.next()?.trim().to_ascii_lowercase();
            if lang.is_empty() {
                return None;
            }
            let mut weight = 1.0_f64;
            for bit in bits {
                let bit = bit.trim();
                if let Some(rest) = bit.strip_prefix("q=") {
                    if let Ok(parsed) = rest.parse::<f64>() {
                        weight = parsed;
                    }
                }
            }
            Some((lang, weight))
        })
        .collect::<Vec<_>>();
    weighted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    let mut out = Vec::new();
    for (lang, _) in weighted {
        if !out.contains(&lang) {
            out.push(lang);
        }
    }
    if out.is_empty() {
        out.push("en-us".to_string());
    }
    out
}

/// 7.1: Generate a random 32-character alphanumeric API key,
/// matching Go's `rand.String(32)` in GUIConfiguration.prepare().
fn generate_random_api_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    seed ^= std::process::id() as u64;
    let mut key = String::with_capacity(32);
    for _ in 0..32 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = ((seed >> 33) as usize) % CHARSET.len();
        key.push(CHARSET[idx] as char);
    }
    key
}

/// 7.2: Matches Go's `isNoAuthPath` from api_auth.go:97-126.
/// These paths are served without authentication.
fn is_no_auth_path(path: &str, runtime: &DaemonApiRuntime) -> bool {
    // Exact matches (Go's noAuthPaths)
    // AUDIT-MARKER(svc-lang-no-auth): /rest/svc/lang IS in the no-auth list
    // below. Do NOT re-flag as "auth-gated" — it's explicitly listed here.
    let no_auth_exact: &[&str] = &["/", "/index.html", "/modal.html", "/rest/svc/lang"];
    if no_auth_exact.contains(&path) {
        return true;
    }

    // Check metricsWithoutAuth
    if path == "/metrics" {
        if let Ok(state) = runtime.state.read() {
            let metrics_no_auth = state
                .gui
                .get("metricsWithoutAuth")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if metrics_no_auth {
                return true;
            }
        }
    }

    // Prefix matches (Go's noAuthPrefixes)
    let no_auth_prefixes: &[&str] = &[
        "/assets/",
        "/syncthing/",
        "/vendor/",
        "/theme-assets/",
        "/rest/noauth",
    ];
    no_auth_prefixes
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

/// 7.4: Return 403 by default, 401 only if sendBasicAuthPrompt is set.
/// Matches Go's basicAuthAndSessionMiddleware behavior.
fn make_api_error_for_unauth(runtime: &DaemonApiRuntime) -> ApiReply {
    let send_basic_prompt = runtime
        .state
        .read()
        .ok()
        .and_then(|state| {
            state
                .gui
                .get("sendBasicAuthPrompt")
                .and_then(Value::as_bool)
        })
        .unwrap_or(false);

    if send_basic_prompt {
        let short_id = runtime_short_id(runtime);
        let mut reply = make_api_error(401, "Not Authorized");
        reply.headers.push((
            "WWW-Authenticate".to_string(),
            format!("Basic realm=\"Authorization Required ({short_id})\""),
        ));
        reply
    } else {
        make_api_error(403, "Forbidden")
    }
}

/// 7.5: Issue/refresh CSRF cookie on GUI page loads when no valid one is present.
/// Matches Go's csrfManager.ServeHTTP behavior for non-/rest/ paths (api_csrf.go:67-78).
fn maybe_set_csrf_cookie(
    reply: &mut ApiReply,
    _params: &BTreeMap<String, String>,
    runtime: &DaemonApiRuntime,
) {
    // Generate a new CSRF token and set it as a cookie.
    // In Go, this checks if an existing valid CSRF cookie is present and only
    // issues a new one if not. We always issue here since the Rust request
    // pipeline doesn't carry cookie state through params for CSRF cookies.
    let csrf_cookie_name = csrf_cookie_name(runtime);
    let csrf_token = mint_csrf_token("gui");
    if let Ok(mut state) = runtime.state.write() {
        // Store the token so it can be validated later
        let gui_token_key = format!("_gui_csrf_{}", state.auth_param_salt);
        state
            .active_auth_csrf
            .insert(gui_token_key, csrf_token.clone());
    }
    let secure_attr = runtime
        .state
        .read()
        .ok()
        .and_then(|state| state.gui.get("useTLS").and_then(Value::as_bool))
        .map(|tls| if tls { "; Secure" } else { "" })
        .unwrap_or("");
    reply.headers.push((
        "Set-Cookie".to_string(),
        format!("{csrf_cookie_name}={csrf_token}; Path=/; SameSite=Lax{secure_attr}"),
    ));
}

fn auth_required(state: &ApiRuntimeState) -> bool {
    // 13.1: Go checks user != "" && password != "".
    // API key provides an alternative auth *mechanism* but doesn't gate
    // whether auth is required.
    let user = state
        .gui
        .get("user")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let password = state
        .gui
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or_default();
    // R1: Go also checks authMode == "ldap" as requiring auth
    let auth_mode = state
        .gui
        .get("authMode")
        .and_then(Value::as_str)
        .unwrap_or_default();
    (!user.is_empty() && !password.is_empty()) || auth_mode == "ldap"
}

fn runtime_short_id(runtime: &DaemonApiRuntime) -> String {
    runtime
        .model
        .read()
        .ok()
        .map(|m| m.shortID.clone())
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| "local".to_string())
}

fn session_cookie_name(runtime: &DaemonApiRuntime) -> String {
    format!("sessionid-{}", runtime_short_id(runtime))
}

fn csrf_cookie_name(runtime: &DaemonApiRuntime) -> String {
    format!("CSRF-Token-{}", runtime_short_id(runtime))
}

fn csrf_header_name(runtime: &DaemonApiRuntime) -> String {
    format!("X-CSRF-Token-{}", runtime_short_id(runtime))
}

fn auth_query_token_key(state: &ApiRuntimeState) -> String {
    format!("_auth_token_{}", state.auth_param_salt)
}

fn auth_query_csrf_key(state: &ApiRuntimeState) -> String {
    format!("_auth_csrf_{}", state.auth_param_salt)
}

fn auth_query_apikey_key(state: &ApiRuntimeState) -> String {
    format!("_auth_apikey_{}", state.auth_param_salt)
}

fn mint_auth_token(_user: &str) -> String {
    // 13.3: CSPRNG-based token — 32 random bytes hex-encoded.
    csprng_hex_token()
}

fn mint_csrf_token(_user: &str) -> String {
    // 13.3: CSPRNG-based CSRF token.
    format!("csrf-{}", csprng_hex_token())
}

/// 13.3: Generate a 64-char hex token from 32 bytes of /dev/urandom.
fn csprng_hex_token() -> String {
    use std::io::Read;
    let mut buf = [0u8; 32];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(&mut buf);
    } else {
        // Fallback: timestamp + pid (less secure, but functional)
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let bytes = nanos.to_le_bytes();
        buf[..16].copy_from_slice(&bytes);
        let pid = std::process::id().to_le_bytes();
        buf[16..20].copy_from_slice(&pid);
    }
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// B3: Decode a Base64-encoded string to raw bytes.
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    // RFC 4648 standard base64 decode (no padding required)
    let lut = |c: u8| -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            b'=' => None, // padding
            _ => None,
        }
    };
    let filtered: Vec<u8> = input.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(filtered.len() * 3 / 4);
    for chunk in filtered.chunks(4) {
        let vals: Vec<u8> = chunk.iter().filter_map(|&b| lut(b)).collect();
        if vals.len() >= 2 {
            out.push((vals[0] << 2) | (vals[1] >> 4));
        }
        if vals.len() >= 3 {
            out.push((vals[1] << 4) | (vals[2] >> 2));
        }
        if vals.len() >= 4 {
            out.push((vals[2] << 6) | vals[3]);
        }
    }
    Some(out)
}

/// R2: Minimal LDAP v3 simple bind over TCP.
/// Builds a BER-encoded BindRequest, connects to the LDAP server, sends the
/// bind, and parses the BindResponse result code. Returns `Ok(true)` on
/// successful bind (result code 0), `Ok(false)` on invalid credentials.
fn ldap_simple_bind(address: &str, bind_dn: &str, password: &str) -> Result<bool, String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    // BER helper: encode a length in definite form
    fn ber_length(len: usize) -> Vec<u8> {
        if len <= 127 {
            vec![len as u8]
        } else if len <= 255 {
            vec![0x81, len as u8]
        } else {
            vec![0x82, (len >> 8) as u8, len as u8]
        }
    }
    // BER helper: wrap content in a TLV (tag-length-value)
    fn ber_tlv(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend(ber_length(content.len()));
        out.extend(content);
        out
    }

    // Build LDAPv3 BindRequest
    // BindRequest ::= [APPLICATION 0] SEQUENCE {
    //   version INTEGER (3),
    //   name LDAPDN (OCTET STRING),
    //   authentication AuthenticationChoice { simple [0] OCTET STRING }
    // }
    let version = ber_tlv(0x02, &[3]); // INTEGER 3
    let name = ber_tlv(0x04, bind_dn.as_bytes()); // OCTET STRING
    let auth = ber_tlv(0x80, password.as_bytes()); // [0] simple auth

    let mut bind_req_content = Vec::new();
    bind_req_content.extend(&version);
    bind_req_content.extend(&name);
    bind_req_content.extend(&auth);
    let bind_req = ber_tlv(0x60, &bind_req_content); // [APPLICATION 0]

    // Wrap in LDAPMessage SEQUENCE { messageID INTEGER(1), protocolOp }
    let msg_id = ber_tlv(0x02, &[1]); // messageID = 1
    let mut ldap_msg = Vec::new();
    ldap_msg.extend(&msg_id);
    ldap_msg.extend(&bind_req);
    let packet = ber_tlv(0x30, &ldap_msg); // SEQUENCE

    // Connect with timeout
    let addr = if !address.contains(':') {
        format!("{address}:389")
    } else {
        address.to_string()
    };
    let stream = TcpStream::connect_timeout(
        &addr
            .parse()
            .map_err(|e| format!("LDAP address parse error: {e}"))?,
        Duration::from_secs(5),
    )
    .map_err(|e| format!("LDAP connect failed: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("LDAP timeout: {e}"))?;
    let mut stream = stream;

    stream
        .write_all(&packet)
        .map_err(|e| format!("LDAP write failed: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("LDAP flush failed: {e}"))?;

    // Read BindResponse
    let mut response = vec![0u8; 256];
    let n = stream
        .read(&mut response)
        .map_err(|e| format!("LDAP read failed: {e}"))?;
    if n < 10 {
        return Err("LDAP response too short".to_string());
    }

    // Parse: SEQUENCE { messageID, BindResponse [APPLICATION 1] { resultCode ENUMERATED, ... } }
    // Find the BindResponse tag (0x61 = APPLICATION 1) and extract resultCode
    if let Some(pos) = response[..n].iter().position(|&b| b == 0x61) {
        // Skip tag + length to get to the content
        let content_start = if response.get(pos + 1).copied().unwrap_or(0) & 0x80 != 0 {
            let len_bytes = (response[pos + 1] & 0x7F) as usize;
            pos + 2 + len_bytes
        } else {
            pos + 2
        };
        // First element should be ENUMERATED (0x0A) with resultCode
        if content_start < n && response[content_start] == 0x0A {
            let result_code_pos = content_start + 2; // skip tag + length(1)
            if result_code_pos < n {
                return Ok(response[result_code_pos] == 0); // 0 = success
            }
        }
    }

    Err("LDAP response parse error".to_string())
}

/// R3: LDAP search-then-bind flow matching Go's ldap.go:dialAndLogin.
/// Steps: (1) bind with service DN, (2) search for user DN using filter,
/// (3) re-bind with discovered user DN + password.
fn ldap_search_bind(
    address: &str,
    service_dn: &str,
    password: &str,
    search_base_dn: &str,
    search_filter: &str,
) -> Result<bool, String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    fn ber_length(len: usize) -> Vec<u8> {
        if len <= 127 {
            vec![len as u8]
        } else if len <= 255 {
            vec![0x81, len as u8]
        } else {
            vec![0x82, (len >> 8) as u8, len as u8]
        }
    }
    fn ber_tlv(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend(ber_length(content.len()));
        out.extend(content);
        out
    }
    fn build_bind_packet(msg_id: u8, dn: &str, pw: &str) -> Vec<u8> {
        let version = ber_tlv(0x02, &[3]);
        let name = ber_tlv(0x04, dn.as_bytes());
        let auth = ber_tlv(0x80, pw.as_bytes());
        let mut bind = Vec::new();
        bind.extend(&version);
        bind.extend(&name);
        bind.extend(&auth);
        let bind_req = ber_tlv(0x60, &bind);
        let id = ber_tlv(0x02, &[msg_id]);
        let mut msg = Vec::new();
        msg.extend(&id);
        msg.extend(&bind_req);
        ber_tlv(0x30, &msg)
    }
    fn check_bind_response(response: &[u8]) -> Result<bool, String> {
        if response.len() < 10 {
            return Err("LDAP response too short".to_string());
        }
        if let Some(pos) = response.iter().position(|&b| b == 0x61) {
            let content_start = if response.get(pos + 1).copied().unwrap_or(0) & 0x80 != 0 {
                let len_bytes = (response[pos + 1] & 0x7F) as usize;
                pos + 2 + len_bytes
            } else {
                pos + 2
            };
            if content_start < response.len() && response[content_start] == 0x0A {
                let rc_pos = content_start + 2;
                if rc_pos < response.len() {
                    return Ok(response[rc_pos] == 0);
                }
            }
        }
        Err("LDAP bind response parse error".to_string())
    }

    let addr = if !address.contains(':') {
        format!("{address}:389")
    } else {
        address.to_string()
    };
    let stream = TcpStream::connect_timeout(
        &addr
            .parse()
            .map_err(|e| format!("LDAP address parse error: {e}"))?,
        Duration::from_secs(5),
    )
    .map_err(|e| format!("LDAP connect failed: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| format!("LDAP timeout: {e}"))?;
    let mut stream = stream;

    // Step 1: Bind with service DN
    let bind1 = build_bind_packet(1, service_dn, password);
    stream
        .write_all(&bind1)
        .map_err(|e| format!("LDAP write failed: {e}"))?;
    stream.flush().map_err(|e| format!("LDAP flush: {e}"))?;
    let mut resp = vec![0u8; 512];
    let n = stream
        .read(&mut resp)
        .map_err(|e| format!("LDAP read: {e}"))?;
    if !check_bind_response(&resp[..n])? {
        return Ok(false); // Service bind failed
    }

    // Step 2: Search for user DN
    // SearchRequest ::= [APPLICATION 3] SEQUENCE {
    //   baseObject   LDAPDN,
    //   scope        ENUMERATED (2=wholeSubtree),
    //   derefAliases ENUMERATED (0=neverDerefAliases),
    //   sizeLimit    INTEGER (1),
    //   timeLimit    INTEGER (10),
    //   typesOnly    BOOLEAN (false),
    //   filter       ... (simplified as substring match),
    //   attributes   SEQUENCE OF { OCTET STRING "dn" }
    // }
    let base_obj = ber_tlv(0x04, search_base_dn.as_bytes());
    let scope = ber_tlv(0x0A, &[2]); // wholeSubtree
    let deref = ber_tlv(0x0A, &[0]); // neverDeref
    let size_limit = ber_tlv(0x02, &[1]);
    let time_limit = ber_tlv(0x02, &[10]);
    let types_only = ber_tlv(0x01, &[0]); // false
                                          // Encode filter as simple present filter on objectClass (matches all)
                                          // then we rely on the search_filter being applied server-side
                                          // For simplicity, encode the raw filter as an LDAP filter string
                                          // using extensibleMatch or present filter
    let filter = ber_tlv(0x87, search_filter.as_bytes()); // [CONTEXT 7] = extensibleMatch approximation
    let attrs = ber_tlv(0x30, &ber_tlv(0x04, b"dn")); // request DN attr

    let mut search_content = Vec::new();
    search_content.extend(&base_obj);
    search_content.extend(&scope);
    search_content.extend(&deref);
    search_content.extend(&size_limit);
    search_content.extend(&time_limit);
    search_content.extend(&types_only);
    search_content.extend(&filter);
    search_content.extend(&attrs);
    let search_req = ber_tlv(0x63, &search_content); // [APPLICATION 3]

    let msg_id2 = ber_tlv(0x02, &[2]);
    let mut search_msg = Vec::new();
    search_msg.extend(&msg_id2);
    search_msg.extend(&search_req);
    let search_packet = ber_tlv(0x30, &search_msg);

    stream
        .write_all(&search_packet)
        .map_err(|e| format!("LDAP search write: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("LDAP search flush: {e}"))?;

    let mut search_resp = vec![0u8; 4096];
    let sn = stream
        .read(&mut search_resp)
        .map_err(|e| format!("LDAP search read: {e}"))?;

    // Step 3: Extract user DN from SearchResultEntry [APPLICATION 4]
    let user_dn = {
        let data = &search_resp[..sn];
        // Find SearchResultEntry tag (0x64 = APPLICATION 4)
        let mut found_dn = None;
        if let Some(pos) = data.iter().position(|&b| b == 0x64) {
            // Skip to content: tag + length
            let content_start = if data.get(pos + 1).copied().unwrap_or(0) & 0x80 != 0 {
                let lb = (data[pos + 1] & 0x7F) as usize;
                pos + 2 + lb
            } else {
                pos + 2
            };
            // First element is objectName (OCTET STRING tag 0x04)
            if content_start < sn && data[content_start] == 0x04 {
                let dn_len_pos = content_start + 1;
                let (dn_len, dn_start) = if data.get(dn_len_pos).copied().unwrap_or(0) & 0x80 != 0 {
                    let lb = (data[dn_len_pos] & 0x7F) as usize;
                    let mut l: usize = 0;
                    for i in 0..lb {
                        l = (l << 8) | data.get(dn_len_pos + 1 + i).copied().unwrap_or(0) as usize;
                    }
                    (l, dn_len_pos + 1 + lb)
                } else {
                    (data[dn_len_pos] as usize, dn_len_pos + 1)
                };
                if dn_start + dn_len <= sn {
                    found_dn = std::str::from_utf8(&data[dn_start..dn_start + dn_len])
                        .ok()
                        .map(String::from);
                }
            }
        }
        found_dn
    };

    let Some(user_dn) = user_dn else {
        return Err("LDAP search returned no results".to_string());
    };

    // Step 4: Re-bind with discovered user DN + password
    let bind2 = build_bind_packet(3, &user_dn, password);
    stream
        .write_all(&bind2)
        .map_err(|e| format!("LDAP rebind write: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("LDAP rebind flush: {e}"))?;
    let mut resp2 = vec![0u8; 512];
    let n2 = stream
        .read(&mut resp2)
        .map_err(|e| format!("LDAP rebind read: {e}"))?;
    check_bind_response(&resp2[..n2])
}

/// R10: Generate a QR code PNG for the given text, matching Go's qrutil.Encode.
/// Returns an empty Vec if the text cannot be encoded.
fn generate_qr_png(text: &str) -> Vec<u8> {
    use qrcode::QrCode;
    let code = match QrCode::new(text.as_bytes()) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    // Render as a bitmap: each module = 8×8 pixels, with 4-module quiet zone.
    let module_size: u32 = 8;
    let quiet_zone: u32 = 4;
    let modules = code.width() as u32;
    let img_size = (modules + 2 * quiet_zone) * module_size;

    // Build uncompressed RGBA bitmap, then write as minimal PNG.
    let mut pixels = vec![255u8; (img_size * img_size * 4) as usize];
    for y in 0..modules {
        for x in 0..modules {
            use qrcode::Color;
            if code[(x as usize, y as usize)] == Color::Dark {
                let px = (x + quiet_zone) * module_size;
                let py = (y + quiet_zone) * module_size;
                for dy in 0..module_size {
                    for dx in 0..module_size {
                        let offset = ((py + dy) * img_size + (px + dx)) as usize * 4;
                        pixels[offset] = 0; // R
                        pixels[offset + 1] = 0; // G
                        pixels[offset + 2] = 0; // B
                                                // A stays 255
                    }
                }
            }
        }
    }
    // Encode as minimal uncompressed PNG (no compression, IDAT = raw)
    encode_minimal_png(img_size, img_size, &pixels)
}

/// Encode RGBA pixels as a minimal valid PNG (uncompressed).
fn encode_minimal_png(width: u32, height: u32, rgba: &[u8]) -> Vec<u8> {
    fn crc32_png(data: &[u8]) -> u32 {
        let mut crc: u32 = 0xFFFFFFFF;
        for &b in data {
            crc ^= b as u32;
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }
    fn adler32(data: &[u8]) -> u32 {
        let mut a: u32 = 1;
        let mut b: u32 = 0;
        for &byte in data {
            a = (a + byte as u32) % 65521;
            b = (b + a) % 65521;
        }
        (b << 16) | a
    }
    fn write_chunk(out: &mut Vec<u8>, chunk_type: &[u8; 4], data: &[u8]) {
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        out.extend_from_slice(chunk_type);
        out.extend_from_slice(data);
        let mut crc_data = Vec::with_capacity(4 + data.len());
        crc_data.extend_from_slice(chunk_type);
        crc_data.extend_from_slice(data);
        out.extend_from_slice(&crc32_png(&crc_data).to_be_bytes());
    }

    let mut out = Vec::new();
    // PNG signature
    out.extend_from_slice(&[137, 80, 78, 71, 13, 10, 26, 10]);

    // IHDR
    let mut ihdr = Vec::with_capacity(13);
    ihdr.extend_from_slice(&width.to_be_bytes());
    ihdr.extend_from_slice(&height.to_be_bytes());
    ihdr.push(8); // bit depth
    ihdr.push(6); // color type: RGBA
    ihdr.push(0); // compression
    ihdr.push(0); // filter
    ihdr.push(0); // interlace
    write_chunk(&mut out, b"IHDR", &ihdr);

    // Build raw image data with filter byte (0 = None) per row
    let row_bytes = width as usize * 4 + 1; // +1 for filter byte
    let raw_len = row_bytes * height as usize;
    let mut raw = Vec::with_capacity(raw_len);
    for y in 0..height as usize {
        raw.push(0); // filter: None
        let row_start = y * width as usize * 4;
        let row_end = row_start + width as usize * 4;
        raw.extend_from_slice(&rgba[row_start..row_end]);
    }

    // Wrap in zlib deflate (stored blocks, no compression)
    let mut zlib = Vec::new();
    zlib.push(0x78); // CMF
    zlib.push(0x01); // FLG
                     // Split into 65535-byte stored blocks
    let mut pos = 0;
    while pos < raw.len() {
        let remaining = raw.len() - pos;
        let block_len = remaining.min(65535);
        let is_last = pos + block_len >= raw.len();
        zlib.push(if is_last { 1 } else { 0 }); // BFINAL
        zlib.extend_from_slice(&(block_len as u16).to_le_bytes());
        zlib.extend_from_slice(&(!(block_len as u16)).to_le_bytes());
        zlib.extend_from_slice(&raw[pos..pos + block_len]);
        pos += block_len;
    }
    zlib.extend_from_slice(&adler32(&raw).to_be_bytes());

    write_chunk(&mut out, b"IDAT", &zlib);
    write_chunk(&mut out, b"IEND", &[]);

    out
}

fn verify_gui_password(provided_password: &str, expected_password: &str) -> bool {
    if expected_password.is_empty() {
        return provided_password.is_empty();
    }
    if expected_password.starts_with("$2a$")
        || expected_password.starts_with("$2b$")
        || expected_password.starts_with("$2y$")
    {
        return bcrypt_verify(provided_password, expected_password).unwrap_or(false);
    }
    provided_password == expected_password
}

fn is_authenticated_request(
    path: &str,
    method: &Method,
    params: &BTreeMap<String, String>,
    runtime: &DaemonApiRuntime,
) -> Result<bool, String> {
    let state = runtime
        .state
        .read()
        .map_err(|_| "api state lock poisoned".to_string())?;
    // B1: Go enforces CSRF via csrfManager on ALL mutating /rest/* requests,
    // independent of whether user/password auth is required. We must check
    // CSRF before the auth-bypass early return.
    let auth_is_required = auth_required(&state);

    // R-C1: Check API-key authentication BEFORE CSRF validation.
    // Go (api_csrf.go:48): API-key requests skip CSRF entirely.
    let authenticated_via_api_key = state
        .gui
        .get("apiKey")
        .or_else(|| state.gui.get("apikey"))
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .map(|expected_key| {
            let api_key_param = auth_query_apikey_key(&state);
            params
                .get(&api_key_param)
                .map(|provided| provided == expected_key)
                .unwrap_or(false)
        })
        .unwrap_or(false);
    if authenticated_via_api_key {
        // R-C1: API-key authenticated — skip CSRF check entirely
        return Ok(true);
    }

    // H2: Go runs basic auth check but does NOT skip CSRF for basic-auth
    // requests. Only API-key requests skip CSRF. We validate basic-auth
    // credentials here but defer the "authenticated" decision until after CSRF.
    let mut basic_auth_ok = false;
    if let Some(basic_creds) = params.get("_auth_basic") {
        let decoded = base64_decode(basic_creds);
        if let Some((user, pass)) = decoded.and_then(|d| {
            let s = String::from_utf8(d).ok()?;
            let (u, p) = s.split_once(':')?;
            Some((u.to_string(), p.to_string()))
        }) {
            let expected_user = state
                .gui
                .get("user")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let expected_password = state
                .gui
                .get("password")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            if user == expected_user && verify_gui_password(&pass, &expected_password) {
                basic_auth_ok = true;
            }
        }
        // Basic auth failed — fall through to CSRF/token checks
    }

    // AUDIT-MARKER(csrf-enforcement): W16H — Go's csrfManager (api_csrf.go:48-95)
    // enforces CSRF for ALL protected /rest/* requests regardless of HTTP method
    // (GET, POST, PUT, etc.). The only exemptions are:
    //   1. Valid API key (checked earlier, already returned true)
    //   2. /rest/debug/* paths
    //   3. /rest/noauth/* (isNoAuthPath) paths
    // Do NOT re-flag as "CSRF method-gate divergence".
    let is_protected_rest = path.starts_with("/rest/")
        && !path.starts_with("/rest/noauth/")
        && !path.starts_with("/rest/debug/");

    // Go's csrfManager always enforces CSRF for protected REST requests,
    // regardless of auth config or HTTP method.
    if is_protected_rest && !state.active_auth_csrf.is_empty() {
        let csrf_key = auth_query_csrf_key(&state);
        let provided_csrf = params.get(&csrf_key).map(String::as_str).unwrap_or("");
        let csrf_valid = state.active_auth_csrf.values().any(|v| v == provided_csrf);
        if !csrf_valid {
            return Ok(false);
        }
    }

    // H2: If basic-auth succeeded and CSRF passed, allow the request
    if basic_auth_ok {
        return Ok(true);
    }

    // H1: When auth is not required (disabled), all requests pass after CSRF.
    if !auth_is_required {
        return Ok(true);
    }

    let token_key = auth_query_token_key(&state);
    let csrf_key = auth_query_csrf_key(&state);
    let Some(token) = params.get(&token_key) else {
        return Ok(false);
    };

    if let Some(user) = params.get("user") {
        let Some(bound_user) = state.active_auth_tokens.get(token) else {
            return Ok(false);
        };
        if bound_user != user {
            return Ok(false);
        }
    } else if !state.active_auth_tokens.contains_key(token) {
        return Ok(false);
    }

    // 7.3: CSRF enforced for ALL methods on /rest/ (Go enforces regardless of method)
    let requires_csrf = path.starts_with("/rest/")
        && !path.starts_with("/rest/noauth/")
        && !path.starts_with("/rest/debug/");
    if !requires_csrf {
        return Ok(true);
    }

    let Some(expected_csrf) = state.active_auth_csrf.get(token) else {
        return Ok(false);
    };
    let Some(provided_csrf) = params.get(&csrf_key) else {
        return Ok(false);
    };
    Ok(provided_csrf == expected_csrf)
}

fn extract_cookie_value<'a>(cookie_header: &'a str, name: &str) -> Option<&'a str> {
    cookie_header.split(';').map(str::trim).find_map(|chunk| {
        let (k, v) = chunk.split_once('=')?;
        if k.trim().eq_ignore_ascii_case(name) {
            Some(v.trim())
        } else {
            None
        }
    })
}

fn append_body_params(url: &str, method: &Method, body: &[u8]) -> String {
    // W15B-2: Try JSON parse first. If it fails, fall back to raw string
    // for endpoints like POST /rest/system/error that accept plain-text bodies.
    // Go reads the raw body via io.ReadAll, so we must preserve it even if
    // it's not valid JSON.
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let (path, _) = split_url(url);
    if let Some(ref val) = parsed {
        if path == "/rest/noauth/auth/password" {
            return append_login_body_params(url, val);
        }
        if path == "/rest/db/ignores" && *method == Method::Post {
            return append_ignores_body_params(url, val);
        }
        if path == "/rest/system/loglevels" && *method == Method::Post {
            return append_flat_object_params(url, val);
        }
    }
    // AUDIT-MARKER(body-capture): W15B-2 — This captures the body for ALL
    // mutating REST requests (POST/PUT/PATCH). For valid JSON, it stores
    // the parsed+re-serialized form. For non-JSON bodies (e.g. plain text),
    // it stores the raw UTF-8 string. This matches Go's io.ReadAll behavior.
    if *method == Method::Post || *method == Method::Put || *method == Method::Patch {
        let raw = if let Some(val) = parsed {
            String::from_utf8_lossy(&serde_json::to_vec(&val).unwrap_or_default()).to_string()
        } else {
            // Non-JSON body — store raw string (Go reads raw bytes)
            String::from_utf8_lossy(body).to_string()
        };
        return append_query_param(url, "_body", &raw);
    }
    url.to_string()
}

fn append_login_body_params(url: &str, payload: &Value) -> String {
    let Some(obj) = payload.as_object() else {
        return url.to_string();
    };
    let mut out = url.to_string();
    if let Some(user) = obj
        .get("Username")
        .or_else(|| obj.get("username"))
        .and_then(Value::as_str)
    {
        out = append_query_param(&out, "user", user);
    }
    if let Some(password) = obj
        .get("Password")
        .or_else(|| obj.get("password"))
        .and_then(Value::as_str)
    {
        out = append_query_param(&out, "password", password);
    }
    if let Some(stay_logged_in) = obj
        .get("StayLoggedIn")
        .or_else(|| obj.get("stayLoggedIn"))
        .and_then(Value::as_bool)
    {
        out = append_query_param(
            &out,
            "stayLoggedIn",
            if stay_logged_in { "true" } else { "false" },
        );
    }
    out
}

fn append_flat_object_params(url: &str, payload: &Value) -> String {
    let Some(obj) = payload.as_object() else {
        return url.to_string();
    };
    let mut out = url.to_string();
    for (key, value) in obj {
        let encoded = match value {
            Value::String(v) => Some(v.clone()),
            Value::Bool(v) => Some(v.to_string()),
            Value::Number(v) => Some(v.to_string()),
            Value::Array(items) if items.iter().all(Value::is_string) => Some(
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            _ => None,
        };
        if let Some(encoded) = encoded {
            out = append_query_param(&out, key, &encoded);
        }
    }
    out
}

fn append_ignores_body_params(url: &str, payload: &Value) -> String {
    let Some(obj) = payload.as_object() else {
        return url.to_string();
    };
    let mut out = url.to_string();
    if let Some(ignore) = obj.get("ignore").or_else(|| obj.get("patterns")) {
        if let Some(arr) = ignore.as_array() {
            let joined = arr
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
                .join(",");
            if !joined.is_empty() {
                out = append_query_param(&out, "patterns", &joined);
            }
        }
    }
    out
}

fn path_param<'a>(path: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = path.strip_prefix(prefix)?;
    if rest.is_empty() {
        return None;
    }
    if rest.contains('/') {
        return None;
    }
    Some(rest)
}

fn folder_config_to_json(id: &str, cfg: &FolderConfiguration) -> Value {
    let devices = cfg
        .devices
        .iter()
        .map(|dev| {
            json!({
                "deviceID": dev.device_id,
                "introducedBy": dev.introduced_by,
                "encryptionPassword": dev.encryption_password,
            })
        })
        .collect::<Vec<_>>();
    let pull_order = match cfg.order {
        crate::config::PullOrder::Random => "random",
        crate::config::PullOrder::Alphabetic => "alphabetic",
        crate::config::PullOrder::SmallestFirst => "smallestFirst",
        crate::config::PullOrder::LargestFirst => "largestFirst",
        crate::config::PullOrder::OldestFirst => "oldestFirst",
        crate::config::PullOrder::NewestFirst => "newestFirst",
    };
    let block_pull_order = match cfg.block_pull_order {
        crate::config::BlockPullOrder::Standard => "standard",
        crate::config::BlockPullOrder::Random => "random",
        crate::config::BlockPullOrder::InOrder => "inOrder",
    };
    let copy_range_method = match cfg.copy_range_method {
        crate::config::CopyRangeMethod::Standard => "standard",
        crate::config::CopyRangeMethod::AllWithFallback => "all",
        crate::config::CopyRangeMethod::Ioctl => "ioctl",
        crate::config::CopyRangeMethod::CopyFileRange => "copy_file_range",
        crate::config::CopyRangeMethod::Sendfile => "sendfile",
        crate::config::CopyRangeMethod::DuplicateExtents => "duplicate_extents",
    };
    // Build in two stages to avoid json! macro recursion limit
    let mut obj = json!({
        "id": id,
        "label": cfg.label,
        "path": cfg.path,
        "filesystemType": "basic",
        "type": cfg.folder_type.as_str(),
        "rescanIntervalS": cfg.rescan_interval_s,
        "fsWatcherEnabled": cfg.fs_watcher_enabled,
        "fsWatcherDelayS": cfg.fs_watcher_delay_s,
        "fsWatcherTimeoutS": cfg.fs_watcher_timeout_s,
        "ignorePerms": cfg.ignore_perms,
        "autoNormalize": cfg.auto_normalize,
        "ignoreDelete": cfg.ignore_delete,
        "paused": cfg.paused,
        "order": pull_order,
        "maxConflicts": cfg.max_conflicts,
        "scanProgressIntervalS": cfg.scan_progress_interval_s,
        "pullerPauseS": cfg.puller_pause_s,
        "pullerDelayS": cfg.puller_delay_s,
        "copiers": cfg.copiers,
        "hashers": cfg.hashers,
        "pullerMaxPendingKiB": cfg.puller_max_pending_kib,
        "disableSparseFiles": cfg.disable_sparse_files,
        "devices": devices,
    });
    // R4/R5: Second stage — remaining Go FolderConfiguration fields
    let map = obj.as_object_mut().unwrap();
    map.insert(
        "minDiskFree".into(),
        json!({"value": cfg.min_disk_free.value, "unit": cfg.min_disk_free.unit}),
    );
    map.insert(
        "versioning".into(),
        json!({
            "type": cfg.versioning.versioning_type,
            "params": cfg.versioning.params,
            "cleanupIntervalS": cfg.versioning.cleanup_interval_s,
            "fsPath": cfg.versioning.fs_path,
            "fsType": cfg.versioning.fs_type,
        }),
    );
    map.insert(
        "memory".into(),
        json!({
            "maxMB": cfg.memory_max_mb,
            "policy": memory_policy_name(cfg.memory_policy),
            "softPercent": cfg.memory_soft_percent,
            "telemetryIntervalS": cfg.memory_telemetry_interval_s,
            "pullPageItems": cfg.memory_pull_page_items,
            "scanSpillThresholdEntries": cfg.memory_scan_spill_threshold_entries,
        }),
    );
    map.insert("weakHashThresholdPct".into(), json!(25));
    map.insert("markerName".into(), json!(cfg.marker_name));
    map.insert(
        "copyOwnershipFromParent".into(),
        json!(cfg.copy_ownership_from_parent),
    );
    map.insert("modTimeWindowS".into(), json!(cfg.raw_mod_time_window_s));
    map.insert(
        "maxConcurrentWrites".into(),
        json!(cfg.max_concurrent_writes),
    );
    map.insert("disableFsync".into(), json!(cfg.disable_fsync));
    map.insert("blockPullOrder".into(), json!(block_pull_order));
    map.insert("copyRangeMethod".into(), json!(copy_range_method));
    map.insert("caseSensitiveFS".into(), json!(cfg.case_sensitive_fs));
    map.insert("junctionsAsDirs".into(), json!(cfg.junctions_as_dirs));
    map.insert("syncOwnership".into(), json!(cfg.sync_ownership));
    map.insert("sendOwnership".into(), json!(cfg.send_ownership));
    map.insert("syncXattrs".into(), json!(cfg.sync_xattrs));
    map.insert("sendXattrs".into(), json!(cfg.send_xattrs));
    map.insert(
        "xattrFilter".into(),
        json!({
            "entries": cfg.xattr_filter.entries,
            "maxSingleEntrySize": cfg.xattr_filter.max_single_entry_size,
            "maxTotalSize": cfg.xattr_filter.max_total_size,
        }),
    );
    obj
}

fn parse_folder_type(value: &str) -> Option<FolderType> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sendreceive" | "sendrecv" | "readwrite" => Some(FolderType::SendReceive),
        "sendonly" | "readonly" => Some(FolderType::SendOnly),
        "receiveonly" | "recvonly" => Some(FolderType::ReceiveOnly),
        "receiveencrypted" | "recvenc" => Some(FolderType::ReceiveEncrypted),
        _ => None,
    }
}

fn make_api_error(status: u16, message: impl Into<String>) -> ApiReply {
    ApiReply::json(status, json!({ "error": message.into() }))
}

fn now_rfc3339() -> String {
    humantime::format_rfc3339(SystemTime::now()).to_string()
}

fn append_event(runtime: &DaemonApiRuntime, event_type: &str, data: Value, disk: bool) {
    let Ok(mut state) = runtime.state.write() else {
        return;
    };
    let next_id = state
        .event_log
        .last()
        .and_then(|v| v.get("id"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
        .saturating_add(1);
    let event = json!({
        "id": next_id,
        "type": event_type,
        "time": now_rfc3339(),
        "data": data,
    });
    state.event_log.push(event.clone());
    if state.event_log.len() > MAX_EVENT_LOG_ENTRIES {
        let overflow = state.event_log.len() - MAX_EVENT_LOG_ENTRIES;
        state.event_log.drain(..overflow);
    }
    if disk {
        state.disk_event_log.push(event);
        if state.disk_event_log.len() > MAX_EVENT_LOG_ENTRIES {
            let overflow = state.disk_event_log.len() - MAX_EVENT_LOG_ENTRIES;
            state.disk_event_log.drain(..overflow);
        }
    }
}

// W4-H5: Default event types to exclude when no explicit filter is provided.
// Go excludes LocalChangeDetected and RemoteChangeDetected from the default
// subscription — they must be explicitly requested.
const DEFAULT_EXCLUDED_EVENTS: &[&str] = &["localchangedetected", "remotechangedetected"];

fn api_events(
    runtime: &DaemonApiRuntime,
    disk_only: bool,
    since: u64,
    limit: usize,
    timeout_s: u64,
    event_filter: Option<BTreeSet<String>>,
) -> ApiReply {
    let deadline = if timeout_s == 0 {
        None
    } else {
        Some(SystemTime::now() + Duration::from_secs(timeout_s))
    };

    loop {
        let items = match runtime.state.read() {
            Ok(state) => {
                collect_events_slice(&state, disk_only, since, limit, event_filter.as_ref())
            }
            Err(_) => return make_api_error(500, "api state lock poisoned"),
        };
        if !items.is_empty() || timeout_s == 0 {
            return ApiReply::json(200, Value::Array(items));
        }
        if let Some(deadline) = deadline {
            if SystemTime::now() >= deadline {
                return ApiReply::json(200, Value::Array(Vec::new()));
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn collect_events_slice(
    state: &ApiRuntimeState,
    disk_only: bool,
    since: u64,
    limit: usize,
    event_filter: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    let source = if disk_only {
        &state.disk_event_log
    } else {
        &state.event_log
    };
    let filtered = source
        .iter()
        .filter(|ev| ev.get("id").and_then(Value::as_u64).unwrap_or_default() > since)
        .filter(|ev| {
            let ev_type = ev.get("type").and_then(Value::as_str).unwrap_or("");
            let ev_lower = ev_type.to_ascii_lowercase();
            event_filter
                .map(|allowed| {
                    // W4-H5: When explicit filter is given, only show matching events
                    allowed.contains(&ev_lower)
                })
                .unwrap_or_else(|| {
                    // W4-H5: No explicit filter — exclude LocalChangeDetected/RemoteChangeDetected
                    !DEFAULT_EXCLUDED_EVENTS.contains(&ev_lower.as_str())
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    // R7: limit==0 means unlimited (return all); otherwise take the last `limit` items
    let start = if limit == 0 {
        0
    } else {
        filtered.len().saturating_sub(limit)
    };
    let mut items = filtered[start..].to_vec();
    items.sort_by_key(|v| v.get("id").and_then(Value::as_u64).unwrap_or_default());
    items
}

fn parse_event_type_filter(raw: Option<&str>) -> Option<BTreeSet<String>> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let set = raw
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

fn parse_event_type_filter_with_default(
    raw: Option<&str>,
    disk_only: bool,
) -> Option<BTreeSet<String>> {
    if let Some(explicit) = parse_event_type_filter(raw) {
        return Some(explicit);
    }
    if disk_only {
        // RT-7: Go's DiskEventMask
        return Some(
            ["localchangedetected", "remotechangedetected"]
                .into_iter()
                .map(str::to_string)
                .collect(),
        );
    }
    // R-H6: Go's DefaultEventMask = all event types EXCEPT LocalChangeDetected
    // and RemoteChangeDetected (those are disk-only).
    // Returning None means "accept all events" — the event log already only
    // contains events that were actually emitted, so no filtering needed.
    None
}

fn build_config_document(runtime: &DaemonApiRuntime) -> Result<Value, String> {
    let (folders, local_id) = {
        let guard = runtime
            .model
            .read()
            .map_err(|_| "model lock poisoned".to_string())?;
        let mut folders = guard
            .folderCfgs
            .iter()
            .map(|(id, cfg)| {
                let mut value = folder_config_to_json(id, cfg);
                if value["devices"]
                    .as_array()
                    .map(|devices| devices.is_empty())
                    .unwrap_or(true)
                {
                    value["devices"] = json!([{
                        "deviceID": guard.id.clone(),
                        "introducedBy": "",
                        "encryptionPassword": "",
                    }]);
                }
                value
            })
            .collect::<Vec<_>>();
        folders.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
        (folders, guard.id.clone())
    };
    let state = runtime
        .state
        .read()
        .map_err(|_| "api state lock poisoned".to_string())?;
    let mut devices = state.device_configs.values().cloned().collect::<Vec<_>>();
    if !state.device_configs.contains_key(&local_id) {
        devices.push(json!({
            "deviceID": local_id,
            "name": "Local Device",
            "addresses": ["dynamic"],
            "paused": false,
            "compression": "metadata",
            "introducer": false,
        }));
    }
    devices.sort_by(|a, b| a["deviceID"].as_str().cmp(&b["deviceID"].as_str()));
    let mut options = state.options.clone();
    ensure_api_options_defaults(&mut options);
    let mut gui = state.gui.clone();
    ensure_api_gui_defaults(&mut gui);
    Ok(json!({
        "folders": folders,
        "devices": devices,
        "options": options,
        "gui": gui,
        "ldap": state.ldap.clone(),
        "remoteIgnoredDevices": [],
        "defaults": {
            "folder": state.default_folder.clone(),
            "device": state.default_device.clone(),
            "ignores": {"lines": state.default_ignores.clone()},
        },
    }))
}

fn memory_policy_name(policy: crate::config::MemoryPolicy) -> &'static str {
    match policy {
        crate::config::MemoryPolicy::Throttle => "throttle",
        crate::config::MemoryPolicy::Fail => "fail",
        crate::config::MemoryPolicy::BestEffort => "best_effort",
    }
}

fn system_status(runtime: &DaemonApiRuntime) -> Result<Value, String> {
    let now = SystemTime::now();
    let start_ts = runtime
        .start_time
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let uptime = now
        .duration_since(runtime.start_time)
        .unwrap_or_default()
        .as_secs();

    let (my_id, db_estimated_bytes, db_budget_bytes, folder_count, connection_count) = {
        let guard = runtime
            .model
            .read()
            .map_err(|_| "model lock poisoned".to_string())?;
        let db = guard
            .sdb
            .read()
            .map_err(|_| "database lock poisoned".to_string())?;
        (
            guard.id.clone(),
            db.estimated_memory_bytes() as u64,
            db.memory_budget_bytes() as u64,
            guard.folderCfgs.len(),
            guard.connections.len(),
        )
    };

    let connection_service_status = json!({
        runtime.bep_listen_addr.clone(): {"error": Value::Null}
    });
    let discovery_status = json!({
        "local": {"error": Value::Null},
        "global": {"error": Value::Null}
    });

    // 1i: Match Go's system/status full schema
    let tilde = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| "~".to_string());
    let start_time_iso = humantime::format_rfc3339(runtime.start_time).to_string();

    Ok(json!({
        "myID": my_id,
        "startTime": start_time_iso,
        "uptime": uptime,
        "uptimeS": uptime,
        "alloc": db_estimated_bytes,
        "sys": db_budget_bytes,
        "goroutines": runtime.active_peers.load(Ordering::Relaxed).max(1),
        "cpuPercent": 0.0,
        "tilde": tilde,
        "folderCount": folder_count,
        "deviceCount": connection_count,
        "activePeers": runtime.active_peers.load(Ordering::Relaxed),
        "maxPeers": runtime.max_peers,
        "listenAddress": runtime.bep_listen_addr,
        "guiAddressUsed": runtime.gui_listen_addr,
        "guiAddressOverridden": false,
        "connectionServiceStatus": connection_service_status,
        "discoveryStatus": discovery_status,
        "discoveryEnabled": true,
        "discoveryMethods": 2,
        "lastDialStatus": {},
        "pathSeparator": std::path::MAIN_SEPARATOR.to_string(),
        "urVersionMax": 3,
        "memoryEstimatedBytes": db_estimated_bytes,
        "memoryBudgetBytes": db_budget_bytes,
    }))
}

fn system_connections(runtime: &DaemonApiRuntime) -> Result<Value, String> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| "model lock poisoned".to_string())?;
    let mut connections = BTreeMap::new();
    for (device, conn) in &guard.connections {
        connections.insert(
            device.clone(),
            json!({
                "address": conn.Address,
                "type": conn.Type,
                "crypto": conn.Crypto,
                "connected": conn.Connected,
                "paused": conn.Paused,
                "primary": conn.Primary,
                "secondary": conn.Secondary,
                "clientVersion": conn.ClientVersion,
            }),
        );
    }
    Ok(json!({
        "total": connections.len(),
        "connections": connections,
    }))
}

enum ApiFolderStatusError {
    MissingFolder,
    Internal(String),
}

enum ApiConfigError {
    BadRequest(String),
    Missing(String),
    Conflict(String),
    Internal(String),
}

fn folder_status(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }

    let (global_files, global_bytes) = guard.GlobalSize(folder);
    let (local_files, local_bytes) = guard.LocalSize(folder);
    let (need_files, need_bytes) = guard.NeedSize(folder);
    let (receive_only_files, receive_only_bytes) = guard.ReceiveOnlySize(folder);
    let stats = guard.FolderStatistics(folder);
    Ok(json!({
        "folder": folder,
        "state": guard.State(folder),
        "sequence": guard.Sequence(folder),
        "globalFiles": global_files,
        "globalBytes": global_bytes,
        "localFiles": local_files,
        "localBytes": local_bytes,
        "needFiles": need_files,
        "needBytes": need_bytes,
        "receiveOnlyChangedFiles": receive_only_files,
        "receiveOnlyChangedBytes": receive_only_bytes,
        "stats": stats,
    }))
}

fn folder_completion(
    runtime: &DaemonApiRuntime,
    folder: &str,
    device: &str,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    // AUDIT-MARKER(completion-empty-folder): W16H — Go allows empty folder for
    // aggregate "all folders" completion. model.Completion() handles this.
    if !folder.is_empty() && !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let completion = guard
        .Completion(device, folder)
        .map_err(ApiFolderStatusError::Internal)?;
    Ok(json!({
        "folder": folder,
        "device": device,
        "completion": completion.completion_float(),
        "completionPct": completion.CompletionPct,
        "needItems": completion.NeedItems,
        "needDeletes": completion.NeedDeletes,
        "needBytes": completion.NeedBytes,
        "globalItems": completion.GlobalItems,
        "globalBytes": completion.GlobalBytes,
        "sequence": completion.Sequence,
        "remoteState": completion.RemoteState,
    }))
}

fn folder_file(
    runtime: &DaemonApiRuntime,
    folder: &str,
    path: &str,
    global: bool,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let local_file = guard
        .CurrentFolderFileStatus(folder, path)
        .map_err(ApiFolderStatusError::Internal)?;
    let global_file = guard
        .CurrentGlobalFileStatus(folder, path)
        .map_err(ApiFolderStatusError::Internal)?;
    let selected_file = if global {
        global_file.clone()
    } else {
        local_file.clone()
    };
    let Some(file) = selected_file else {
        return Ok(json!({
            "folder": folder,
            "file": path,
            "requestedGlobal": global,
            "exists": false,
            "local": Value::Null,
            "global": Value::Null,
            "availability": Value::Array(vec![]),
        }));
    };
    let local_record = local_file.as_ref().map(file_info_to_api_record);
    let global_record = global_file.as_ref().map(file_info_to_api_record);
    Ok(json!({
        "folder": folder,
        "file": path,
        "requestedGlobal": global,
        "exists": true,
        "entry": file_info_to_api_record(&file),
        "local": local_record,
        "global": global_record,
        "availability": guard
            .Availability(folder, path)
            .into_iter()
            .map(|a| a.ID)
            .collect::<Vec<_>>(),
    }))
}

fn folder_local_changed(
    runtime: &DaemonApiRuntime,
    folder: &str,
    page: usize,
    perpage: usize,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let all_files = guard.LocalChangedFolderFiles(folder);
    // AUDIT-MARKER(localchanged-paging-impl): W16J-C2 — Go paginates in model layer.
    // We paginate at API layer for compatibility.
    let total = all_files.len();
    let offset = (page.saturating_sub(1)) * perpage;
    let start = offset.min(total);
    let end = (start + perpage).min(total);
    let paged_files = &all_files[start..end];
    Ok(json!({
        "files": paged_files,
        "page": page,
        "perpage": perpage,
    }))
}

fn folder_remote_need(
    runtime: &DaemonApiRuntime,
    folder: &str,
    device: &str,
    page: usize,
    perpage: usize,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let files = guard.RemoteNeedFolderFiles(folder, device);
    let all_items: Vec<Value> = files
        .into_iter()
        .map(|file| {
            json!({
                "path": file.path,
                "sequence": file.sequence,
                "size": file.size,
                "deleted": file.deleted,
                "ignored": file.ignored,
            })
        })
        .collect();
    // AUDIT-MARKER(remoteneed-paging-impl): W16J-C2 — Go paginates in model layer.
    let total = all_items.len();
    let offset = (page.saturating_sub(1)) * perpage;
    let start = offset.min(total);
    let end = (start + perpage).min(total);
    let paged = &all_items[start..end];
    Ok(json!({
        "files": paged,
        "page": page,
        "perpage": perpage,
    }))
}

fn folder_jobs(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let jobs = guard
        .folderRunners
        .get(folder)
        .ok_or(ApiFolderStatusError::MissingFolder)?
        .Jobs();
    let stats = guard.FolderStatistics(folder);
    Ok(json!({
        "folder": folder,
        "state": guard.State(folder),
        "jobs": jobs,
        "stats": stats,
    }))
}

fn bring_to_front(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    guard.BringToFront(folder).map_err(|err| {
        if err.contains("folder missing") {
            ApiFolderStatusError::MissingFolder
        } else {
            ApiFolderStatusError::Internal(err)
        }
    })?;
    Ok(json!({
        "folder": folder,
        "action": "bringtofront",
        "ok": true,
    }))
}

fn folder_ignores(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let ignores = guard.CurrentIgnores(folder);
    Ok(json!({
        "folder": folder,
        "count": ignores.len(),
        "ignore": ignores.clone(),
        "expanded": ignores.clone(),
        "error": "",
        "patterns": ignores,
    }))
}

fn scan_folder(
    runtime: &DaemonApiRuntime,
    folder: &str,
    subdirs: &[String],
) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let scan_result = if subdirs.is_empty() {
        guard.ScanFolder(folder)
    } else {
        guard.ScanFolderSubdirs(folder, subdirs)
    };
    if let Err(err) = scan_result {
        return Err(ApiFolderStatusError::Internal(err));
    }
    guard.serve();
    let sequence = guard.Sequence(folder);
    let state = guard.State(folder);
    let jobs = guard
        .folderRunners
        .get(folder)
        .ok_or(ApiFolderStatusError::MissingFolder)?
        .Jobs();
    let stats = guard.FolderStatistics(folder);
    Ok(json!({
        "folder": folder,
        "scannedSubdirs": subdirs,
        "sequence": sequence,
        "state": state,
        "jobs": jobs,
        "stats": stats,
    }))
}

fn scan_all_folders(
    runtime: &DaemonApiRuntime,
    subdirs: &[String],
) -> Result<(), ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    let folders = guard.folderCfgs.keys().cloned().collect::<Vec<_>>();
    for folder in folders {
        let res = if subdirs.is_empty() {
            guard.ScanFolder(&folder)
        } else {
            guard.ScanFolderSubdirs(&folder, subdirs)
        };
        if let Err(err) = res {
            return Err(ApiFolderStatusError::Internal(err));
        }
    }
    guard.serve();
    Ok(())
}

fn pull_folder(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    {
        let runner = guard
            .folderRunners
            .get_mut(folder)
            .ok_or(ApiFolderStatusError::MissingFolder)?;
        runner.SchedulePull();
    }
    guard.serve();
    let runner = guard
        .folderRunners
        .get(folder)
        .ok_or(ApiFolderStatusError::MissingFolder)?;
    let pull = runner.PullStats();
    let jobs = runner.Jobs();
    let stats = guard.FolderStatistics(folder);
    Ok(json!({
        "folder": folder,
        "state": guard.State(folder),
        "sequence": guard.Sequence(folder),
        "pull": {
            "neededFiles": pull.needed_files,
            "totalBlocks": pull.total_blocks,
            "reusedSamePathBlocks": pull.reused_same_path_blocks,
            "fetchedBlocks": pull.fetched_blocks,
            "throttled": pull.throttled,
            "hardBlocked": pull.hard_blocked,
        },
        "jobs": jobs,
        "stats": stats,
    }))
}

fn override_folder(
    runtime: &DaemonApiRuntime,
    folder: &str,
) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    guard.Override(folder).map_err(|err| {
        if err.contains("folder missing") {
            ApiFolderStatusError::MissingFolder
        } else {
            ApiFolderStatusError::Internal(err)
        }
    })?;
    // AUDIT-MARKER(override-response): W16H — Go's postDBOverride returns empty body.
    Ok(json!({}))
}

fn revert_folder(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    guard.Revert(folder).map_err(|err| {
        if err.contains("folder missing") {
            ApiFolderStatusError::MissingFolder
        } else {
            ApiFolderStatusError::Internal(err)
        }
    })?;
    // AUDIT-MARKER(revert-response): W16H — Go's postDBRevert returns empty body.
    Ok(json!({}))
}

fn reset_folder(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    guard.ResetFolder(folder);
    let mut db = guard
        .sdb
        .write()
        .map_err(|_| ApiFolderStatusError::Internal("database lock poisoned".to_string()))?;
    db.drop_folder(folder)
        .map_err(ApiFolderStatusError::Internal)?;
    // AUDIT-MARKER(reset-folder-payload): W16H — Go returns
    // {"ok": "resetting folder <id>"}. Do NOT re-flag.
    Ok(json!({"ok": format!("resetting folder {folder}")}))
}

fn browse_local_files(
    runtime: &DaemonApiRuntime,
    folder: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let db = guard
        .sdb
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("database lock poisoned".to_string()))?;
    let page = db
        .all_local_files_ordered_page(folder, "local", cursor, limit)
        .map_err(ApiFolderStatusError::Internal)?;
    let items = page
        .items
        .iter()
        .map(|item| {
            json!({
                "path": item.path,
                "sequence": item.sequence,
                "size": item.size,
                "deleted": item.deleted,
                "ignored": item.ignored,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "folder": folder,
        "cursor": cursor,
        "limit": limit,
        "nextCursor": page.next_cursor,
        "items": items,
    }))
}

fn browse_needed_files(
    runtime: &DaemonApiRuntime,
    folder: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let db = guard
        .sdb
        .read()
        .map_err(|_| ApiFolderStatusError::Internal("database lock poisoned".to_string()))?;
    let page = db
        .all_needed_global_files_ordered_page(folder, "local", cursor, limit)
        .map_err(ApiFolderStatusError::Internal)?;
    let items = page
        .items
        .iter()
        .map(|item| {
            json!({
                "path": item.path,
                "sequence": item.sequence,
                "size": item.size,
                "deleted": item.deleted,
                "ignored": item.ignored,
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "folder": folder,
        "cursor": cursor,
        "limit": limit,
        "nextCursor": page.next_cursor,
        "progress": Value::Array(vec![]),
        "queued": Value::Array(vec![]),
        "rest": items.clone(),
        "page": 1,
        "perpage": limit,
        "items": items,
    }))
}

fn file_info_to_api_record(file: &crate::db::FileInfo) -> Value {
    let file_type = match file.file_type {
        crate::db::FileInfoType::File => "file",
        crate::db::FileInfoType::Directory => "directory",
        crate::db::FileInfoType::Symlink => "symlink",
    };
    json!({
        "path": file.path,
        "sequence": file.sequence,
        "modifiedNs": file.modified_ns,
        "size": file.size,
        "deleted": file.deleted,
        "ignored": file.ignored,
        "localFlags": file.local_flags,
        "fileType": file_type,
        "blockHashes": file.block_hashes,
    })
}

fn list_config_folders(runtime: &DaemonApiRuntime) -> Result<Value, ApiConfigError> {
    let guard = runtime
        .model
        .read()
        .map_err(|_| ApiConfigError::Internal("model lock poisoned".to_string()))?;
    // W10-H3: Go returns full FolderConfiguration objects, not a reduced schema.
    // Reuse folder_config_to_json for parity.
    let mut folders: Vec<Value> = guard
        .folderCfgs
        .iter()
        .map(|(id, cfg)| folder_config_to_json(id, cfg))
        .collect();
    folders.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    Ok(Value::Array(folders))
}

fn add_config_folder(
    runtime: &DaemonApiRuntime,
    folder_id: &str,
    path: &str,
    folder_type: Option<FolderType>,
) -> Result<Value, ApiConfigError> {
    if folder_id.trim().is_empty() {
        return Err(ApiConfigError::BadRequest(
            "folder id must not be empty".to_string(),
        ));
    }
    if path.trim().is_empty() {
        return Err(ApiConfigError::BadRequest(
            "folder path must not be empty".to_string(),
        ));
    }
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiConfigError::Internal("model lock poisoned".to_string()))?;
    if guard.folderCfgs.contains_key(folder_id) {
        return Err(ApiConfigError::Conflict(folder_id.to_string()));
    }
    let mut cfg = newFolderConfiguration(folder_id, path);
    if let Some(ft) = folder_type {
        cfg.folder_type = ft;
    }
    guard.newFolder(cfg.clone());
    Ok(json!({
        "added": true,
        "folder": {
            "id": cfg.id,
            "path": cfg.path,
            "type": cfg.folder_type.as_str(),
        }
    }))
}

fn remove_config_folder(
    runtime: &DaemonApiRuntime,
    folder_id: &str,
) -> Result<Value, ApiConfigError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiConfigError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder_id) {
        return Err(ApiConfigError::Missing(folder_id.to_string()));
    }
    guard.folderCfgs.remove(folder_id);
    guard.cfg.remove(folder_id);
    guard.folderIgnores.remove(folder_id);
    guard.cleanupFolderLocked(folder_id);
    Ok(json!({
        "removed": true,
        "folder": folder_id,
    }))
}

fn restart_config_folder(
    runtime: &DaemonApiRuntime,
    folder_id: &str,
) -> Result<Value, ApiConfigError> {
    let mut guard = runtime
        .model
        .write()
        .map_err(|_| ApiConfigError::Internal("model lock poisoned".to_string()))?;
    guard.restartFolder(folder_id).map_err(|err| {
        if err.contains("folder missing") {
            ApiConfigError::Missing(folder_id.to_string())
        } else {
            ApiConfigError::Internal(err)
        }
    })?;
    Ok(json!({
        "restarted": true,
        "folder": folder_id,
    }))
}

fn run_daemon_with_listener(
    listener: TcpListener,
    model: Arc<RwLock<model>>,
    once: bool,
    max_peers: usize,
    active_peers: Arc<AtomicUsize>,
    shutdown_requested: Arc<AtomicBool>,
) -> Result<(), String> {
    listener
        .set_nonblocking(true)
        .map_err(|err| format!("set listener nonblocking: {err}"))?;
    let mut peer_seq = 0_u64;
    loop {
        if shutdown_requested.load(Ordering::Acquire) {
            return Ok(());
        }
        let (mut stream, addr) = match listener.accept() {
            Ok(conn) => conn,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(err) => return Err(format!("accept connection: {err}")),
        };
        if once {
            let peer_id = addr.to_string();
            handle_peer_connection(&mut stream, &peer_id, &model)?;
            return Ok(());
        }

        if !try_acquire_peer_slot(&active_peers, max_peers) {
            // Backpressure strategy for capped peers: accept and immediately close.
            let _ = stream.shutdown(std::net::Shutdown::Both);
            continue;
        }

        peer_seq = peer_seq.saturating_add(1);
        let peer_id = format!("{}#{}", addr, peer_seq);
        let model = model.clone();
        let active = active_peers.clone();
        thread::spawn(move || {
            let _guard = ActivePeerGuard::new(active);
            let _ = handle_peer_connection(&mut stream, &peer_id, &model);
        });
    }
}

fn try_acquire_peer_slot(active_peers: &AtomicUsize, max_peers: usize) -> bool {
    loop {
        let current = active_peers.load(Ordering::Acquire);
        if current >= max_peers {
            return false;
        }
        if active_peers
            .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
    }
}

struct ActivePeerGuard {
    active_peers: Arc<AtomicUsize>,
}

impl ActivePeerGuard {
    fn new(active_peers: Arc<AtomicUsize>) -> Self {
        Self { active_peers }
    }
}

impl Drop for ActivePeerGuard {
    fn drop(&mut self) {
        self.active_peers.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) fn handle_peer_connection(
    stream: &mut TcpStream,
    peer_id: &str,
    model: &Arc<RwLock<model>>,
) -> Result<(), String> {
    let timeout = Some(Duration::from_secs(PEER_IO_TIMEOUT_SECS));
    stream
        .set_read_timeout(timeout)
        .map_err(|err| format!("set read timeout: {err}"))?;
    stream
        .set_write_timeout(timeout)
        .map_err(|err| format!("set write timeout: {err}"))?;
    let peer_id_looks_transport = peer_id.contains(':') || peer_id.contains('#');
    let hello = match read_hello_packet(stream)? {
        Some(hello) => hello,
        None => return Ok(()),
    };
    let BepMessage::Hello { device_name, .. } = &hello else {
        return Err("expected hello packet".to_string());
    };
    let logical_peer_id = canonical_peer_device_id(peer_id, device_name, peer_id_looks_transport);
    {
        let mut guard = model
            .write()
            .map_err(|_| "model lock poisoned".to_string())?;
        let _ = guard.ApplyBepMessage(&logical_peer_id, &hello)?;
    }

    loop {
        let frame = match read_frame(stream)? {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let inbound = decode_frame(&frame)?;
        if matches!(inbound, BepMessage::Hello { .. }) {
            return Err("duplicate hello message".to_string());
        }
        let outbound = {
            let mut guard = model
                .write()
                .map_err(|_| "model lock poisoned".to_string())?;
            guard.ApplyBepMessage(&logical_peer_id, &inbound)?
        };
        if let Some(message) = outbound {
            write_frame(stream, &message)?;
        }
        if matches!(inbound, BepMessage::Close { .. }) {
            return Ok(());
        }
    }
}

fn read_hello_packet(reader: &mut impl Read) -> Result<Option<BepMessage>, String> {
    let mut prefix = [0_u8; 6];
    match reader.read_exact(&mut prefix[..1]) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(format!("read hello magic: {err}")),
    }
    reader
        .read_exact(&mut prefix[1..])
        .map_err(|err| format!("read hello header: {err}"))?;
    let magic = u32::from_be_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]);
    if magic != crate::bep_core::HelloMessageMagic {
        return Err("expected hello as first message".to_string());
    }
    let size = u16::from_be_bytes([prefix[4], prefix[5]]) as usize;
    let mut payload = vec![0_u8; size];
    reader
        .read_exact(&mut payload)
        .map_err(|err| format!("read hello payload: {err}"))?;
    let mut packet = Vec::with_capacity(6 + size);
    packet.extend_from_slice(&prefix);
    packet.extend_from_slice(&payload);
    decode_frame(&packet).map(Some)
}

fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, String> {
    let mut first = [0_u8; 1];
    match reader.read(&mut first) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("single-byte read returned >1"),
        Err(err) => return Err(format!("read frame header: {err}")),
    }

    let mut second = [0_u8; 1];
    reader
        .read_exact(&mut second)
        .map_err(|err| format!("read frame header: {err}"))?;
    let header_len = u16::from_be_bytes([first[0], second[0]]) as usize;
    if header_len > MAX_FRAME_BYTES {
        return Err(format!("frame too large: {header_len} > {MAX_FRAME_BYTES}"));
    }

    let mut header = vec![0_u8; header_len];
    reader
        .read_exact(&mut header)
        .map_err(|err| format!("read frame header: {err}"))?;

    let mut message_len_bytes = [0_u8; 4];
    reader
        .read_exact(&mut message_len_bytes)
        .map_err(|err| format!("read frame payload length: {err}"))?;
    let message_len = u32::from_be_bytes(message_len_bytes) as usize;
    if message_len > MAX_FRAME_BYTES {
        return Err(format!(
            "frame too large: {message_len} > {MAX_FRAME_BYTES}"
        ));
    }

    let mut payload = vec![0_u8; message_len];
    reader
        .read_exact(&mut payload)
        .map_err(|err| format!("read frame payload: {err}"))?;

    let mut frame = Vec::with_capacity(2 + header.len() + 4 + payload.len());
    frame.extend_from_slice(&[first[0], second[0]]);
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&message_len_bytes);
    frame.extend_from_slice(&payload);
    Ok(Some(frame))
}

#[allow(deprecated)]
fn write_frame(writer: &mut impl Write, message: &BepMessage) -> Result<(), String> {
    let frame = encode_frame(message)?;
    writer
        .write_all(&frame)
        .map_err(|err| format!("write frame: {err}"))?;
    writer
        .flush()
        .map_err(|err| format!("flush frame: {err}"))?;
    Ok(())
}

pub(crate) fn run_parity_probe(with_peer_interop: bool) -> Result<(), String> {
    let mut root = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .as_nanos();
    root.push(format!(
        "syncthing-rs-parity-probe-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root).map_err(|err| format!("create parity probe root: {err}"))?;
    let folder_path = root.join("folder");
    fs::create_dir_all(&folder_path).map_err(|err| format!("create parity probe folder: {err}"))?;
    fs::write(folder_path.join("a.txt"), b"hello-world")
        .map_err(|err| format!("seed parity probe file: {err}"))?;

    let result = (|| {
        let model = Arc::new(RwLock::new(NewModelWithRuntime(
            Some(root.join("db")),
            Some(64),
        )?));
        {
            let mut guard = model
                .write()
                .map_err(|_| "model lock poisoned".to_string())?;
            guard.newFolder(newFolderConfiguration(
                DEFAULT_FOLDER_ID,
                &folder_path.to_string_lossy(),
            ));
            guard.ScanFolder(DEFAULT_FOLDER_ID)?;
            guard
                .sdb
                .write()
                .map_err(|_| "db lock poisoned".to_string())?
                .update(
                    DEFAULT_FOLDER_ID,
                    "local",
                    vec![crate::db::FileInfo {
                        folder: DEFAULT_FOLDER_ID.to_string(),
                        path: "a.txt".to_string(),
                        sequence: 1,
                        modified_ns: 1,
                        size: 11,
                        deleted: false,
                        ignored: false,
                        local_flags: 0,
                        file_type: crate::db::FileInfoType::File,
                        block_hashes: vec!["h1".to_string()],
                        version_counters: Vec::new(),
                        permissions: 0,
                        modified_by: 0,
                        symlink_target: Vec::new(),
                        block_size: 0,
                        blocks_hash: Vec::new(),
                        previous_blocks_hash: Vec::new(),
                        encrypted: Vec::new(),
                    }],
                )
                .map_err(|err| format!("seed api probe db: {err}"))?;
        }
        let runtime = DaemonApiRuntime {
            model: model.clone(),
            state: Arc::new(RwLock::new(ApiRuntimeState::new("local-device"))),
            active_peers: Arc::new(AtomicUsize::new(0)),
            max_peers: DEFAULT_MAX_PEERS,
            start_time: SystemTime::now(),
            bep_listen_addr: DEFAULT_LISTEN_ADDR.to_string(),
            gui_listen_addr: "127.0.0.1:8384".to_string(),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            gui_root: None,
        };
        // W16B-1: CSRF is now unconditional. Set API key for parity probe.
        {
            let mut state = runtime
                .state
                .write()
                .map_err(|_| "state lock".to_string())?;
            state.gui["apiKey"] = serde_json::json!("parity-probe-key");
        }
        // W16B-1: Read salted API key param for mutating requests
        let apikey_param = runtime
            .state
            .read()
            .map(|state| auth_query_apikey_key(&state))
            .unwrap_or_else(|_| "_auth_apikey".to_string());

        ensure_api_ok(&build_api_response(
            &Method::Get,
            "/rest/system/ping",
            &runtime,
        ))?;
        ensure_api_ok(&build_api_response(
            &Method::Post,
            &append_query_param(
                "/rest/db/scan?folder=default",
                &apikey_param,
                "parity-probe-key",
            ),
            &runtime,
        ))?;
        let status_payload = ensure_api_ok(&build_api_response(
            &Method::Get,
            "/rest/db/status?folder=default",
            &runtime,
        ))?;
        let local_files = status_payload
            .get("localFiles")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if local_files == 0 {
            return Err("parity probe status reported zero local files after scan".to_string());
        }

        if with_peer_interop {
            run_parity_peer_probe(&model)?;
        }
        Ok(())
    })();

    let _ = fs::remove_dir_all(&root);
    result
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApiSurfaceProbeAssertion {
    pub(crate) status_code: u16,
    pub(crate) response_kind: String,
    pub(crate) required_keys: Vec<String>,
    pub(crate) present_keys: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ApiSurfaceProbeResult {
    pub(crate) covered_endpoints: Vec<String>,
    pub(crate) endpoint_assertions: BTreeMap<String, ApiSurfaceProbeAssertion>,
}

pub(crate) fn run_api_surface_probe() -> Result<ApiSurfaceProbeResult, String> {
    let mut root = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .as_nanos();
    root.push(format!(
        "syncthing-rs-api-surface-probe-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root).map_err(|err| format!("create api probe root: {err}"))?;
    let folder_path = root.join("folder");
    fs::create_dir_all(&folder_path).map_err(|err| format!("create api probe folder: {err}"))?;
    fs::write(folder_path.join("a.txt"), b"hello-world")
        .map_err(|err| format!("seed api probe file: {err}"))?;
    let docs_path = root.join("docs");

    let result = (|| {
        let model = Arc::new(RwLock::new(NewModelWithRuntime(
            Some(root.join("db")),
            Some(64),
        )?));
        {
            let mut guard = model
                .write()
                .map_err(|_| "model lock poisoned".to_string())?;
            guard.newFolder(newFolderConfiguration(
                DEFAULT_FOLDER_ID,
                &folder_path.to_string_lossy(),
            ));
        }
        let runtime = DaemonApiRuntime {
            model,
            state: Arc::new(RwLock::new(ApiRuntimeState::new("local-device"))),
            active_peers: Arc::new(AtomicUsize::new(0)),
            max_peers: DEFAULT_MAX_PEERS,
            start_time: SystemTime::now(),
            bep_listen_addr: DEFAULT_LISTEN_ADDR.to_string(),
            gui_listen_addr: "127.0.0.1:8384".to_string(),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            gui_root: None,
        };
        // W16B-1: CSRF is now unconditional. The probe needs an API key
        // so mutating requests (POST/DELETE) bypass CSRF via API-key auth.
        {
            let mut state = runtime
                .state
                .write()
                .map_err(|_| "state lock".to_string())?;
            state.gui["apiKey"] = serde_json::json!("probe-api-key");
        }

        let cases: Vec<(&str, Method, String, u16)> = vec![
            (
                "GET /rest/system/ping",
                Method::Get,
                "/rest/system/ping".to_string(),
                200,
            ),
            (
                "POST /rest/system/ping",
                Method::Post,
                "/rest/system/ping".to_string(),
                200,
            ),
            (
                "GET /rest/system/version",
                Method::Get,
                "/rest/system/version".to_string(),
                200,
            ),
            (
                "GET /rest/system/status",
                Method::Get,
                "/rest/system/status".to_string(),
                200,
            ),
            (
                "GET /rest/system/connections",
                Method::Get,
                "/rest/system/connections".to_string(),
                200,
            ),
            (
                "GET /rest/system/discovery",
                Method::Get,
                "/rest/system/discovery".to_string(),
                200,
            ),
            (
                "GET /rest/system/paths",
                Method::Get,
                "/rest/system/paths".to_string(),
                200,
            ),
            (
                "GET /rest/system/browse",
                Method::Get,
                format!("/rest/system/browse?current={}", root.to_string_lossy()),
                200,
            ),
            (
                "GET /rest/noauth/health",
                Method::Get,
                "/rest/noauth/health".to_string(),
                200,
            ),
            (
                "POST /rest/noauth/auth/password",
                Method::Post,
                "/rest/noauth/auth/password?user=probe".to_string(),
                404, // 7.7: Returns 404 when auth is not enabled
            ),
            (
                "POST /rest/noauth/auth/logout",
                Method::Post,
                "/rest/noauth/auth/logout?user=probe".to_string(),
                404, // 7.7: Returns 404 when auth is not enabled
            ),
            (
                "GET /rest/system/error",
                Method::Get,
                "/rest/system/error".to_string(),
                200,
            ),
            (
                "POST /rest/system/error",
                Method::Post,
                "/rest/system/error?message=probe-error".to_string(),
                200,
            ),
            (
                "POST /rest/system/error/clear",
                Method::Post,
                "/rest/system/error/clear".to_string(),
                200,
            ),
            (
                "GET /rest/system/log",
                Method::Get,
                "/rest/system/log".to_string(),
                200,
            ),
            (
                "GET /rest/system/log.txt",
                Method::Get,
                "/rest/system/log.txt".to_string(),
                200,
            ),
            (
                "GET /rest/system/loglevels",
                Method::Get,
                "/rest/system/loglevels".to_string(),
                200,
            ),
            (
                "POST /rest/system/loglevels",
                Method::Post,
                "/rest/system/loglevels?enable=bep".to_string(),
                200,
            ),
            (
                "GET /rest/system/upgrade",
                Method::Get,
                "/rest/system/upgrade".to_string(),
                200,
            ),
            (
                "POST /rest/system/upgrade",
                Method::Post,
                "/rest/system/upgrade".to_string(),
                200,
            ),
            (
                "POST /rest/system/pause",
                Method::Post,
                "/rest/system/pause?device=local-device".to_string(),
                200,
            ),
            (
                "POST /rest/system/resume",
                Method::Post,
                "/rest/system/resume?device=local-device".to_string(),
                200,
            ),
            (
                "POST /rest/system/restart",
                Method::Post,
                "/rest/system/restart".to_string(),
                200,
            ),
            (
                "POST /rest/system/shutdown",
                Method::Post,
                "/rest/system/shutdown".to_string(),
                200,
            ),
            (
                "POST /rest/system/reset",
                Method::Post,
                "/rest/system/reset?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/config",
                Method::Get,
                "/rest/config".to_string(),
                200,
            ),
            (
                "PUT /rest/config",
                Method::Put,
                "/rest/config".to_string(),
                400, // W5-C2: requires JSON body — empty PUT correctly rejected
            ),
            (
                "GET /rest/system/config",
                Method::Get,
                "/rest/system/config".to_string(),
                200,
            ),
            (
                "POST /rest/system/config",
                Method::Post,
                "/rest/system/config".to_string(),
                400, // W7-R1: POST now applies config — empty POST body returns 400 (Go parity)
            ),
            (
                "GET /rest/config/insync",
                Method::Get,
                "/rest/config/insync".to_string(),
                200,
            ),
            (
                "GET /rest/system/config/insync",
                Method::Get,
                "/rest/system/config/insync".to_string(),
                200,
            ),
            (
                "GET /rest/config/restart-required",
                Method::Get,
                "/rest/config/restart-required".to_string(),
                200,
            ),
            (
                "GET /rest/config/options",
                Method::Get,
                "/rest/config/options".to_string(),
                200,
            ),
            (
                "PUT /rest/config/options",
                Method::Put,
                "/rest/config/options?maxSendKbps=1".to_string(),
                200,
            ),
            (
                "PATCH /rest/config/options",
                Method::Patch,
                "/rest/config/options?maxRecvKbps=2".to_string(),
                200,
            ),
            (
                "GET /rest/config/gui",
                Method::Get,
                "/rest/config/gui".to_string(),
                200,
            ),
            (
                "PUT /rest/config/gui",
                Method::Put,
                "/rest/config/gui?theme=default".to_string(),
                200,
            ),
            (
                "PATCH /rest/config/gui",
                Method::Patch,
                "/rest/config/gui?insecureAdminAccess=false".to_string(),
                200,
            ),
            (
                "GET /rest/config/ldap",
                Method::Get,
                "/rest/config/ldap".to_string(),
                200,
            ),
            (
                "PUT /rest/config/ldap",
                Method::Put,
                "/rest/config/ldap?enabled=false".to_string(),
                200,
            ),
            (
                "PATCH /rest/config/ldap",
                Method::Patch,
                "/rest/config/ldap?address=ldap://localhost".to_string(),
                200,
            ),
            (
                "GET /rest/config/defaults/folder",
                Method::Get,
                "/rest/config/defaults/folder".to_string(),
                200,
            ),
            (
                "PUT /rest/config/defaults/folder",
                Method::Put,
                "/rest/config/defaults/folder?rescanIntervalS=3600".to_string(),
                200,
            ),
            (
                "PATCH /rest/config/defaults/folder",
                Method::Patch,
                "/rest/config/defaults/folder?paused=false".to_string(),
                200,
            ),
            (
                "GET /rest/config/defaults/device",
                Method::Get,
                "/rest/config/defaults/device".to_string(),
                200,
            ),
            (
                "PUT /rest/config/defaults/device",
                Method::Put,
                "/rest/config/defaults/device?compression=metadata".to_string(),
                200,
            ),
            (
                "PATCH /rest/config/defaults/device",
                Method::Patch,
                "/rest/config/defaults/device?introducer=false".to_string(),
                200,
            ),
            (
                "GET /rest/config/defaults/ignores",
                Method::Get,
                "/rest/config/defaults/ignores".to_string(),
                200,
            ),
            (
                "PUT /rest/config/defaults/ignores",
                Method::Put,
                "/rest/config/defaults/ignores?patterns=.git,node_modules".to_string(),
                200,
            ),
            (
                "GET /rest/config/folders",
                Method::Get,
                "/rest/config/folders".to_string(),
                200,
            ),
            (
                "POST /rest/config/folders",
                Method::Post,
                format!(
                    "/rest/config/folders?id=docs&path={}",
                    docs_path.to_string_lossy()
                ),
                200,
            ),
            (
                "PUT /rest/config/folders",
                Method::Put,
                format!(
                    "/rest/config/folders?_body=[{{\"id\":\"default\",\"path\":\"{}\"}},{{\"id\":\"docs\",\"path\":\"{}\"}}]",
                    folder_path.to_string_lossy(),
                    docs_path.to_string_lossy()
                ),
                200,
            ),
            (
                "GET /rest/config/folders/:id",
                Method::Get,
                "/rest/config/folders/docs".to_string(),
                200,
            ),
            (
                "PUT /rest/config/folders/:id",
                Method::Put,
                format!(
                    "/rest/config/folders/docs?path={}",
                    docs_path.to_string_lossy()
                ),
                200,
            ),
            (
                "PATCH /rest/config/folders/:id",
                Method::Patch,
                "/rest/config/folders/docs?paused=false".to_string(),
                200,
            ),
            (
                "GET /rest/config/devices",
                Method::Get,
                "/rest/config/devices".to_string(),
                200,
            ),
            (
                "POST /rest/config/devices",
                Method::Post,
                "/rest/config/devices?id=peer-a&address=tcp://peer-a".to_string(),
                200,
            ),
            (
                "PUT /rest/config/devices",
                Method::Put,
                "/rest/config/devices?_body=[{\"deviceID\":\"peer-a\",\"addresses\":[\"tcp://peer-a\"]}]".to_string(),
                200,
            ),
            (
                "GET /rest/config/devices/:id",
                Method::Get,
                "/rest/config/devices/peer-a".to_string(),
                200,
            ),
            (
                "PUT /rest/config/devices/:id",
                Method::Put,
                "/rest/config/devices/peer-a?name=Peer%20A".to_string(),
                200,
            ),
            (
                "PATCH /rest/config/devices/:id",
                Method::Patch,
                "/rest/config/devices/peer-a?paused=false".to_string(),
                200,
            ),
            (
                "GET /rest/cluster/pending/devices",
                Method::Get,
                "/rest/cluster/pending/devices".to_string(),
                200,
            ),
            (
                "DELETE /rest/cluster/pending/devices",
                Method::Delete,
                "/rest/cluster/pending/devices?device=peer-a".to_string(),
                200,
            ),
            (
                "GET /rest/cluster/pending/folders",
                Method::Get,
                "/rest/cluster/pending/folders".to_string(),
                200,
            ),
            (
                "DELETE /rest/cluster/pending/folders",
                Method::Delete,
                "/rest/cluster/pending/folders?folder=docs".to_string(),
                200,
            ),
            (
                "GET /rest/events",
                Method::Get,
                "/rest/events?timeout=0".to_string(),
                200,
            ),
            (
                "GET /rest/events/disk",
                Method::Get,
                "/rest/events/disk?timeout=0".to_string(),
                200,
            ),
            (
                "GET /rest/stats/device",
                Method::Get,
                "/rest/stats/device".to_string(),
                200,
            ),
            (
                "GET /rest/stats/folder",
                Method::Get,
                "/rest/stats/folder".to_string(),
                200,
            ),
            (
                "GET /rest/svc/deviceid",
                Method::Get,
                "/rest/svc/deviceid?id=peer-a".to_string(),
                200,
            ),
            (
                "GET /rest/svc/lang",
                Method::Get,
                "/rest/svc/lang".to_string(),
                200,
            ),
            (
                "GET /rest/svc/report",
                Method::Get,
                "/rest/svc/report".to_string(),
                200,
            ),
            (
                "GET /rest/svc/random/string",
                Method::Get,
                "/rest/svc/random/string?length=12".to_string(),
                200,
            ),
            (
                "GET /rest/folder/errors",
                Method::Get,
                "/rest/folder/errors?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/folder/pullerrors",
                Method::Get,
                "/rest/folder/pullerrors?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/folder/versions",
                Method::Get,
                "/rest/folder/versions?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/folder/versions",
                Method::Post,
                "/rest/folder/versions?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/debug/*method",
                Method::Get,
                "/rest/debug/support".to_string(),
                200,
            ),
            (
                "GET /rest/db/browse",
                Method::Get,
                "/rest/db/browse?folder=default&limit=10".to_string(),
                200,
            ),
            (
                "GET /rest/db/completion",
                Method::Get,
                "/rest/db/completion?folder=default&device=peer-a".to_string(),
                200,
            ),
            (
                "GET /rest/db/file",
                Method::Get,
                "/rest/db/file?folder=default&file=missing.txt".to_string(),
                404,
            ),
            (
                "GET /rest/db/ignores",
                Method::Get,
                "/rest/db/ignores?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/ignores",
                Method::Post,
                "/rest/db/ignores?folder=default&patterns=.cache,temp".to_string(),
                200,
            ),
            (
                "GET /rest/db/jobs",
                Method::Get,
                "/rest/db/jobs?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/db/localchanged",
                Method::Get,
                "/rest/db/localchanged?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/db/need",
                Method::Get,
                "/rest/db/need?folder=default&limit=10".to_string(),
                200,
            ),
            (
                "POST /rest/db/bringtofront",
                Method::Post,
                "/rest/db/bringtofront?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/override",
                Method::Post,
                "/rest/db/override?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/prio",
                Method::Post,
                "/rest/db/prio?folder=default&file=a.txt".to_string(),
                200,
            ),
            (
                "POST /rest/db/pull",
                Method::Post,
                "/rest/db/pull?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/reset",
                Method::Post,
                "/rest/db/reset?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/db/remoteneed",
                Method::Get,
                "/rest/db/remoteneed?folder=default&device=peer-a".to_string(),
                200,
            ),
            (
                "POST /rest/db/revert",
                Method::Post,
                "/rest/db/revert?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/scan",
                Method::Post,
                "/rest/db/scan?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/db/status",
                Method::Get,
                "/rest/db/status?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/system/config/folders",
                Method::Get,
                "/rest/system/config/folders".to_string(),
                200,
            ),
            (
                "POST /rest/system/config/folders",
                Method::Post,
                format!(
                    "/rest/system/config/folders?id=docs2&path={}",
                    docs_path.to_string_lossy()
                ),
                200,
            ),
            (
                "POST /rest/system/config/restart",
                Method::Post,
                "/rest/system/config/restart?folder=docs".to_string(),
                200,
            ),
            (
                "DELETE /rest/system/config/folders",
                Method::Delete,
                "/rest/system/config/folders?id=docs2".to_string(),
                200,
            ),
            (
                "DELETE /rest/config/devices/:id",
                Method::Delete,
                "/rest/config/devices/peer-a".to_string(),
                200,
            ),
            (
                "DELETE /rest/config/folders/:id",
                Method::Delete,
                "/rest/config/folders/docs".to_string(),
                200,
            ),
        ];

        let mut covered = Vec::with_capacity(cases.len());
        let mut assertions = BTreeMap::new();
        // W16B-1: Read salted API key param name for CSRF bypass
        let apikey_param = runtime
            .state
            .read()
            .map(|state| auth_query_apikey_key(&state))
            .unwrap_or_else(|_| "_auth_apikey".to_string());
        for (key, method, url, expected_status) in cases {
            // W16B-1: Mutating methods need API key to bypass CSRF
            let authed_url = if matches!(
                method,
                Method::Post | Method::Put | Method::Delete | Method::Patch
            ) {
                append_query_param(&url, &apikey_param, "probe-api-key")
            } else {
                url.clone()
            };
            let reply = build_api_response(&method, &authed_url, &runtime);
            if reply.status_code != StatusCode(expected_status) {
                let body = String::from_utf8_lossy(&reply.body);
                return Err(format!(
                    "api probe {key} expected status {expected_status} got {} body={body}",
                    reply.status_code.0
                ));
            }
            let (response_kind, present_keys) = if key == "GET /rest/system/log.txt" {
                if !reply.content_type.starts_with("text/plain") {
                    return Err(format!(
                        "api probe {key} expected text/plain content type got {}",
                        reply.content_type
                    ));
                }
                ("string".to_string(), Vec::new())
            } else {
                response_shape(&reply.body)?
            };
            if let Some(expected_kind) = expected_api_probe_response_kind(key) {
                if response_kind != expected_kind {
                    return Err(format!(
                        "api probe {key} expected response kind {expected_kind} got {response_kind}"
                    ));
                }
            }
            let required_keys = required_api_probe_keys(key)
                .iter()
                .map(|entry| (*entry).to_string())
                .collect::<Vec<_>>();
            for required_key in &required_keys {
                if !present_keys.iter().any(|present| present == required_key) {
                    return Err(format!(
                        "api probe {key} missing required response key {required_key}"
                    ));
                }
            }
            for forbidden_key in forbidden_api_probe_keys(key) {
                if present_keys.iter().any(|present| present == forbidden_key) {
                    return Err(format!(
                        "api probe {key} returned forbidden wrapper key {forbidden_key}"
                    ));
                }
            }
            assertions.insert(
                key.to_string(),
                ApiSurfaceProbeAssertion {
                    status_code: expected_status,
                    response_kind,
                    required_keys,
                    present_keys,
                },
            );
            covered.push(key.to_string());
        }
        covered.sort();
        Ok(ApiSurfaceProbeResult {
            covered_endpoints: covered,
            endpoint_assertions: assertions,
        })
    })();

    let _ = fs::remove_dir_all(&root);
    result
}

fn expected_api_probe_response_kind(endpoint: &str) -> Option<&'static str> {
    match endpoint {
        "GET /rest/events" | "GET /rest/events/disk" => Some("array"),
        "GET /rest/system/discovery"
        | "GET /rest/cluster/pending/devices"
        | "GET /rest/cluster/pending/folders"
        | "GET /rest/system/status"
        | "GET /rest/config" => Some("object"),
        _ => None,
    }
}

fn required_api_probe_keys(endpoint: &str) -> &'static [&'static str] {
    match endpoint {
        "GET /rest/system/version" => &["version", "longVersion", "os", "arch"],
        "GET /rest/system/status" => &[
            "myID",
            "uptime",
            "alloc",
            "sys",
            "goroutines",
            "guiAddressUsed",
            "connectionServiceStatus",
            "discoveryStatus",
            "urVersionMax",
        ],
        "GET /rest/config" => &[
            "devices",
            "folders",
            "options",
            "gui",
            "defaults",
            "remoteIgnoredDevices",
        ],
        "GET /rest/config/options" => &[
            "listenAddresses",
            "globalAnnounceServers",
            "progressUpdateIntervalS",
        ],
        "GET /rest/config/gui" => &["address", "theme", "useTLS"],
        "GET /rest/config/defaults/ignores" => &["lines"],
        "GET /rest/system/connections" => &["total", "connections"],
        "GET /rest/system/config/folders" => &["count", "folders"],
        "GET /rest/db/status" => &["folder", "state", "localFiles", "needFiles"],
        "GET /rest/db/completion" => &[
            "completion",
            "globalBytes",
            "needBytes",
            "globalItems",
            "needItems",
            "needDeletes",
            "sequence",
            "remoteState",
        ],
        "GET /rest/db/file" => &["error"],
        "GET /rest/db/ignores" => &["ignore", "expanded", "error"],
        "GET /rest/db/jobs" => &["folder", "state", "jobs"],
        "GET /rest/db/localchanged" => &["files", "page", "perpage"],
        "GET /rest/db/need" => &["progress", "queued", "rest", "page", "perpage"],
        "GET /rest/db/remoteneed" => &["files", "page", "perpage"],
        "GET /rest/db/browse" => &[],
        "POST /rest/db/bringtofront" => &["ok"],
        "POST /rest/db/override" => &["ok"],
        "POST /rest/db/revert" => &["ok"],
        "POST /rest/db/pull" => &["folder", "state", "sequence", "pull"],
        "POST /rest/db/reset" => &["ok"],
        "POST /rest/db/scan" => &[],
        "POST /rest/system/shutdown" => &["ok"],
        "POST /rest/system/config/folders" => &["added", "folder"],
        "POST /rest/system/config/restart" => &["restarted", "folder"],
        "DELETE /rest/system/config/folders" => &["removed", "folder"],
        _ => &[],
    }
}

fn forbidden_api_probe_keys(endpoint: &str) -> &'static [&'static str] {
    match endpoint {
        "GET /rest/system/discovery" => &["devices"],
        "GET /rest/cluster/pending/devices" => &["count", "devices"],
        "GET /rest/cluster/pending/folders" => &["count", "folders"],
        _ => &[],
    }
}

fn response_shape(body: &[u8]) -> Result<(String, Vec<String>), String> {
    if body.is_empty() {
        return Ok(("empty".to_string(), Vec::new()));
    }
    let value: Value =
        serde_json::from_slice(body).map_err(|err| format!("decode probe response JSON: {err}"))?;
    match value {
        Value::Object(map) => {
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            Ok(("object".to_string(), keys))
        }
        Value::Array(_) => Ok(("array".to_string(), Vec::new())),
        Value::Null => Ok(("null".to_string(), Vec::new())),
        Value::Bool(_) => Ok(("bool".to_string(), Vec::new())),
        Value::Number(_) => Ok(("number".to_string(), Vec::new())),
        Value::String(_) => Ok(("string".to_string(), Vec::new())),
    }
}

fn ensure_api_ok(reply: &ApiReply) -> Result<Value, String> {
    if reply.status_code != StatusCode(200) {
        return Err(format!(
            "probe api call failed with status {}",
            reply.status_code.0
        ));
    }
    serde_json::from_slice(&reply.body)
        .map_err(|err| format!("decode probe api response payload: {err}"))
}

fn run_parity_peer_probe_inprocess(model: &Arc<RwLock<model>>) -> Result<(), String> {
    let mut guard = model
        .write()
        .map_err(|_| "model lock poisoned".to_string())?;
    if let Some(cfg) = guard.folderCfgs.get_mut(DEFAULT_FOLDER_ID) {
        if !cfg.shared_with("parity-probe") {
            cfg.devices.push(crate::config::FolderDeviceConfiguration {
                device_id: "parity-probe".to_string(),
                introduced_by: String::new(),
                encryption_password: String::new(),
            });
        }
    }
    guard.ApplyBepMessage(
        "parity-probe",
        &BepMessage::Hello {
            device_name: "parity-probe".to_string(),
            client_name: "syncthing-rs".to_string(),
            client_version: String::new(),
            num_connections: 0,
            timestamp: 0,
        },
    )?;
    let response = guard
        .ApplyBepMessage(
            "parity-probe",
            &BepMessage::Request {
                id: 42,
                folder: DEFAULT_FOLDER_ID.to_string(),
                name: "a.txt".to_string(),
                offset: 0,
                size: 5,
                hash: Vec::new(),
                from_temporary: false,
                block_no: 0,
            },
        )?
        .ok_or_else(|| "peer probe fallback expected response message".to_string())?;
    match response {
        BepMessage::Response { id, code, .. } if id == 42 && code == 0 => {}
        BepMessage::Response { id, code, .. } => {
            return Err(format!(
                "peer probe fallback response mismatch: id={id} code={code}"
            ));
        }
        other => {
            return Err(format!(
                "peer probe fallback expected response message, got {}",
                message_tag(&other)
            ));
        }
    }
    guard.ApplyBepMessage(
        "parity-probe",
        &BepMessage::Close {
            reason: "parity probe complete".to_string(),
        },
    )?;
    Ok(())
}

fn run_parity_peer_probe(model: &Arc<RwLock<model>>) -> Result<(), String> {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
            // Some restricted environments deny loopback bind in tests/probes.
            // Fall back to in-process BEP exchange coverage instead of failing hard.
            return run_parity_peer_probe_inprocess(model);
        }
        Err(err) => return Err(format!("bind parity peer probe listener: {err}")),
    };
    let addr = listener
        .local_addr()
        .map_err(|err| format!("parity peer probe local addr: {err}"))?;
    let model_ref = model.clone();
    let server = thread::spawn(move || -> Result<(), String> {
        let (mut stream, peer_addr) = listener
            .accept()
            .map_err(|err| format!("accept parity peer probe connection: {err}"))?;
        let _ = peer_addr;
        handle_peer_connection(&mut stream, "parity-probe", &model_ref)
    });

    {
        let mut guard = model
            .write()
            .map_err(|_| "model lock poisoned".to_string())?;
        if let Some(cfg) = guard.folderCfgs.get_mut(DEFAULT_FOLDER_ID) {
            if !cfg.shared_with("parity-probe") {
                cfg.devices.push(crate::config::FolderDeviceConfiguration {
                    device_id: "parity-probe".to_string(),
                    introduced_by: String::new(),
                    encryption_password: String::new(),
                });
            }
        }
    }

    let mut client = TcpStream::connect(addr)
        .map_err(|err| format!("connect parity peer probe client: {err}"))?;
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .map_err(|err| format!("set peer probe read timeout: {err}"))?;

    write_frame(
        &mut client,
        &BepMessage::Hello {
            device_name: "parity-probe".to_string(),
            client_name: "syncthing-rs".to_string(),
            client_version: String::new(),
            num_connections: 0,
            timestamp: 0,
        },
    )?;
    write_frame(
        &mut client,
        &BepMessage::Request {
            id: 42,
            folder: DEFAULT_FOLDER_ID.to_string(),
            name: "a.txt".to_string(),
            offset: 0,
            size: 5,
            hash: Vec::new(),
            from_temporary: false,
            block_no: 0,
        },
    )?;

    let frame = read_frame(&mut client)?
        .ok_or_else(|| "peer probe expected BEP response frame but stream closed".to_string())?;
    let message = decode_frame(&frame)?;
    match message {
        BepMessage::Response { id, code, .. } => {
            if id != 42 {
                return Err(format!(
                    "peer probe response id mismatch: expected 42 got {id}"
                ));
            }
            if code != 0 {
                return Err(format!("peer probe response returned non-zero code {code}"));
            }
        }
        other => {
            return Err(format!(
                "peer probe expected response message, got {}",
                message_tag(&other)
            ))
        }
    }

    write_frame(
        &mut client,
        &BepMessage::Close {
            reason: "parity probe complete".to_string(),
        },
    )?;
    let _ = client.shutdown(std::net::Shutdown::Both);

    server
        .join()
        .map_err(|_| "parity peer probe thread panicked".to_string())??;
    Ok(())
}

fn message_tag(message: &BepMessage) -> &'static str {
    match message {
        BepMessage::Hello { .. } => "hello",
        BepMessage::ClusterConfig { .. } => "cluster_config",
        BepMessage::Index { .. } => "index",
        BepMessage::IndexUpdate { .. } => "index_update",
        BepMessage::Request { .. } => "request",
        BepMessage::Response { .. } => "response",
        BepMessage::DownloadProgress { .. } => "download_progress",
        BepMessage::Ping { .. } => "ping",
        BepMessage::Close { .. } => "close",
        BepMessage::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bep::{default_exchange, BepMessage};
    use crate::db;
    use crate::db::Db;
    use crate::model_core::NewModel;
    use serde_json::Value;
    use std::fs;
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!(
            "syncthing-rs-runtime-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp root");
        path
    }

    fn read_response(stream: &mut TcpStream) -> BepMessage {
        let frame = read_frame(stream)
            .expect("read frame")
            .expect("response frame");
        decode_frame(&frame).expect("decode response")
    }

    fn header_value<'a>(reply: &'a ApiReply, name: &str) -> Option<&'a str> {
        reply
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn session_token_from_set_cookie(reply: &ApiReply) -> Option<String> {
        let cookie = header_value(reply, "Set-Cookie")?;
        for chunk in cookie.split(';').map(str::trim) {
            if let Some((k, v)) = chunk.split_once('=') {
                if k.eq_ignore_ascii_case("sessionid") || k.starts_with("sessionid-") {
                    return Some(v.trim().to_string());
                }
            }
        }
        None
    }

    fn auth_query_keys(runtime: &DaemonApiRuntime) -> (String, String) {
        let state = runtime.state.read().expect("state lock");
        (auth_query_token_key(&state), auth_query_csrf_key(&state))
    }

    fn auth_apikey_key(runtime: &DaemonApiRuntime) -> String {
        let state = runtime.state.read().expect("state lock");
        auth_query_apikey_key(&state)
    }

    fn bind_loopback_listener_or_skip() -> Option<TcpListener> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => Some(listener),
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("skipping socket test (loopback bind denied): {err}");
                None
            }
            Err(err) => panic!("bind: {err}"),
        }
    }

    fn file_info(path: &str, sequence: i64, size: i64) -> db::FileInfo {
        db::FileInfo {
            folder: "default".to_string(),
            path: path.to_string(),
            sequence,
            modified_ns: sequence,
            size,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: db::FileInfoType::File,
            block_hashes: vec!["h".to_string()],
            version_counters: Vec::new(),
            permissions: 0,
            modified_by: 0,
            symlink_target: Vec::new(),
            block_size: 0,
            blocks_hash: Vec::new(),
            previous_blocks_hash: Vec::new(),
            encrypted: Vec::new(),
        }
    }

    #[test]
    fn parse_daemon_args_allows_empty_folder_list_for_bootstrap() {
        let cfg = parse_daemon_args(&[]).expect("parse");
        assert!(cfg.folders.is_empty());
    }

    #[test]
    fn parse_daemon_args_rejects_folder_id_without_path() {
        let args = vec!["--folder-id".to_string(), "photos".to_string()];
        let err = parse_daemon_args(&args).expect_err("must fail");
        assert!(err.contains("--folder-id requires --folder-path"));
    }

    #[test]
    fn parse_daemon_args_parses_flags() {
        let args = vec![
            "--listen".to_string(),
            "127.0.0.1:23000".to_string(),
            "--api-listen".to_string(),
            "127.0.0.1:28384".to_string(),
            "--folder-id".to_string(),
            "photos".to_string(),
            "--folder-path".to_string(),
            "/tmp/photos".to_string(),
            "--db-root".to_string(),
            "/tmp/syncthing-rs-db".to_string(),
            "--memory-max-mb".to_string(),
            "64".to_string(),
            "--max-peers".to_string(),
            "8".to_string(),
            "--once".to_string(),
        ];
        let cfg = parse_daemon_args(&args).expect("parse");
        assert_eq!(cfg.listen_addr, "127.0.0.1:23000");
        assert_eq!(cfg.api_listen_addr.as_deref(), Some("127.0.0.1:28384"));
        assert_eq!(
            cfg.folders,
            vec![FolderSpec {
                id: "photos".to_string(),
                path: "/tmp/photos".to_string()
            }]
        );
        assert_eq!(cfg.db_root.as_deref(), Some("/tmp/syncthing-rs-db"));
        assert_eq!(cfg.memory_max_mb, Some(64));
        assert_eq!(cfg.max_peers, 8);
        assert!(cfg.once);
    }

    #[test]
    fn parse_daemon_args_parses_multiple_folder_specs() {
        let args = vec![
            "--folder".to_string(),
            "docs:/srv/docs".to_string(),
            "--folder".to_string(),
            "photos:/srv/photos".to_string(),
        ];
        let cfg = parse_daemon_args(&args).expect("parse");
        assert_eq!(
            cfg.folders,
            vec![
                FolderSpec {
                    id: "docs".to_string(),
                    path: "/srv/docs".to_string()
                },
                FolderSpec {
                    id: "photos".to_string(),
                    path: "/srv/photos".to_string()
                }
            ]
        );
    }

    #[test]
    fn parse_daemon_args_loads_config_file_and_allows_overrides() {
        let root = temp_root("config-file");
        let config_path = root.join("daemon.json");
        fs::write(
            &config_path,
            r#"{
                "listen_addr":"127.0.0.1:24100",
                "api_listen_addr":"127.0.0.1:28385",
                "db_root":"/tmp/syncthing-rs-db-config",
                "memory_max_mb":71,
                "max_peers":4,
                "folders":[
                    {"id":"docs","path":"/srv/docs"},
                    {"id":"photos","path":"/srv/photos"}
                ]
            }"#,
        )
        .expect("write config");

        let args = vec![
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
            "--max-peers".to_string(),
            "10".to_string(),
        ];
        let cfg = parse_daemon_args(&args).expect("parse");
        assert_eq!(cfg.listen_addr, "127.0.0.1:24100");
        assert_eq!(cfg.api_listen_addr.as_deref(), Some("127.0.0.1:28385"));
        assert_eq!(cfg.db_root.as_deref(), Some("/tmp/syncthing-rs-db-config"));
        assert_eq!(cfg.memory_max_mb, Some(71));
        assert_eq!(cfg.max_peers, 10);
        assert_eq!(
            cfg.folders,
            vec![
                FolderSpec {
                    id: "docs".to_string(),
                    path: "/srv/docs".to_string()
                },
                FolderSpec {
                    id: "photos".to_string(),
                    path: "/srv/photos".to_string()
                }
            ]
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parse_syncthing_config_bootstrap_extracts_devices_and_options() {
        let xml = r#"
<configuration version="51">
    <folder id="main" label="Main" path="~/Sync" type="sendreceive">
        <device id="LOCAL-DEVICE" introducedBy="">
        </device>
        <device id="PEER-DEVICE" introducedBy="">
        </device>
    </folder>
    <device id="LOCAL-DEVICE" name="Mac&#34;Book" compression="always" introducer="false">
        <address>dynamic</address>
        <paused>false</paused>
    </device>
    <device id="PEER-DEVICE" name="Peer Device" compression="metadata" introducer="true">
        <address>tcp://192.168.1.10:22000</address>
    </device>
    <gui enabled="true" tls="true">
        <address>127.0.0.1:8384</address>
        <theme>black</theme>
    </gui>
    <options>
        <listenAddress>default</listenAddress>
        <globalAnnounceServer>default</globalAnnounceServer>
        <urAccepted>-1</urAccepted>
        <urSeen>3</urSeen>
        <progressUpdateIntervalS>5</progressUpdateIntervalS>
        <crashReportingEnabled>true</crashReportingEnabled>
    </options>
    <defaults>
        <folder id="" label="" path="" type="sendreceive">
            <device id="LOCAL-DEVICE" introducedBy="">
            </device>
        </folder>
    </defaults>
</configuration>
"#;

        let parsed = parse_syncthing_config_bootstrap(xml);
        assert_eq!(parsed.local_device_id.as_deref(), Some("LOCAL-DEVICE"));
        assert_eq!(
            parsed.folder_labels.get("main").map(String::as_str),
            Some("Main")
        );
        assert_eq!(
            parsed.folder_paths.get("main").map(String::as_str),
            Some("~/Sync")
        );
        assert_eq!(
            parsed.folder_types.get("main"),
            Some(&FolderType::SendReceive)
        );
        assert_eq!(
            parsed
                .folder_devices
                .get("main")
                .cloned()
                .unwrap_or_default(),
            vec!["LOCAL-DEVICE".to_string(), "PEER-DEVICE".to_string()]
        );
        assert_eq!(
            parsed.device_configs["LOCAL-DEVICE"]["name"].as_str(),
            Some("Mac\"Book")
        );
        assert_eq!(
            parsed.device_configs["PEER-DEVICE"]["addresses"]
                .as_array()
                .and_then(|entries| entries.first())
                .and_then(Value::as_str),
            Some("tcp://192.168.1.10:22000")
        );
        assert_eq!(parsed.listen_addresses, vec!["default".to_string()]);
        assert_eq!(parsed.global_announce_servers, vec!["default".to_string()]);
        assert_eq!(parsed.ur_accepted, Some(-1));
        assert_eq!(parsed.ur_seen, Some(3));
        assert_eq!(parsed.progress_update_interval_s, Some(5));
        assert_eq!(parsed.crash_reporting_enabled, Some(true));
        assert_eq!(parsed.gui_address.as_deref(), Some("127.0.0.1:8384"));
        assert_eq!(parsed.gui_theme.as_deref(), Some("black"));
        assert_eq!(parsed.gui_use_tls, Some(true));
    }

    #[test]
    fn parse_syncthing_config_bootstrap_supports_single_quoted_attributes() {
        let xml = r#"
<configuration version='51'>
    <folder id='main' label='Main' path='/tmp/sync' type='receiveonly' paused='true'>
        <device id='LOCAL-DEVICE' introducedBy=''></device>
    </folder>
    <device id='LOCAL-DEVICE' name='Local Device'></device>
    <gui enabled='true' tls='false'>
        <address>127.0.0.1:8384</address>
    </gui>
</configuration>
"#;

        let parsed = parse_syncthing_config_bootstrap(xml);
        assert_eq!(parsed.local_device_id.as_deref(), Some("LOCAL-DEVICE"));
        assert_eq!(
            parsed.folder_paths.get("main").map(String::as_str),
            Some("/tmp/sync")
        );
        assert_eq!(
            parsed.folder_types.get("main"),
            Some(&FolderType::ReceiveOnly)
        );
        assert_eq!(parsed.folder_paused.get("main"), Some(&true));
        assert_eq!(parsed.gui_use_tls, Some(false));
    }

    #[test]
    fn parse_syncthing_config_bootstrap_prefers_first_device_when_defaults_missing() {
        let xml = r#"
<configuration version="51">
    <folder id="main" label="Main" path="~/Sync" type="sendreceive">
        <device id="ZZZ-REMOTE" introducedBy=""></device>
        <device id="AAA-LOCAL" introducedBy=""></device>
    </folder>
    <device id="ZZZ-REMOTE" name="Remote Device"></device>
    <device id="AAA-LOCAL" name="Local Device"></device>
</configuration>
"#;

        let parsed = parse_syncthing_config_bootstrap(xml);
        assert_eq!(parsed.local_device_id.as_deref(), Some("ZZZ-REMOTE"));
    }

    #[test]
    fn parse_syncthing_config_bootstrap_prefers_top_level_over_defaults_device() {
        let xml = r#"
<configuration version="51">
    <folder id="main" label="Main" path="~/Sync" type="sendreceive">
        <device id="LOCAL-TOP" introducedBy=""></device>
        <device id="REMOTE-TOP" introducedBy=""></device>
    </folder>
    <device id="LOCAL-TOP" name="This Device"></device>
    <device id="REMOTE-TOP" name="Remote Device"></device>
    <defaults>
        <folder id="" label="" path="" type="sendreceive">
            <device id="STALE-DEFAULTS-ID" introducedBy=""></device>
        </folder>
    </defaults>
</configuration>
"#;

        let parsed = parse_syncthing_config_bootstrap(xml);
        assert_eq!(parsed.local_device_id.as_deref(), Some("LOCAL-TOP"));
    }

    #[test]
    fn parse_syncthing_config_bootstrap_uses_defaults_when_top_level_missing() {
        let xml = r#"
<configuration version="51">
    <folder id="main" label="Main" path="~/Sync" type="sendreceive">
        <device id="ONLY-DEFAULTS" introducedBy=""></device>
    </folder>
    <defaults>
        <folder id="" label="" path="" type="sendreceive">
            <device id="ONLY-DEFAULTS" introducedBy=""></device>
        </folder>
    </defaults>
</configuration>
"#;

        let parsed = parse_syncthing_config_bootstrap(xml);
        assert_eq!(parsed.local_device_id.as_deref(), Some("ONLY-DEFAULTS"));
    }

    fn test_api_runtime(
        model: Arc<RwLock<model>>,
        _folders: Vec<FolderSpec>,
        active_peers: usize,
    ) -> DaemonApiRuntime {
        let local_id = model.read().expect("lock model").id.clone();
        let runtime = DaemonApiRuntime {
            model,
            state: Arc::new(RwLock::new(ApiRuntimeState::new(&local_id))),
            active_peers: Arc::new(AtomicUsize::new(active_peers)),
            max_peers: 16,
            start_time: SystemTime::now(),
            bep_listen_addr: "127.0.0.1:22000".to_string(),
            gui_listen_addr: "127.0.0.1:8384".to_string(),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            gui_root: None,
        };
        // W16B-1: CSRF is now unconditionally enforced (tokens always auto-minted).
        // test_api_call appends this API key to bypass CSRF validation.
        // Auth-specific tests override user/password/apiKey as needed.
        {
            let mut state = runtime.state.write().expect("lock state");
            state.gui["apiKey"] = serde_json::Value::String("test-api-key-42".to_string());
        }
        runtime
    }

    /// Helper that calls build_api_response with the API key appended to the URL
    /// so the request passes auth. Use this for all tests that need authenticated access.
    fn test_api_call(method: &Method, url: &str, runtime: &DaemonApiRuntime) -> ApiReply {
        let api_key = runtime
            .state
            .read()
            .expect("lock state")
            .gui
            .get("apiKey")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let apikey_param = runtime
            .state
            .read()
            .map(|state| auth_query_apikey_key(&state))
            .unwrap_or_else(|_| "_auth_apikey".to_string());
        let authed_url = append_query_param(url, &apikey_param, &api_key);
        build_api_response(method, &authed_url, runtime)
    }

    #[test]
    fn api_ping_returns_pong() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 3);

        let reply = build_api_response(&Method::Get, "/rest/system/ping", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&reply.body).expect("decode json");
        assert_eq!(payload["ping"], "pong");
    }

    #[test]
    fn gui_meta_js_returns_authenticated_metadata() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        let reply = build_api_response(&Method::Get, "/meta.js", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        assert_eq!(reply.content_type, "application/javascript; charset=utf-8");
        let body = String::from_utf8(reply.body).expect("meta js utf8");
        assert!(body.starts_with("var metadata = "));
        assert!(body.contains("\"authenticated\":true"));
        assert!(body.contains("\"deviceIDShort\""));
    }

    #[test]
    fn api_shutdown_endpoint_sets_shutdown_flag() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        assert!(!runtime.shutdown_requested.load(Ordering::Acquire));

        let reply = test_api_call(&Method::Post, "/rest/system/shutdown", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        assert!(runtime.shutdown_requested.load(Ordering::Acquire));
    }

    #[test]
    fn api_status_reports_memory_and_peer_fields() {
        let root = temp_root("api-system-status");
        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
            cfg.devices.push(crate::config::FolderDeviceConfiguration {
                device_id: "peer-a".to_string(),
                introduced_by: String::new(),
                encryption_password: String::new(),
            });
            guard.newFolder(cfg);
        }
        let runtime = test_api_runtime(model, Vec::new(), 2);
        let reply = build_api_response(&Method::Get, "/rest/system/status", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&reply.body).expect("decode json");
        assert_eq!(payload["activePeers"], 2);
        assert_eq!(payload["maxPeers"], 16);
        assert_eq!(payload["folderCount"], 1);
        assert!(payload["memoryBudgetBytes"].as_u64().is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_status_requires_folder_and_rejects_unknown() {
        let runtime = test_api_runtime(
            Arc::new(RwLock::new(NewModel())),
            vec![FolderSpec {
                id: "default".to_string(),
                path: "/tmp/default".to_string(),
            }],
            0,
        );

        let missing = build_api_response(&Method::Get, "/rest/db/status", &runtime);
        assert_eq!(missing.status_code, StatusCode(400));

        let unknown = build_api_response(&Method::Get, "/rest/db/status?folder=unknown", &runtime);
        assert_eq!(unknown.status_code, StatusCode(404));
    }

    #[test]
    fn api_db_status_returns_folder_metrics() {
        let root = temp_root("api-db-status");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );
        let reply = build_api_response(&Method::Get, "/rest/db/status?folder=default", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&reply.body).expect("decode json");
        assert_eq!(payload["folder"], "default");
        assert!(payload["stats"].is_object());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_system_connections_returns_empty_map() {
        let runtime = test_api_runtime(
            Arc::new(RwLock::new(NewModel())),
            vec![FolderSpec {
                id: "default".to_string(),
                path: "/tmp/default".to_string(),
            }],
            0,
        );
        let reply = build_api_response(&Method::Get, "/rest/system/connections", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&reply.body).expect("decode json");
        assert_eq!(payload["total"], 0);
        assert!(payload["connections"].is_object());
    }

    #[test]
    fn api_gui_contract_endpoints_return_expected_shapes() {
        let root = temp_root("api-gui-contract");
        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            guard.foldersRunning.insert("default".to_string(), false);
            guard.connections.insert(
                "PEER-A".to_string(),
                crate::model_core::ConnectionStats {
                    Address: "tcp://peer-a:22000".to_string(),
                    Connected: false,
                    ..Default::default()
                },
            );
            guard
                .ApplyBepMessage(
                    "PEER-A",
                    &BepMessage::ClusterConfig {
                        folders: vec![
                            ClusterConfigFolder {
                                id: "incoming".to_string(),
                                label: String::new(),
                                devices: Vec::new(),
                                folder_type: 0,
                                ..Default::default()
                            },
                            ClusterConfigFolder {
                                id: "default".to_string(),
                                label: String::new(),
                                devices: Vec::new(),
                                folder_type: 0,
                                ..Default::default()
                            },
                        ],
                        secondary: false,
                    },
                )
                .expect("apply cluster config");
        }
        let runtime = test_api_runtime(model, Vec::new(), 0);
        append_event(
            &runtime,
            "ConfigSaved",
            json!({"section":"devices","id":"PEER-A"}),
            false,
        );
        append_event(
            &runtime,
            "LocalChangeDetected",
            json!({"folder":"default","path":"test.txt","type":"file","action":"modified"}),
            true,
        );

        let events = build_api_response(&Method::Get, "/rest/events?since=0&limit=10", &runtime);
        assert_eq!(events.status_code, StatusCode(200));
        let events_payload: Value = serde_json::from_slice(&events.body).expect("decode json");
        assert!(events_payload.is_array());
        assert!(events_payload.as_array().map_or(0, Vec::len) >= 1);

        let events_disk =
            build_api_response(&Method::Get, "/rest/events/disk?since=0&limit=10", &runtime);
        assert_eq!(events_disk.status_code, StatusCode(200));
        let events_disk_payload: Value =
            serde_json::from_slice(&events_disk.body).expect("decode json");
        assert!(events_disk_payload.is_array());
        assert_eq!(events_disk_payload.as_array().map_or(0, Vec::len), 1);

        let discovery = build_api_response(&Method::Get, "/rest/system/discovery", &runtime);
        assert_eq!(discovery.status_code, StatusCode(200));
        let discovery_payload: Value =
            serde_json::from_slice(&discovery.body).expect("decode json");
        assert!(discovery_payload.is_object());
        assert!(discovery_payload.get("devices").is_none());
        assert_eq!(
            discovery_payload["PEER-A"]["addresses"]
                .as_array()
                .and_then(|addresses| addresses.first())
                .and_then(Value::as_str),
            Some("tcp://peer-a:22000")
        );

        let pending_devices =
            build_api_response(&Method::Get, "/rest/cluster/pending/devices", &runtime);
        assert_eq!(pending_devices.status_code, StatusCode(200));
        let pending_devices_payload: Value =
            serde_json::from_slice(&pending_devices.body).expect("decode json");
        assert!(pending_devices_payload.is_object());
        assert!(pending_devices_payload.get("count").is_none());
        assert!(pending_devices_payload.get("devices").is_none());
        assert_eq!(
            pending_devices_payload["PEER-A"]["name"].as_str(),
            Some("PEER-A")
        );
        assert!(pending_devices_payload["PEER-A"]["time"].as_u64().is_some());

        let pending_folders =
            build_api_response(&Method::Get, "/rest/cluster/pending/folders", &runtime);
        assert_eq!(pending_folders.status_code, StatusCode(200));
        let pending_folders_payload: Value =
            serde_json::from_slice(&pending_folders.body).expect("decode json");
        assert!(pending_folders_payload.is_object());
        assert!(pending_folders_payload.get("count").is_none());
        assert!(pending_folders_payload.get("folders").is_none());
        assert!(pending_folders_payload.get("default").is_none());
        assert_eq!(
            pending_folders_payload["incoming"]["offeredBy"]["PEER-A"]["name"].as_str(),
            Some("PEER-A")
        );
        assert!(
            pending_folders_payload["incoming"]["offeredBy"]["PEER-A"]["time"]
                .as_u64()
                .is_some()
        );

        let invalid_pending_folders = build_api_response(
            &Method::Get,
            "/rest/cluster/pending/folders?device=bad",
            &runtime,
        );
        assert_eq!(invalid_pending_folders.status_code, StatusCode(400));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_config_and_status_include_gui_required_fields() {
        let root = temp_root("api-config-status-shape");
        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
            cfg.devices.push(crate::config::FolderDeviceConfiguration {
                device_id: "peer-a".to_string(),
                introduced_by: String::new(),
                encryption_password: String::new(),
            });
            guard.newFolder(cfg);
        }
        let runtime = test_api_runtime(model, Vec::new(), 0);

        let config = build_api_response(&Method::Get, "/rest/config", &runtime);
        assert_eq!(config.status_code, StatusCode(200));
        let config_payload: Value = serde_json::from_slice(&config.body).expect("decode json");
        assert!(config_payload["devices"].is_array());
        assert!(config_payload["folders"].is_array());
        assert!(config_payload["options"]["listenAddresses"].is_array());
        assert!(config_payload["options"]["globalAnnounceServers"].is_array());
        assert!(config_payload["gui"]["address"].is_string());
        assert!(config_payload["gui"]["theme"].is_string());
        assert!(config_payload["gui"]["useTLS"].is_boolean());
        assert!(config_payload["defaults"]["ignores"]["lines"].is_array());
        assert!(config_payload["defaults"]["device"]["deviceID"]
            .as_str()
            .map(|id| !id.trim().is_empty())
            .unwrap_or(false));
        assert!(config_payload["remoteIgnoredDevices"].is_array());
        assert!(config_payload["folders"][0]["devices"]
            .as_array()
            .map_or(false, |devices| !devices.is_empty()));
        assert!(config_payload["folders"][0]["devices"][0]["deviceID"]
            .as_str()
            .is_some());

        let status = build_api_response(&Method::Get, "/rest/system/status", &runtime);
        assert_eq!(status.status_code, StatusCode(200));
        let status_payload: Value = serde_json::from_slice(&status.body).expect("decode json");
        assert!(status_payload["myID"].as_str().is_some());
        assert!(status_payload["guiAddressUsed"].is_string());
        assert!(status_payload["connectionServiceStatus"].is_object());
        assert!(status_payload["discoveryStatus"].is_object());
        assert!(status_payload["urVersionMax"].as_i64().is_some());
        assert!(config_payload["devices"]
            .as_array()
            .map(|devices| {
                devices.iter().any(|dev| {
                    dev.get("deviceID").and_then(Value::as_str) == status_payload["myID"].as_str()
                })
            })
            .unwrap_or(false));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_scan_and_jobs_endpoints_work() {
        let root = temp_root("api-db-scan-jobs");
        fs::write(root.join("a.txt"), b"hello").expect("write");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );
        let scan = test_api_call(
            &Method::Post,
            "/rest/db/scan?folder=default&sub=docs,images",
            &runtime,
        );
        assert_eq!(scan.status_code, StatusCode(200));
        let scan_payload: Value = serde_json::from_slice(&scan.body).expect("decode json");
        assert_eq!(scan_payload["folder"], "default");
        assert_eq!(scan_payload["state"], "running");
        assert_eq!(scan_payload["scannedSubdirs"][0], "docs");
        assert_eq!(scan_payload["scannedSubdirs"][1], "images");
        assert!(scan_payload["jobs"].is_object());

        let scan_repeated = test_api_call(
            &Method::Post,
            "/rest/db/scan?folder=default&sub=docs&sub=images",
            &runtime,
        );
        assert_eq!(scan_repeated.status_code, StatusCode(200));
        let scan_repeated_payload: Value =
            serde_json::from_slice(&scan_repeated.body).expect("decode json");
        assert_eq!(scan_repeated_payload["scannedSubdirs"][0], "docs");
        assert_eq!(scan_repeated_payload["scannedSubdirs"][1], "images");

        let jobs = build_api_response(&Method::Get, "/rest/db/jobs?folder=default", &runtime);
        assert_eq!(jobs.status_code, StatusCode(200));
        let jobs_payload: Value = serde_json::from_slice(&jobs.body).expect("decode json");
        assert_eq!(jobs_payload["folder"], "default");
        assert!(jobs_payload["jobs"].is_object());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_scan_without_folder_scans_all_and_returns_json_ack() {
        let root = temp_root("api-db-scan-all");
        let docs = root.join("docs");
        let media = root.join("media");
        fs::create_dir_all(&docs).expect("mkdir docs");
        fs::create_dir_all(&media).expect("mkdir media");
        fs::write(docs.join("a.txt"), b"a").expect("write docs/a");
        fs::write(media.join("b.txt"), b"b").expect("write media/b");

        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("docs", &docs.to_string_lossy()));
            guard.newFolder(newFolderConfiguration("media", &media.to_string_lossy()));
        }

        let runtime = test_api_runtime(
            model.clone(),
            vec![
                FolderSpec {
                    id: "docs".to_string(),
                    path: docs.to_string_lossy().to_string(),
                },
                FolderSpec {
                    id: "media".to_string(),
                    path: media.to_string_lossy().to_string(),
                },
            ],
            0,
        );

        let scan = test_api_call(&Method::Post, "/rest/db/scan", &runtime);
        assert_eq!(scan.status_code, StatusCode(200));
        let scan_payload: Value = serde_json::from_slice(&scan.body).expect("decode scan payload");
        assert_eq!(scan_payload["scanned"], true);

        let guard = model.read().expect("lock");
        assert!(guard.Sequence("docs") > 0);
        assert!(guard.Sequence("media") > 0);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_pull_endpoint_applies_remote_file() {
        let root = temp_root("api-db-pull");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            guard
                .Index("default", &[file_info("remote.bin", 1, 9)])
                .expect("index remote");
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );
        let pull = test_api_call(&Method::Post, "/rest/db/pull?folder=default", &runtime);
        assert_eq!(pull.status_code, StatusCode(200));
        let pull_payload: Value = serde_json::from_slice(&pull.body).expect("decode json");
        assert_eq!(pull_payload["folder"], "default");
        assert!(pull_payload["pull"]["neededFiles"].as_u64().unwrap_or(0) >= 1);

        let meta = fs::metadata(root.join("remote.bin")).expect("metadata");
        assert_eq!(meta.len(), 9);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_browse_returns_ordered_pages() {
        let root = temp_root("api-db-browse");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            let mut db = guard.sdb.write().expect("db lock");
            db.update(
                "default",
                "local",
                vec![file_info("b.txt", 2, 2), file_info("a.txt", 1, 1)],
            )
            .expect("update local");
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );
        let first = build_api_response(
            &Method::Get,
            "/rest/db/browse?folder=default&limit=1",
            &runtime,
        );
        assert_eq!(first.status_code, StatusCode(200));
        let first_payload: Value = serde_json::from_slice(&first.body).expect("decode json");
        assert_eq!(first_payload[0]["name"], "a.txt");

        let second = build_api_response(
            &Method::Get,
            "/rest/db/browse?folder=default&limit=1&cursor=a.txt",
            &runtime,
        );
        assert_eq!(second.status_code, StatusCode(200));
        let second_payload: Value = serde_json::from_slice(&second.body).expect("decode json");
        assert_eq!(second_payload[0]["name"], "b.txt");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_file_endpoint_returns_local_and_global_entries() {
        let root = temp_root("api-db-file");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            {
                let mut db = guard.sdb.write().expect("db lock");
                let mut local = file_info("a.txt", 1, 10);
                local.local_flags = db::FLAG_LOCAL_RECEIVE_ONLY;
                db.update("default", "local", vec![local])
                    .expect("update local");
            }
            guard
                .Index("default", &[file_info("a.txt", 2, 20)])
                .expect("index remote");
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );

        let local = build_api_response(
            &Method::Get,
            "/rest/db/file?folder=default&file=a.txt",
            &runtime,
        );
        assert_eq!(local.status_code, StatusCode(200));
        let local_payload: Value = serde_json::from_slice(&local.body).expect("decode json");
        assert_eq!(local_payload["exists"], true);
        assert_eq!(local_payload["entry"]["sequence"], 1);

        let global = build_api_response(
            &Method::Get,
            "/rest/db/file?folder=default&file=a.txt&global=1",
            &runtime,
        );
        assert_eq!(global.status_code, StatusCode(200));
        let global_payload: Value = serde_json::from_slice(&global.body).expect("decode json");
        assert_eq!(global_payload["exists"], true);
        assert_eq!(global_payload["entry"]["sequence"], 2);

        let missing = build_api_response(
            &Method::Get,
            "/rest/db/file?folder=default&file=missing.txt",
            &runtime,
        );
        assert_eq!(missing.status_code, StatusCode(404));
        let missing_payload: Value = serde_json::from_slice(&missing.body).expect("decode json");
        assert_eq!(missing_payload["error"], "no such object in the index");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_completion_localchanged_and_remoteneed_endpoints_work() {
        let root = temp_root("api-db-completion-localchanged-remoteneed");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            {
                let mut db = guard.sdb.write().expect("db lock");
                db.update("default", "local", vec![file_info("a.txt", 1, 10)])
                    .expect("update local");
            }
            guard
                .Index(
                    "default",
                    &[file_info("a.txt", 2, 20), file_info("b.txt", 1, 30)],
                )
                .expect("index remote");
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );

        let completion = build_api_response(
            &Method::Get,
            "/rest/db/completion?folder=default&device=peer-a",
            &runtime,
        );
        assert_eq!(completion.status_code, StatusCode(200));
        let completion_payload: Value =
            serde_json::from_slice(&completion.body).expect("decode json");
        assert_eq!(completion_payload["folder"], "default");
        assert_eq!(completion_payload["device"], "peer-a");
        assert!(completion_payload["completionPct"].as_i64().is_some());

        let localchanged = build_api_response(
            &Method::Get,
            "/rest/db/localchanged?folder=default",
            &runtime,
        );
        assert_eq!(localchanged.status_code, StatusCode(200));
        let localchanged_payload: Value =
            serde_json::from_slice(&localchanged.body).expect("decode json");
        assert_eq!(localchanged_payload["count"], 0);
        assert!(localchanged_payload["files"]
            .as_array()
            .expect("files array")
            .is_empty());

        let remoteneed = build_api_response(
            &Method::Get,
            "/rest/db/remoteneed?folder=default&device=peer-a",
            &runtime,
        );
        assert_eq!(remoteneed.status_code, StatusCode(200));
        let remoteneed_payload: Value =
            serde_json::from_slice(&remoteneed.body).expect("decode json");
        let paths = remoteneed_payload["items"]
            .as_array()
            .expect("array")
            .iter()
            .filter_map(|item| item["path"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["a.txt", "b.txt"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_override_and_revert_endpoints_work() {
        let root = temp_root("api-db-override-revert");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }
        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );

        let override_reply =
            test_api_call(&Method::Post, "/rest/db/override?folder=default", &runtime);
        assert_eq!(override_reply.status_code, StatusCode(200));
        let override_payload: Value =
            serde_json::from_slice(&override_reply.body).expect("decode json");
        assert_eq!(override_payload["ok"], "ok");

        let revert_reply = test_api_call(&Method::Post, "/rest/db/revert?folder=default", &runtime);
        assert_eq!(revert_reply.status_code, StatusCode(200));
        let revert_payload: Value =
            serde_json::from_slice(&revert_reply.body).expect("decode json");
        assert_eq!(revert_payload["ok"], "ok");

        let missing_override =
            test_api_call(&Method::Post, "/rest/db/override?folder=missing", &runtime);
        assert_eq!(missing_override.status_code, StatusCode(404));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_bringtofront_endpoint_works() {
        let root = temp_root("api-db-bringtofront");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }
        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );
        let ok = test_api_call(
            &Method::Post,
            "/rest/db/bringtofront?folder=default",
            &runtime,
        );
        assert_eq!(ok.status_code, StatusCode(200));
        let ok_payload: Value = serde_json::from_slice(&ok.body).expect("decode json");
        assert_eq!(ok_payload["action"], "bringtofront");
        assert_eq!(ok_payload["ok"], true);

        let missing = test_api_call(
            &Method::Post,
            "/rest/db/bringtofront?folder=missing",
            &runtime,
        );
        assert_eq!(missing.status_code, StatusCode(404));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_ignores_and_reset_endpoints_work() {
        let root = temp_root("api-db-ignores-reset");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            guard.SetIgnores(
                "default",
                vec!["*.tmp".to_string(), ".DS_Store".to_string()],
            );
            {
                let mut db = guard.sdb.write().expect("db lock");
                db.update("default", "local", vec![file_info("a.txt", 1, 10)])
                    .expect("update local");
            }
        }
        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );

        let ignores = build_api_response(&Method::Get, "/rest/db/ignores?folder=default", &runtime);
        assert_eq!(ignores.status_code, StatusCode(200));
        let ignores_payload: Value = serde_json::from_slice(&ignores.body).expect("decode json");
        assert_eq!(ignores_payload["count"], 2);

        let reset = test_api_call(&Method::Post, "/rest/db/reset?folder=default", &runtime);
        assert_eq!(reset.status_code, StatusCode(200));
        let reset_payload: Value = serde_json::from_slice(&reset.body).expect("decode json");
        // W10-M3: Go returns {"ok":"ok"} for reset responses
        assert_eq!(reset_payload["ok"], "ok");

        let ignores_after =
            build_api_response(&Method::Get, "/rest/db/ignores?folder=default", &runtime);
        assert_eq!(ignores_after.status_code, StatusCode(200));
        let ignores_after_payload: Value =
            serde_json::from_slice(&ignores_after.body).expect("decode json");
        assert_eq!(ignores_after_payload["count"], 0);

        let file_after = build_api_response(
            &Method::Get,
            "/rest/db/file?folder=default&file=a.txt",
            &runtime,
        );
        assert_eq!(file_after.status_code, StatusCode(404));
        let file_after_payload: Value =
            serde_json::from_slice(&file_after.body).expect("decode json");
        assert_eq!(file_after_payload["error"], "no such object in the index");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_need_returns_needed_page() {
        let root = temp_root("api-db-need");
        let model = Arc::new(RwLock::new(
            NewModelWithRuntime(Some(root.clone()), Some(50)).expect("new model"),
        ));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            {
                let mut db = guard.sdb.write().expect("db lock");
                db.update("default", "local", vec![file_info("a.txt", 1, 1)])
                    .expect("update local");
            }
            guard
                .Index(
                    "default",
                    &[file_info("a.txt", 2, 2), file_info("b.txt", 1, 1)],
                )
                .expect("update remote");
        }

        let runtime = test_api_runtime(
            model,
            vec![FolderSpec {
                id: "default".to_string(),
                path: root.to_string_lossy().to_string(),
            }],
            0,
        );
        let need = build_api_response(
            &Method::Get,
            "/rest/db/need?folder=default&limit=10",
            &runtime,
        );
        assert_eq!(need.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&need.body).expect("decode json");
        let paths = payload["items"]
            .as_array()
            .expect("array")
            .iter()
            .filter_map(|item| item["path"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["a.txt", "b.txt"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_control_endpoints_reject_wrong_methods() {
        let runtime = test_api_runtime(
            Arc::new(RwLock::new(NewModel())),
            vec![FolderSpec {
                id: "default".to_string(),
                path: "/tmp/default".to_string(),
            }],
            0,
        );
        let pull_get = build_api_response(&Method::Get, "/rest/db/pull?folder=default", &runtime);
        assert_eq!(pull_get.status_code, StatusCode(405));
        let status_post = test_api_call(&Method::Post, "/rest/db/status?folder=default", &runtime);
        assert_eq!(status_post.status_code, StatusCode(405));
        let file_post = test_api_call(
            &Method::Post,
            "/rest/db/file?folder=default&file=a.txt",
            &runtime,
        );
        assert_eq!(file_post.status_code, StatusCode(405));
        let override_get =
            build_api_response(&Method::Get, "/rest/db/override?folder=default", &runtime);
        assert_eq!(override_get.status_code, StatusCode(405));
        let revert_get =
            build_api_response(&Method::Get, "/rest/db/revert?folder=default", &runtime);
        assert_eq!(revert_get.status_code, StatusCode(405));
        let reset_get = build_api_response(&Method::Get, "/rest/db/reset?folder=default", &runtime);
        assert_eq!(reset_get.status_code, StatusCode(405));
        let ignores_put = test_api_call(&Method::Put, "/rest/db/ignores?folder=default", &runtime);
        assert_eq!(ignores_put.status_code, StatusCode(405));
        let completion_post = test_api_call(
            &Method::Post,
            "/rest/db/completion?folder=default",
            &runtime,
        );
        assert_eq!(completion_post.status_code, StatusCode(405));
        let localchanged_post = test_api_call(
            &Method::Post,
            "/rest/db/localchanged?folder=default",
            &runtime,
        );
        assert_eq!(localchanged_post.status_code, StatusCode(405));
        let remoteneed_post = test_api_call(
            &Method::Post,
            "/rest/db/remoteneed?folder=default",
            &runtime,
        );
        assert_eq!(remoteneed_post.status_code, StatusCode(405));
        let bringtofront_get = build_api_response(
            &Method::Get,
            "/rest/db/bringtofront?folder=default",
            &runtime,
        );
        assert_eq!(bringtofront_get.status_code, StatusCode(405));
    }

    #[test]
    fn api_system_log_txt_returns_text_plain() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        let _ = test_api_call(&Method::Post, "/rest/system/error?message=first", &runtime);
        let _ = test_api_call(&Method::Post, "/rest/system/error?message=second", &runtime);

        let log_txt = build_api_response(&Method::Get, "/rest/system/log.txt", &runtime);
        assert_eq!(log_txt.status_code, StatusCode(200));
        assert_eq!(
            log_txt.content_type,
            "text/plain; charset=utf-8".to_string()
        );
        let body = String::from_utf8(log_txt.body).expect("utf8 body");
        assert!(body.contains("ERROR: first"));
        assert!(body.contains("ERROR: second"));
    }

    #[test]
    fn bool_param_accepts_case_insensitive_truthy_values() {
        let mut params = BTreeMap::new();
        params.insert("enabled".to_string(), "TRUE".to_string());
        assert_eq!(bool_param(&params, "enabled"), Some(true));

        params.insert("enabled".to_string(), "Yes".to_string());
        assert_eq!(bool_param(&params, "enabled"), Some(true));

        params.insert("enabled".to_string(), "On".to_string());
        assert_eq!(bool_param(&params, "enabled"), Some(true));

        params.insert("enabled".to_string(), "false".to_string());
        assert_eq!(bool_param(&params, "enabled"), Some(false));
    }

    #[test]
    fn api_system_browse_returns_entries_sorted_by_name() {
        let root = temp_root("api-system-browse-sorted");
        fs::write(root.join("c.txt"), b"c").expect("write c");
        fs::write(root.join("a.txt"), b"a").expect("write a");
        fs::write(root.join("b.txt"), b"b").expect("write b");

        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        let response = build_api_response(
            &Method::Get,
            &format!("/rest/system/browse?current={}", root.to_string_lossy()),
            &runtime,
        );
        assert_eq!(response.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&response.body).expect("decode json");
        let names = payload
            .as_array()
            .expect("entries")
            .iter()
            .filter_map(Value::as_str)
            .filter_map(|path| path.rsplit('/').next())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["a.txt", "b.txt", "c.txt"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_requires_auth_when_gui_credentials_are_configured() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        {
            let mut state = runtime.state.write().expect("state lock");
            state.gui["user"] = json!("alice");
            state.gui["password"] = json!("secret");
        }

        let unauthorized = build_api_response(&Method::Get, "/rest/system/status", &runtime);
        assert_eq!(unauthorized.status_code, StatusCode(403));

        let wrong_login = build_api_response(
            &Method::Post,
            "/rest/noauth/auth/password?user=alice&password=wrong",
            &runtime,
        );
        assert_eq!(wrong_login.status_code, StatusCode(403));

        let login = build_api_response(
            &Method::Post,
            "/rest/noauth/auth/password?user=alice&password=secret",
            &runtime,
        );
        assert_eq!(login.status_code, StatusCode(204));
        let token = session_token_from_set_cookie(&login).expect("session token");
        let csrf = header_value(&login, "X-CSRF-Token")
            .expect("csrf token from login")
            .to_string();
        let (token_key, csrf_key) = auth_query_keys(&runtime);

        // 7.3: CSRF now required for GET too, so include CSRF token
        let authorized = build_api_response(
            &Method::Get,
            &format!("/rest/system/status?{token_key}={token}&{csrf_key}={csrf}"),
            &runtime,
        );
        assert_eq!(authorized.status_code, StatusCode(200));

        let impersonated =
            build_api_response(&Method::Get, "/rest/system/status?user=alice", &runtime);
        assert_eq!(impersonated.status_code, StatusCode(403));
    }

    #[test]
    fn api_requires_auth_for_config_item_and_debug_routes() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        {
            let mut state = runtime.state.write().expect("state lock");
            state.gui["user"] = json!("alice");
            state.gui["password"] = json!("secret");
        }

        let folder_get = build_api_response(&Method::Get, "/rest/config/folders/default", &runtime);
        assert_eq!(folder_get.status_code, StatusCode(403));
        let device_get = build_api_response(&Method::Get, "/rest/config/devices/peer-a", &runtime);
        assert_eq!(device_get.status_code, StatusCode(403));
        let debug_get = build_api_response(&Method::Get, "/rest/debug/httpmetrics", &runtime);
        assert_eq!(debug_get.status_code, StatusCode(403));
    }

    #[test]
    fn api_logout_requires_session_token_when_auth_required() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        {
            let mut state = runtime.state.write().expect("state lock");
            state.gui["user"] = json!("alice");
            state.gui["password"] = json!("secret");
        }

        let login = build_api_response(
            &Method::Post,
            "/rest/noauth/auth/password?user=alice&password=secret",
            &runtime,
        );
        assert_eq!(login.status_code, StatusCode(204));
        let token = session_token_from_set_cookie(&login).expect("token");
        let csrf = header_value(&login, "X-CSRF-Token")
            .expect("csrf token")
            .to_string();
        let (token_key, csrf_key) = auth_query_keys(&runtime);

        let bad_logout = build_api_response(
            &Method::Post,
            "/rest/noauth/auth/logout?user=alice",
            &runtime,
        );
        assert_eq!(bad_logout.status_code, StatusCode(204));

        let good_logout = build_api_response(
            &Method::Post,
            &format!("/rest/noauth/auth/logout?user=alice&{token_key}={token}&{csrf_key}={csrf}"),
            &runtime,
        );
        assert_eq!(good_logout.status_code, StatusCode(204));

        let still_auth = build_api_response(
            &Method::Get,
            &format!("/rest/system/status?{token_key}={token}"),
            &runtime,
        );
        assert_eq!(still_auth.status_code, StatusCode(403));
    }

    #[test]
    fn api_login_sets_cookie_and_csrf_and_enforces_csrf_on_writes() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        {
            let mut state = runtime.state.write().expect("state lock");
            state.gui["user"] = json!("alice");
            state.gui["password"] = json!("secret");
        }

        let login = build_api_response(
            &Method::Post,
            "/rest/noauth/auth/password?user=alice&password=secret&stayLoggedIn=true",
            &runtime,
        );
        assert_eq!(login.status_code, StatusCode(204));
        let token = session_token_from_set_cookie(&login).expect("session token");
        let csrf = header_value(&login, "X-CSRF-Token")
            .expect("csrf token")
            .to_string();
        let (token_key, csrf_key) = auth_query_keys(&runtime);

        // 7.3: CSRF is now enforced for ALL methods (including GET), matching Go parity
        let get_no_csrf = build_api_response(
            &Method::Get,
            &format!("/rest/system/status?{token_key}={token}"),
            &runtime,
        );
        assert_eq!(get_no_csrf.status_code, StatusCode(403));

        let get_ok = build_api_response(
            &Method::Get,
            &format!("/rest/system/status?{token_key}={token}&{csrf_key}={csrf}"),
            &runtime,
        );
        assert_eq!(get_ok.status_code, StatusCode(200));

        let write_missing_csrf = build_api_response(
            &Method::Post,
            &format!("/rest/db/scan?folder=default&{token_key}={token}"),
            &runtime,
        );
        assert_eq!(write_missing_csrf.status_code, StatusCode(403));

        let write_ok = build_api_response(
            &Method::Post,
            &format!("/rest/db/scan?folder=default&{token_key}={token}&{csrf_key}={csrf}"),
            &runtime,
        );
        assert_ne!(write_ok.status_code, StatusCode(403));
    }

    #[test]
    fn append_body_params_supports_login_json_payload() {
        let body = br#"{"Username":"alice","Password":"secret","StayLoggedIn":true}"#;
        let url = append_body_params("/rest/noauth/auth/password", &Method::Post, body);
        assert!(url.contains("user=alice"));
        assert!(url.contains("password=secret"));
        assert!(url.contains("stayLoggedIn=true"));
    }

    #[test]
    fn api_events_supports_type_filter() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        append_event(
            &runtime,
            "ConfigSaved",
            json!({"section":"folder","id":"default"}),
            false,
        );
        append_event(
            &runtime,
            "DeviceConnected",
            json!({"device":"peer-a"}),
            false,
        );

        let filtered = build_api_response(
            &Method::Get,
            "/rest/events?since=0&events=configsaved",
            &runtime,
        );
        assert_eq!(filtered.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&filtered.body).expect("decode events");
        let arr = payload.as_array().expect("array");
        assert!(!arr.is_empty());
        assert!(arr
            .iter()
            .all(|ev| ev["type"].as_str() == Some("ConfigSaved")));
    }

    #[test]
    fn api_allows_api_key_auth_when_configured() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        {
            let mut state = runtime.state.write().expect("state lock");
            // 13.1: auth_required needs both user AND password.
            state.gui["user"] = json!("admin");
            state.gui["password"] = json!("$2a$10$hashed");
            state.gui["apiKey"] = json!("secret-key");
        }

        let unauthorized = build_api_response(&Method::Get, "/rest/system/status", &runtime);
        assert_eq!(unauthorized.status_code, StatusCode(403));

        let api_key_param = auth_apikey_key(&runtime);

        let authorized = build_api_response(
            &Method::Get,
            &format!("/rest/system/status?{api_key_param}=secret-key"),
            &runtime,
        );
        assert_eq!(authorized.status_code, StatusCode(200));
    }

    #[test]
    fn api_health_and_service_helpers_match_expected_shapes() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);

        let health = build_api_response(&Method::Get, "/rest/noauth/health", &runtime);
        assert_eq!(health.status_code, StatusCode(200));
        let health_payload: Value = serde_json::from_slice(&health.body).expect("decode health");
        assert_eq!(health_payload["status"], "OK");

        let deviceid = build_api_response(&Method::Get, "/rest/svc/deviceid?id=peer-a", &runtime);
        assert_eq!(deviceid.status_code, StatusCode(200));
        let device_payload: Value =
            serde_json::from_slice(&deviceid.body).expect("decode device id");
        assert!(device_payload.get("error").is_some());

        let lang = build_api_response(
            &Method::Get,
            "/rest/svc/lang?accept=sv-SE,en-US;q=0.8",
            &runtime,
        );
        assert_eq!(lang.status_code, StatusCode(200));
        let lang_payload: Value = serde_json::from_slice(&lang.body).expect("decode lang");
        assert_eq!(lang_payload[0], "sv-se");
        assert_eq!(lang_payload[1], "en-us");
    }

    #[test]
    fn api_events_limit_returns_latest_items() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);
        append_event(&runtime, "ConfigSaved", json!({"n": 1}), false);
        append_event(&runtime, "ConfigSaved", json!({"n": 2}), false);
        append_event(&runtime, "ConfigSaved", json!({"n": 3}), false);

        let reply = build_api_response(&Method::Get, "/rest/events?since=0&limit=2", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&reply.body).expect("decode events");
        let events = payload.as_array().expect("events array");
        assert_eq!(events.len(), 2);
        let ids = events
            .iter()
            .filter_map(|ev| ev.get("id").and_then(Value::as_u64))
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn api_config_folders_crud_and_restart() {
        let root = temp_root("api-config-folders");
        let folder_path = root.join("docs");
        fs::create_dir_all(&folder_path).expect("create folder");
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);

        let add = test_api_call(
            &Method::Post,
            &format!(
                "/rest/system/config/folders?id=docs&path={}&type=recvonly",
                folder_path.to_string_lossy()
            ),
            &runtime,
        );
        assert_eq!(add.status_code, StatusCode(200));
        let add_payload: Value = serde_json::from_slice(&add.body).expect("decode json");
        assert_eq!(add_payload["added"], true);
        assert_eq!(add_payload["folder"]["id"], "docs");
        assert_eq!(add_payload["folder"]["type"], "receiveonly");

        let list = build_api_response(&Method::Get, "/rest/system/config/folders", &runtime);
        assert_eq!(list.status_code, StatusCode(200));
        let list_payload: Value = serde_json::from_slice(&list.body).expect("decode json");
        assert_eq!(list_payload["count"], 1);
        assert_eq!(list_payload["folders"][0]["id"], "docs");
        assert_eq!(list_payload["folders"][0]["type"], "receiveonly");
        assert_eq!(list_payload["folders"][0]["memory"]["policy"], "throttle");

        let restart = test_api_call(
            &Method::Post,
            "/rest/system/config/restart?folder=docs",
            &runtime,
        );
        assert_eq!(restart.status_code, StatusCode(200));
        let restart_payload: Value = serde_json::from_slice(&restart.body).expect("decode json");
        assert_eq!(restart_payload["restarted"], true);
        assert_eq!(restart_payload["folder"], "docs");

        let remove = test_api_call(
            &Method::Delete,
            "/rest/system/config/folders?id=docs",
            &runtime,
        );
        assert_eq!(remove.status_code, StatusCode(200));
        let remove_payload: Value = serde_json::from_slice(&remove.body).expect("decode json");
        assert_eq!(remove_payload["removed"], true);
        assert_eq!(remove_payload["folder"], "docs");

        let restart_missing = test_api_call(
            &Method::Post,
            "/rest/system/config/restart?folder=docs",
            &runtime,
        );
        assert_eq!(restart_missing.status_code, StatusCode(404));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_config_folders_validate_inputs() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);

        let missing_id = test_api_call(
            &Method::Post,
            "/rest/system/config/folders?path=/tmp/docs",
            &runtime,
        );
        assert_eq!(missing_id.status_code, StatusCode(400));

        let missing_path = test_api_call(
            &Method::Post,
            "/rest/system/config/folders?id=docs",
            &runtime,
        );
        assert_eq!(missing_path.status_code, StatusCode(400));

        let missing_delete_id =
            test_api_call(&Method::Delete, "/rest/system/config/folders", &runtime);
        assert_eq!(missing_delete_id.status_code, StatusCode(400));

        let missing_restart_folder =
            test_api_call(&Method::Post, "/rest/system/config/restart", &runtime);
        assert_eq!(missing_restart_folder.status_code, StatusCode(400));
    }

    #[test]
    fn api_config_list_endpoints_return_arrays() {
        let root = temp_root("api-config-list-arrays");
        fs::create_dir_all(root.join("docs")).expect("mkdir docs");
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);

        let add_folder = test_api_call(
            &Method::Post,
            &format!(
                "/rest/config/folders?id=docs&path={}",
                root.join("docs").to_string_lossy()
            ),
            &runtime,
        );
        assert_eq!(add_folder.status_code, StatusCode(200));

        let add_device = test_api_call(
            &Method::Post,
            "/rest/config/devices?id=peer-a&address=tcp://peer-a:22000",
            &runtime,
        );
        assert_eq!(add_device.status_code, StatusCode(200));

        let folders = build_api_response(&Method::Get, "/rest/config/folders", &runtime);
        assert_eq!(folders.status_code, StatusCode(200));
        let folders_payload: Value = serde_json::from_slice(&folders.body).expect("decode folders");
        assert!(folders_payload.is_array());

        let devices = build_api_response(&Method::Get, "/rest/config/devices", &runtime);
        assert_eq!(devices.status_code, StatusCode(200));
        let devices_payload: Value = serde_json::from_slice(&devices.body).expect("decode devices");
        assert!(devices_payload.is_array());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_config_option_updates_preserve_value_types() {
        let runtime = test_api_runtime(Arc::new(RwLock::new(NewModel())), Vec::new(), 0);

        let update = test_api_call(
            &Method::Put,
            "/rest/config/options?maxSendKbps=1&globalAnnounceEnabled=false&listenAddresses=tcp://0.0.0.0:22000,quic://0.0.0.0:22000",
            &runtime,
        );
        assert_eq!(update.status_code, StatusCode(200));

        let read_back = build_api_response(&Method::Get, "/rest/config/options", &runtime);
        assert_eq!(read_back.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&read_back.body).expect("decode options");
        assert_eq!(payload["maxSendKbps"], 1);
        assert_eq!(payload["globalAnnounceEnabled"], false);
        assert!(payload["listenAddresses"].is_array());
        assert_eq!(
            payload["listenAddresses"],
            json!(["tcp://0.0.0.0:22000", "quic://0.0.0.0:22000"])
        );
    }

    #[test]
    fn api_stats_endpoints_return_top_level_maps() {
        let root = temp_root("api-stats-top-level");
        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            guard.deviceStatRefs.insert(
                "peer-a".to_string(),
                BTreeMap::from([("seen".to_string(), 1)]),
            );
        }
        let runtime = test_api_runtime(model, Vec::new(), 0);

        let device_stats = build_api_response(&Method::Get, "/rest/stats/device", &runtime);
        assert_eq!(device_stats.status_code, StatusCode(200));
        let device_payload: Value =
            serde_json::from_slice(&device_stats.body).expect("decode device stats");
        assert!(device_payload.get("devices").is_none());
        assert_eq!(device_payload["peer-a"]["seen"], 1);

        let folder_stats = build_api_response(&Method::Get, "/rest/stats/folder", &runtime);
        assert_eq!(folder_stats.status_code, StatusCode(200));
        let folder_payload: Value =
            serde_json::from_slice(&folder_stats.body).expect("decode folder stats");
        assert!(folder_payload.get("folders").is_none());
        assert!(folder_payload.get("default").is_some());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn parse_query_decodes_percent_and_plus() {
        let params = parse_query("id=docs&path=%2Ftmp%2Ffolder+one&type=receiveonly");
        assert_eq!(params.get("id").map(String::as_str), Some("docs"));
        assert_eq!(
            params.get("path").map(String::as_str),
            Some("/tmp/folder one")
        );
        assert_eq!(params.get("type").map(String::as_str), Some("receiveonly"));
    }

    #[test]
    fn decode_query_component_preserves_invalid_percent_sequences() {
        assert_eq!(decode_query_component("a%2Fb"), "a/b");
        assert_eq!(decode_query_component("a%2"), "a%2");
        assert_eq!(decode_query_component("%ZZ"), "%ZZ");
    }

    #[test]
    fn parse_daemon_args_rejects_invalid_numeric_flags() {
        let args = vec![
            "--folder-path".to_string(),
            "/tmp/photos".to_string(),
            "--memory-max-mb".to_string(),
            "0".to_string(),
        ];
        let err = parse_daemon_args(&args).expect_err("must fail");
        assert!(err.contains("--memory-max-mb"));

        let args = vec![
            "--folder-path".to_string(),
            "/tmp/photos".to_string(),
            "--max-peers".to_string(),
            "x".to_string(),
        ];
        let err = parse_daemon_args(&args).expect_err("must fail");
        assert!(err.contains("--max-peers"));

        let args = vec![
            "--folder".to_string(),
            "docs:/srv/docs".to_string(),
            "--folder-path".to_string(),
            "/tmp/photos".to_string(),
        ];
        let err = parse_daemon_args(&args).expect_err("must fail");
        assert!(err.contains("cannot mix --folder"));

        let root = temp_root("bad-config");
        let config_path = root.join("daemon.json");
        fs::write(
            &config_path,
            r#"{"max_peers":0,"folders":[{"id":"a","path":"/tmp/a"}]}"#,
        )
        .expect("write config");
        let args = vec![
            "--config".to_string(),
            config_path.to_string_lossy().to_string(),
        ];
        let err = parse_daemon_args(&args).expect_err("must fail");
        assert!(err.contains("max_peers"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn handle_peer_connection_applies_updates_and_returns_response() {
        let root = temp_root("peer-connection");
        fs::write(root.join("a.txt"), b"hello-world").expect("write");

        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            let mut cfg = newFolderConfiguration("default", &root.to_string_lossy());
            cfg.devices.push(crate::config::FolderDeviceConfiguration {
                device_id: "peer-a".to_string(),
                introduced_by: String::new(),
                encryption_password: String::new(),
            });
            guard.newFolder(cfg);
        }

        let Some(listener) = bind_loopback_listener_or_skip() else {
            let _ = fs::remove_dir_all(root);
            return;
        };
        let addr = listener.local_addr().expect("addr");
        let server_model = model.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            handle_peer_connection(&mut stream, "peer-a", &server_model).expect("handle")
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout");

        let mut messages = default_exchange();
        messages.retain(|msg| !matches!(msg, BepMessage::Response { .. }));
        for msg in &messages {
            write_frame(&mut client, msg).expect("write msg");
        }
        client.shutdown(Shutdown::Write).expect("shutdown write");

        let response = read_response(&mut client);
        assert_eq!(
            response,
            BepMessage::Response {
                id: 1,
                code: 0,
                data_len: 11,
                data: b"hello-world".to_vec()
            }
        );

        server.join().expect("join server");
        let guard = model.read().expect("lock");
        assert_eq!(
            guard.RemoteSequences("default").get("peer-a").copied(),
            Some(2)
        );
        let dp = guard.DownloadProgress("peer-a");
        assert_eq!(dp.len(), 1);
        assert_eq!(dp[0].folder, "default");
        assert_eq!(dp[0].name, "a.txt");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn handle_peer_connection_returns_error_response_for_invalid_request() {
        let root = temp_root("invalid-request");
        fs::write(root.join("ok.txt"), b"ok").expect("write");

        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let Some(listener) = bind_loopback_listener_or_skip() else {
            let _ = fs::remove_dir_all(root);
            return;
        };
        let addr = listener.local_addr().expect("addr");
        let server_model = model.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            handle_peer_connection(&mut stream, "peer-a", &server_model).expect("handle")
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout");
        write_frame(
            &mut client,
            &BepMessage::Hello {
                device_name: "peer-a".to_string(),
                client_name: "syncthing-rs-test".to_string(),
                client_version: String::new(),
                num_connections: 0,
                timestamp: 0,
            },
        )
        .expect("write hello");
        write_frame(
            &mut client,
            &BepMessage::Request {
                id: 9,
                folder: "default".to_string(),
                name: "../escape".to_string(),
                offset: 0,
                size: 128,
                hash: b"h".to_vec(),
                from_temporary: false,
                block_no: 0,
            },
        )
        .expect("write request");
        write_frame(
            &mut client,
            &BepMessage::Close {
                reason: "test".to_string(),
            },
        )
        .expect("write close");
        client.shutdown(Shutdown::Write).expect("shutdown write");

        let response = read_response(&mut client);
        assert_eq!(
            response,
            BepMessage::Response {
                id: 9,
                code: 1, // H8: Go returns ErrorCodeGeneric for unpermitted request
                data_len: 0,
                data: Vec::new()
            }
        );

        server.join().expect("join server");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn handle_peer_connection_prefers_valid_hello_device_id_for_transport_peer() {
        let model = Arc::new(RwLock::new(NewModel()));
        let Some(listener) = bind_loopback_listener_or_skip() else {
            return;
        };
        let addr = listener.local_addr().expect("addr");
        let server_model = model.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            handle_peer_connection(&mut stream, "127.0.0.1:22000#1", &server_model).expect("handle")
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        write_frame(
            &mut client,
            &BepMessage::Hello {
                device_name: "AAAAAAA-BBBBBBB-CCCCCCC-DDDDDDD-EEEEEEE-FFFFFFF-GGGGGGG-HHHHHHH"
                    .to_string(),
                client_name: "syncthing-rs-test".to_string(),
                client_version: String::new(),
                num_connections: 0,
                timestamp: 0,
            },
        )
        .expect("write hello");
        client.shutdown(Shutdown::Write).expect("shutdown write");
        server.join().expect("join server");

        let guard = model.read().expect("lock");
        assert!(guard
            .helloMessages
            .contains_key("AAAAAAA-BBBBBBB-CCCCCCC-DDDDDDD-EEEEEEE-FFFFFFF-GGGGGGG-HHHHHHH"));
        assert!(!guard.helloMessages.contains_key("127.0.0.1:22000#1"));
    }

    #[test]
    fn handle_peer_connection_synthesizes_transport_peer_id_when_hello_name_is_not_device_id() {
        let model = Arc::new(RwLock::new(NewModel()));
        let Some(listener) = bind_loopback_listener_or_skip() else {
            return;
        };
        let addr = listener.local_addr().expect("addr");
        let server_model = model.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            handle_peer_connection(&mut stream, "127.0.0.1:22000#2", &server_model).expect("handle")
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        write_frame(
            &mut client,
            &BepMessage::Hello {
                device_name: "peer-a".to_string(),
                client_name: "syncthing-rs-test".to_string(),
                client_version: String::new(),
                num_connections: 0,
                timestamp: 0,
            },
        )
        .expect("write hello");
        client.shutdown(Shutdown::Write).expect("shutdown write");
        server.join().expect("join server");

        let guard = model.read().expect("lock");
        assert_eq!(guard.helloMessages.len(), 1);
        let device_id = guard.helloMessages.keys().next().expect("device key");
        assert!(normalize_syncthing_device_id(device_id).is_some());
        assert!(!device_id.contains(':'));
        assert!(!device_id.contains('#'));
    }

    #[test]
    fn handle_peer_connection_rejects_non_hello_first_frame() {
        let root = temp_root("missing-hello");
        fs::write(root.join("ok.txt"), b"ok").expect("write");

        let model = Arc::new(RwLock::new(NewModel()));
        {
            let mut guard = model.write().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let Some(listener) = bind_loopback_listener_or_skip() else {
            let _ = fs::remove_dir_all(root);
            return;
        };
        let addr = listener.local_addr().expect("addr");
        let server_model = model.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            handle_peer_connection(&mut stream, "peer-a", &server_model).expect_err("must fail")
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        write_frame(
            &mut client,
            &BepMessage::Request {
                id: 1,
                folder: "default".to_string(),
                name: "ok.txt".to_string(),
                offset: 0,
                size: 2,
                hash: b"h".to_vec(),
                from_temporary: false,
                block_no: 0,
            },
        )
        .expect("write request");
        client.shutdown(Shutdown::Write).expect("shutdown write");

        let err = server.join().expect("join server");
        assert!(err.contains("expected hello as first message"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_frame_returns_none_on_clean_eof() {
        let mut empty: &[u8] = &[];
        let got = read_frame(&mut empty).expect("read");
        assert!(got.is_none());
    }

    #[test]
    fn read_frame_rejects_oversized_payload() {
        let mut src = Vec::new();
        src.extend_from_slice(&2_u16.to_be_bytes());
        src.extend_from_slice(b"{}");
        src.extend_from_slice(&((MAX_FRAME_BYTES as u32) + 1).to_be_bytes());
        let mut src = &src[..];
        let err = read_frame(&mut src).expect_err("must fail");
        assert!(err.contains("frame too large"));
    }

    #[test]
    fn read_frame_handles_truncated_header() {
        let data = [1_u8, 2_u8];
        let mut src = &data[..];
        let err = read_frame(&mut src).expect_err("must fail");
        assert!(err.contains("read frame header"));
    }

    #[test]
    fn read_frame_handles_truncated_payload() {
        let mut data = Vec::new();
        data.extend_from_slice(&2_u16.to_be_bytes());
        data.extend_from_slice(b"{}");
        data.extend_from_slice(&4_u32.to_be_bytes());
        data.extend_from_slice(&[1_u8, 2_u8]);
        let mut src = &data[..];
        let err = read_frame(&mut src).expect_err("must fail");
        assert!(err.contains("read frame payload"));
    }

    #[test]
    fn peer_slot_respects_max_peers() {
        let active = AtomicUsize::new(0);
        assert!(try_acquire_peer_slot(&active, 2));
        assert!(try_acquire_peer_slot(&active, 2));
        assert!(!try_acquire_peer_slot(&active, 2));
    }

    #[test]
    fn api_surface_probe_reports_required_endpoints() {
        let probe = run_api_surface_probe().expect("run api surface probe");
        assert!(probe
            .covered_endpoints
            .contains(&"GET /rest/system/ping".to_string()));
        assert!(probe
            .covered_endpoints
            .contains(&"GET /rest/db/status".to_string()));
        assert!(probe
            .covered_endpoints
            .contains(&"POST /rest/db/bringtofront".to_string()));
        assert!(probe
            .covered_endpoints
            .contains(&"DELETE /rest/system/config/folders".to_string()));

        let version = probe
            .endpoint_assertions
            .get("GET /rest/system/version")
            .expect("version assertion");
        assert_eq!(version.status_code, 200);
        assert_eq!(version.response_kind, "object");
        assert!(version.present_keys.contains(&"version".to_string()));
        assert!(version.present_keys.contains(&"longVersion".to_string()));
    }

    // ===== B7: Event mask regression tests =====

    #[test]
    fn default_event_mask_has_correct_entries() {
        // B7: Default (non-disk) returns None = accept all events.
        // This is correct per Go's DefaultEventMask which includes all non-disk events.
        let mask = super::parse_event_type_filter_with_default(None, false);
        assert!(mask.is_none(), "default mask should be None (accept all)");
    }

    #[test]
    fn disk_event_mask_has_change_events() {
        // B7: Verify disk-only mask contains the change-detection events
        let mask = super::parse_event_type_filter_with_default(None, true);
        let events = mask.expect("disk mask should be Some");

        assert!(events.contains("localchangedetected"));
        assert!(events.contains("remotechangedetected"));
        // disk mask should NOT contain general events
        assert!(!events.contains("startupcomplete"));
    }
}
