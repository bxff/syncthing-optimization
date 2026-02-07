use crate::bep::{decode_frame, encode_frame, BepMessage};
use crate::db::Db;
use crate::model_core::{model, newFolderConfiguration, NewModel, NewModelWithRuntime};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tiny_http::{Header, Method, Response, Server, StatusCode};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:22000";
const DEFAULT_FOLDER_ID: &str = "default";
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_PEERS: usize = 32;

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

    let folders = if !folders.is_empty() {
        if folder_id_set || folder_path_set {
            return Err(
                "cannot mix --folder with --folder-id/--folder-path; use one style".to_string(),
            );
        }
        folders
    } else {
        let path = folder_path.ok_or_else(|| {
            "one folder is required: use --folder <id>:<path> or --folder-path <path>".to_string()
        })?;
        vec![FolderSpec {
            id: folder_id,
            path,
        }]
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
    let model = Arc::new(Mutex::new(NewModelWithRuntime(
        config.db_root.as_ref().map(PathBuf::from),
        config.memory_max_mb,
    )));
    {
        let mut guard = model
            .lock()
            .map_err(|_| "model lock poisoned".to_string())?;
        for folder in &config.folders {
            guard.newFolder(newFolderConfiguration(&folder.id, &folder.path));
        }
    }

    let active_peers = Arc::new(AtomicUsize::new(0));
    if let Some(api_addr) = config.api_listen_addr.as_ref() {
        let runtime = DaemonApiRuntime {
            model: model.clone(),
            active_peers: active_peers.clone(),
            max_peers: config.max_peers,
            start_time: SystemTime::now(),
            bep_listen_addr: config.listen_addr.clone(),
        };
        let _api_thread = start_api_server(api_addr, runtime)?;
    }

    let listener = TcpListener::bind(&config.listen_addr)
        .map_err(|err| format!("listen {}: {err}", config.listen_addr))?;
    run_daemon_with_listener(listener, model, config.once, config.max_peers, active_peers)
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

#[derive(Clone)]
struct DaemonApiRuntime {
    model: Arc<Mutex<model>>,
    active_peers: Arc<AtomicUsize>,
    max_peers: usize,
    start_time: SystemTime,
    bep_listen_addr: String,
}

struct ApiReply {
    status_code: StatusCode,
    body: Vec<u8>,
}

impl ApiReply {
    fn json(status_code: u16, payload: Value) -> Self {
        let body = serde_json::to_vec(&payload)
            .unwrap_or_else(|_| b"{\"error\":\"encode error\"}".to_vec());
        Self {
            status_code: StatusCode(status_code),
            body,
        }
    }
}

fn start_api_server(
    addr: &str,
    runtime: DaemonApiRuntime,
) -> Result<thread::JoinHandle<()>, String> {
    let server = Server::http(addr).map_err(|err| format!("listen api {addr}: {err}"))?;
    let handle = thread::spawn(move || {
        for request in server.incoming_requests() {
            let reply = build_api_response(request.method(), request.url(), &runtime);
            let mut response = Response::from_data(reply.body).with_status_code(reply.status_code);
            if let Ok(content_type) =
                Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            {
                response = response.with_header(content_type);
            }
            let _ = request.respond(response);
        }
    });
    Ok(handle)
}

fn build_api_response(method: &Method, url: &str, runtime: &DaemonApiRuntime) -> ApiReply {
    let (path, query) = split_url(url);
    match path {
        "/rest/system/ping" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            ApiReply::json(200, json!({ "ping": "pong" }))
        }
        "/rest/system/version" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            ApiReply::json(
                200,
                json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "longVersion": format!("syncthing-rs {}", env!("CARGO_PKG_VERSION")),
                    "os": std::env::consts::OS,
                    "arch": std::env::consts::ARCH,
                }),
            )
        }
        "/rest/system/status" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            match system_status(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(err) => ApiReply::json(500, json!({ "error": err })),
            }
        }
        "/rest/system/connections" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            match system_connections(runtime) {
                Ok(payload) => ApiReply::json(200, payload),
                Err(err) => ApiReply::json(500, json!({ "error": err })),
            }
        }
        "/rest/system/config/folders" => match method {
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
            Method::Post => {
                let params = parse_query(query);
                let Some(folder_id) = params.get("id") else {
                    return ApiReply::json(400, json!({ "error": "missing id query parameter" }));
                };
                let Some(path) = params.get("path") else {
                    return ApiReply::json(400, json!({ "error": "missing path query parameter" }));
                };
                match add_config_folder(runtime, folder_id, path) {
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
                let params = parse_query(query);
                let Some(folder_id) = params.get("id") else {
                    return ApiReply::json(400, json!({ "error": "missing id query parameter" }));
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
            _ => ApiReply::json(405, json!({ "error": "method not allowed" })),
        },
        "/rest/system/config/restart" => {
            if method != &Method::Post {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder_id) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            let device = params
                .get("device")
                .map(|value| value.as_str())
                .unwrap_or("remote");
            match folder_completion(runtime, folder, device) {
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
        "/rest/db/file" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            let Some(path) = params.get("file") else {
                return ApiReply::json(400, json!({ "error": "missing file query parameter" }));
            };
            let global = params
                .get("global")
                .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
                .unwrap_or(false);
            match folder_file(runtime, folder, path, global) {
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
        "/rest/db/localchanged" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            match folder_local_changed(runtime, folder) {
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            let device = params
                .get("device")
                .map(|value| value.as_str())
                .unwrap_or("remote");
            match folder_remote_need(runtime, folder, device) {
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
        "/rest/db/ignores" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
        "/rest/db/browse" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            let cursor = params.get("cursor").map(|v| v.as_str());
            let limit = parse_limit(&params, "limit", 2000, 10_000);
            match browse_local_files(runtime, folder, cursor, limit) {
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
        "/rest/db/need" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            let cursor = params.get("cursor").map(|v| v.as_str());
            let limit = parse_limit(&params, "limit", 2000, 10_000);
            match browse_needed_files(runtime, folder, cursor, limit) {
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
        "/rest/db/jobs" => {
            if method != &Method::Get {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            let subdirs = parse_subdirs(&params);
            match scan_folder(runtime, folder, &subdirs) {
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
        "/rest/db/pull" => {
            if method != &Method::Post {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
            };
            match pull_folder(runtime, folder) {
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
        "/rest/db/override" => {
            if method != &Method::Post {
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
                return ApiReply::json(405, json!({ "error": "method not allowed" }));
            }
            let params = parse_query(query);
            let Some(folder) = params.get("folder") else {
                return ApiReply::json(400, json!({ "error": "missing folder query parameter" }));
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
        if key.is_empty() {
            continue;
        }
        out.insert(key.to_string(), value.to_string());
    }
    out
}

fn parse_subdirs(params: &BTreeMap<String, String>) -> Vec<String> {
    params
        .get("sub")
        .map(|v| {
            v.split(',')
                .map(str::trim)
                .filter(|segment| !segment.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
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

    let (my_id, db_estimated_bytes, db_budget_bytes, folder_count) = {
        let guard = runtime
            .model
            .lock()
            .map_err(|_| "model lock poisoned".to_string())?;
        let db = guard
            .sdb
            .lock()
            .map_err(|_| "database lock poisoned".to_string())?;
        (
            guard.id.clone(),
            db.estimated_memory_bytes() as u64,
            db.memory_budget_bytes() as u64,
            guard.folderCfgs.len(),
        )
    };

    Ok(json!({
        "myID": my_id,
        "startTime": start_ts,
        "uptimeS": uptime,
        "folderCount": folder_count,
        "activePeers": runtime.active_peers.load(Ordering::Relaxed),
        "maxPeers": runtime.max_peers,
        "listenAddress": runtime.bep_listen_addr,
        "memoryEstimatedBytes": db_estimated_bytes,
        "memoryBudgetBytes": db_budget_bytes,
    }))
}

fn system_connections(runtime: &DaemonApiRuntime) -> Result<Value, String> {
    let guard = runtime
        .model
        .lock()
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
        .lock()
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
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let completion = guard.Completion(folder, device);
    Ok(json!({
        "folder": folder,
        "device": device,
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
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let file = if global {
        guard.CurrentGlobalFile(folder, path)
    } else {
        guard.CurrentFolderFile(folder, path)
    };
    let Some(file) = file else {
        return Ok(json!({
            "folder": folder,
            "file": path,
            "global": global,
            "exists": false,
        }));
    };
    let file_type = match file.file_type {
        crate::db::FileInfoType::File => "file",
        crate::db::FileInfoType::Directory => "directory",
        crate::db::FileInfoType::Symlink => "symlink",
    };
    Ok(json!({
        "folder": folder,
        "file": path,
        "global": global,
        "exists": true,
        "entry": {
            "path": file.path,
            "sequence": file.sequence,
            "modifiedNs": file.modified_ns,
            "size": file.size,
            "deleted": file.deleted,
            "ignored": file.ignored,
            "localFlags": file.local_flags,
            "fileType": file_type,
            "blockHashes": file.block_hashes,
        }
    }))
}

fn folder_local_changed(
    runtime: &DaemonApiRuntime,
    folder: &str,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let files = guard.LocalChangedFolderFiles(folder);
    Ok(json!({
        "folder": folder,
        "count": files.len(),
        "files": files,
    }))
}

fn folder_remote_need(
    runtime: &DaemonApiRuntime,
    folder: &str,
    device: &str,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let files = guard.RemoteNeedFolderFiles(folder, device);
    let items = files
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
        .collect::<Vec<_>>();
    Ok(json!({
        "folder": folder,
        "device": device,
        "count": items.len(),
        "items": items,
    }))
}

fn folder_jobs(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .lock()
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
        .lock()
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
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let ignores = guard.CurrentIgnores(folder);
    Ok(json!({
        "folder": folder,
        "count": ignores.len(),
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
        .lock()
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

fn pull_folder(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .lock()
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
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    guard.Override(folder).map_err(|err| {
        if err.contains("folder missing") {
            ApiFolderStatusError::MissingFolder
        } else {
            ApiFolderStatusError::Internal(err)
        }
    })?;
    Ok(json!({
        "folder": folder,
        "action": "override",
        "ok": true,
    }))
}

fn revert_folder(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    guard.Revert(folder).map_err(|err| {
        if err.contains("folder missing") {
            ApiFolderStatusError::MissingFolder
        } else {
            ApiFolderStatusError::Internal(err)
        }
    })?;
    Ok(json!({
        "folder": folder,
        "action": "revert",
        "ok": true,
    }))
}

fn reset_folder(runtime: &DaemonApiRuntime, folder: &str) -> Result<Value, ApiFolderStatusError> {
    let mut guard = runtime
        .model
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    guard.ResetFolder(folder);
    let mut db = guard
        .sdb
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("database lock poisoned".to_string()))?;
    db.drop_folder(folder)
        .map_err(ApiFolderStatusError::Internal)?;
    Ok(json!({
        "folder": folder,
        "action": "reset",
        "ok": true,
    }))
}

fn browse_local_files(
    runtime: &DaemonApiRuntime,
    folder: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value, ApiFolderStatusError> {
    let guard = runtime
        .model
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let db = guard
        .sdb
        .lock()
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
        .lock()
        .map_err(|_| ApiFolderStatusError::Internal("model lock poisoned".to_string()))?;
    if !guard.folderCfgs.contains_key(folder) {
        return Err(ApiFolderStatusError::MissingFolder);
    }
    let db = guard
        .sdb
        .lock()
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
        "items": items,
    }))
}

fn list_config_folders(runtime: &DaemonApiRuntime) -> Result<Value, ApiConfigError> {
    let guard = runtime
        .model
        .lock()
        .map_err(|_| ApiConfigError::Internal("model lock poisoned".to_string()))?;
    let mut folders = guard
        .folderCfgs
        .values()
        .map(|cfg| {
            json!({
                "id": cfg.id,
                "path": cfg.path,
                "folderType": cfg.folder_type.as_str(),
                "memoryMaxMB": cfg.memory_max_mb,
                "memoryPolicy": memory_policy_name(cfg.memory_policy),
            })
        })
        .collect::<Vec<_>>();
    folders.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    Ok(json!({
        "count": folders.len(),
        "folders": folders,
    }))
}

fn add_config_folder(
    runtime: &DaemonApiRuntime,
    folder_id: &str,
    path: &str,
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
        .lock()
        .map_err(|_| ApiConfigError::Internal("model lock poisoned".to_string()))?;
    if guard.folderCfgs.contains_key(folder_id) {
        return Err(ApiConfigError::Conflict(folder_id.to_string()));
    }
    let cfg = newFolderConfiguration(folder_id, path);
    guard.newFolder(cfg.clone());
    Ok(json!({
        "added": true,
        "folder": {
            "id": cfg.id,
            "path": cfg.path,
        }
    }))
}

fn remove_config_folder(
    runtime: &DaemonApiRuntime,
    folder_id: &str,
) -> Result<Value, ApiConfigError> {
    let mut guard = runtime
        .model
        .lock()
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
        .lock()
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
    model: Arc<Mutex<model>>,
    once: bool,
    max_peers: usize,
    active_peers: Arc<AtomicUsize>,
) -> Result<(), String> {
    let mut peer_seq = 0_u64;
    loop {
        let (mut stream, addr) = listener
            .accept()
            .map_err(|err| format!("accept connection: {err}"))?;
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
    model: &Arc<Mutex<model>>,
) -> Result<(), String> {
    let mut seen_hello = false;
    loop {
        let frame = match read_frame(stream)? {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let inbound = decode_frame(&frame)?;
        if !seen_hello {
            if !matches!(inbound, BepMessage::Hello { .. }) {
                return Err("expected hello as first message".to_string());
            }
            seen_hello = true;
        } else if matches!(inbound, BepMessage::Hello { .. }) {
            return Err("duplicate hello message".to_string());
        }
        let outbound = {
            let mut guard = model
                .lock()
                .map_err(|_| "model lock poisoned".to_string())?;
            guard.ApplyBepMessage(peer_id, &inbound)?
        };
        if let Some(message) = outbound {
            write_frame(stream, &message)?;
        }
        if matches!(inbound, BepMessage::Close { .. }) {
            return Ok(());
        }
    }
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
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.join("db")),
            Some(64),
        )));
        {
            let mut guard = model
                .lock()
                .map_err(|_| "model lock poisoned".to_string())?;
            guard.newFolder(newFolderConfiguration(
                DEFAULT_FOLDER_ID,
                &folder_path.to_string_lossy(),
            ));
        }
        let runtime = DaemonApiRuntime {
            model: model.clone(),
            active_peers: Arc::new(AtomicUsize::new(0)),
            max_peers: DEFAULT_MAX_PEERS,
            start_time: SystemTime::now(),
            bep_listen_addr: DEFAULT_LISTEN_ADDR.to_string(),
        };

        ensure_api_ok(&build_api_response(
            &Method::Get,
            "/rest/system/ping",
            &runtime,
        ))?;
        ensure_api_ok(&build_api_response(
            &Method::Post,
            "/rest/db/scan?folder=default",
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

pub(crate) fn run_api_surface_probe() -> Result<Vec<String>, String> {
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
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.join("db")),
            Some(64),
        )));
        {
            let mut guard = model
                .lock()
                .map_err(|_| "model lock poisoned".to_string())?;
            guard.newFolder(newFolderConfiguration(
                DEFAULT_FOLDER_ID,
                &folder_path.to_string_lossy(),
            ));
        }
        let runtime = DaemonApiRuntime {
            model,
            active_peers: Arc::new(AtomicUsize::new(0)),
            max_peers: DEFAULT_MAX_PEERS,
            start_time: SystemTime::now(),
            bep_listen_addr: DEFAULT_LISTEN_ADDR.to_string(),
        };

        let cases: Vec<(&str, Method, String, u16)> = vec![
            (
                "GET /rest/system/ping",
                Method::Get,
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
                "GET /rest/system/config/folders",
                Method::Get,
                "/rest/system/config/folders".to_string(),
                200,
            ),
            (
                "POST /rest/system/config/folders",
                Method::Post,
                format!(
                    "/rest/system/config/folders?id=docs&path={}",
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
                "/rest/system/config/folders?id=docs".to_string(),
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
                "GET /rest/db/completion",
                Method::Get,
                "/rest/db/completion?folder=default&device=peer-a".to_string(),
                200,
            ),
            (
                "GET /rest/db/file",
                Method::Get,
                "/rest/db/file?folder=default&file=a.txt".to_string(),
                200,
            ),
            (
                "GET /rest/db/localchanged",
                Method::Get,
                "/rest/db/localchanged?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/db/remoteneed",
                Method::Get,
                "/rest/db/remoteneed?folder=default&device=peer-a".to_string(),
                200,
            ),
            (
                "GET /rest/db/ignores",
                Method::Get,
                "/rest/db/ignores?folder=default".to_string(),
                200,
            ),
            (
                "GET /rest/db/browse",
                Method::Get,
                "/rest/db/browse?folder=default&limit=10".to_string(),
                200,
            ),
            (
                "GET /rest/db/need",
                Method::Get,
                "/rest/db/need?folder=default&limit=10".to_string(),
                200,
            ),
            (
                "GET /rest/db/jobs",
                Method::Get,
                "/rest/db/jobs?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/pull",
                Method::Post,
                "/rest/db/pull?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/override",
                Method::Post,
                "/rest/db/override?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/revert",
                Method::Post,
                "/rest/db/revert?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/bringtofront",
                Method::Post,
                "/rest/db/bringtofront?folder=default".to_string(),
                200,
            ),
            (
                "POST /rest/db/reset",
                Method::Post,
                "/rest/db/reset?folder=default".to_string(),
                200,
            ),
        ];

        let mut covered = Vec::with_capacity(cases.len());
        for (key, method, url, expected_status) in cases {
            let reply = build_api_response(&method, &url, &runtime);
            if reply.status_code != StatusCode(expected_status) {
                let body = String::from_utf8_lossy(&reply.body);
                return Err(format!(
                    "api probe {key} expected status {expected_status} got {} body={body}",
                    reply.status_code.0
                ));
            }
            covered.push(key.to_string());
        }
        covered.sort();
        Ok(covered)
    })();

    let _ = fs::remove_dir_all(&root);
    result
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

fn run_parity_peer_probe(model: &Arc<Mutex<model>>) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|err| format!("bind parity peer probe listener: {err}"))?;
    let addr = listener
        .local_addr()
        .map_err(|err| format!("parity peer probe local addr: {err}"))?;
    let model_ref = model.clone();
    let server = thread::spawn(move || -> Result<(), String> {
        let (mut stream, peer_addr) = listener
            .accept()
            .map_err(|err| format!("accept parity peer probe connection: {err}"))?;
        handle_peer_connection(&mut stream, &peer_addr.to_string(), &model_ref)
    });

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
            hash: "probe".to_string(),
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bep::{default_exchange, BepMessage};
    use crate::db;
    use crate::db::Db;
    use serde_json::Value;
    use std::fs;
    use std::net::{Shutdown, TcpStream};
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
        }
    }

    #[test]
    fn parse_daemon_args_requires_folder_path() {
        let err = parse_daemon_args(&[]).expect_err("must fail");
        assert!(err.contains("one folder is required"));
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

    fn test_api_runtime(
        model: Arc<Mutex<model>>,
        _folders: Vec<FolderSpec>,
        active_peers: usize,
    ) -> DaemonApiRuntime {
        let runtime = DaemonApiRuntime {
            model,
            active_peers: Arc::new(AtomicUsize::new(active_peers)),
            max_peers: 16,
            start_time: SystemTime::now(),
            bep_listen_addr: "127.0.0.1:22000".to_string(),
        };
        runtime
    }

    #[test]
    fn api_ping_returns_pong() {
        let runtime = test_api_runtime(Arc::new(Mutex::new(NewModel())), Vec::new(), 3);

        let reply = build_api_response(&Method::Get, "/rest/system/ping", &runtime);
        assert_eq!(reply.status_code, StatusCode(200));
        let payload: Value = serde_json::from_slice(&reply.body).expect("decode json");
        assert_eq!(payload["ping"], "pong");
    }

    #[test]
    fn api_status_reports_memory_and_peer_fields() {
        let root = temp_root("api-system-status");
        let model = Arc::new(Mutex::new(NewModel()));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
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
            Arc::new(Mutex::new(NewModel())),
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
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
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
            Arc::new(Mutex::new(NewModel())),
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
    fn api_db_scan_and_jobs_endpoints_work() {
        let root = temp_root("api-db-scan-jobs");
        fs::write(root.join("a.txt"), b"hello").expect("write");
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
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
        let scan = build_api_response(
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

        let jobs = build_api_response(&Method::Get, "/rest/db/jobs?folder=default", &runtime);
        assert_eq!(jobs.status_code, StatusCode(200));
        let jobs_payload: Value = serde_json::from_slice(&jobs.body).expect("decode json");
        assert_eq!(jobs_payload["folder"], "default");
        assert!(jobs_payload["jobs"].is_object());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_pull_endpoint_applies_remote_file() {
        let root = temp_root("api-db-pull");
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
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
        let pull = build_api_response(&Method::Post, "/rest/db/pull?folder=default", &runtime);
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
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            let mut db = guard.sdb.lock().expect("db lock");
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
        assert_eq!(first_payload["items"][0]["path"], "a.txt");
        let next = first_payload["nextCursor"]
            .as_str()
            .expect("next cursor")
            .to_string();

        let second = build_api_response(
            &Method::Get,
            &format!("/rest/db/browse?folder=default&limit=1&cursor={next}"),
            &runtime,
        );
        assert_eq!(second.status_code, StatusCode(200));
        let second_payload: Value = serde_json::from_slice(&second.body).expect("decode json");
        assert_eq!(second_payload["items"][0]["path"], "b.txt");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_file_endpoint_returns_local_and_global_entries() {
        let root = temp_root("api-db-file");
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            {
                let mut db = guard.sdb.lock().expect("db lock");
                db.update("default", "local", vec![file_info("a.txt", 1, 10)])
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
        assert_eq!(missing.status_code, StatusCode(200));
        let missing_payload: Value = serde_json::from_slice(&missing.body).expect("decode json");
        assert_eq!(missing_payload["exists"], false);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_completion_localchanged_and_remoteneed_endpoints_work() {
        let root = temp_root("api-db-completion-localchanged-remoteneed");
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            {
                let mut db = guard.sdb.lock().expect("db lock");
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
        assert_eq!(localchanged_payload["count"], 1);
        assert_eq!(localchanged_payload["files"][0], "a.txt");

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
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
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
            build_api_response(&Method::Post, "/rest/db/override?folder=default", &runtime);
        assert_eq!(override_reply.status_code, StatusCode(200));
        let override_payload: Value =
            serde_json::from_slice(&override_reply.body).expect("decode json");
        assert_eq!(override_payload["action"], "override");
        assert_eq!(override_payload["ok"], true);

        let revert_reply =
            build_api_response(&Method::Post, "/rest/db/revert?folder=default", &runtime);
        assert_eq!(revert_reply.status_code, StatusCode(200));
        let revert_payload: Value =
            serde_json::from_slice(&revert_reply.body).expect("decode json");
        assert_eq!(revert_payload["action"], "revert");
        assert_eq!(revert_payload["ok"], true);

        let missing_override =
            build_api_response(&Method::Post, "/rest/db/override?folder=missing", &runtime);
        assert_eq!(missing_override.status_code, StatusCode(404));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_bringtofront_endpoint_works() {
        let root = temp_root("api-db-bringtofront");
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
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
        let ok = build_api_response(
            &Method::Post,
            "/rest/db/bringtofront?folder=default",
            &runtime,
        );
        assert_eq!(ok.status_code, StatusCode(200));
        let ok_payload: Value = serde_json::from_slice(&ok.body).expect("decode json");
        assert_eq!(ok_payload["action"], "bringtofront");
        assert_eq!(ok_payload["ok"], true);

        let missing = build_api_response(
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
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            guard.SetIgnores(
                "default",
                vec!["*.tmp".to_string(), ".DS_Store".to_string()],
            );
            {
                let mut db = guard.sdb.lock().expect("db lock");
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

        let reset = build_api_response(&Method::Post, "/rest/db/reset?folder=default", &runtime);
        assert_eq!(reset.status_code, StatusCode(200));
        let reset_payload: Value = serde_json::from_slice(&reset.body).expect("decode json");
        assert_eq!(reset_payload["action"], "reset");
        assert_eq!(reset_payload["ok"], true);

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
        assert_eq!(file_after.status_code, StatusCode(200));
        let file_after_payload: Value =
            serde_json::from_slice(&file_after.body).expect("decode json");
        assert_eq!(file_after_payload["exists"], false);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_db_need_returns_needed_page() {
        let root = temp_root("api-db-need");
        let model = Arc::new(Mutex::new(NewModelWithRuntime(
            Some(root.clone()),
            Some(50),
        )));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
            {
                let mut db = guard.sdb.lock().expect("db lock");
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
            Arc::new(Mutex::new(NewModel())),
            vec![FolderSpec {
                id: "default".to_string(),
                path: "/tmp/default".to_string(),
            }],
            0,
        );
        let pull_get = build_api_response(&Method::Get, "/rest/db/pull?folder=default", &runtime);
        assert_eq!(pull_get.status_code, StatusCode(405));
        let status_post =
            build_api_response(&Method::Post, "/rest/db/status?folder=default", &runtime);
        assert_eq!(status_post.status_code, StatusCode(405));
        let file_post = build_api_response(
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
        let ignores_post =
            build_api_response(&Method::Post, "/rest/db/ignores?folder=default", &runtime);
        assert_eq!(ignores_post.status_code, StatusCode(405));
        let completion_post = build_api_response(
            &Method::Post,
            "/rest/db/completion?folder=default",
            &runtime,
        );
        assert_eq!(completion_post.status_code, StatusCode(405));
        let localchanged_post = build_api_response(
            &Method::Post,
            "/rest/db/localchanged?folder=default",
            &runtime,
        );
        assert_eq!(localchanged_post.status_code, StatusCode(405));
        let remoteneed_post = build_api_response(
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
    fn api_config_folders_crud_and_restart() {
        let root = temp_root("api-config-folders");
        let folder_path = root.join("docs");
        fs::create_dir_all(&folder_path).expect("create folder");
        let runtime = test_api_runtime(Arc::new(Mutex::new(NewModel())), Vec::new(), 0);

        let add = build_api_response(
            &Method::Post,
            &format!(
                "/rest/system/config/folders?id=docs&path={}",
                folder_path.to_string_lossy()
            ),
            &runtime,
        );
        assert_eq!(add.status_code, StatusCode(200));
        let add_payload: Value = serde_json::from_slice(&add.body).expect("decode json");
        assert_eq!(add_payload["added"], true);
        assert_eq!(add_payload["folder"]["id"], "docs");

        let list = build_api_response(&Method::Get, "/rest/system/config/folders", &runtime);
        assert_eq!(list.status_code, StatusCode(200));
        let list_payload: Value = serde_json::from_slice(&list.body).expect("decode json");
        assert_eq!(list_payload["count"], 1);
        assert_eq!(list_payload["folders"][0]["id"], "docs");
        assert_eq!(list_payload["folders"][0]["folderType"], "sendrecv");
        assert_eq!(list_payload["folders"][0]["memoryPolicy"], "throttle");

        let restart = build_api_response(
            &Method::Post,
            "/rest/system/config/restart?folder=docs",
            &runtime,
        );
        assert_eq!(restart.status_code, StatusCode(200));
        let restart_payload: Value = serde_json::from_slice(&restart.body).expect("decode json");
        assert_eq!(restart_payload["restarted"], true);
        assert_eq!(restart_payload["folder"], "docs");

        let remove = build_api_response(
            &Method::Delete,
            "/rest/system/config/folders?id=docs",
            &runtime,
        );
        assert_eq!(remove.status_code, StatusCode(200));
        let remove_payload: Value = serde_json::from_slice(&remove.body).expect("decode json");
        assert_eq!(remove_payload["removed"], true);
        assert_eq!(remove_payload["folder"], "docs");

        let restart_missing = build_api_response(
            &Method::Post,
            "/rest/system/config/restart?folder=docs",
            &runtime,
        );
        assert_eq!(restart_missing.status_code, StatusCode(404));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn api_config_folders_validate_inputs() {
        let runtime = test_api_runtime(Arc::new(Mutex::new(NewModel())), Vec::new(), 0);

        let missing_id = build_api_response(
            &Method::Post,
            "/rest/system/config/folders?path=/tmp/docs",
            &runtime,
        );
        assert_eq!(missing_id.status_code, StatusCode(400));

        let missing_path = build_api_response(
            &Method::Post,
            "/rest/system/config/folders?id=docs",
            &runtime,
        );
        assert_eq!(missing_path.status_code, StatusCode(400));

        let missing_delete_id =
            build_api_response(&Method::Delete, "/rest/system/config/folders", &runtime);
        assert_eq!(missing_delete_id.status_code, StatusCode(400));

        let missing_restart_folder =
            build_api_response(&Method::Post, "/rest/system/config/restart", &runtime);
        assert_eq!(missing_restart_folder.status_code, StatusCode(400));
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

        let model = Arc::new(Mutex::new(NewModel()));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
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
        let guard = model.lock().expect("lock");
        assert_eq!(
            guard.RemoteSequences("default").get("remote").copied(),
            Some(2)
        );
        assert_eq!(guard.DownloadProgress("peer-a"), vec!["default:a.txt:2"]);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn handle_peer_connection_returns_error_response_for_invalid_request() {
        let root = temp_root("invalid-request");
        fs::write(root.join("ok.txt"), b"ok").expect("write");

        let model = Arc::new(Mutex::new(NewModel()));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
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
                hash: "h".to_string(),
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
                code: 1,
                data_len: 0,
                data: Vec::new()
            }
        );

        server.join().expect("join server");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn handle_peer_connection_rejects_non_hello_first_frame() {
        let root = temp_root("missing-hello");
        fs::write(root.join("ok.txt"), b"ok").expect("write");

        let model = Arc::new(Mutex::new(NewModel()));
        {
            let mut guard = model.lock().expect("lock");
            guard.newFolder(newFolderConfiguration("default", &root.to_string_lossy()));
        }

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
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
                hash: "h".to_string(),
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
        let covered = run_api_surface_probe().expect("run api surface probe");
        assert!(covered.contains(&"GET /rest/system/ping".to_string()));
        assert!(covered.contains(&"GET /rest/db/status".to_string()));
        assert!(covered.contains(&"POST /rest/db/bringtofront".to_string()));
        assert!(covered.contains(&"DELETE /rest/system/config/folders".to_string()));
    }
}
