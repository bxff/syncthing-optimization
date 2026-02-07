use crate::bep::{decode_frame, encode_frame, BepMessage};
use crate::model_core::{model, newFolderConfiguration, NewModel, NewModelWithRuntime};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:22000";
const DEFAULT_FOLDER_ID: &str = "default";
const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_MAX_PEERS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DaemonConfig {
    pub(crate) listen_addr: String,
    pub(crate) folder_id: String,
    pub(crate) folder_path: String,
    pub(crate) db_root: Option<String>,
    pub(crate) memory_max_mb: Option<usize>,
    pub(crate) max_peers: usize,
    pub(crate) once: bool,
}

pub(crate) fn parse_daemon_args(args: &[String]) -> Result<DaemonConfig, String> {
    let mut listen_addr = DEFAULT_LISTEN_ADDR.to_string();
    let mut folder_id = DEFAULT_FOLDER_ID.to_string();
    let mut folder_path: Option<String> = None;
    let mut db_root: Option<String> = None;
    let mut memory_max_mb: Option<usize> = None;
    let mut max_peers = DEFAULT_MAX_PEERS;
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
            }
            "--folder-id" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--folder-id requires a value".to_string())?;
                folder_id = value.clone();
            }
            "--folder-path" => {
                i += 1;
                let value = args
                    .get(i)
                    .ok_or_else(|| "--folder-path requires a value".to_string())?;
                folder_path = Some(value.clone());
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

    let folder_path = folder_path.ok_or_else(|| "--folder-path is required".to_string())?;
    Ok(DaemonConfig {
        listen_addr,
        folder_id,
        folder_path,
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
        let mut guard = model.lock().map_err(|_| "model lock poisoned".to_string())?;
        guard.newFolder(newFolderConfiguration(&config.folder_id, &config.folder_path));
    }

    let listener = TcpListener::bind(&config.listen_addr)
        .map_err(|err| format!("listen {}: {err}", config.listen_addr))?;
    run_daemon_with_listener(listener, model, config.once, config.max_peers)
}

fn run_daemon_with_listener(
    listener: TcpListener,
    model: Arc<Mutex<model>>,
    once: bool,
    max_peers: usize,
) -> Result<(), String> {
    let active_peers = Arc::new(AtomicUsize::new(0));
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
    loop {
        let frame = match read_frame(stream)? {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let inbound = decode_frame(&frame)?;
        let outbound = {
            let mut guard = model.lock().map_err(|_| "model lock poisoned".to_string())?;
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

    let mut header = [0_u8; 8];
    header[0] = first[0];
    reader
        .read_exact(&mut header[1..])
        .map_err(|err| format!("read frame header: {err}"))?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(format!("frame too large: {len} > {MAX_FRAME_BYTES}"));
    }
    let mut payload = vec![0_u8; len];
    reader
        .read_exact(&mut payload)
        .map_err(|err| format!("read frame payload: {err}"))?;

    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&payload);
    Ok(Some(frame))
}

fn write_frame(writer: &mut impl Write, message: &BepMessage) -> Result<(), String> {
    let frame = encode_frame(message)?;
    writer
        .write_all(&frame)
        .map_err(|err| format!("write frame: {err}"))?;
    writer.flush().map_err(|err| format!("flush frame: {err}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bep::{default_exchange, BepMessage};
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

    #[test]
    fn parse_daemon_args_requires_folder_path() {
        let err = parse_daemon_args(&[]).expect_err("must fail");
        assert!(err.contains("--folder-path is required"));
    }

    #[test]
    fn parse_daemon_args_parses_flags() {
        let args = vec![
            "--listen".to_string(),
            "127.0.0.1:23000".to_string(),
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
        assert_eq!(cfg.folder_id, "photos");
        assert_eq!(cfg.folder_path, "/tmp/photos");
        assert_eq!(cfg.db_root.as_deref(), Some("/tmp/syncthing-rs-db"));
        assert_eq!(cfg.memory_max_mb, Some(64));
        assert_eq!(cfg.max_peers, 8);
        assert!(cfg.once);
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
                data_len: 11
            }
        );

        server.join().expect("join server");
        let guard = model.lock().expect("lock");
        assert_eq!(guard.RemoteSequences("default").get("remote").copied(), Some(2));
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
                data_len: 0
            }
        );

        server.join().expect("join server");
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
        src.extend_from_slice(&((MAX_FRAME_BYTES as u32) + 1).to_le_bytes());
        src.extend_from_slice(&0_u32.to_le_bytes());
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
        data.extend_from_slice(&4_u32.to_le_bytes());
        data.extend_from_slice(&0_u32.to_le_bytes());
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
}
