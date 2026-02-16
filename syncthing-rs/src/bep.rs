use crate::bep_core::{
    FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_APPEND,
    FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_FORGET, HelloMessageMagic,
    MessageCompression_MESSAGE_COMPRESSION_LZ4, MessageCompression_MESSAGE_COMPRESSION_NONE,
    MessageType_MESSAGE_TYPE_CLOSE, MessageType_MESSAGE_TYPE_CLUSTER_CONFIG,
    MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS, MessageType_MESSAGE_TYPE_INDEX,
    MessageType_MESSAGE_TYPE_INDEX_UPDATE, MessageType_MESSAGE_TYPE_PING,
    MessageType_MESSAGE_TYPE_REQUEST, MessageType_MESSAGE_TYPE_RESPONSE,
};
use crate::bep_proto::bep as pb;
use prost::Message;
use serde::{Deserialize, Serialize};

const MAX_HEADER_BYTES: usize = 32_767;
const MAX_MESSAGE_LEN: usize = 500 * 1_000_000;
const MAX_HELLO_BYTES: usize = 32_767;
const COMPRESSION_THRESHOLD_BYTES: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BepMessage {
    Hello {
        device_name: String,
        client_name: String,
        // 8.1: preserve all wire fields
        client_version: String,
        num_connections: i32,
        timestamp: i64,
    },
    ClusterConfig {
        // 8.2: full folder/device data, not just IDs
        folders: Vec<ClusterConfigFolder>,
    },
    Index {
        folder: String,
        files: Vec<IndexEntry>,
        // B1: wire-preserved last_sequence (end-to-end, not recomputed)
        last_sequence: i64,
    },
    IndexUpdate {
        folder: String,
        files: Vec<IndexEntry>,
        // 8.6: preserve prev_sequence from wire
        prev_sequence: i64,
        // A1: preserve last_sequence from wire (Go uses this end-to-end)
        last_sequence: i64,
    },
    Request {
        // B2: i32 to preserve negative IDs from Go peers
        id: i32,
        folder: String,
        name: String,
        // BEP-4: signed values from proto, no .max(0) clamping
        offset: i64,
        size: i32,
        hash: Vec<u8>,
        // 8.5: preserve from_temporary and block_no
        from_temporary: bool,
        block_no: i32,
    },
    Response {
        // B2: i32 to preserve negative IDs from Go peers
        id: i32,
        // 8.7: signed code, don't clamp negatives
        code: i32,
        data_len: u32,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        data: Vec<u8>,
    },
    DownloadProgress {
        folder: String,
        updates: Vec<DownloadProgressEntry>,
    },
    Ping {
        timestamp_ms: u64,
    },
    Close {
        reason: String,
    },
}

// 8.2: Full ClusterConfig folder with device lists
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterConfigFolder {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) devices: Vec<ClusterConfigDevice>,
    pub(crate) folder_type: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterConfigDevice {
    pub(crate) id: Vec<u8>,
    pub(crate) name: String,
    pub(crate) addresses: Vec<String>,
    pub(crate) compression: i32,
    pub(crate) introducer: bool,
    pub(crate) index_id: u64,
    pub(crate) max_sequence: i64,
    pub(crate) encryption_password_token: Vec<u8>,
    pub(crate) skip_introduction_removals: bool,
}

// 8.3: Full IndexEntry with all FileInfo fields
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct IndexEntry {
    pub(crate) path: String,
    // A6: signed sequence from wire — no .max(0) clamping
    pub(crate) sequence: i64,
    pub(crate) deleted: bool,
    // A6: signed size from wire — no .max(0) clamping
    pub(crate) size: i64,
    // A3: Full block tuples (hash, offset, size) — not hash-only
    pub(crate) blocks: Vec<BlockEntry>,
    // 8.3: additional fields from pb::FileInfo
    pub(crate) file_type: i32,
    pub(crate) permissions: u32,
    pub(crate) modified_s: i64,
    pub(crate) modified_ns: i32,
    pub(crate) modified_by: u64,
    pub(crate) no_permissions: bool,
    pub(crate) invalid: bool,
    pub(crate) local_flags: u32,
    // A8: Symlink target as raw bytes (Vec<u8>) for wire fidelity
    #[serde(default)]
    pub(crate) symlink_target: Vec<u8>,
    pub(crate) block_size: i32,
    // A7: Full version vector — u64 counters end-to-end
    pub(crate) version_counters: Vec<(u64, u64)>,
    // A4: Additional wire fields preserved from pb::FileInfo
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) blocks_hash: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) previous_blocks_hash: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) encrypted: Vec<u8>,
}

// A3: Block entry with full (hash, offset, size) tuple from wire
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct BlockEntry {
    pub(crate) hash: String, // hex-encoded for lossless round-trip
    pub(crate) offset: i64,
    pub(crate) size: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DownloadProgressEntry {
    pub(crate) name: String,
    // A7: u64 version counters end-to-end
    pub(crate) version: Vec<(u64, u64)>,
    pub(crate) block_indexes: Vec<i32>,
    pub(crate) block_size: i32,
    // A9: Raw enum value from wire, not string-collapsed
    pub(crate) update_type: i32,
}

fn message_type_of(message: &BepMessage) -> Result<i32, String> {
    let t = match message {
        BepMessage::Hello { .. } => return Err("hello uses dedicated hello packet".to_string()),
        BepMessage::ClusterConfig { .. } => MessageType_MESSAGE_TYPE_CLUSTER_CONFIG,
        BepMessage::Index { .. } => MessageType_MESSAGE_TYPE_INDEX,
        BepMessage::IndexUpdate { .. } => MessageType_MESSAGE_TYPE_INDEX_UPDATE,
        BepMessage::Request { .. } => MessageType_MESSAGE_TYPE_REQUEST,
        BepMessage::Response { .. } => MessageType_MESSAGE_TYPE_RESPONSE,
        BepMessage::DownloadProgress { .. } => MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS,
        BepMessage::Ping { .. } => MessageType_MESSAGE_TYPE_PING,
        BepMessage::Close { .. } => MessageType_MESSAGE_TYPE_CLOSE,
    };
    Ok(t)
}

// 8.8: Compression policy enum matching Go's protocol.Compression
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompressionPolicy {
    Never,
    Metadata, // compress only cluster_config, index, index_update
    Always,
}

fn should_compress(message: &BepMessage, payload_len: usize) -> bool {
    should_compress_with_policy(message, payload_len, CompressionPolicy::Metadata)
}

fn should_compress_with_policy(
    message: &BepMessage,
    payload_len: usize,
    policy: CompressionPolicy,
) -> bool {
    if payload_len < COMPRESSION_THRESHOLD_BYTES {
        return false;
    }
    match policy {
        CompressionPolicy::Never => false,
        CompressionPolicy::Always => !matches!(message, BepMessage::Response { .. }),
        CompressionPolicy::Metadata => matches!(
            message,
            BepMessage::ClusterConfig { .. }
                | BepMessage::Index { .. }
                | BepMessage::IndexUpdate { .. }
        ),
    }
}

pub(crate) fn encode_frame(message: &BepMessage) -> Result<Vec<u8>, String> {
    if matches!(message, BepMessage::Hello { .. }) {
        return encode_hello_packet(message);
    }

    let message_type = message_type_of(message)?;
    let payload = encode_payload(message)?;
    let mut compression = MessageCompression_MESSAGE_COMPRESSION_NONE;
    let mut encoded_payload = payload;

    if should_compress(message, encoded_payload.len()) {
        let compressed = lz4_compress_wire(&encoded_payload)?;
        let min_gain_threshold = encoded_payload
            .len()
            .saturating_sub(encoded_payload.len().saturating_div(32));
        if compressed.len() < min_gain_threshold {
            compression = MessageCompression_MESSAGE_COMPRESSION_LZ4;
            encoded_payload = compressed;
        }
    }

    if encoded_payload.len() > MAX_MESSAGE_LEN {
        return Err(format!(
            "payload too large: {} > {}",
            encoded_payload.len(),
            MAX_MESSAGE_LEN
        ));
    }

    let header = pb::Header {
        r#type: message_type,
        compression,
    };
    let header_bytes = proto_encode(&header, "header")?;
    if header_bytes.len() > MAX_HEADER_BYTES || header_bytes.len() > u16::MAX as usize {
        return Err(format!("header too large: {}", header_bytes.len()));
    }

    let mut out = Vec::with_capacity(2 + header_bytes.len() + 4 + encoded_payload.len());
    out.extend_from_slice(&(header_bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(&(encoded_payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&encoded_payload);
    Ok(out)
}

pub(crate) fn decode_frame(frame: &[u8]) -> Result<BepMessage, String> {
    if is_hello_packet(frame) {
        return decode_hello_packet(frame);
    }
    if frame.len() < 6 {
        return Err("frame too short".to_string());
    }

    let header_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    if header_len > MAX_HEADER_BYTES {
        return Err(format!(
            "header too large: {} > {}",
            header_len, MAX_HEADER_BYTES
        ));
    }
    let header_end = 2 + header_len;
    if frame.len() < header_end + 4 {
        return Err("invalid frame length: truncated header".to_string());
    }

    let header: pb::Header = proto_decode(&frame[2..header_end], "header")?;
    let message_len = u32::from_be_bytes([
        frame[header_end],
        frame[header_end + 1],
        frame[header_end + 2],
        frame[header_end + 3],
    ]) as usize;
    if message_len > MAX_MESSAGE_LEN {
        return Err(format!(
            "message length {} exceeds maximum {}",
            message_len, MAX_MESSAGE_LEN
        ));
    }
    let message_start = header_end + 4;
    if frame.len() != message_start + message_len {
        return Err(format!(
            "invalid frame length: header={} payload={} total={}",
            header_len,
            message_len,
            frame.len()
        ));
    }

    let payload = &frame[message_start..];
    let decoded_payload = match header.compression {
        MessageCompression_MESSAGE_COMPRESSION_NONE => payload.to_vec(),
        MessageCompression_MESSAGE_COMPRESSION_LZ4 => lz4_decompress_wire(payload)?,
        other => return Err(format!("unknown message compression {other}")),
    };

    decode_payload(header.r#type, &decoded_payload)
}

// A10: Old Syncthing v0.x magic number for legacy detection (Go: Version13HelloMagic)
const HELLO_MAGIC_OLD: u32 = 0x9F79BC40;

fn is_hello_packet(frame: &[u8]) -> bool {
    if frame.len() < 6 {
        return false;
    }
    let magic = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
    magic == HelloMessageMagic || magic == HELLO_MAGIC_OLD
}

fn encode_hello_packet(message: &BepMessage) -> Result<Vec<u8>, String> {
    // 8.1: preserve all wire fields in Hello
    let (device_name, client_name, client_version, num_connections, timestamp) = match message {
        BepMessage::Hello {
            device_name,
            client_name,
            client_version,
            num_connections,
            timestamp,
        } => (
            device_name,
            client_name,
            client_version,
            *num_connections,
            *timestamp,
        ),
        _ => return Err("encode_hello_packet requires hello message".to_string()),
    };
    let hello = pb::Hello {
        device_name: device_name.clone(),
        client_name: client_name.clone(),
        client_version: client_version.clone(),
        num_connections,
        timestamp,
    };
    let payload = proto_encode(&hello, "hello")?;
    if payload.len() > MAX_HELLO_BYTES {
        return Err(format!("hello message too big: {}", payload.len()));
    }
    let mut out = Vec::with_capacity(6 + payload.len());
    out.extend_from_slice(&HelloMessageMagic.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn decode_hello_packet(packet: &[u8]) -> Result<BepMessage, String> {
    if packet.len() < 6 {
        return Err("hello packet too short".to_string());
    }
    let magic = u32::from_be_bytes([packet[0], packet[1], packet[2], packet[3]]);
    // A10: Classify magic — old protocol vs unknown
    if magic == HELLO_MAGIC_OLD {
        return Err("old protocol version (v0.x)".to_string());
    }
    if magic != HelloMessageMagic {
        return Err("not a BEP connection".to_string());
    }
    let size = u16::from_be_bytes([packet[4], packet[5]]) as usize;
    if size > MAX_HELLO_BYTES {
        return Err(format!("hello message too big: {}", size));
    }
    if packet.len() != 6 + size {
        return Err("invalid hello packet length".to_string());
    }
    let hello: pb::Hello = proto_decode(&packet[6..], "hello")?;
    // 8.1/8.9: Map 1:1 — don't pack fields into client_name
    Ok(BepMessage::Hello {
        device_name: hello.device_name,
        client_name: hello.client_name,
        client_version: hello.client_version,
        num_connections: hello.num_connections,
        timestamp: hello.timestamp,
    })
}

fn encode_payload(message: &BepMessage) -> Result<Vec<u8>, String> {
    match message {
        BepMessage::Hello { .. } => Err("hello uses dedicated packet".to_string()),
        // 8.2: ClusterConfig encodes full folder/device data
        BepMessage::ClusterConfig { folders } => {
            let wire = pb::ClusterConfig {
                folders: folders
                    .iter()
                    .map(|f| pb::Folder {
                        id: f.id.clone(),
                        label: f.label.clone(),
                        devices: f
                            .devices
                            .iter()
                            .map(|d| pb::Device {
                                id: d.id.clone(),
                                name: d.name.clone(),
                                addresses: d.addresses.clone(),
                                compression: d.compression,
                                introducer: d.introducer,
                                index_id: d.index_id,
                                max_sequence: d.max_sequence,
                                encryption_password_token: d.encryption_password_token.clone(),
                                skip_introduction_removals: d.skip_introduction_removals,
                                cert_name: String::new(),
                            })
                            .collect(),
                        r#type: f.folder_type,
                        stop_reason: 0,
                    })
                    .collect(),
                secondary: false,
            };
            proto_encode(&wire, "cluster config")
        }
        BepMessage::Index {
            folder,
            files,
            last_sequence,
        } => {
            // A2: Use wire-preserved last_sequence verbatim — no fallback recompute
            let wire = pb::Index {
                folder: folder.clone(),
                files: files.iter().map(index_entry_to_wire).collect(),
                last_sequence: *last_sequence,
            };
            proto_encode(&wire, "index")
        }
        // 8.6: IndexUpdate carries prev_sequence from message
        // A1: IndexUpdate carries both last_sequence and prev_sequence
        BepMessage::IndexUpdate {
            folder,
            files,
            prev_sequence,
            last_sequence,
        } => {
            let wire = pb::IndexUpdate {
                folder: folder.clone(),
                files: files.iter().map(index_entry_to_wire).collect(),
                last_sequence: *last_sequence,
                prev_sequence: *prev_sequence,
            };
            proto_encode(&wire, "index update")
        }
        // 8.5: Request carries from_temporary and block_no
        // B2: id is i32 — pass through directly
        BepMessage::Request {
            id,
            folder,
            name,
            offset,
            size,
            hash,
            from_temporary,
            block_no,
        } => {
            let offset = i64::try_from(*offset)
                .map_err(|_| format!("request offset too large: {offset}"))?;
            let size =
                i32::try_from(*size).map_err(|_| format!("request size too large: {size}"))?;
            let wire = pb::Request {
                id: *id,
                folder: folder.clone(),
                name: name.clone(),
                offset,
                size,
                hash: hash.clone(),
                from_temporary: *from_temporary,
                block_no: *block_no,
            };
            proto_encode(&wire, "request")
        }
        // 8.7: Response code is now i32 — no clamping
        // B2: id is i32 — pass through directly
        BepMessage::Response {
            id,
            code,
            data,
            data_len: _,
        } => {
            let wire = pb::Response {
                id: *id,
                data: data.clone(),
                code: *code,
            };
            proto_encode(&wire, "response")
        }
        BepMessage::DownloadProgress { folder, updates } => {
            let wire = pb::DownloadProgress {
                folder: folder.clone(),
                updates: updates
                    .iter()
                    .map(|update| pb::FileDownloadProgressUpdate {
                        // A9: Raw enum value preserved from wire
                        update_type: update.update_type,
                        name: update.name.clone(),
                        // A7: u64 version vector
                        version: if update.version.is_empty() {
                            None
                        } else {
                            Some(pb::Vector {
                                counters: update
                                    .version
                                    .iter()
                                    .map(|(id, value)| pb::Counter {
                                        id: *id,
                                        value: *value,
                                    })
                                    .collect(),
                            })
                        },
                        block_indexes: update.block_indexes.clone(),
                        block_size: update.block_size,
                    })
                    .collect(),
            };
            proto_encode(&wire, "download progress")
        }
        BepMessage::Ping { .. } => proto_encode(&pb::Ping {}, "ping"),
        BepMessage::Close { reason } => proto_encode(
            &pb::Close {
                reason: reason.clone(),
            },
            "close",
        ),
    }
}

fn decode_payload(message_type: i32, payload: &[u8]) -> Result<BepMessage, String> {
    match message_type {
        // 8.2: ClusterConfig preserves full folder/device data
        MessageType_MESSAGE_TYPE_CLUSTER_CONFIG => {
            let msg: pb::ClusterConfig = proto_decode(payload, "cluster config")?;
            Ok(BepMessage::ClusterConfig {
                folders: msg
                    .folders
                    .into_iter()
                    .filter(|f| !f.id.is_empty())
                    .map(|f| ClusterConfigFolder {
                        id: f.id,
                        label: f.label,
                        folder_type: f.r#type,
                        devices: f
                            .devices
                            .into_iter()
                            .map(|d| ClusterConfigDevice {
                                id: d.id,
                                name: d.name,
                                addresses: d.addresses,
                                compression: d.compression,
                                introducer: d.introducer,
                                index_id: d.index_id,
                                max_sequence: d.max_sequence,
                                encryption_password_token: d.encryption_password_token,
                                skip_introduction_removals: d.skip_introduction_removals,
                            })
                            .collect(),
                    })
                    .collect(),
            })
        }
        MessageType_MESSAGE_TYPE_INDEX => {
            let msg: pb::Index = proto_decode(payload, "index")?;
            Ok(BepMessage::Index {
                folder: msg.folder,
                files: msg.files.iter().map(index_entry_from_wire).collect(),
                // B1: preserve wire last_sequence
                last_sequence: msg.last_sequence,
            })
        }
        // 8.6: IndexUpdate preserves prev_sequence
        // A1: IndexUpdate preserves both last_sequence and prev_sequence
        MessageType_MESSAGE_TYPE_INDEX_UPDATE => {
            let msg: pb::IndexUpdate = proto_decode(payload, "index update")?;
            Ok(BepMessage::IndexUpdate {
                folder: msg.folder,
                files: msg.files.iter().map(index_entry_from_wire).collect(),
                prev_sequence: msg.prev_sequence,
                last_sequence: msg.last_sequence,
            })
        }
        // 8.5: Request preserves from_temporary and block_no
        // B2: id passed through as i32 — no clamping
        MessageType_MESSAGE_TYPE_REQUEST => {
            let msg: pb::Request = proto_decode(payload, "request")?;
            Ok(BepMessage::Request {
                id: msg.id,
                folder: msg.folder,
                name: msg.name,
                offset: msg.offset,
                size: msg.size,
                hash: msg.hash,
                from_temporary: msg.from_temporary,
                block_no: msg.block_no,
            })
        }
        // 8.7: Response code is i32 — don't clamp negatives
        // B2: id passed through as i32 — no clamping
        MessageType_MESSAGE_TYPE_RESPONSE => {
            let msg: pb::Response = proto_decode(payload, "response")?;
            Ok(BepMessage::Response {
                id: msg.id,
                code: msg.code,
                data_len: msg.data.len() as u32,
                data: msg.data,
            })
        }
        MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS => {
            let msg: pb::DownloadProgress = proto_decode(payload, "download progress")?;
            Ok(BepMessage::DownloadProgress {
                folder: msg.folder,
                updates: msg
                    .updates
                    .into_iter()
                    .map(|u| DownloadProgressEntry {
                        name: u.name,
                        // A7: u64 version vector preserved from wire
                        version: u
                            .version
                            .as_ref()
                            .map(|v| v.counters.iter().map(|c| (c.id, c.value)).collect())
                            .unwrap_or_default(),
                        block_indexes: u.block_indexes,
                        block_size: u.block_size,
                        // A9: Raw enum value preserved
                        update_type: u.update_type,
                    })
                    .collect(),
            })
        }
        MessageType_MESSAGE_TYPE_PING => {
            let _: pb::Ping = proto_decode(payload, "ping")?;
            Ok(BepMessage::Ping { timestamp_ms: 0 })
        }
        MessageType_MESSAGE_TYPE_CLOSE => {
            let msg: pb::Close = proto_decode(payload, "close")?;
            Ok(BepMessage::Close { reason: msg.reason })
        }
        other => Err(format!("unknown message type {other}")),
    }
}

// 8.3: index_entry_to_wire preserves all FileInfo fields
fn index_entry_to_wire(entry: &IndexEntry) -> pb::FileInfo {
    pb::FileInfo {
        name: entry.path.clone(),
        r#type: entry.file_type,
        // A6: signed size preserved
        size: entry.size,
        permissions: entry.permissions,
        modified_s: entry.modified_s,
        modified_by: entry.modified_by,
        // A7: u64 version vector
        version: if entry.version_counters.is_empty() {
            None
        } else {
            Some(pb::Vector {
                counters: entry
                    .version_counters
                    .iter()
                    .map(|(id, value)| pb::Counter {
                        id: *id,
                        value: *value,
                    })
                    .collect(),
            })
        },
        // A6: signed sequence preserved
        sequence: entry.sequence,
        // A3: Full block tuples (hash, offset, size)
        blocks: entry
            .blocks
            .iter()
            .map(|b| pb::BlockInfo {
                hash: decode_hex_string(&b.hash),
                offset: b.offset,
                size: b.size,
            })
            .collect(),
        // A8: Symlink target as raw bytes
        symlink_target: entry.symlink_target.clone(),
        // A4: Preserved wire fields
        blocks_hash: entry.blocks_hash.clone(),
        previous_blocks_hash: entry.previous_blocks_hash.clone(),
        encrypted: entry.encrypted.clone(),
        modified_ns: entry.modified_ns,
        block_size: entry.block_size,
        platform: None,
        // BEP-2: local_flags must be 0 on wire — Go strips internal flags
        local_flags: 0,
        version_hash: Vec::new(),
        inode_change_ns: 0,
        encryption_trailer_size: 0,
        deleted: entry.deleted,
        invalid: entry.invalid,
        no_permissions: entry.no_permissions,
    }
}

// 8.3/8.4: index_entry_from_wire preserves all fields
fn index_entry_from_wire(file: &pb::FileInfo) -> IndexEntry {
    IndexEntry {
        path: file.name.clone(),
        // A6: Preserve signed sequence from wire — no .max(0) clamping
        sequence: file.sequence,
        deleted: file.deleted,
        // A6: Preserve signed size from wire — no .max(0) clamping
        size: file.size,
        // A3: Full block tuples (hash, offset, size)
        blocks: file
            .blocks
            .iter()
            .map(|b| BlockEntry {
                hash: encode_hex_string(&b.hash),
                offset: b.offset,
                size: b.size,
            })
            .collect(),
        file_type: file.r#type,
        permissions: file.permissions,
        modified_s: file.modified_s,
        modified_ns: file.modified_ns,
        modified_by: file.modified_by,
        no_permissions: file.no_permissions,
        invalid: file.invalid,
        // A5: Ignore incoming wire local_flags — Go treats them as untrusted
        local_flags: 0,
        // A8: Symlink target preserved as raw bytes
        symlink_target: file.symlink_target.clone(),
        block_size: file.block_size,
        // A7: u64 version vector from wire
        version_counters: file
            .version
            .as_ref()
            .map(|v| v.counters.iter().map(|c| (c.id, c.value)).collect())
            .unwrap_or_default(),
        // A4: Preserved wire fields
        blocks_hash: file.blocks_hash.clone(),
        previous_blocks_hash: file.previous_blocks_hash.clone(),
        encrypted: file.encrypted.clone(),
    }
}

/// Encode raw bytes as lowercase hex string (lossless)
fn encode_hex_string(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Decode hex string back to raw bytes; falls back to UTF-8 bytes if not valid hex
fn decode_hex_string(hex: &str) -> Vec<u8> {
    if hex.len() % 2 == 0 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        (0..hex.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
            .collect()
    } else {
        hex.as_bytes().to_vec()
    }
}

fn lz4_compress_wire(payload: &[u8]) -> Result<Vec<u8>, String> {
    if payload.len() > u32::MAX as usize {
        return Err(format!(
            "payload too large for lz4 prefix: {}",
            payload.len()
        ));
    }
    let compressed = lz4_flex::block::compress(payload);
    let mut out = Vec::with_capacity(4 + compressed.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

fn lz4_decompress_wire(payload: &[u8]) -> Result<Vec<u8>, String> {
    if payload.len() < 4 {
        return Err("lz4 payload too short for size prefix".to_string());
    }
    let declared = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    if declared > MAX_MESSAGE_LEN {
        return Err(format!(
            "declared decompressed payload too large: {} > {}",
            declared, MAX_MESSAGE_LEN
        ));
    }
    lz4_flex::block::decompress(&payload[4..], declared)
        .map_err(|err| format!("lz4 decompress payload: {err}"))
}

fn proto_encode<M: Message>(message: &M, what: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut out)
        .map_err(|err| format!("serialize {what}: {err}"))?;
    Ok(out)
}

fn proto_decode<M: Message + Default>(bytes: &[u8], what: &str) -> Result<M, String> {
    M::decode(bytes).map_err(|err| format!("decode {what}: {err}"))
}

pub(crate) fn default_exchange() -> Vec<BepMessage> {
    vec![
        BepMessage::Hello {
            device_name: "rust-node-a".to_string(),
            client_name: "syncthing-rs".to_string(),
            client_version: String::new(),
            num_connections: 0,
            timestamp: 0,
        },
        BepMessage::ClusterConfig {
            folders: vec![ClusterConfigFolder {
                id: "default".to_string(),
                label: String::new(),
                devices: Vec::new(),
                folder_type: 0,
            }],
        },
        BepMessage::Index {
            folder: "default".to_string(),
            files: vec![IndexEntry {
                path: "a.txt".to_string(),
                sequence: 1,
                deleted: false,
                size: 100,
                blocks: vec![BlockEntry {
                    hash: "aa".to_string(),
                    offset: 0,
                    size: 0,
                }],
                ..IndexEntry::default()
            }],
            last_sequence: 0,
        },
        BepMessage::IndexUpdate {
            folder: "default".to_string(),
            files: vec![IndexEntry {
                path: "a.txt".to_string(),
                sequence: 2,
                deleted: false,
                size: 110,
                blocks: vec![BlockEntry {
                    hash: "bb".to_string(),
                    offset: 0,
                    size: 0,
                }],
                ..IndexEntry::default()
            }],
            prev_sequence: 0,
            last_sequence: 0,
        },
        BepMessage::Request {
            id: 1,
            folder: "default".to_string(),
            name: "a.txt".to_string(),
            offset: 0,
            size: 110,
            hash: Vec::new(),
            from_temporary: false,
            block_no: 0,
        },
        BepMessage::Response {
            id: 1,
            code: 0,
            data_len: 110,
            data: Vec::new(),
        },
        BepMessage::DownloadProgress {
            folder: "default".to_string(),
            updates: vec![DownloadProgressEntry {
                name: "a.txt".to_string(),
                version: vec![(0, 2)],
                block_indexes: vec![0],
                block_size: 131_072,
                // A9: 0 = FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_APPEND
                update_type: 0,
            }],
        },
        BepMessage::Ping {
            timestamp_ms: 1_738_958_400_000,
        },
        BepMessage::Close {
            reason: "normal shutdown".to_string(),
        },
    ]
}

pub(crate) fn message_name(message: &BepMessage) -> &'static str {
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
    use std::panic;

    fn parse_header(frame: &[u8]) -> pb::Header {
        let header_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        proto_decode(&frame[2..(2 + header_len)], "header").expect("decode header")
    }

    #[test]
    fn frame_round_trip() {
        let msg = BepMessage::IndexUpdate {
            folder: "default".to_string(),
            files: vec![IndexEntry {
                path: "file.txt".to_string(),
                sequence: 42,
                deleted: false,
                size: 900,
                blocks: vec![BlockEntry {
                    hash: "ab12".to_string(),
                    offset: 0,
                    size: 0,
                }],
                ..IndexEntry::default()
            }],
            prev_sequence: 0,
            last_sequence: 0,
        };

        let frame = encode_frame(&msg).expect("encode");
        let decoded = decode_frame(&frame).expect("decode");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn hello_round_trip_uses_magic_packet() {
        let msg = BepMessage::Hello {
            device_name: "a".to_string(),
            client_name: "b".to_string(),
            client_version: String::new(),
            num_connections: 0,
            timestamp: 0,
        };
        let frame = encode_frame(&msg).expect("encode hello");
        assert_eq!(
            u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]),
            HelloMessageMagic
        );
        let decoded = decode_frame(&frame).expect("decode hello");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn detects_frame_corruption() {
        let msg = BepMessage::Ping { timestamp_ms: 7 };
        let mut frame = encode_frame(&msg).expect("encode");
        frame[0] = 0xFF;
        frame[1] = 0xFF;

        let err = decode_frame(&frame).expect_err("must fail");
        assert!(err.contains("header too large"));
    }

    #[test]
    fn response_message_uses_uncompressed_payload() {
        let message = BepMessage::Response {
            id: 1,
            code: 0,
            data_len: 11,
            data: Vec::new(),
        };
        let frame = encode_frame(&message).expect("encode");
        let header = parse_header(&frame);
        assert_eq!(
            header.compression,
            MessageCompression_MESSAGE_COMPRESSION_NONE
        );
    }

    #[test]
    fn large_messages_can_use_lz4_compression() {
        let big = "0123456789abcdef".repeat(1024);
        let message = BepMessage::IndexUpdate {
            folder: "default".to_string(),
            files: vec![IndexEntry {
                path: "big.bin".to_string(),
                sequence: 7,
                deleted: false,
                size: 1024 * 16,
                blocks: vec![
                    BlockEntry {
                        hash: big.clone(),
                        offset: 0,
                        size: 0,
                    },
                    BlockEntry {
                        hash: big,
                        offset: 0,
                        size: 0,
                    },
                ],
                ..IndexEntry::default()
            }],
            prev_sequence: 0,
            last_sequence: 0,
        };

        let frame = encode_frame(&message).expect("encode");
        let header = parse_header(&frame);
        assert_eq!(
            header.compression,
            MessageCompression_MESSAGE_COMPRESSION_LZ4
        );
        let decoded = decode_frame(&frame).expect("decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn rejects_oversized_uncompressed_payload_before_decode() {
        let header = pb::Header {
            r#type: MessageType_MESSAGE_TYPE_PING,
            compression: MessageCompression_MESSAGE_COMPRESSION_NONE,
        };
        let header_bytes = proto_encode(&header, "header").expect("encode header");
        let payload = vec![0_u8; MAX_MESSAGE_LEN + 1];
        let mut frame = Vec::new();
        frame.extend_from_slice(&(header_bytes.len() as u16).to_be_bytes());
        frame.extend_from_slice(&header_bytes);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);

        let err = decode_frame(&frame).expect_err("must reject oversized payload");
        assert!(err.contains("message length") || err.contains("payload too large"));
    }

    #[test]
    fn lz4_prefix_uses_big_endian_size() {
        let data = vec![1_u8, 2_u8, 3_u8, 4_u8];
        let compressed = lz4_compress_wire(&data).expect("compress");
        let declared =
            u32::from_be_bytes([compressed[0], compressed[1], compressed[2], compressed[3]]);
        assert_eq!(declared, data.len() as u32);
        let out = lz4_decompress_wire(&compressed).expect("decompress");
        assert_eq!(out, data);
    }

    #[test]
    fn decode_random_frames_never_panics() {
        let mut seed: u64 = 0xA1B2_C3D4_E5F6_1020;
        for _ in 0..512 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let len = ((seed >> 32) as usize) % 4096;
            let mut data = vec![0_u8; len];
            for byte in &mut data {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                *byte = (seed >> 56) as u8;
            }
            let result = panic::catch_unwind(|| {
                let _ = decode_frame(&data);
            });
            assert!(result.is_ok(), "decode_frame panicked for len={len}");
        }
    }
}
