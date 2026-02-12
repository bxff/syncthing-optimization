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

const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_MESSAGE_LEN: usize = 500 * 1_000_000;
const MAX_HELLO_BYTES: usize = 32_767;
const COMPRESSION_THRESHOLD_BYTES: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum BepMessage {
    Hello {
        device_name: String,
        client_name: String,
    },
    ClusterConfig {
        folders: Vec<String>,
    },
    Index {
        folder: String,
        files: Vec<IndexEntry>,
    },
    IndexUpdate {
        folder: String,
        files: Vec<IndexEntry>,
    },
    Request {
        id: u32,
        folder: String,
        name: String,
        offset: u64,
        size: u32,
        hash: String,
    },
    Response {
        id: u32,
        code: u32,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IndexEntry {
    pub(crate) path: String,
    pub(crate) sequence: u64,
    pub(crate) deleted: bool,
    pub(crate) size: u64,
    pub(crate) block_hashes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DownloadProgressEntry {
    pub(crate) name: String,
    pub(crate) version: u64,
    pub(crate) block_indexes: Vec<u32>,
    pub(crate) block_size: u32,
    pub(crate) update_type: String,
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

fn should_compress(message: &BepMessage, payload_len: usize) -> bool {
    !matches!(message, BepMessage::Response { .. }) && payload_len >= COMPRESSION_THRESHOLD_BYTES
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

fn is_hello_packet(frame: &[u8]) -> bool {
    frame.len() >= 6
        && u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) == HelloMessageMagic
}

fn encode_hello_packet(message: &BepMessage) -> Result<Vec<u8>, String> {
    let (device_name, client_name) = match message {
        BepMessage::Hello {
            device_name,
            client_name,
        } => (device_name, client_name),
        _ => return Err("encode_hello_packet requires hello message".to_string()),
    };
    let hello = pb::Hello {
        device_name: device_name.clone(),
        client_name: client_name.clone(),
        client_version: String::new(),
        num_connections: 0,
        timestamp: 0,
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
    if magic != HelloMessageMagic {
        return Err("unknown hello magic".to_string());
    }
    let size = u16::from_be_bytes([packet[4], packet[5]]) as usize;
    if size > MAX_HELLO_BYTES {
        return Err(format!("hello message too big: {}", size));
    }
    if packet.len() != 6 + size {
        return Err("invalid hello packet length".to_string());
    }
    let hello: pb::Hello = proto_decode(&packet[6..], "hello")?;
    Ok(BepMessage::Hello {
        device_name: hello.device_name,
        client_name: hello.client_name,
    })
}

fn encode_payload(message: &BepMessage) -> Result<Vec<u8>, String> {
    match message {
        BepMessage::Hello { .. } => Err("hello uses dedicated packet".to_string()),
        BepMessage::ClusterConfig { folders } => {
            let wire = pb::ClusterConfig {
                folders: folders
                    .iter()
                    .map(|id| pb::Folder {
                        id: id.clone(),
                        label: String::new(),
                        devices: Vec::new(),
                        r#type: 0,
                        stop_reason: 0,
                    })
                    .collect(),
                secondary: false,
            };
            proto_encode(&wire, "cluster config")
        }
        BepMessage::Index { folder, files } => {
            let last_sequence = files.iter().map(|f| f.sequence as i64).max().unwrap_or(0);
            let wire = pb::Index {
                folder: folder.clone(),
                files: files.iter().map(index_entry_to_wire).collect(),
                last_sequence,
            };
            proto_encode(&wire, "index")
        }
        BepMessage::IndexUpdate { folder, files } => {
            let last_sequence = files.iter().map(|f| f.sequence as i64).max().unwrap_or(0);
            let wire = pb::IndexUpdate {
                folder: folder.clone(),
                files: files.iter().map(index_entry_to_wire).collect(),
                last_sequence,
                prev_sequence: 0,
            };
            proto_encode(&wire, "index update")
        }
        BepMessage::Request {
            id,
            folder,
            name,
            offset,
            size,
            hash,
        } => {
            let id = i32::try_from(*id).map_err(|_| format!("request id too large: {id}"))?;
            let offset = i64::try_from(*offset)
                .map_err(|_| format!("request offset too large: {offset}"))?;
            let size =
                i32::try_from(*size).map_err(|_| format!("request size too large: {size}"))?;
            let wire = pb::Request {
                id,
                folder: folder.clone(),
                name: name.clone(),
                offset,
                size,
                hash: hash.as_bytes().to_vec(),
                from_temporary: false,
                block_no: 0,
            };
            proto_encode(&wire, "request")
        }
        BepMessage::Response {
            id,
            code,
            data,
            data_len: _,
        } => {
            let id = i32::try_from(*id).map_err(|_| format!("response id too large: {id}"))?;
            let code =
                i32::try_from(*code).map_err(|_| format!("response code too large: {code}"))?;
            let wire = pb::Response {
                id,
                data: data.clone(),
                code,
            };
            proto_encode(&wire, "response")
        }
        BepMessage::DownloadProgress { folder, updates } => {
            let wire = pb::DownloadProgress {
                folder: folder.clone(),
                updates: updates
                    .iter()
                    .map(|update| pb::FileDownloadProgressUpdate {
                        update_type: if update.update_type.eq_ignore_ascii_case("forget") {
                            FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_FORGET
                        } else {
                            FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_APPEND
                        },
                        name: update.name.clone(),
                        version: Some(pb::Vector {
                            counters: vec![pb::Counter {
                                id: 0,
                                value: update.version,
                            }],
                        }),
                        block_indexes: update
                            .block_indexes
                            .iter()
                            .map(|idx| i32::try_from(*idx).unwrap_or(i32::MAX))
                            .collect(),
                        block_size: i32::try_from(update.block_size).unwrap_or(i32::MAX),
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
        MessageType_MESSAGE_TYPE_CLUSTER_CONFIG => {
            let msg: pb::ClusterConfig = proto_decode(payload, "cluster config")?;
            Ok(BepMessage::ClusterConfig {
                folders: msg
                    .folders
                    .into_iter()
                    .map(|f| f.id)
                    .filter(|id| !id.is_empty())
                    .collect(),
            })
        }
        MessageType_MESSAGE_TYPE_INDEX => {
            let msg: pb::Index = proto_decode(payload, "index")?;
            Ok(BepMessage::Index {
                folder: msg.folder,
                files: msg.files.iter().map(index_entry_from_wire).collect(),
            })
        }
        MessageType_MESSAGE_TYPE_INDEX_UPDATE => {
            let msg: pb::IndexUpdate = proto_decode(payload, "index update")?;
            Ok(BepMessage::IndexUpdate {
                folder: msg.folder,
                files: msg.files.iter().map(index_entry_from_wire).collect(),
            })
        }
        MessageType_MESSAGE_TYPE_REQUEST => {
            let msg: pb::Request = proto_decode(payload, "request")?;
            Ok(BepMessage::Request {
                id: msg.id.max(0) as u32,
                folder: msg.folder,
                name: msg.name,
                offset: msg.offset.max(0) as u64,
                size: msg.size.max(0) as u32,
                hash: String::from_utf8_lossy(&msg.hash).to_string(),
            })
        }
        MessageType_MESSAGE_TYPE_RESPONSE => {
            let msg: pb::Response = proto_decode(payload, "response")?;
            Ok(BepMessage::Response {
                id: msg.id.max(0) as u32,
                code: msg.code.max(0) as u32,
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
                        version: u
                            .version
                            .as_ref()
                            .and_then(|v| v.counters.iter().map(|c| c.value).max())
                            .unwrap_or_default(),
                        block_indexes: u
                            .block_indexes
                            .into_iter()
                            .map(|idx| idx.max(0) as u32)
                            .collect(),
                        block_size: u.block_size.max(0) as u32,
                        update_type: if u.update_type
                            == FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_FORGET
                        {
                            "forget".to_string()
                        } else {
                            "append".to_string()
                        },
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

fn index_entry_to_wire(entry: &IndexEntry) -> pb::FileInfo {
    pb::FileInfo {
        name: entry.path.clone(),
        r#type: 0,
        size: entry.size as i64,
        permissions: 0,
        modified_s: 0,
        modified_by: 0,
        version: None,
        sequence: entry.sequence as i64,
        blocks: entry
            .block_hashes
            .iter()
            .map(|hash| pb::BlockInfo {
                hash: hash.as_bytes().to_vec(),
                offset: 0,
                size: 0,
            })
            .collect(),
        symlink_target: Vec::new(),
        blocks_hash: Vec::new(),
        previous_blocks_hash: Vec::new(),
        encrypted: Vec::new(),
        modified_ns: 0,
        block_size: 0,
        platform: None,
        local_flags: 0,
        version_hash: Vec::new(),
        inode_change_ns: 0,
        encryption_trailer_size: 0,
        deleted: entry.deleted,
        invalid: false,
        no_permissions: false,
    }
}

fn index_entry_from_wire(file: &pb::FileInfo) -> IndexEntry {
    IndexEntry {
        path: file.name.clone(),
        sequence: file.sequence.max(0) as u64,
        deleted: file.deleted,
        size: file.size.max(0) as u64,
        block_hashes: file
            .blocks
            .iter()
            .map(|b| String::from_utf8_lossy(&b.hash).to_string())
            .collect(),
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
        },
        BepMessage::ClusterConfig {
            folders: vec!["default".to_string()],
        },
        BepMessage::Index {
            folder: "default".to_string(),
            files: vec![IndexEntry {
                path: "a.txt".to_string(),
                sequence: 1,
                deleted: false,
                size: 100,
                block_hashes: vec!["h1".to_string()],
            }],
        },
        BepMessage::IndexUpdate {
            folder: "default".to_string(),
            files: vec![IndexEntry {
                path: "a.txt".to_string(),
                sequence: 2,
                deleted: false,
                size: 110,
                block_hashes: vec!["h2".to_string()],
            }],
        },
        BepMessage::Request {
            id: 1,
            folder: "default".to_string(),
            name: "a.txt".to_string(),
            offset: 0,
            size: 110,
            hash: "h2".to_string(),
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
                version: 2,
                block_indexes: vec![0],
                block_size: 131_072,
                update_type: "append".to_string(),
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
                block_hashes: vec!["ab12".to_string()],
            }],
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
                block_hashes: vec![big.clone(), big],
            }],
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
