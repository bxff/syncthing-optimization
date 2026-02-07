use crate::bep_core::{
    Header, MessageCompression_MESSAGE_COMPRESSION_LZ4,
    MessageCompression_MESSAGE_COMPRESSION_NONE, MessageType_MESSAGE_TYPE_CLOSE,
    MessageType_MESSAGE_TYPE_CLUSTER_CONFIG, MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS,
    MessageType_MESSAGE_TYPE_INDEX, MessageType_MESSAGE_TYPE_INDEX_UPDATE,
    MessageType_MESSAGE_TYPE_PING, MessageType_MESSAGE_TYPE_REQUEST,
    MessageType_MESSAGE_TYPE_RESPONSE,
};
use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use serde::{Deserialize, Serialize};

const MAX_HEADER_BYTES: usize = 32 * 1024;
const COMPRESSION_THRESHOLD_BYTES: usize = 128;
const MESSAGE_TYPE_HELLO: i32 = -1;

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

fn message_type_of(message: &BepMessage) -> i32 {
    match message {
        BepMessage::Hello { .. } => MESSAGE_TYPE_HELLO,
        BepMessage::ClusterConfig { .. } => MessageType_MESSAGE_TYPE_CLUSTER_CONFIG,
        BepMessage::Index { .. } => MessageType_MESSAGE_TYPE_INDEX,
        BepMessage::IndexUpdate { .. } => MessageType_MESSAGE_TYPE_INDEX_UPDATE,
        BepMessage::Request { .. } => MessageType_MESSAGE_TYPE_REQUEST,
        BepMessage::Response { .. } => MessageType_MESSAGE_TYPE_RESPONSE,
        BepMessage::DownloadProgress { .. } => MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS,
        BepMessage::Ping { .. } => MessageType_MESSAGE_TYPE_PING,
        BepMessage::Close { .. } => MessageType_MESSAGE_TYPE_CLOSE,
    }
}

fn should_compress(message: &BepMessage, payload_len: usize) -> bool {
    !matches!(message, BepMessage::Response { .. }) && payload_len >= COMPRESSION_THRESHOLD_BYTES
}

pub(crate) fn encode_frame(message: &BepMessage) -> Result<Vec<u8>, String> {
    let payload =
        serde_json::to_vec(message).map_err(|err| format!("serialize message payload: {err}"))?;
    let mut compression = MessageCompression_MESSAGE_COMPRESSION_NONE;
    let mut encoded_payload = payload;

    if should_compress(message, encoded_payload.len()) {
        let compressed = compress_prepend_size(&encoded_payload);
        // Mirror Syncthing's approach: compress only when savings are meaningful.
        let min_gain_threshold = encoded_payload
            .len()
            .saturating_sub(encoded_payload.len().saturating_div(32));
        if compressed.len() < min_gain_threshold {
            compression = MessageCompression_MESSAGE_COMPRESSION_LZ4;
            encoded_payload = compressed;
        }
    }

    if encoded_payload.len() > u32::MAX as usize {
        return Err(format!("payload too large: {}", encoded_payload.len()));
    }

    let header = Header {
        Type: message_type_of(message),
        Compression: compression,
    };
    let header_bytes =
        serde_json::to_vec(&header).map_err(|err| format!("serialize header: {err}"))?;
    if header_bytes.len() > u16::MAX as usize {
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

    let header: Header = serde_json::from_slice(&frame[2..header_end])
        .map_err(|err| format!("decode header: {err}"))?;
    let message_len = u32::from_be_bytes([
        frame[header_end],
        frame[header_end + 1],
        frame[header_end + 2],
        frame[header_end + 3],
    ]) as usize;
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
    let decoded_payload = match header.Compression {
        MessageCompression_MESSAGE_COMPRESSION_NONE => payload.to_vec(),
        MessageCompression_MESSAGE_COMPRESSION_LZ4 => decompress_size_prepended(payload)
            .map_err(|err| format!("lz4 decompress payload: {err}"))?,
        other => return Err(format!("unknown message compression {other}")),
    };

    let message: BepMessage = serde_json::from_slice(&decoded_payload)
        .map_err(|err| format!("decode message payload: {err}"))?;
    if message_type_of(&message) != header.Type {
        return Err(format!(
            "message type mismatch: header={} payload={}",
            header.Type,
            message_type_of(&message)
        ));
    }

    Ok(message)
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
    use crate::bep_core::{Header, MessageCompression_MESSAGE_COMPRESSION_NONE};

    fn parse_header(frame: &[u8]) -> Header {
        let header_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        serde_json::from_slice(&frame[2..(2 + header_len)]).expect("decode header")
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
            header.Compression,
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
            header.Compression,
            MessageCompression_MESSAGE_COMPRESSION_LZ4
        );
        let decoded = decode_frame(&frame).expect("decode");
        assert_eq!(decoded, message);
    }
}
