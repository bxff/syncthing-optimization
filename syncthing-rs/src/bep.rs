use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

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
    Ping {
        timestamp_ms: u64,
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

pub(crate) fn encode_frame(message: &BepMessage) -> Result<Vec<u8>, String> {
    let payload = serde_json::to_vec(message).map_err(|err| format!("serialize message: {err}"))?;
    if payload.len() > u32::MAX as usize {
        return Err(format!("payload too large: {}", payload.len()));
    }

    let mut hasher = Hasher::new();
    hasher.update(&payload);
    let checksum = hasher.finalize();

    let mut out = Vec::with_capacity(payload.len() + 8);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

pub(crate) fn decode_frame(frame: &[u8]) -> Result<BepMessage, String> {
    if frame.len() < 8 {
        return Err("frame too short".to_string());
    }

    let len = u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    let checksum = u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);
    if frame.len() != len + 8 {
        return Err(format!(
            "invalid frame length: header={len} body={} total={}",
            frame.len().saturating_sub(8),
            frame.len()
        ));
    }

    let payload = &frame[8..];
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let observed = hasher.finalize();
    if observed != checksum {
        return Err(format!(
            "checksum mismatch: expected={checksum} observed={observed}"
        ));
    }

    serde_json::from_slice(payload).map_err(|err| format!("decode message: {err}"))
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
        BepMessage::Ping {
            timestamp_ms: 1_738_958_400_000,
        },
    ]
}

pub(crate) fn message_name(message: &BepMessage) -> &'static str {
    match message {
        BepMessage::Hello { .. } => "hello",
        BepMessage::ClusterConfig { .. } => "cluster_config",
        BepMessage::Index { .. } => "index",
        BepMessage::IndexUpdate { .. } => "index_update",
        BepMessage::Ping { .. } => "ping",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn detects_checksum_corruption() {
        let msg = BepMessage::Ping { timestamp_ms: 7 };
        let mut frame = encode_frame(&msg).expect("encode");
        let idx = frame.len() - 1;
        frame[idx] ^= 0x01;

        let err = decode_frame(&frame).expect_err("must fail");
        assert!(err.contains("checksum mismatch"));
    }
}
