#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

pub(crate) const CompressionNever: i32 = 0;
pub(crate) const CompressionAlways: i32 = 1;
pub(crate) const CompressionMetadata: i32 = 2;
pub(crate) const Compression_COMPRESSION_NEVER: i32 = CompressionNever;
pub(crate) const Compression_COMPRESSION_ALWAYS: i32 = CompressionAlways;
pub(crate) const Compression_COMPRESSION_METADATA: i32 = CompressionMetadata;

pub(crate) const ErrorCodeNoError: i32 = 0;
pub(crate) const ErrorCodeGeneric: i32 = 1;
pub(crate) const ErrorCodeNoSuchFile: i32 = 2;
pub(crate) const ErrorCodeInvalidFile: i32 = 3;
pub(crate) const ErrorCode_ERROR_CODE_NO_ERROR: i32 = ErrorCodeNoError;
pub(crate) const ErrorCode_ERROR_CODE_GENERIC: i32 = ErrorCodeGeneric;
pub(crate) const ErrorCode_ERROR_CODE_NO_SUCH_FILE: i32 = ErrorCodeNoSuchFile;
pub(crate) const ErrorCode_ERROR_CODE_INVALID_FILE: i32 = ErrorCodeInvalidFile;

pub(crate) const FileDownloadProgressUpdateTypeAppend: i32 = 0;
pub(crate) const FileDownloadProgressUpdateTypeForget: i32 = 1;
pub(crate) const FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_APPEND: i32 =
    FileDownloadProgressUpdateTypeAppend;
pub(crate) const FileDownloadProgressUpdateType_FILE_DOWNLOAD_PROGRESS_UPDATE_TYPE_FORGET: i32 =
    FileDownloadProgressUpdateTypeForget;

pub(crate) const FileInfoTypeFile: i32 = 0;
pub(crate) const FileInfoTypeDirectory: i32 = 1;
pub(crate) const FileInfoTypeSymlinkFile: i32 = 2;
pub(crate) const FileInfoTypeSymlinkDirectory: i32 = 3;
pub(crate) const FileInfoTypeSymlink: i32 = 4;
pub(crate) const FileInfoType_FILE_INFO_TYPE_FILE: i32 = FileInfoTypeFile;
pub(crate) const FileInfoType_FILE_INFO_TYPE_DIRECTORY: i32 = FileInfoTypeDirectory;
pub(crate) const FileInfoType_FILE_INFO_TYPE_SYMLINK_FILE: i32 = FileInfoTypeSymlinkFile;
pub(crate) const FileInfoType_FILE_INFO_TYPE_SYMLINK_DIRECTORY: i32 = FileInfoTypeSymlinkDirectory;
pub(crate) const FileInfoType_FILE_INFO_TYPE_SYMLINK: i32 = FileInfoTypeSymlink;

pub(crate) const FolderStopReasonRunning: i32 = 0;
pub(crate) const FolderStopReasonPaused: i32 = 1;
pub(crate) const FolderStopReason_FOLDER_STOP_REASON_RUNNING: i32 = FolderStopReasonRunning;
pub(crate) const FolderStopReason_FOLDER_STOP_REASON_PAUSED: i32 = FolderStopReasonPaused;

pub(crate) const FolderTypeSendReceive: i32 = 0;
pub(crate) const FolderTypeSendOnly: i32 = 1;
pub(crate) const FolderTypeReceiveOnly: i32 = 2;
pub(crate) const FolderTypeReceiveEncrypted: i32 = 3;
pub(crate) const FolderType_FOLDER_TYPE_SEND_RECEIVE: i32 = FolderTypeSendReceive;
pub(crate) const FolderType_FOLDER_TYPE_SEND_ONLY: i32 = FolderTypeSendOnly;
pub(crate) const FolderType_FOLDER_TYPE_RECEIVE_ONLY: i32 = FolderTypeReceiveOnly;
pub(crate) const FolderType_FOLDER_TYPE_RECEIVE_ENCRYPTED: i32 = FolderTypeReceiveEncrypted;

pub(crate) const MessageCompression_MESSAGE_COMPRESSION_NONE: i32 = 0;
pub(crate) const MessageCompression_MESSAGE_COMPRESSION_LZ4: i32 = 1;

pub(crate) const MessageType_MESSAGE_TYPE_CLUSTER_CONFIG: i32 = 0;
pub(crate) const MessageType_MESSAGE_TYPE_INDEX: i32 = 1;
pub(crate) const MessageType_MESSAGE_TYPE_INDEX_UPDATE: i32 = 2;
pub(crate) const MessageType_MESSAGE_TYPE_REQUEST: i32 = 3;
pub(crate) const MessageType_MESSAGE_TYPE_RESPONSE: i32 = 4;
pub(crate) const MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS: i32 = 5;
pub(crate) const MessageType_MESSAGE_TYPE_PING: i32 = 6;
pub(crate) const MessageType_MESSAGE_TYPE_CLOSE: i32 = 7;

pub(crate) const FlagLocalUnsupported: u32 = 1 << 0;
pub(crate) const FlagLocalIgnored: u32 = 1 << 1;
pub(crate) const FlagLocalMustRescan: u32 = 1 << 2;
pub(crate) const FlagLocalReceiveOnly: u32 = 1 << 3;
pub(crate) const FlagLocalGlobal: u32 = 1 << 4;
pub(crate) const FlagLocalNeeded: u32 = 1 << 5;
pub(crate) const FlagLocalRemoteInvalid: u32 = 1 << 6;
pub(crate) const LocalAllFlags: u32 = FlagLocalIgnored
    | FlagLocalMustRescan
    | FlagLocalReceiveOnly
    | FlagLocalRemoteInvalid
    | FlagLocalNeeded
    | FlagLocalUnsupported
    | FlagLocalGlobal;
pub(crate) const LocalConflictFlags: u32 =
    FlagLocalUnsupported | FlagLocalIgnored | FlagLocalReceiveOnly;
pub(crate) const LocalInvalidFlags: u32 = FlagLocalUnsupported
    | FlagLocalIgnored
    | FlagLocalMustRescan
    | FlagLocalReceiveOnly
    | FlagLocalRemoteInvalid;

pub(crate) const HelloMessageMagic: u32 = 0x2EA7D90B;
pub(crate) const Version13HelloMagic: u32 = 0x9F79BC40;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct BlockInfo {
    pub(crate) Hash: Vec<u8>,
    pub(crate) Offset: i64,
    pub(crate) Size: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Close {
    pub(crate) Reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct ClusterConfig {
    pub(crate) Folders: Vec<Folder>,
    pub(crate) Secondary: bool,
}

impl ClusterConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.Folders.is_empty() {
            return Err("cluster config requires at least one folder".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Counter {
    pub(crate) Id: i64,
    pub(crate) Value: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Device {
    pub(crate) Id: String,
    pub(crate) Name: String,
    pub(crate) Addresses: Vec<String>,
    pub(crate) Compression: i32,
    pub(crate) Introducer: bool,
    pub(crate) IndexId: u64,
    pub(crate) MaxSequence: i64,
    pub(crate) CertName: String,
    pub(crate) EncryptionPasswordToken: String,
    pub(crate) SkipIntroductionRemovals: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct DownloadProgress {
    pub(crate) Folder: String,
    pub(crate) Updates: Vec<FileDownloadProgressUpdate>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct FileDownloadProgressUpdate {
    pub(crate) Name: String,
    pub(crate) Version: Option<Vector>,
    pub(crate) BlockIndexes: Vec<i32>,
    pub(crate) BlockSize: i32,
    pub(crate) UpdateType: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct FileInfo {
    pub(crate) Name: String,
    pub(crate) Type: i32,
    pub(crate) Size: i64,
    pub(crate) Permissions: u32,
    pub(crate) ModifiedS: i64,
    pub(crate) ModifiedNs: i32,
    pub(crate) ModifiedBy: i64,
    pub(crate) Deleted: bool,
    pub(crate) Invalid: bool,
    pub(crate) NoPermissions: bool,
    pub(crate) Sequence: i64,
    pub(crate) BlockSize: i32,
    pub(crate) Blocks: Vec<BlockInfo>,
    pub(crate) SymlinkTarget: String,
    pub(crate) LocalFlags: u32,
    pub(crate) VersionHash: Vec<u8>,
    pub(crate) InodeChangeNs: i64,
    pub(crate) EncryptionTrailerSize: i64,
    pub(crate) BlocksHash: Vec<u8>,
    pub(crate) Platform: PlatformData,
    pub(crate) Version: Vector,
    pub(crate) Encrypted: bool,
    pub(crate) PreviousBlocksHash: Vec<u8>,
}

impl FileInfo {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.Name.is_empty() {
            return Err("file info name is required".to_string());
        }
        if self.Type == FileInfoTypeDirectory && !self.Blocks.is_empty() {
            return Err("directories must not contain block payloads".to_string());
        }
        if self.Deleted && self.Size > 0 {
            return Err("deleted files must not advertise positive size".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct FileInfoComparison {
    pub(crate) ModTimeWindow: i64,
    pub(crate) IgnorePerms: bool,
    pub(crate) IgnoreBlocks: bool,
    pub(crate) IgnoreFlags: bool,
    pub(crate) IgnoreOwnership: bool,
    pub(crate) IgnoreXattrs: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Folder {
    pub(crate) Id: String,
    pub(crate) Label: String,
    pub(crate) Devices: Vec<Device>,
    pub(crate) Type: i32,
    pub(crate) StopReason: i32,
}

impl Folder {
    pub(crate) fn is_active(&self) -> bool {
        self.StopReason == FolderStopReasonRunning
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Header {
    pub(crate) Type: i32,
    pub(crate) Compression: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Hello {
    pub(crate) DeviceName: String,
    pub(crate) ClientName: String,
    pub(crate) ClientVersion: String,
    pub(crate) NumConnections: i32,
    pub(crate) Timestamp: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Index {
    pub(crate) Folder: String,
    pub(crate) Files: Vec<FileInfo>,
    pub(crate) LastSequence: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct IndexUpdate {
    pub(crate) Folder: String,
    pub(crate) Files: Vec<FileInfo>,
    pub(crate) LastSequence: i64,
    pub(crate) PrevSequence: i64,
}

impl IndexUpdate {
    pub(crate) fn validate_sequence(&self) -> Result<(), String> {
        if self.LastSequence < self.PrevSequence {
            return Err("index update sequence regressed".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Ping {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct PlatformData {
    pub(crate) Unix: Option<UnixData>,
    pub(crate) Linux: Option<UnixData>,
    pub(crate) Darwin: Option<UnixData>,
    pub(crate) Freebsd: Option<UnixData>,
    pub(crate) Netbsd: Option<UnixData>,
    pub(crate) Windows: Option<WindowsData>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Request {
    pub(crate) Id: i32,
    pub(crate) Folder: String,
    pub(crate) Name: String,
    pub(crate) Offset: i64,
    pub(crate) Size: i32,
    pub(crate) Hash: Vec<u8>,
    pub(crate) FromTemporary: bool,
    pub(crate) BlockNo: i32,
}

impl Request {
    pub(crate) fn end_offset(&self) -> i64 {
        self.Offset.saturating_add(self.Size as i64)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Response {
    pub(crate) Id: i32,
    pub(crate) Code: i32,
    pub(crate) Data: Vec<u8>,
}

impl Response {
    pub(crate) fn is_success(&self) -> bool {
        self.Code == ErrorCodeNoError
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct UnixData {
    pub(crate) Uid: i32,
    pub(crate) Gid: i32,
    pub(crate) OwnerName: String,
    pub(crate) GroupName: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Vector {
    pub(crate) Counters: Vec<Counter>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct WindowsData {
    pub(crate) OwnerName: String,
    pub(crate) OwnerIsGroup: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct Xattr {
    pub(crate) Name: String,
    pub(crate) Value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct XattrData {
    pub(crate) Xattrs: Vec<Xattr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) struct readWriter {
    pub(crate) r: usize,
    pub(crate) w: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WireMessage {
    Hello(Hello),
    ClusterConfig(ClusterConfig),
    Index(Index),
    IndexUpdate(IndexUpdate),
    Request(Request),
    Response(Response),
    DownloadProgress(DownloadProgress),
    Ping(Ping),
    Close(Close),
}

pub(crate) fn encode_wire_message(message: &WireMessage) -> Result<Vec<u8>, String> {
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

pub(crate) fn decode_wire_message(frame: &[u8]) -> Result<WireMessage, String> {
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

pub(crate) fn apply_index_update(index: &mut Index, update: &IndexUpdate) -> Result<(), String> {
    update.validate_sequence()?;
    if index.Folder != update.Folder {
        return Err("index update folder mismatch".to_string());
    }
    if update.PrevSequence != index.LastSequence {
        return Err("index update previous sequence mismatch".to_string());
    }

    index.Files = update.Files.clone();
    index.LastSequence = update.LastSequence;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_message_round_trip() {
        let msg = WireMessage::Request(Request {
            Id: 7,
            Folder: "default".to_string(),
            Name: "a.bin".to_string(),
            Offset: 0,
            Size: 128,
            Hash: vec![1, 2, 3],
            FromTemporary: false,
            BlockNo: 0,
        });

        let frame = encode_wire_message(&msg).expect("encode");
        let decoded = decode_wire_message(&frame).expect("decode");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn file_validation_rejects_directory_blocks() {
        let file = FileInfo {
            Name: "dir".to_string(),
            Type: FileInfoTypeDirectory,
            Blocks: vec![BlockInfo {
                Hash: vec![1],
                Offset: 0,
                Size: 128,
            }],
            ..Default::default()
        };

        let err = file.validate().expect_err("must reject");
        assert!(err.contains("directories must not contain block payloads"));
    }

    #[test]
    fn apply_index_update_enforces_prev_sequence() {
        let mut index = Index {
            Folder: "docs".to_string(),
            LastSequence: 3,
            Files: Vec::new(),
        };
        let update = IndexUpdate {
            Folder: "docs".to_string(),
            PrevSequence: 3,
            LastSequence: 4,
            Files: vec![FileInfo {
                Name: "a.txt".to_string(),
                Type: FileInfoTypeFile,
                Size: 1,
                ..Default::default()
            }],
        };

        apply_index_update(&mut index, &update).expect("update");
        assert_eq!(index.LastSequence, 4);
        assert_eq!(index.Files.len(), 1);
    }
}
