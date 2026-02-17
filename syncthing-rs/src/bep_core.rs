#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use crate::bep::{
    decode_frame, encode_frame, BepMessage, ClusterConfigDevice, ClusterConfigFolder,
    DownloadProgressEntry, IndexEntry,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const CompressionMetadata: i32 = 0;
pub(crate) const CompressionNever: i32 = 1;
pub(crate) const CompressionAlways: i32 = 2;
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

/// 11.3: Convert raw device ID bytes to hex string (lossless).
pub(crate) fn device_id_from_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 11.3: Convert hex-encoded device ID string back to raw bytes.
#[allow(dead_code)]
pub(crate) fn device_id_to_bytes(id: &str) -> Vec<u8> {
    if id.len() % 2 == 0 && id.chars().all(|c| c.is_ascii_hexdigit()) {
        (0..id.len())
            .step_by(2)
            .filter_map(|i| u8::from_str_radix(&id[i..i + 2], 16).ok())
            .collect()
    } else {
        id.as_bytes().to_vec()
    }
}

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
    // 2d: Proto field is uint64 — use u64 end-to-end
    pub(crate) Id: u64,
    pub(crate) Value: u64,
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
    pub(crate) EncryptionPasswordToken: Vec<u8>, // 8.10: bytes, not String
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
    // 2b: Symlink target as raw bytes for wire fidelity (proto is bytes)
    pub(crate) SymlinkTarget: Vec<u8>,
    pub(crate) LocalFlags: u32,
    pub(crate) VersionHash: Vec<u8>,
    pub(crate) InodeChangeNs: i64,
    pub(crate) EncryptionTrailerSize: i64,
    pub(crate) BlocksHash: Vec<u8>,
    pub(crate) Platform: PlatformData,
    pub(crate) Version: Vector,
    // 2c: Encrypted as raw bytes (proto is bytes, not bool)
    pub(crate) Encrypted: Vec<u8>,
    pub(crate) PreviousBlocksHash: Vec<u8>,
}

impl FileInfo {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.Name.is_empty() {
            return Err("file info name is required".to_string());
        }
        if self.Deleted && !self.Blocks.is_empty() {
            return Err("deleted files must not contain blocks".to_string());
        }
        if self.Type == FileInfoTypeDirectory && !self.Blocks.is_empty() {
            return Err("directories must not contain block payloads".to_string());
        }
        if !self.Deleted
            && !self.IsInvalid()
            && self.Type == FileInfoTypeFile
            && self.Blocks.is_empty()
        {
            return Err("regular files must contain at least one block".to_string());
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
    pub(crate) Xattrs: Option<XattrData>,
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
    let bep = wire_to_bep(message)?;
    encode_frame(&bep)
}

pub(crate) fn decode_wire_message(frame: &[u8]) -> Result<WireMessage, String> {
    let bep = decode_frame(frame)?;
    bep_to_wire(&bep)
}

fn wire_to_bep(message: &WireMessage) -> Result<BepMessage, String> {
    match message {
        // 8.9: Hello maps 1:1 — stop packing fields into client_name
        WireMessage::Hello(hello) => Ok(BepMessage::Hello {
            device_name: hello.DeviceName.clone(),
            client_name: hello.ClientName.clone(),
            client_version: hello.ClientVersion.clone(),
            num_connections: hello.NumConnections,
            timestamp: hello.Timestamp,
        }),
        // 8.2: ClusterConfig carries full folder/device data
        WireMessage::ClusterConfig(cfg) => Ok(BepMessage::ClusterConfig {
            folders: cfg
                .Folders
                .iter()
                .map(|f| ClusterConfigFolder {
                    id: f.Id.clone(),
                    label: f.Label.clone(),
                    folder_type: f.Type,
                    devices: f
                        .Devices
                        .iter()
                        .map(|d| ClusterConfigDevice {
                            // BC1: use device_id_to_bytes for proper 32-byte raw ID
                            id: device_id_to_bytes(&d.Id),
                            name: d.Name.clone(),
                            addresses: d.Addresses.clone(),
                            compression: d.Compression,
                            introducer: d.Introducer,
                            index_id: d.IndexId,
                            max_sequence: d.MaxSequence,
                            encryption_password_token: d.EncryptionPasswordToken.clone(),
                            skip_introduction_removals: d.SkipIntroductionRemovals,
                        })
                        .collect(),
                })
                .collect(),
        }),
        WireMessage::Index(index) => Ok(BepMessage::Index {
            folder: index.Folder.clone(),
            files: index.Files.iter().map(fileinfo_to_index_entry).collect(),
            last_sequence: index.LastSequence,
        }),
        // 8.6: IndexUpdate preserves prev_sequence
        WireMessage::IndexUpdate(update) => Ok(BepMessage::IndexUpdate {
            folder: update.Folder.clone(),
            files: update.Files.iter().map(fileinfo_to_index_entry).collect(),
            prev_sequence: update.PrevSequence,
            last_sequence: update.LastSequence,
        }),
        // 8.5: Request preserves from_temporary and block_no
        // B2: id passed through as i32 — no clamping
        WireMessage::Request(req) => Ok(BepMessage::Request {
            id: req.Id,
            folder: req.Folder.clone(),
            name: req.Name.clone(),
            offset: req.Offset,
            size: req.Size,
            hash: req.Hash.clone(),
            from_temporary: req.FromTemporary,
            block_no: req.BlockNo,
        }),
        // 8.7: Response code is i32, don't clamp
        // B2: id passed through as i32 — no clamping
        WireMessage::Response(resp) => Ok(BepMessage::Response {
            id: resp.Id,
            code: resp.Code,
            data_len: resp.Data.len() as u32,
            data: resp.Data.clone(),
        }),
        WireMessage::DownloadProgress(progress) => Ok(BepMessage::DownloadProgress {
            folder: progress.Folder.clone(),
            updates: progress
                .Updates
                .iter()
                .map(|u| DownloadProgressEntry {
                    name: u.Name.clone(),
                    version: u
                        .Version
                        .as_ref()
                        .map(|v| {
                            v.Counters
                                .iter()
                                .map(|c| (c.Id as u64, c.Value as u64))
                                .collect()
                        })
                        .unwrap_or_default(),
                    block_indexes: u.BlockIndexes.clone(),
                    block_size: u.BlockSize,
                    // A9: Raw i32 enum value
                    update_type: u.UpdateType,
                })
                .collect(),
        }),
        WireMessage::Ping(_) => Ok(BepMessage::Ping { timestamp_ms: 0 }),
        WireMessage::Close(close) => Ok(BepMessage::Close {
            reason: close.Reason.clone(),
        }),
    }
}

fn bep_to_wire(message: &BepMessage) -> Result<WireMessage, String> {
    match message {
        // 8.9: Hello maps 1:1
        BepMessage::Hello {
            device_name,
            client_name,
            client_version,
            num_connections,
            timestamp,
        } => Ok(WireMessage::Hello(Hello {
            DeviceName: device_name.clone(),
            ClientName: client_name.clone(),
            ClientVersion: client_version.clone(),
            NumConnections: *num_connections,
            Timestamp: *timestamp,
        })),
        // 8.2: ClusterConfig carries full folder data
        BepMessage::ClusterConfig { folders } => Ok(WireMessage::ClusterConfig(ClusterConfig {
            Folders: folders
                .iter()
                .map(|f| Folder {
                    Id: f.id.clone(),
                    Label: f.label.clone(),
                    Type: f.folder_type,
                    Devices: f
                        .devices
                        .iter()
                        .map(|d| Device {
                            // 11.3: Device IDs are 32-byte binary — hex-encode for lossless round-trip.
                            Id: device_id_from_bytes(&d.id),
                            Name: d.name.clone(),
                            Addresses: d.addresses.clone(),
                            Compression: d.compression,
                            Introducer: d.introducer,
                            IndexId: d.index_id,
                            MaxSequence: d.max_sequence,
                            EncryptionPasswordToken: d.encryption_password_token.clone(),
                            SkipIntroductionRemovals: d.skip_introduction_removals,
                            CertName: String::new(),
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })),
        BepMessage::Index {
            folder,
            files,
            last_sequence,
        } => Ok(WireMessage::Index(Index {
            Folder: folder.clone(),
            Files: files.iter().map(index_entry_to_fileinfo).collect(),
            // A2: preserve wire last_sequence verbatim
            LastSequence: *last_sequence,
        })),
        // 8.6: IndexUpdate preserves prev_sequence
        BepMessage::IndexUpdate {
            folder,
            files,
            prev_sequence,
            last_sequence,
        } => Ok(WireMessage::IndexUpdate(IndexUpdate {
            Folder: folder.clone(),
            Files: files.iter().map(index_entry_to_fileinfo).collect(),
            LastSequence: *last_sequence,
            PrevSequence: *prev_sequence,
        })),
        // 8.5: Request preserves from_temporary and block_no
        BepMessage::Request {
            id,
            folder,
            name,
            offset,
            size,
            hash,
            from_temporary,
            block_no,
        } => Ok(WireMessage::Request(Request {
            Id: *id as i32,
            Folder: folder.clone(),
            Name: name.clone(),
            Offset: *offset as i64,
            Size: *size as i32,
            Hash: hash.clone(),
            FromTemporary: *from_temporary,
            BlockNo: *block_no,
        })),
        // 8.7: Response code is i32
        BepMessage::Response { id, code, data, .. } => Ok(WireMessage::Response(Response {
            Id: *id as i32,
            Code: *code,
            Data: data.clone(),
        })),
        BepMessage::DownloadProgress { folder, updates } => {
            Ok(WireMessage::DownloadProgress(DownloadProgress {
                Folder: folder.clone(),
                Updates: updates
                    .iter()
                    .map(|u| FileDownloadProgressUpdate {
                        Name: u.name.clone(),
                        Version: if u.version.is_empty() {
                            None
                        } else {
                            Some(Vector {
                                Counters: u
                                    .version
                                    .iter()
                                    .map(|(id, value)| Counter {
                                        Id: *id,
                                        Value: *value,
                                    })
                                    .collect(),
                            })
                        },
                        BlockIndexes: u.block_indexes.clone(),
                        BlockSize: u.block_size,
                        // A9: Raw i32 enum value
                        UpdateType: u.update_type,
                    })
                    .collect(),
            }))
        }
        BepMessage::Ping { .. } => Ok(WireMessage::Ping(Ping {})),
        BepMessage::Close { reason } => Ok(WireMessage::Close(Close {
            Reason: reason.clone(),
        })),
    }
}

// 8.3: fileinfo_to_index_entry preserves all FileInfo fields
fn fileinfo_to_index_entry(file: &FileInfo) -> IndexEntry {
    IndexEntry {
        path: file.Name.clone(),
        // A6: Keep as i64 — no clamping
        sequence: file.Sequence,
        deleted: file.Deleted,
        // A6: Keep as i64 — no clamping
        size: file.Size,
        // A3: Full block tuples
        blocks: file
            .Blocks
            .iter()
            .map(|b| crate::bep::BlockEntry {
                hash: encode_hex(&b.Hash),
                offset: b.Offset,
                size: b.Size,
            })
            .collect(),
        file_type: file.Type,
        permissions: file.Permissions as u32,
        modified_s: file.ModifiedS,
        modified_ns: file.ModifiedNs,
        modified_by: file.ModifiedBy as u64,
        no_permissions: file.NoPermissions,
        invalid: file.Invalid,
        local_flags: file.LocalFlags as u32,
        // A8: symlink as raw bytes (already Vec<u8>)
        symlink_target: file.SymlinkTarget.clone(),
        block_size: file.BlockSize,
        // A7: u64 version counters
        version_counters: file
            .Version
            .Counters
            .iter()
            .map(|c| (c.Id, c.Value))
            .collect(),
        // A4: Preserve wire fields
        blocks_hash: file.BlocksHash.clone(),
        previous_blocks_hash: file.PreviousBlocksHash.clone(),
        encrypted: file.Encrypted.clone(),
    }
}

fn index_entry_to_fileinfo(entry: &IndexEntry) -> FileInfo {
    // 2a: Full field-for-field reconstruction from IndexEntry
    FileInfo {
        Name: entry.path.clone(),
        Type: entry.file_type,
        Size: entry.size,
        Permissions: entry.permissions,
        ModifiedS: entry.modified_s,
        ModifiedNs: entry.modified_ns,
        ModifiedBy: entry.modified_by as i64,
        Deleted: entry.deleted,
        Invalid: entry.invalid,
        NoPermissions: entry.no_permissions,
        Sequence: entry.sequence,
        BlockSize: entry.block_size,
        Blocks: entry
            .blocks
            .iter()
            .map(|b| BlockInfo {
                Hash: decode_hex_or_utf8(&b.hash),
                Offset: b.offset,
                Size: b.size,
            })
            .collect(),
        // 2b: SymlinkTarget as raw bytes
        SymlinkTarget: entry.symlink_target.clone(),
        LocalFlags: entry.local_flags,
        Version: Vector {
            Counters: entry
                .version_counters
                .iter()
                .map(|(id, val)| Counter {
                    Id: *id,
                    Value: *val,
                })
                .collect(),
        },
        BlocksHash: entry.blocks_hash.clone(),
        PreviousBlocksHash: entry.previous_blocks_hash.clone(),
        // 2c: Encrypted as raw bytes
        Encrypted: entry.encrypted.clone(),
        ..Default::default()
    }
}

fn decode_hex_or_utf8(value: &str) -> Vec<u8> {
    if value.len() % 2 == 0 && value.as_bytes().iter().all(|b| b.is_ascii_hexdigit()) {
        let mut out = Vec::with_capacity(value.len() / 2);
        for pair in value.as_bytes().chunks_exact(2) {
            if let Ok(hex) = std::str::from_utf8(pair) {
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    out.push(byte);
                    continue;
                }
            }
            return value.as_bytes().to_vec();
        }
        return out;
    }
    value.as_bytes().to_vec()
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

pub(crate) fn apply_index_update(index: &mut Index, update: &IndexUpdate) -> Result<(), String> {
    update.validate_sequence()?;
    if index.Folder != update.Folder {
        return Err("index update folder mismatch".to_string());
    }
    if update.PrevSequence != index.LastSequence {
        return Err("index update previous sequence mismatch".to_string());
    }

    let mut merged = index
        .Files
        .iter()
        .map(|file| (file.Name.clone(), file.clone()))
        .collect::<BTreeMap<_, _>>();
    for file in &update.Files {
        merged.insert(file.Name.clone(), file.clone());
    }
    index.Files = merged.into_values().collect();
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
    fn file_validation_rejects_deleted_files_with_blocks() {
        let file = FileInfo {
            Name: "gone.txt".to_string(),
            Type: FileInfoTypeFile,
            Deleted: true,
            Blocks: vec![BlockInfo {
                Hash: vec![1],
                Offset: 0,
                Size: 1,
            }],
            ..Default::default()
        };
        let err = file.validate().expect_err("must reject");
        assert!(err.contains("deleted files must not contain blocks"));
    }

    #[test]
    fn file_validation_rejects_non_deleted_regular_file_without_blocks() {
        let file = FileInfo {
            Name: "live.txt".to_string(),
            Type: FileInfoTypeFile,
            Deleted: false,
            Invalid: false,
            Blocks: Vec::new(),
            ..Default::default()
        };
        let err = file.validate().expect_err("must reject");
        assert!(err.contains("regular files must contain at least one block"));
    }

    #[test]
    fn compression_enum_values_match_go_wire_numbers() {
        assert_eq!(Compression_COMPRESSION_METADATA, 0);
        assert_eq!(Compression_COMPRESSION_NEVER, 1);
        assert_eq!(Compression_COMPRESSION_ALWAYS, 2);
    }

    #[test]
    fn apply_index_update_enforces_prev_sequence() {
        let mut index = Index {
            Folder: "docs".to_string(),
            LastSequence: 3,
            Files: vec![FileInfo {
                Name: "old.txt".to_string(),
                Type: FileInfoTypeFile,
                Size: 7,
                ..Default::default()
            }],
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
        assert_eq!(index.Files.len(), 2);
        assert!(index.Files.iter().any(|f| f.Name == "old.txt"));
        assert!(index.Files.iter().any(|f| f.Name == "a.txt"));
    }
}
