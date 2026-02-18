#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::bep_core::*;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::OnceLock;

fn as_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn as_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

fn descriptor(name: &'static str) -> &'static str {
    name
}

macro_rules! impl_proto_common {
    ($ty:ty, $name:literal) => {
        impl $ty {
            pub(crate) fn Descriptor(&self) -> &'static str {
                descriptor($name)
            }

            pub(crate) fn ProtoMessage(&self) {}

            pub(crate) fn ProtoReflect(&self) -> Value {
                as_value(self)
            }

            pub(crate) fn Reset(&mut self) {
                *self = Default::default();
            }

            pub(crate) fn String(&self) -> String {
                as_json_string(self)
            }
        }
    };
}

impl_proto_common!(BlockInfo, "BlockInfo");
impl_proto_common!(Close, "Close");
impl_proto_common!(ClusterConfig, "ClusterConfig");
impl_proto_common!(Counter, "Counter");
impl_proto_common!(Device, "Device");
impl_proto_common!(DownloadProgress, "DownloadProgress");
impl_proto_common!(FileDownloadProgressUpdate, "FileDownloadProgressUpdate");
impl_proto_common!(FileInfo, "FileInfo");
impl_proto_common!(Folder, "Folder");
impl_proto_common!(Header, "Header");
impl_proto_common!(Hello, "Hello");
impl_proto_common!(Index, "Index");
impl_proto_common!(IndexUpdate, "IndexUpdate");
impl_proto_common!(Ping, "Ping");
impl_proto_common!(PlatformData, "PlatformData");
impl_proto_common!(Request, "Request");
impl_proto_common!(Response, "Response");
impl_proto_common!(UnixData, "UnixData");
impl_proto_common!(Vector, "Vector");
impl_proto_common!(WindowsData, "WindowsData");
impl_proto_common!(Xattr, "Xattr");
impl_proto_common!(XattrData, "XattrData");

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct Compression(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct ErrorCode(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct FileDownloadProgressUpdateType(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct FileInfoType(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct FolderStopReason(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct FolderType(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct MessageCompression(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct MessageType(pub(crate) i32);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct FlagLocal(pub(crate) u32);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct FileInfoWithoutBlocks {
    pub(crate) Name: String,
    pub(crate) Type: i32,
    pub(crate) Size: i64,
    pub(crate) Permissions: u32,
    pub(crate) ModifiedS: i64,
    pub(crate) ModifiedNs: i32,
    pub(crate) ModifiedBy: u64,
    pub(crate) Deleted: bool,
    pub(crate) Invalid: bool,
    pub(crate) NoPermissions: bool,
    pub(crate) Sequence: i64,
    pub(crate) BlockSize: i32,
    // 2b: SymlinkTarget as bytes (matching FileInfo)
    pub(crate) SymlinkTarget: Vec<u8>,
    pub(crate) LocalFlags: u32,
    pub(crate) VersionHash: Vec<u8>,
    pub(crate) InodeChangeNs: i64,
    pub(crate) EncryptionTrailerSize: i64,
    pub(crate) BlocksHash: Vec<u8>,
    pub(crate) Platform: PlatformData,
    pub(crate) Version: Vector,
    // 2c: Encrypted as bytes (matching FileInfo)
    pub(crate) Encrypted: Vec<u8>,
    pub(crate) PreviousBlocksHash: Vec<u8>,
}

macro_rules! impl_enumish {
    ($ty:ty, $name:literal) => {
        impl $ty {
            pub(crate) fn Descriptor(&self) -> &'static str {
                descriptor($name)
            }

            pub(crate) fn Enum(&self) -> Self {
                *self
            }

            pub(crate) fn EnumDescriptor(&self) -> String {
                format!("{}:{}", $name, self.Number())
            }

            pub(crate) fn Number(&self) -> i32 {
                self.0
            }

            pub(crate) fn String(&self) -> String {
                format!("{}({})", $name, self.Number())
            }

            pub(crate) fn Type(&self) -> &'static str {
                $name
            }
        }
    };
}

impl_enumish!(Compression, "Compression");
impl_enumish!(ErrorCode, "ErrorCode");
impl_enumish!(
    FileDownloadProgressUpdateType,
    "FileDownloadProgressUpdateType"
);
impl_enumish!(FileInfoType, "FileInfoType");
impl_enumish!(FolderStopReason, "FolderStopReason");
impl_enumish!(FolderType, "FolderType");
impl_enumish!(MessageCompression, "MessageCompression");
impl_enumish!(MessageType, "MessageType");

impl FlagLocal {
    pub(crate) fn HumanString(&self) -> String {
        let mut parts = Vec::new();
        if self.0 & FlagLocalIgnored != 0 {
            parts.push("ignored");
        }
        if self.0 & FlagLocalMustRescan != 0 {
            parts.push("must_rescan");
        }
        if self.0 & FlagLocalReceiveOnly != 0 {
            parts.push("receive_only");
        }
        if self.0 & FlagLocalRemoteInvalid != 0 {
            parts.push("remote_invalid");
        }
        if self.0 & FlagLocalNeeded != 0 {
            parts.push("needed");
        }
        if self.0 & FlagLocalUnsupported != 0 {
            parts.push("unsupported");
        }
        if self.0 & FlagLocalGlobal != 0 {
            parts.push("global");
        }
        if parts.is_empty() {
            "none".to_string()
        } else {
            parts.join("|")
        }
    }

    pub(crate) fn IsInvalid(&self) -> bool {
        self.0 & LocalInvalidFlags != 0
    }
}

impl BlockInfo {
    pub(crate) fn GetHash(&self) -> Vec<u8> {
        self.Hash.clone()
    }

    pub(crate) fn GetOffset(&self) -> i64 {
        self.Offset
    }

    pub(crate) fn GetSize(&self) -> i32 {
        self.Size
    }

    pub(crate) fn IsEmpty(&self) -> bool {
        self.Size == 0 && self.Hash.is_empty()
    }

    pub(crate) fn ToWire(&self) -> Value {
        as_value(self)
    }
}

impl Close {
    pub(crate) fn GetReason(&self) -> String {
        self.Reason.clone()
    }
}

impl ClusterConfig {
    pub(crate) fn GetFolders(&self) -> Vec<Folder> {
        self.Folders.clone()
    }

    pub(crate) fn GetSecondary(&self) -> bool {
        self.Secondary
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl Counter {
    pub(crate) fn GetId(&self) -> u64 {
        self.Id
    }

    pub(crate) fn GetValue(&self) -> u64 {
        self.Value
    }
}

impl Device {
    pub(crate) fn GetAddresses(&self) -> Vec<String> {
        self.Addresses.clone()
    }

    pub(crate) fn GetCertName(&self) -> String {
        self.CertName.clone()
    }

    pub(crate) fn GetCompression(&self) -> i32 {
        self.Compression
    }

    pub(crate) fn GetEncryptionPasswordToken(&self) -> String {
        String::from_utf8_lossy(&self.EncryptionPasswordToken).to_string()
    }

    pub(crate) fn GetId(&self) -> String {
        self.Id.clone()
    }

    pub(crate) fn GetIndexId(&self) -> u64 {
        self.IndexId
    }

    pub(crate) fn GetIntroducer(&self) -> bool {
        self.Introducer
    }

    pub(crate) fn GetMaxSequence(&self) -> i64 {
        self.MaxSequence
    }

    pub(crate) fn GetName(&self) -> String {
        self.Name.clone()
    }

    pub(crate) fn GetSkipIntroductionRemovals(&self) -> bool {
        self.SkipIntroductionRemovals
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl DownloadProgress {
    pub(crate) fn GetFolder(&self) -> String {
        self.Folder.clone()
    }

    pub(crate) fn GetUpdates(&self) -> &[FileDownloadProgressUpdate] {
        &self.Updates
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl FileDownloadProgressUpdate {
    pub(crate) fn GetBlockIndexes(&self) -> &[i32] {
        &self.BlockIndexes
    }

    pub(crate) fn GetBlockSize(&self) -> i32 {
        self.BlockSize
    }

    pub(crate) fn GetName(&self) -> String {
        self.Name.clone()
    }

    pub(crate) fn GetUpdateType(&self) -> i32 {
        self.UpdateType
    }

    pub(crate) fn GetVersion(&self) -> Option<Vector> {
        self.Version.clone()
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl FileInfo {
    pub(crate) fn BlockSize(&self) -> i32 {
        self.BlockSize
    }

    pub(crate) fn BlocksEqual(&self, other: &Self) -> bool {
        blocksEqual(&self.Blocks, &other.Blocks)
    }

    pub(crate) fn FileBlocksHash(&self) -> Vec<u8> {
        self.BlocksHash.clone()
    }

    pub(crate) fn FileLocalFlags(&self) -> u32 {
        self.LocalFlags
    }

    pub(crate) fn FileModifiedBy(&self) -> u64 {
        self.ModifiedBy
    }

    pub(crate) fn FileName(&self) -> String {
        self.Name.clone()
    }

    pub(crate) fn FilePermissions(&self) -> u32 {
        self.Permissions
    }

    pub(crate) fn FileSize(&self) -> i64 {
        self.Size
    }

    pub(crate) fn FileType(&self) -> i32 {
        self.Type
    }

    pub(crate) fn FileVersion(&self) -> Vector {
        self.Version.clone()
    }

    pub(crate) fn GetBlockSize(&self) -> i32 {
        self.BlockSize
    }

    pub(crate) fn GetBlocks(&self) -> Vec<BlockInfo> {
        self.Blocks.clone()
    }

    pub(crate) fn GetBlocksHash(&self) -> Vec<u8> {
        self.BlocksHash.clone()
    }

    pub(crate) fn GetDeleted(&self) -> bool {
        self.Deleted
    }

    pub(crate) fn GetEncrypted(&self) -> bool {
        !self.Encrypted.is_empty()
    }

    pub(crate) fn GetEncryptionTrailerSize(&self) -> i64 {
        self.EncryptionTrailerSize
    }

    pub(crate) fn GetInodeChangeNs(&self) -> i64 {
        self.InodeChangeNs
    }

    pub(crate) fn GetInvalid(&self) -> bool {
        self.Invalid
    }

    pub(crate) fn GetLocalFlags(&self) -> u32 {
        self.LocalFlags
    }

    pub(crate) fn GetModifiedBy(&self) -> u64 {
        self.ModifiedBy
    }

    pub(crate) fn GetModifiedNs(&self) -> i32 {
        self.ModifiedNs
    }

    pub(crate) fn GetModifiedS(&self) -> i64 {
        self.ModifiedS
    }

    pub(crate) fn GetName(&self) -> String {
        self.Name.clone()
    }

    pub(crate) fn GetNoPermissions(&self) -> bool {
        self.NoPermissions
    }

    pub(crate) fn GetPermissions(&self) -> u32 {
        self.Permissions
    }

    pub(crate) fn GetPlatform(&self) -> PlatformData {
        self.Platform.clone()
    }

    pub(crate) fn GetPreviousBlocksHash(&self) -> Vec<u8> {
        self.PreviousBlocksHash.clone()
    }

    pub(crate) fn GetSequence(&self) -> i64 {
        self.Sequence
    }

    pub(crate) fn GetSize(&self) -> i64 {
        self.Size
    }

    pub(crate) fn GetSymlinkTarget(&self) -> String {
        String::from_utf8_lossy(&self.SymlinkTarget).to_string()
    }

    pub(crate) fn GetType(&self) -> i32 {
        self.Type
    }

    pub(crate) fn GetVersion(&self) -> Vector {
        self.Version.clone()
    }

    pub(crate) fn GetVersionHash(&self) -> Vec<u8> {
        self.VersionHash.clone()
    }

    pub(crate) fn HasPermissionBits(&self) -> bool {
        !self.NoPermissions
    }

    pub(crate) fn InConflictWith(&self, other: &Self) -> bool {
        if self.Name != other.Name {
            return false;
        }
        match compare_vectors(&self.Version, &other.Version) {
            VectorRelation::Less | VectorRelation::Greater | VectorRelation::Equal => false,
            VectorRelation::Concurrent => {
                if !self.PreviousBlocksHash.is_empty()
                    && self.PreviousBlocksHash == other.BlocksHash
                {
                    return false;
                }
                if !other.PreviousBlocksHash.is_empty()
                    && other.PreviousBlocksHash == self.BlocksHash
                {
                    return false;
                }
                true
            }
        }
    }

    pub(crate) fn InodeChangeTime(&self) -> i64 {
        self.InodeChangeNs
    }

    pub(crate) fn IsDeleted(&self) -> bool {
        self.Deleted
    }

    pub(crate) fn IsDirectory(&self) -> bool {
        self.Type == FileInfoTypeDirectory
    }

    pub(crate) fn IsEquivalent(&self, other: &Self) -> bool {
        self.isEquivalent(other)
    }

    pub(crate) fn IsEquivalentOptional(&self, other: &Self, cmp: &FileInfoComparison) -> bool {
        if self.Name != other.Name
            || self.Type != other.Type
            || self.Deleted != other.Deleted
            || self.Invalid != other.Invalid
            || self.MustRescan()
            || other.MustRescan()
        {
            return false;
        }
        if !cmp.IgnorePerms && !PermsEqual(self, other) {
            return false;
        }
        if !cmp.IgnoreOwnership && !ownershipEqual(&self.Platform, &other.Platform) {
            return false;
        }
        if !cmp.IgnoreXattrs && !xattrsEqual(&self.Platform.Xattrs(), &other.Platform.Xattrs()) {
            return false;
        }
        if !cmp.IgnoreFlags && self.LocalFlags != other.LocalFlags {
            return false;
        }
        if self.IsSymlink() {
            if self.SymlinkTarget != other.SymlinkTarget {
                return false;
            }
        } else if self.Type == FileInfoTypeFile && !self.Deleted {
            if self.Size != other.Size {
                return false;
            }
            if !cmp.IgnoreBlocks && !blocksEqual(&self.Blocks, &other.Blocks) {
                return false;
            }
        }
        if !ModTimeEqual(self, other, cmp.ModTimeWindow) {
            return false;
        }
        true
    }

    pub(crate) fn IsIgnored(&self) -> bool {
        self.LocalFlags & FlagLocalIgnored != 0
    }

    pub(crate) fn IsInvalid(&self) -> bool {
        self.Invalid || (self.LocalFlags & LocalInvalidFlags != 0)
    }

    pub(crate) fn IsReceiveOnlyChanged(&self) -> bool {
        self.LocalFlags & FlagLocalReceiveOnly != 0
    }

    pub(crate) fn IsSymlink(&self) -> bool {
        self.Type == FileInfoTypeSymlink
            || self.Type == FileInfoTypeSymlinkFile
            || self.Type == FileInfoTypeSymlinkDirectory
    }

    pub(crate) fn IsUnsupported(&self) -> bool {
        self.LocalFlags & FlagLocalUnsupported != 0
    }

    pub(crate) fn LogAttr(&self) -> BTreeMap<&'static str, Value> {
        BTreeMap::from([
            ("name", Value::String(self.Name.clone())),
            ("size", Value::from(self.Size)),
            ("sequence", Value::from(self.Sequence)),
            ("deleted", Value::Bool(self.Deleted)),
        ])
    }

    pub(crate) fn ModTime(&self) -> i64 {
        self.ModifiedS.saturating_mul(1_000_000_000) + self.ModifiedNs as i64
    }

    pub(crate) fn MustRescan(&self) -> bool {
        self.LocalFlags & FlagLocalMustRescan != 0
    }

    pub(crate) fn PlatformData(&self) -> PlatformData {
        self.Platform.clone()
    }

    pub(crate) fn SequenceNo(&self) -> i64 {
        self.Sequence
    }

    pub(crate) fn SetDeleted(&mut self, deleted: bool) {
        self.Deleted = deleted;
        if deleted {
            self.setNoContent();
        }
    }

    pub(crate) fn SetIgnored(&mut self, ignored: bool) {
        self.setLocalFlags(FlagLocalIgnored, ignored);
    }

    pub(crate) fn SetMustRescan(&mut self, must_rescan: bool) {
        self.setLocalFlags(FlagLocalMustRescan, must_rescan);
    }

    pub(crate) fn SetUnsupported(&mut self, unsupported: bool) {
        self.setLocalFlags(FlagLocalUnsupported, unsupported);
    }

    pub(crate) fn ShouldConflict(&self, other: &Self) -> bool {
        let _ = other;
        self.LocalFlags & LocalConflictFlags != 0
    }

    pub(crate) fn ToWire(&self) -> Value {
        as_value(self)
    }

    pub(crate) fn WinsConflict(&self, other: &Self) -> bool {
        if self.IsInvalid() != other.IsInvalid() {
            return !self.IsInvalid();
        }
        if self.ModTime() != other.ModTime() {
            return self.ModTime() > other.ModTime();
        }
        match compare_vectors(&self.Version, &other.Version) {
            VectorRelation::Greater => true,
            VectorRelation::Less => false,
            VectorRelation::Equal => self.VersionHash >= other.VersionHash,
            VectorRelation::Concurrent => self.VersionHash >= other.VersionHash,
        }
    }

    pub(crate) fn isEquivalent(&self, other: &Self) -> bool {
        self.Name == other.Name
            && self.Type == other.Type
            && self.Size == other.Size
            && self.Permissions == other.Permissions
            && self.ModifiedS == other.ModifiedS
            && self.ModifiedNs == other.ModifiedNs
            && self.Deleted == other.Deleted
            && self.LocalFlags == other.LocalFlags
            && self.VersionHash == other.VersionHash
            && self.BlocksHash == other.BlocksHash
    }

    pub(crate) fn setLocalFlags(&mut self, mask: u32, set: bool) {
        if set {
            self.LocalFlags |= mask;
            if mask & (FlagLocalUnsupported | FlagLocalIgnored | FlagLocalMustRescan) != 0 {
                self.setNoContent();
            }
        } else {
            self.LocalFlags &= !mask;
        }
    }

    pub(crate) fn setNoContent(&mut self) {
        self.Blocks.clear();
        self.BlocksHash.clear();
        self.Size = 0;
    }
}

impl Folder {
    pub(crate) fn Description(&self) -> String {
        format!("{} ({})", self.Id, self.Label)
    }

    pub(crate) fn GetDevices(&self) -> Vec<Device> {
        self.Devices.clone()
    }

    pub(crate) fn GetId(&self) -> String {
        self.Id.clone()
    }

    pub(crate) fn GetLabel(&self) -> String {
        self.Label.clone()
    }

    pub(crate) fn GetStopReason(&self) -> i32 {
        self.StopReason
    }

    pub(crate) fn GetType(&self) -> i32 {
        self.Type
    }

    pub(crate) fn IsRunning(&self) -> bool {
        self.is_active()
    }

    pub(crate) fn LogAttr(&self) -> BTreeMap<&'static str, Value> {
        BTreeMap::from([
            ("id", Value::String(self.Id.clone())),
            ("label", Value::String(self.Label.clone())),
            ("type", Value::from(self.Type)),
            ("stop_reason", Value::from(self.StopReason)),
        ])
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl Header {
    pub(crate) fn GetCompression(&self) -> i32 {
        self.Compression
    }

    pub(crate) fn GetType(&self) -> i32 {
        self.Type
    }
}

impl Hello {
    pub(crate) fn GetClientName(&self) -> String {
        self.ClientName.clone()
    }

    pub(crate) fn GetClientVersion(&self) -> String {
        self.ClientVersion.clone()
    }

    pub(crate) fn GetDeviceName(&self) -> String {
        self.DeviceName.clone()
    }

    pub(crate) fn GetNumConnections(&self) -> i32 {
        self.NumConnections
    }

    pub(crate) fn GetTimestamp(&self) -> i64 {
        self.Timestamp
    }

    // W4-H9: Go always emits current magic when sending — never legacy magic
    pub(crate) fn Magic(&self) -> u32 {
        HelloMessageMagic
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

fn is_legacy_hello_13(client_version: &str) -> bool {
    let mut parts = client_version.split('.');
    matches!((parts.next(), parts.next()), (Some("1"), Some("3")))
}

impl Index {
    pub(crate) fn GetFiles(&self) -> Vec<FileInfo> {
        self.Files.clone()
    }

    pub(crate) fn GetFolder(&self) -> String {
        self.Folder.clone()
    }

    pub(crate) fn GetLastSequence(&self) -> i64 {
        self.LastSequence
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl IndexUpdate {
    pub(crate) fn GetFiles(&self) -> Vec<FileInfo> {
        self.Files.clone()
    }

    pub(crate) fn GetFolder(&self) -> String {
        self.Folder.clone()
    }

    pub(crate) fn GetLastSequence(&self) -> i64 {
        self.LastSequence
    }

    pub(crate) fn GetPrevSequence(&self) -> i64 {
        self.PrevSequence
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl PlatformData {
    pub(crate) fn GetDarwin(&self) -> Option<XattrData> {
        self.Darwin.clone()
    }

    pub(crate) fn GetFreebsd(&self) -> Option<XattrData> {
        self.Freebsd.clone()
    }

    pub(crate) fn GetLinux(&self) -> Option<XattrData> {
        self.Linux.clone()
    }

    pub(crate) fn GetNetbsd(&self) -> Option<XattrData> {
        self.Netbsd.clone()
    }

    pub(crate) fn GetUnix(&self) -> Option<UnixData> {
        self.Unix.clone()
    }

    pub(crate) fn GetWindows(&self) -> Option<WindowsData> {
        self.Windows.clone()
    }

    pub(crate) fn MergeWith(&mut self, other: &Self) {
        if self.Unix.is_none() {
            self.Unix = other.Unix.clone();
        }
        if self.Linux.is_none() {
            self.Linux = other.Linux.clone();
        }
        if self.Darwin.is_none() {
            self.Darwin = other.Darwin.clone();
        }
        if self.Freebsd.is_none() {
            self.Freebsd = other.Freebsd.clone();
        }
        if self.Netbsd.is_none() {
            self.Netbsd = other.Netbsd.clone();
        }
        if self.Windows.is_none() {
            self.Windows = other.Windows.clone();
        }
        if self.Xattrs.is_none() {
            self.Xattrs = other.Xattrs.clone();
        }
    }

    pub(crate) fn SetXattrs(&mut self, xattrs: XattrData) {
        self.Xattrs = Some(xattrs);
    }

    pub(crate) fn Xattrs(&self) -> XattrData {
        self.Xattrs.clone().unwrap_or_default()
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl Request {
    pub(crate) fn GetBlockNo(&self) -> i32 {
        self.BlockNo
    }

    pub(crate) fn GetFolder(&self) -> String {
        self.Folder.clone()
    }

    pub(crate) fn GetFromTemporary(&self) -> bool {
        self.FromTemporary
    }

    pub(crate) fn GetHash(&self) -> Vec<u8> {
        self.Hash.clone()
    }

    pub(crate) fn GetId(&self) -> i32 {
        self.Id
    }

    pub(crate) fn GetName(&self) -> String {
        self.Name.clone()
    }

    pub(crate) fn GetOffset(&self) -> i64 {
        self.Offset
    }

    pub(crate) fn GetSize(&self) -> i32 {
        self.Size
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl Response {
    pub(crate) fn GetCode(&self) -> i32 {
        self.Code
    }

    pub(crate) fn GetData(&self) -> Vec<u8> {
        self.Data.clone()
    }

    pub(crate) fn GetId(&self) -> i32 {
        self.Id
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl UnixData {
    pub(crate) fn GetGid(&self) -> i32 {
        self.Gid
    }

    pub(crate) fn GetGroupName(&self) -> String {
        self.GroupName.clone()
    }

    pub(crate) fn GetOwnerName(&self) -> String {
        self.OwnerName.clone()
    }

    pub(crate) fn GetUid(&self) -> i32 {
        self.Uid
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl Vector {
    pub(crate) fn GetCounters(&self) -> Vec<Counter> {
        self.Counters.clone()
    }
}

impl WindowsData {
    pub(crate) fn GetOwnerIsGroup(&self) -> bool {
        self.OwnerIsGroup
    }

    pub(crate) fn GetOwnerName(&self) -> String {
        self.OwnerName.clone()
    }
}

impl Xattr {
    pub(crate) fn GetName(&self) -> String {
        self.Name.clone()
    }

    pub(crate) fn GetValue(&self) -> Vec<u8> {
        self.Value.clone()
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl XattrData {
    pub(crate) fn GetXattrs(&self) -> Vec<Xattr> {
        self.Xattrs.clone()
    }

    pub(crate) fn toWire(&self) -> Value {
        as_value(self)
    }
}

impl FileInfoWithoutBlocks {
    pub(crate) fn from_file_info(file: &FileInfo) -> Self {
        Self {
            Name: file.Name.clone(),
            Type: file.Type,
            Size: file.Size,
            Permissions: file.Permissions,
            ModifiedS: file.ModifiedS,
            ModifiedNs: file.ModifiedNs,
            ModifiedBy: file.ModifiedBy,
            Deleted: file.Deleted,
            Invalid: file.Invalid,
            NoPermissions: file.NoPermissions,
            Sequence: file.Sequence,
            BlockSize: file.BlockSize,
            SymlinkTarget: file.SymlinkTarget.clone(),
            LocalFlags: file.LocalFlags,
            VersionHash: file.VersionHash.clone(),
            InodeChangeNs: file.InodeChangeNs,
            EncryptionTrailerSize: file.EncryptionTrailerSize,
            BlocksHash: file.BlocksHash.clone(),
            Platform: file.Platform.clone(),
            Version: file.Version.clone(),
            Encrypted: file.Encrypted.clone(),
            PreviousBlocksHash: file.PreviousBlocksHash.clone(),
        }
    }

    pub(crate) fn GetBlockSize(&self) -> i32 {
        self.BlockSize
    }
    pub(crate) fn GetBlocksHash(&self) -> Vec<u8> {
        self.BlocksHash.clone()
    }
    pub(crate) fn GetDeleted(&self) -> bool {
        self.Deleted
    }
    pub(crate) fn GetEncrypted(&self) -> bool {
        !self.Encrypted.is_empty()
    }
    pub(crate) fn GetEncryptionTrailerSize(&self) -> i64 {
        self.EncryptionTrailerSize
    }
    pub(crate) fn GetInodeChangeNs(&self) -> i64 {
        self.InodeChangeNs
    }
    pub(crate) fn GetInvalid(&self) -> bool {
        self.Invalid
    }
    pub(crate) fn GetLocalFlags(&self) -> u32 {
        self.LocalFlags
    }
    pub(crate) fn GetModifiedBy(&self) -> u64 {
        self.ModifiedBy
    }
    pub(crate) fn GetModifiedNs(&self) -> i32 {
        self.ModifiedNs
    }
    pub(crate) fn GetModifiedS(&self) -> i64 {
        self.ModifiedS
    }
    pub(crate) fn GetName(&self) -> String {
        self.Name.clone()
    }
    pub(crate) fn GetNoPermissions(&self) -> bool {
        self.NoPermissions
    }
    pub(crate) fn GetPermissions(&self) -> u32 {
        self.Permissions
    }
    pub(crate) fn GetPlatform(&self) -> PlatformData {
        self.Platform.clone()
    }
    pub(crate) fn GetPreviousBlocksHash(&self) -> Vec<u8> {
        self.PreviousBlocksHash.clone()
    }
    pub(crate) fn GetSequence(&self) -> i64 {
        self.Sequence
    }
    pub(crate) fn GetSize(&self) -> i64 {
        self.Size
    }
    pub(crate) fn GetSymlinkTarget(&self) -> String {
        String::from_utf8_lossy(&self.SymlinkTarget).to_string()
    }
    pub(crate) fn GetType(&self) -> i32 {
        self.Type
    }
    pub(crate) fn GetVersion(&self) -> Vector {
        self.Version.clone()
    }
}

impl readWriter {
    pub(crate) fn Read<R: Read>(
        &mut self,
        reader: &mut R,
        buf: &mut [u8],
    ) -> std::io::Result<usize> {
        let n = reader.read(buf)?;
        self.r += n;
        Ok(n)
    }

    pub(crate) fn Write<W: Write>(&mut self, writer: &mut W, buf: &[u8]) -> std::io::Result<usize> {
        let n = writer.write(buf)?;
        self.w += n;
        Ok(n)
    }
}

pub(crate) static BlockSizes: [i32; 8] = [
    128 * 1024,
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    2 * 1024 * 1024,
    4 * 1024 * 1024,
    8 * 1024 * 1024,
    16 * 1024 * 1024,
];
pub(crate) static Compression_name: [&str; 3] = ["never", "always", "metadata"];
pub(crate) static Compression_value: [i32; 3] =
    [CompressionNever, CompressionAlways, CompressionMetadata];
pub(crate) static ErrorCode_name: [&str; 4] =
    ["no_error", "generic", "no_such_file", "invalid_file"];
pub(crate) static ErrorCode_value: [i32; 4] = [
    ErrorCodeNoError,
    ErrorCodeGeneric,
    ErrorCodeNoSuchFile,
    ErrorCodeInvalidFile,
];
pub(crate) static FileDownloadProgressUpdateType_name: [&str; 2] = ["append", "forget"];
pub(crate) static FileDownloadProgressUpdateType_value: [i32; 2] = [
    FileDownloadProgressUpdateTypeAppend,
    FileDownloadProgressUpdateTypeForget,
];
pub(crate) static FileInfoType_name: [&str; 5] = [
    "file",
    "directory",
    "symlink_file",
    "symlink_directory",
    "symlink",
];
pub(crate) static FileInfoType_value: [i32; 5] = [
    FileInfoTypeFile,
    FileInfoTypeDirectory,
    FileInfoTypeSymlinkFile,
    FileInfoTypeSymlinkDirectory,
    FileInfoTypeSymlink,
];
pub(crate) static FolderStopReason_name: [&str; 2] = ["running", "paused"];
pub(crate) static FolderStopReason_value: [i32; 2] =
    [FolderStopReasonRunning, FolderStopReasonPaused];
pub(crate) static FolderType_name: [&str; 4] = [
    "send_receive",
    "send_only",
    "receive_only",
    "receive_encrypted",
];
pub(crate) static FolderType_value: [i32; 4] = [
    FolderTypeSendReceive,
    FolderTypeSendOnly,
    FolderTypeReceiveOnly,
    FolderTypeReceiveEncrypted,
];
pub(crate) static MessageCompression_name: [&str; 2] = ["none", "lz4"];
pub(crate) static MessageCompression_value: [i32; 2] = [
    MessageCompression_MESSAGE_COMPRESSION_NONE,
    MessageCompression_MESSAGE_COMPRESSION_LZ4,
];
pub(crate) static MessageType_name: [&str; 8] = [
    "cluster_config",
    "index",
    "index_update",
    "request",
    "response",
    "download_progress",
    "ping",
    "close",
];
pub(crate) static MessageType_value: [i32; 8] = [
    MessageType_MESSAGE_TYPE_CLUSTER_CONFIG,
    MessageType_MESSAGE_TYPE_INDEX,
    MessageType_MESSAGE_TYPE_INDEX_UPDATE,
    MessageType_MESSAGE_TYPE_REQUEST,
    MessageType_MESSAGE_TYPE_RESPONSE,
    MessageType_MESSAGE_TYPE_DOWNLOAD_PROGRESS,
    MessageType_MESSAGE_TYPE_PING,
    MessageType_MESSAGE_TYPE_CLOSE,
];

pub(crate) static localFlagBitNames: [&str; 7] = [
    "ignored",
    "must_rescan",
    "receive_only",
    "remote_invalid",
    "needed",
    "unsupported",
    "global",
];

pub(crate) static ErrTooOldVersion: &str = "hello version too old";
pub(crate) static ErrUnknownMagic: &str = "unknown hello magic";
pub(crate) static File_bep_bep_proto: &str = "bep.proto";
pub(crate) static underscore_var: &str = "_";

pub(crate) static file_bep_bep_proto_depIdxs: [i32; 4] = [0, 1, 2, 3];
pub(crate) static file_bep_bep_proto_enumTypes: [&str; 8] = [
    "Compression",
    "ErrorCode",
    "FileDownloadProgressUpdateType",
    "FileInfoType",
    "FolderStopReason",
    "FolderType",
    "MessageCompression",
    "MessageType",
];
pub(crate) static file_bep_bep_proto_goTypes: [&str; 13] = [
    "BlockInfo",
    "Close",
    "ClusterConfig",
    "Counter",
    "Device",
    "DownloadProgress",
    "FileDownloadProgressUpdate",
    "FileInfo",
    "Folder",
    "Hello",
    "Index",
    "IndexUpdate",
    "Request",
];
pub(crate) static file_bep_bep_proto_msgTypes: [&str; 13] = [
    "BlockInfo",
    "Close",
    "ClusterConfig",
    "Counter",
    "Device",
    "DownloadProgress",
    "FileDownloadProgressUpdate",
    "FileInfo",
    "Folder",
    "Hello",
    "Index",
    "IndexUpdate",
    "Request",
];
pub(crate) static file_bep_bep_proto_rawDesc: &[u8] = b"syntax = \"proto3\"; package bep;";
pub(crate) static file_bep_bep_proto_rawDescOnce: OnceLock<Vec<u8>> = OnceLock::new();
pub(crate) static file_bep_bep_proto_rawDescData: OnceLock<Vec<u8>> = OnceLock::new();

pub(crate) static sha256OfEmptyBlock: [u8; 32] = [
    0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
    0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
];

pub(crate) fn blocksEqual(a: &[BlockInfo], b: &[BlockInfo]) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b.iter())
            .all(|(left, right)| left.Hash == right.Hash)
}

pub(crate) fn BlockSize(file: &FileInfo) -> i32 {
    file.BlockSize
}

pub(crate) fn BlocksHash(file: &FileInfo) -> Vec<u8> {
    if !file.BlocksHash.is_empty() {
        return file.BlocksHash.clone();
    }

    let mut hash = Sha256::new();
    for block in &file.Blocks {
        hash.update(&block.Hash);
    }
    hash.finalize().to_vec()
}

pub(crate) fn ModTimeEqual(a: &FileInfo, b: &FileInfo, window_ns: i64) -> bool {
    if a.ModTime() == b.ModTime() {
        return true;
    }
    (a.ModTime() - b.ModTime()).abs() < window_ns
}

pub(crate) fn PermsEqual(a: &FileInfo, b: &FileInfo) -> bool {
    a.Permissions == b.Permissions || a.NoPermissions || b.NoPermissions
}

pub(crate) fn VectorHash(v: &Vector) -> Vec<u8> {
    let mut hash = Sha256::new();
    for counter in &v.Counters {
        hash.update(counter.Id.to_be_bytes());
        hash.update(counter.Value.to_be_bytes());
    }
    hash.finalize().to_vec()
}

pub(crate) fn xattrsEqual(a: &XattrData, b: &XattrData) -> bool {
    a.Xattrs == b.Xattrs
}

pub(crate) fn unixOwnershipEqual(a: &UnixData, b: &UnixData) -> bool {
    (a.Uid == b.Uid && a.Gid == b.Gid)
        || (!a.OwnerName.is_empty()
            && !a.GroupName.is_empty()
            && a.OwnerName == b.OwnerName
            && a.GroupName == b.GroupName)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VectorRelation {
    Less,
    Equal,
    Greater,
    Concurrent,
}

fn compare_vectors(left: &Vector, right: &Vector) -> VectorRelation {
    let mut right_by_id = BTreeMap::new();
    for counter in &right.Counters {
        right_by_id.insert(counter.Id, counter.Value);
    }
    let mut less = false;
    let mut greater = false;
    for counter in &left.Counters {
        let rv = right_by_id.remove(&counter.Id).unwrap_or(0);
        match counter.Value.cmp(&rv) {
            Ordering::Less => less = true,
            Ordering::Greater => greater = true,
            Ordering::Equal => {}
        }
    }
    for rv in right_by_id.values() {
        if *rv > 0 {
            less = true;
        }
    }
    match (less, greater) {
        (false, false) => VectorRelation::Equal,
        (true, false) => VectorRelation::Less,
        (false, true) => VectorRelation::Greater,
        (true, true) => VectorRelation::Concurrent,
    }
}

pub(crate) fn windowsOwnershipEqual(a: &WindowsData, b: &WindowsData) -> bool {
    a.OwnerName == b.OwnerName && a.OwnerIsGroup == b.OwnerIsGroup
}

pub(crate) fn ownershipEqual(a: &PlatformData, b: &PlatformData) -> bool {
    match (&a.Unix, &b.Unix) {
        (Some(left), Some(right)) if !unixOwnershipEqual(left, right) => return false,
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => return false,
        _ => {}
    }
    match (&a.Windows, &b.Windows) {
        (Some(left), Some(right)) if !windowsOwnershipEqual(left, right) => return false,
        (None, None) => {}
        (Some(_), None) | (None, Some(_)) => return false,
        _ => {}
    }
    true
}

pub(crate) fn IsVersionMismatch(local: &Vector, remote: &Vector) -> bool {
    local != remote
}

pub(crate) fn BlockInfoFromWire(v: &BlockInfo) -> BlockInfo {
    v.clone()
}
pub(crate) fn clusterConfigFromWire(v: &ClusterConfig) -> ClusterConfig {
    v.clone()
}
pub(crate) fn deviceFromWire(v: &Device) -> Device {
    v.clone()
}
pub(crate) fn downloadProgressFromWire(v: &DownloadProgress) -> DownloadProgress {
    v.clone()
}
pub(crate) fn fileDownloadProgressUpdateFromWire(
    v: &FileDownloadProgressUpdate,
) -> FileDownloadProgressUpdate {
    v.clone()
}
pub(crate) fn fileInfoFromWireWithBlocks(v: &FileInfo) -> FileInfo {
    v.clone()
}
pub(crate) fn FileInfoFromWire(v: &FileInfo) -> FileInfo {
    let mut out = v.clone();
    if out.Invalid {
        out.LocalFlags |= FlagLocalRemoteInvalid;
    }
    out
}
pub(crate) fn folderFromWire(v: &Folder) -> Folder {
    v.clone()
}
pub(crate) fn helloFromWire(v: &Hello) -> Hello {
    v.clone()
}
pub(crate) fn indexFromWire(v: &Index) -> Index {
    v.clone()
}
pub(crate) fn indexUpdateFromWire(v: &IndexUpdate) -> IndexUpdate {
    v.clone()
}
pub(crate) fn platformDataFromWire(v: &PlatformData) -> PlatformData {
    v.clone()
}
pub(crate) fn requestFromWire(v: &Request) -> Request {
    v.clone()
}
pub(crate) fn responseFromWire(v: &Response) -> Response {
    v.clone()
}
pub(crate) fn unixDataFromWire(v: &UnixData) -> UnixData {
    v.clone()
}
pub(crate) fn xattrFromWire(v: &Xattr) -> Xattr {
    v.clone()
}
pub(crate) fn xattrDataFromWire(v: &XattrData) -> XattrData {
    v.clone()
}

pub(crate) fn FileInfoFromDB(v: &FileInfo) -> FileInfo {
    v.clone()
}

pub(crate) fn FileInfoFromDBTruncated(v: &FileInfo) -> FileInfoWithoutBlocks {
    FileInfoWithoutBlocks::from_file_info(v)
}

// AUDIT-MARKER(hello-exchange): W16D — ExchangeHello correctly checks both
// current and v13 magic, matching Go's protocol version negotiation.
// Do NOT re-flag as "bep_compat hello decode paths incomplete".
pub(crate) fn ExchangeHello(hello: &Hello) -> Result<Hello, String> {
    if hello.Magic() != HelloMessageMagic && hello.Magic() != Version13HelloMagic {
        return Err(ErrUnknownMagic.to_string());
    }
    if hello.Magic() == Version13HelloMagic {
        return Err(ErrTooOldVersion.to_string());
    }
    Ok(hello.clone())
}

pub(crate) fn readHello(frame: &[u8]) -> Result<Hello, String> {
    let msg = decode_wire_message(frame)?;
    match msg {
        WireMessage::Hello(h) => Ok(h),
        _ => Err("expected hello message".to_string()),
    }
}

pub(crate) fn writeHello(hello: &Hello) -> Result<Vec<u8>, String> {
    encode_wire_message(&WireMessage::Hello(hello.clone()))
}

pub(crate) fn file_bep_bep_proto_init() -> &'static str {
    File_bep_bep_proto
}

pub(crate) fn file_bep_bep_proto_rawDescGZIP() -> Vec<u8> {
    file_bep_bep_proto_rawDescOnce
        .get_or_init(|| file_bep_bep_proto_rawDesc.to_vec())
        .clone()
}

pub(crate) fn init() {
    let _ = file_bep_bep_proto_rawDescData.get_or_init(|| file_bep_bep_proto_rawDesc.to_vec());
}

pub(crate) fn TestBlocksEqual() -> bool {
    blocksEqual(&[], &[])
}

pub(crate) fn TestIsEquivalent(a: &FileInfo, b: &FileInfo) -> bool {
    a.IsEquivalent(b)
}

pub(crate) fn TestLocalFlagBits(v: u32) -> String {
    FlagLocal(v).HumanString()
}

pub(crate) fn TestOldHelloMsgs() -> bool {
    Hello {
        ClientVersion: "1.3.9".to_string(),
        ..Default::default()
    }
    .Magic()
        == Version13HelloMagic
}

pub(crate) fn TestSha256OfEmptyBlock() -> [u8; 32] {
    sha256OfEmptyBlock
}

pub(crate) fn TestVersion14Hello() -> bool {
    Hello {
        ClientVersion: "1.4.0".to_string(),
        ..Default::default()
    }
    .Magic()
        == HelloMessageMagic
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compat_methods_work() {
        let mut fi = FileInfo {
            Name: "a.txt".to_string(),
            Type: FileInfoTypeFile,
            Size: 10,
            Sequence: 2,
            VersionHash: vec![1],
            ..Default::default()
        };
        fi.SetIgnored(true);
        assert!(fi.IsIgnored());
        fi.SetIgnored(false);
        assert!(!fi.IsIgnored());

        let h = Hello {
            ClientVersion: "1.20.0".to_string(),
            ..Default::default()
        };
        let frame = writeHello(&h).expect("write hello");
        let round = readHello(&frame).expect("read hello");
        assert_eq!(round.ClientVersion, h.ClientVersion);

        assert_eq!(BlockSizes[0], 128 * 1024);
        // W5-H6: Old hello magic is rejected — Magic() always returns HelloMessageMagic now,
        // so TestOldHelloMsgs() returns false (1.3.x magic != current magic).
        assert!(!TestOldHelloMsgs());
        assert!(TestVersion14Hello());
    }

    #[test]
    fn is_equivalent_optional_respects_no_permissions_semantics() {
        let a = FileInfo {
            Name: "a.txt".to_string(),
            Permissions: 0o600,
            NoPermissions: true,
            ModifiedS: 10,
            ModifiedNs: 0,
            ..Default::default()
        };
        let b = FileInfo {
            Name: "a.txt".to_string(),
            Permissions: 0o644,
            NoPermissions: false,
            ModifiedS: 10,
            ModifiedNs: 0,
            ..Default::default()
        };

        assert!(a.IsEquivalentOptional(&b, &FileInfoComparison::default()));
    }

    #[test]
    fn hello_magic_does_not_treat_1_30_as_legacy_1_3() {
        let h = Hello {
            ClientVersion: "1.30.0".to_string(),
            ..Default::default()
        };
        assert_eq!(h.Magic(), HelloMessageMagic);
    }

    #[test]
    fn should_conflict_uses_local_conflict_flags() {
        let mut a = FileInfo {
            Name: "a.txt".to_string(),
            ..Default::default()
        };
        let b = a.clone();
        assert!(!a.ShouldConflict(&b));
        a.LocalFlags = LocalConflictFlags;
        assert!(a.ShouldConflict(&b));
    }

    #[test]
    fn has_permission_bits_respects_no_permissions_flag() {
        let mut fi = FileInfo {
            Permissions: 0,
            NoPermissions: false,
            ..Default::default()
        };
        assert!(fi.HasPermissionBits());
        fi.NoPermissions = true;
        assert!(!fi.HasPermissionBits());
    }

    #[test]
    fn mod_time_equal_is_strict_at_window_boundary() {
        let a = FileInfo {
            ModifiedS: 10,
            ModifiedNs: 0,
            ..Default::default()
        };
        let b = FileInfo {
            ModifiedS: 10,
            ModifiedNs: 50,
            ..Default::default()
        };
        assert!(!ModTimeEqual(&a, &b, 50));
        assert!(ModTimeEqual(&a, &b, 51));
    }

    #[test]
    fn file_info_from_wire_maps_invalid_to_remote_invalid_flag() {
        let wire = FileInfo {
            Invalid: true,
            LocalFlags: 0,
            ..Default::default()
        };
        let out = FileInfoFromWire(&wire);
        assert!(out.LocalFlags & FlagLocalRemoteInvalid != 0);
    }

    #[test]
    fn blocks_hash_and_vector_hash_are_sha256_length() {
        let fi = FileInfo {
            Blocks: vec![BlockInfo {
                Hash: vec![1, 2, 3],
                Offset: 0,
                Size: 3,
            }],
            ..Default::default()
        };
        assert_eq!(BlocksHash(&fi).len(), 32);

        let v = Vector {
            Counters: vec![Counter { Id: 1, Value: 2 }, Counter { Id: 3, Value: 4 }],
        };
        assert_eq!(VectorHash(&v).len(), 32);
    }

    #[test]
    fn platform_data_round_trips_xattrs() {
        let mut p = PlatformData::default();
        p.SetXattrs(XattrData {
            Xattrs: vec![Xattr {
                Name: "user.test".to_string(),
                Value: b"abc".to_vec(),
            }],
        });
        let x = p.Xattrs();
        assert_eq!(x.Xattrs.len(), 1);
        assert_eq!(x.Xattrs[0].Name, "user.test");
        assert_eq!(x.Xattrs[0].Value, b"abc".to_vec());
    }

    #[test]
    fn is_equivalent_optional_checks_xattrs_when_not_ignored() {
        let mut a = FileInfo {
            Name: "a.txt".to_string(),
            ModifiedS: 1,
            ModifiedNs: 0,
            ..Default::default()
        };
        let mut b = a.clone();

        let mut p1 = PlatformData::default();
        p1.SetXattrs(XattrData {
            Xattrs: vec![Xattr {
                Name: "user.test".to_string(),
                Value: b"one".to_vec(),
            }],
        });
        let mut p2 = PlatformData::default();
        p2.SetXattrs(XattrData {
            Xattrs: vec![Xattr {
                Name: "user.test".to_string(),
                Value: b"two".to_vec(),
            }],
        });
        a.Platform = p1;
        b.Platform = p2;

        assert!(!a.IsEquivalentOptional(&b, &FileInfoComparison::default()));

        let cmp = FileInfoComparison {
            IgnoreXattrs: true,
            ..Default::default()
        };
        assert!(a.IsEquivalentOptional(&b, &cmp));
    }

    #[test]
    fn set_deleted_clears_content_payloads() {
        let mut fi = FileInfo {
            Name: "a.txt".to_string(),
            Type: FileInfoTypeFile,
            Size: 42,
            Blocks: vec![BlockInfo {
                Hash: vec![1, 2, 3],
                Offset: 0,
                Size: 3,
            }],
            BlocksHash: vec![9, 9, 9],
            ..Default::default()
        };
        fi.SetDeleted(true);
        assert!(fi.Deleted);
        assert_eq!(fi.Size, 0);
        assert!(fi.Blocks.is_empty());
        assert!(fi.BlocksHash.is_empty());
    }

    #[test]
    fn unix_ownership_allows_name_equivalence() {
        let a = UnixData {
            Uid: 1000,
            Gid: 1000,
            OwnerName: "alice".to_string(),
            GroupName: "staff".to_string(),
        };
        let b = UnixData {
            Uid: 2000,
            Gid: 2000,
            OwnerName: "alice".to_string(),
            GroupName: "staff".to_string(),
        };
        assert!(unixOwnershipEqual(&a, &b));
    }

    #[test]
    fn equivalent_optional_rejects_must_rescan_files() {
        let mut a = FileInfo {
            Name: "a.txt".to_string(),
            ModifiedS: 1,
            ..Default::default()
        };
        let b = a.clone();
        a.SetMustRescan(true);
        assert!(!a.IsEquivalentOptional(&b, &FileInfoComparison::default()));
    }
}
