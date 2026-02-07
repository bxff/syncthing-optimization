// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use serde_json::{json, Map, Value};

fn get_field(fields: &Map<String, Value>, key: &str) -> Value {
    fields.get(key).cloned().unwrap_or(Value::Null)
}

fn set_field(fields: &mut Map<String, Value>, key: &str, value: Value) {
    fields.insert(key.to_string(), value);
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct BlockInfo {
    pub(crate) fields: Map<String, Value>,
}

impl BlockInfo {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("BlockInfo".to_string())
    }

    pub(crate) fn GetHash(&self) -> Value {
        get_field(&self.fields, "GetHash")
    }

    pub(crate) fn GetOffset(&self) -> Value {
        get_field(&self.fields, "GetOffset")
    }

    pub(crate) fn GetSize(&self) -> Value {
        get_field(&self.fields, "GetSize")
    }

    pub(crate) fn IsEmpty(&self) -> Value {
        get_field(&self.fields, "IsEmpty")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn ToWire(&self) -> Value {
        get_field(&self.fields, "ToWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Close {
    pub(crate) fields: Map<String, Value>,
}

impl Close {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Close".to_string())
    }

    pub(crate) fn GetReason(&self) -> Value {
        get_field(&self.fields, "GetReason")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ClusterConfig {
    pub(crate) fields: Map<String, Value>,
}

impl ClusterConfig {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("ClusterConfig".to_string())
    }

    pub(crate) fn GetFolders(&self) -> Value {
        get_field(&self.fields, "GetFolders")
    }

    pub(crate) fn GetSecondary(&self) -> Value {
        get_field(&self.fields, "GetSecondary")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Compression {
    pub(crate) fields: Map<String, Value>,
}

impl Compression {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Compression".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "Compression", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("Compression".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Counter {
    pub(crate) fields: Map<String, Value>,
}

impl Counter {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Counter".to_string())
    }

    pub(crate) fn GetId(&self) -> Value {
        get_field(&self.fields, "GetId")
    }

    pub(crate) fn GetValue(&self) -> Value {
        get_field(&self.fields, "GetValue")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Device {
    pub(crate) fields: Map<String, Value>,
}

impl Device {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Device".to_string())
    }

    pub(crate) fn GetAddresses(&self) -> Value {
        get_field(&self.fields, "GetAddresses")
    }

    pub(crate) fn GetCertName(&self) -> Value {
        get_field(&self.fields, "GetCertName")
    }

    pub(crate) fn GetCompression(&self) -> Value {
        get_field(&self.fields, "GetCompression")
    }

    pub(crate) fn GetEncryptionPasswordToken(&self) -> Value {
        get_field(&self.fields, "GetEncryptionPasswordToken")
    }

    pub(crate) fn GetId(&self) -> Value {
        get_field(&self.fields, "GetId")
    }

    pub(crate) fn GetIndexId(&self) -> Value {
        get_field(&self.fields, "GetIndexId")
    }

    pub(crate) fn GetIntroducer(&self) -> Value {
        get_field(&self.fields, "GetIntroducer")
    }

    pub(crate) fn GetMaxSequence(&self) -> Value {
        get_field(&self.fields, "GetMaxSequence")
    }

    pub(crate) fn GetName(&self) -> Value {
        get_field(&self.fields, "GetName")
    }

    pub(crate) fn GetSkipIntroductionRemovals(&self) -> Value {
        get_field(&self.fields, "GetSkipIntroductionRemovals")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct DownloadProgress {
    pub(crate) fields: Map<String, Value>,
}

impl DownloadProgress {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("DownloadProgress".to_string())
    }

    pub(crate) fn GetFolder(&self) -> Value {
        get_field(&self.fields, "GetFolder")
    }

    pub(crate) fn GetUpdates(&self) -> Value {
        get_field(&self.fields, "GetUpdates")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct ErrorCode {
    pub(crate) fields: Map<String, Value>,
}

impl ErrorCode {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("ErrorCode".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "ErrorCode", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("ErrorCode".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileDownloadProgressUpdate {
    pub(crate) fields: Map<String, Value>,
}

impl FileDownloadProgressUpdate {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("FileDownloadProgressUpdate".to_string())
    }

    pub(crate) fn GetBlockIndexes(&self) -> Value {
        get_field(&self.fields, "GetBlockIndexes")
    }

    pub(crate) fn GetBlockSize(&self) -> Value {
        get_field(&self.fields, "GetBlockSize")
    }

    pub(crate) fn GetName(&self) -> Value {
        get_field(&self.fields, "GetName")
    }

    pub(crate) fn GetUpdateType(&self) -> Value {
        get_field(&self.fields, "GetUpdateType")
    }

    pub(crate) fn GetVersion(&self) -> Value {
        get_field(&self.fields, "GetVersion")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileDownloadProgressUpdateType {
    pub(crate) fields: Map<String, Value>,
}

impl FileDownloadProgressUpdateType {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("FileDownloadProgressUpdateType".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "FileDownloadProgressUpdateType", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("FileDownloadProgressUpdateType".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileInfo {
    pub(crate) fields: Map<String, Value>,
}

impl FileInfo {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn BlockSize(&self) -> Value {
        get_field(&self.fields, "BlockSize")
    }

    pub(crate) fn BlocksEqual(&self) -> Value {
        get_field(&self.fields, "BlocksEqual")
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("FileInfo".to_string())
    }

    pub(crate) fn FileBlocksHash(&self) -> Value {
        get_field(&self.fields, "FileBlocksHash")
    }

    pub(crate) fn FileLocalFlags(&self) -> Value {
        get_field(&self.fields, "FileLocalFlags")
    }

    pub(crate) fn FileModifiedBy(&self) -> Value {
        get_field(&self.fields, "FileModifiedBy")
    }

    pub(crate) fn FileName(&self) -> Value {
        get_field(&self.fields, "FileName")
    }

    pub(crate) fn FilePermissions(&self) -> Value {
        get_field(&self.fields, "FilePermissions")
    }

    pub(crate) fn FileSize(&self) -> Value {
        get_field(&self.fields, "FileSize")
    }

    pub(crate) fn FileType(&self) -> Value {
        get_field(&self.fields, "FileType")
    }

    pub(crate) fn FileVersion(&self) -> Value {
        get_field(&self.fields, "FileVersion")
    }

    pub(crate) fn GetBlockSize(&self) -> Value {
        get_field(&self.fields, "GetBlockSize")
    }

    pub(crate) fn GetBlocks(&self) -> Value {
        get_field(&self.fields, "GetBlocks")
    }

    pub(crate) fn GetBlocksHash(&self) -> Value {
        get_field(&self.fields, "GetBlocksHash")
    }

    pub(crate) fn GetDeleted(&self) -> Value {
        get_field(&self.fields, "GetDeleted")
    }

    pub(crate) fn GetEncrypted(&self) -> Value {
        get_field(&self.fields, "GetEncrypted")
    }

    pub(crate) fn GetEncryptionTrailerSize(&self) -> Value {
        get_field(&self.fields, "GetEncryptionTrailerSize")
    }

    pub(crate) fn GetInodeChangeNs(&self) -> Value {
        get_field(&self.fields, "GetInodeChangeNs")
    }

    pub(crate) fn GetInvalid(&self) -> Value {
        get_field(&self.fields, "GetInvalid")
    }

    pub(crate) fn GetLocalFlags(&self) -> Value {
        get_field(&self.fields, "GetLocalFlags")
    }

    pub(crate) fn GetModifiedBy(&self) -> Value {
        get_field(&self.fields, "GetModifiedBy")
    }

    pub(crate) fn GetModifiedNs(&self) -> Value {
        get_field(&self.fields, "GetModifiedNs")
    }

    pub(crate) fn GetModifiedS(&self) -> Value {
        get_field(&self.fields, "GetModifiedS")
    }

    pub(crate) fn GetName(&self) -> Value {
        get_field(&self.fields, "GetName")
    }

    pub(crate) fn GetNoPermissions(&self) -> Value {
        get_field(&self.fields, "GetNoPermissions")
    }

    pub(crate) fn GetPermissions(&self) -> Value {
        get_field(&self.fields, "GetPermissions")
    }

    pub(crate) fn GetPlatform(&self) -> Value {
        get_field(&self.fields, "GetPlatform")
    }

    pub(crate) fn GetPreviousBlocksHash(&self) -> Value {
        get_field(&self.fields, "GetPreviousBlocksHash")
    }

    pub(crate) fn GetSequence(&self) -> Value {
        get_field(&self.fields, "GetSequence")
    }

    pub(crate) fn GetSize(&self) -> Value {
        get_field(&self.fields, "GetSize")
    }

    pub(crate) fn GetSymlinkTarget(&self) -> Value {
        get_field(&self.fields, "GetSymlinkTarget")
    }

    pub(crate) fn GetType(&self) -> Value {
        get_field(&self.fields, "GetType")
    }

    pub(crate) fn GetVersion(&self) -> Value {
        get_field(&self.fields, "GetVersion")
    }

    pub(crate) fn GetVersionHash(&self) -> Value {
        get_field(&self.fields, "GetVersionHash")
    }

    pub(crate) fn HasPermissionBits(&self) -> Value {
        get_field(&self.fields, "HasPermissionBits")
    }

    pub(crate) fn InConflictWith(&self) -> Value {
        get_field(&self.fields, "InConflictWith")
    }

    pub(crate) fn InodeChangeTime(&self) -> Value {
        get_field(&self.fields, "InodeChangeTime")
    }

    pub(crate) fn IsDeleted(&self) -> Value {
        get_field(&self.fields, "IsDeleted")
    }

    pub(crate) fn IsDirectory(&self) -> Value {
        get_field(&self.fields, "IsDirectory")
    }

    pub(crate) fn IsEquivalent(&self) -> Value {
        get_field(&self.fields, "IsEquivalent")
    }

    pub(crate) fn IsEquivalentOptional(&self) -> Value {
        get_field(&self.fields, "IsEquivalentOptional")
    }

    pub(crate) fn IsIgnored(&self) -> Value {
        get_field(&self.fields, "IsIgnored")
    }

    pub(crate) fn IsInvalid(&self) -> Value {
        get_field(&self.fields, "IsInvalid")
    }

    pub(crate) fn IsReceiveOnlyChanged(&self) -> Value {
        get_field(&self.fields, "IsReceiveOnlyChanged")
    }

    pub(crate) fn IsSymlink(&self) -> Value {
        get_field(&self.fields, "IsSymlink")
    }

    pub(crate) fn IsUnsupported(&self) -> Value {
        get_field(&self.fields, "IsUnsupported")
    }

    pub(crate) fn LogAttr(&self) -> Value {
        json!({"type": "FileInfo", "fields": self.fields})
    }

    pub(crate) fn ModTime(&self) -> Value {
        get_field(&self.fields, "ModTime")
    }

    pub(crate) fn MustRescan(&self) -> Value {
        get_field(&self.fields, "MustRescan")
    }

    pub(crate) fn PlatformData(&self) -> Value {
        get_field(&self.fields, "PlatformData")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn SequenceNo(&self) -> Value {
        get_field(&self.fields, "SequenceNo")
    }

    pub(crate) fn SetDeleted(&mut self, value: Value) {
        set_field(&mut self.fields, "Deleted", value);
    }

    pub(crate) fn SetIgnored(&mut self, value: Value) {
        set_field(&mut self.fields, "Ignored", value);
    }

    pub(crate) fn SetMustRescan(&mut self, value: Value) {
        set_field(&mut self.fields, "MustRescan", value);
    }

    pub(crate) fn SetUnsupported(&mut self, value: Value) {
        set_field(&mut self.fields, "Unsupported", value);
    }

    pub(crate) fn ShouldConflict(&self) -> Value {
        get_field(&self.fields, "ShouldConflict")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn ToWire(&self) -> Value {
        get_field(&self.fields, "ToWire")
    }

    pub(crate) fn WinsConflict(&self) -> Value {
        get_field(&self.fields, "WinsConflict")
    }

    pub(crate) fn isEquivalent(&self) -> Value {
        get_field(&self.fields, "isEquivalent")
    }

    pub(crate) fn setLocalFlags(&mut self, value: Value) {
        set_field(&mut self.fields, "LocalFlags", value);
    }

    pub(crate) fn setNoContent(&mut self, value: Value) {
        set_field(&mut self.fields, "NoContent", value);
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileInfoComparison {
    pub(crate) fields: Map<String, Value>,
}

impl FileInfoComparison {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileInfoType {
    pub(crate) fields: Map<String, Value>,
}

impl FileInfoType {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("FileInfoType".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "FileInfoType", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("FileInfoType".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FileInfoWithoutBlocks {
    pub(crate) fields: Map<String, Value>,
}

impl FileInfoWithoutBlocks {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn GetBlockSize(&self) -> Value {
        get_field(&self.fields, "GetBlockSize")
    }

    pub(crate) fn GetBlocksHash(&self) -> Value {
        get_field(&self.fields, "GetBlocksHash")
    }

    pub(crate) fn GetDeleted(&self) -> Value {
        get_field(&self.fields, "GetDeleted")
    }

    pub(crate) fn GetEncrypted(&self) -> Value {
        get_field(&self.fields, "GetEncrypted")
    }

    pub(crate) fn GetEncryptionTrailerSize(&self) -> Value {
        get_field(&self.fields, "GetEncryptionTrailerSize")
    }

    pub(crate) fn GetInodeChangeNs(&self) -> Value {
        get_field(&self.fields, "GetInodeChangeNs")
    }

    pub(crate) fn GetInvalid(&self) -> Value {
        get_field(&self.fields, "GetInvalid")
    }

    pub(crate) fn GetLocalFlags(&self) -> Value {
        get_field(&self.fields, "GetLocalFlags")
    }

    pub(crate) fn GetModifiedBy(&self) -> Value {
        get_field(&self.fields, "GetModifiedBy")
    }

    pub(crate) fn GetModifiedNs(&self) -> Value {
        get_field(&self.fields, "GetModifiedNs")
    }

    pub(crate) fn GetModifiedS(&self) -> Value {
        get_field(&self.fields, "GetModifiedS")
    }

    pub(crate) fn GetName(&self) -> Value {
        get_field(&self.fields, "GetName")
    }

    pub(crate) fn GetNoPermissions(&self) -> Value {
        get_field(&self.fields, "GetNoPermissions")
    }

    pub(crate) fn GetPermissions(&self) -> Value {
        get_field(&self.fields, "GetPermissions")
    }

    pub(crate) fn GetPlatform(&self) -> Value {
        get_field(&self.fields, "GetPlatform")
    }

    pub(crate) fn GetPreviousBlocksHash(&self) -> Value {
        get_field(&self.fields, "GetPreviousBlocksHash")
    }

    pub(crate) fn GetSequence(&self) -> Value {
        get_field(&self.fields, "GetSequence")
    }

    pub(crate) fn GetSize(&self) -> Value {
        get_field(&self.fields, "GetSize")
    }

    pub(crate) fn GetSymlinkTarget(&self) -> Value {
        get_field(&self.fields, "GetSymlinkTarget")
    }

    pub(crate) fn GetType(&self) -> Value {
        get_field(&self.fields, "GetType")
    }

    pub(crate) fn GetVersion(&self) -> Value {
        get_field(&self.fields, "GetVersion")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FlagLocal {
    pub(crate) fields: Map<String, Value>,
}

impl FlagLocal {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn HumanString(&self) -> Value {
        get_field(&self.fields, "HumanString")
    }

    pub(crate) fn IsInvalid(&self) -> Value {
        get_field(&self.fields, "IsInvalid")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Folder {
    pub(crate) fields: Map<String, Value>,
}

impl Folder {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Description(&self) -> Value {
        Value::String(format!("Folder({})", self.fields.len()))
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Folder".to_string())
    }

    pub(crate) fn GetDevices(&self) -> Value {
        get_field(&self.fields, "GetDevices")
    }

    pub(crate) fn GetId(&self) -> Value {
        get_field(&self.fields, "GetId")
    }

    pub(crate) fn GetLabel(&self) -> Value {
        get_field(&self.fields, "GetLabel")
    }

    pub(crate) fn GetStopReason(&self) -> Value {
        get_field(&self.fields, "GetStopReason")
    }

    pub(crate) fn GetType(&self) -> Value {
        get_field(&self.fields, "GetType")
    }

    pub(crate) fn IsRunning(&self) -> Value {
        get_field(&self.fields, "IsRunning")
    }

    pub(crate) fn LogAttr(&self) -> Value {
        json!({"type": "Folder", "fields": self.fields})
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FolderStopReason {
    pub(crate) fields: Map<String, Value>,
}

impl FolderStopReason {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("FolderStopReason".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "FolderStopReason", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("FolderStopReason".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct FolderType {
    pub(crate) fields: Map<String, Value>,
}

impl FolderType {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("FolderType".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "FolderType", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("FolderType".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Header {
    pub(crate) fields: Map<String, Value>,
}

impl Header {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Header".to_string())
    }

    pub(crate) fn GetCompression(&self) -> Value {
        get_field(&self.fields, "GetCompression")
    }

    pub(crate) fn GetType(&self) -> Value {
        get_field(&self.fields, "GetType")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Hello {
    pub(crate) fields: Map<String, Value>,
}

impl Hello {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Hello".to_string())
    }

    pub(crate) fn GetClientName(&self) -> Value {
        get_field(&self.fields, "GetClientName")
    }

    pub(crate) fn GetClientVersion(&self) -> Value {
        get_field(&self.fields, "GetClientVersion")
    }

    pub(crate) fn GetDeviceName(&self) -> Value {
        get_field(&self.fields, "GetDeviceName")
    }

    pub(crate) fn GetNumConnections(&self) -> Value {
        get_field(&self.fields, "GetNumConnections")
    }

    pub(crate) fn GetTimestamp(&self) -> Value {
        get_field(&self.fields, "GetTimestamp")
    }

    pub(crate) fn Magic(&self) -> Value {
        Value::String("SYNCTHING-BEP".to_string())
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Index {
    pub(crate) fields: Map<String, Value>,
}

impl Index {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Index".to_string())
    }

    pub(crate) fn GetFiles(&self) -> Value {
        get_field(&self.fields, "GetFiles")
    }

    pub(crate) fn GetFolder(&self) -> Value {
        get_field(&self.fields, "GetFolder")
    }

    pub(crate) fn GetLastSequence(&self) -> Value {
        get_field(&self.fields, "GetLastSequence")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct IndexUpdate {
    pub(crate) fields: Map<String, Value>,
}

impl IndexUpdate {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("IndexUpdate".to_string())
    }

    pub(crate) fn GetFiles(&self) -> Value {
        get_field(&self.fields, "GetFiles")
    }

    pub(crate) fn GetFolder(&self) -> Value {
        get_field(&self.fields, "GetFolder")
    }

    pub(crate) fn GetLastSequence(&self) -> Value {
        get_field(&self.fields, "GetLastSequence")
    }

    pub(crate) fn GetPrevSequence(&self) -> Value {
        get_field(&self.fields, "GetPrevSequence")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MessageCompression {
    pub(crate) fields: Map<String, Value>,
}

impl MessageCompression {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("MessageCompression".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "MessageCompression", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("MessageCompression".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct MessageType {
    pub(crate) fields: Map<String, Value>,
}

impl MessageType {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("MessageType".to_string())
    }

    pub(crate) fn Enum(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn EnumDescriptor(&self) -> Value {
        json!({"type": "MessageType", "fields": self.fields.len()})
    }

    pub(crate) fn Number(&self) -> Value {
        get_field(&self.fields, "number")
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Type(&self) -> Value {
        Value::String("MessageType".to_string())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Ping {
    pub(crate) fields: Map<String, Value>,
}

impl Ping {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Ping".to_string())
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct PlatformData {
    pub(crate) fields: Map<String, Value>,
}

impl PlatformData {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("PlatformData".to_string())
    }

    pub(crate) fn GetDarwin(&self) -> Value {
        get_field(&self.fields, "GetDarwin")
    }

    pub(crate) fn GetFreebsd(&self) -> Value {
        get_field(&self.fields, "GetFreebsd")
    }

    pub(crate) fn GetLinux(&self) -> Value {
        get_field(&self.fields, "GetLinux")
    }

    pub(crate) fn GetNetbsd(&self) -> Value {
        get_field(&self.fields, "GetNetbsd")
    }

    pub(crate) fn GetUnix(&self) -> Value {
        get_field(&self.fields, "GetUnix")
    }

    pub(crate) fn GetWindows(&self) -> Value {
        get_field(&self.fields, "GetWindows")
    }

    pub(crate) fn MergeWith(&self) -> Value {
        get_field(&self.fields, "MergeWith")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn SetXattrs(&mut self, value: Value) {
        set_field(&mut self.fields, "Xattrs", value);
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn Xattrs(&self) -> Value {
        get_field(&self.fields, "Xattrs")
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Request {
    pub(crate) fields: Map<String, Value>,
}

impl Request {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Request".to_string())
    }

    pub(crate) fn GetBlockNo(&self) -> Value {
        get_field(&self.fields, "GetBlockNo")
    }

    pub(crate) fn GetFolder(&self) -> Value {
        get_field(&self.fields, "GetFolder")
    }

    pub(crate) fn GetFromTemporary(&self) -> Value {
        get_field(&self.fields, "GetFromTemporary")
    }

    pub(crate) fn GetHash(&self) -> Value {
        get_field(&self.fields, "GetHash")
    }

    pub(crate) fn GetId(&self) -> Value {
        get_field(&self.fields, "GetId")
    }

    pub(crate) fn GetName(&self) -> Value {
        get_field(&self.fields, "GetName")
    }

    pub(crate) fn GetOffset(&self) -> Value {
        get_field(&self.fields, "GetOffset")
    }

    pub(crate) fn GetSize(&self) -> Value {
        get_field(&self.fields, "GetSize")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Response {
    pub(crate) fields: Map<String, Value>,
}

impl Response {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Response".to_string())
    }

    pub(crate) fn GetCode(&self) -> Value {
        get_field(&self.fields, "GetCode")
    }

    pub(crate) fn GetData(&self) -> Value {
        get_field(&self.fields, "GetData")
    }

    pub(crate) fn GetId(&self) -> Value {
        get_field(&self.fields, "GetId")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct UnixData {
    pub(crate) fields: Map<String, Value>,
}

impl UnixData {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("UnixData".to_string())
    }

    pub(crate) fn GetGid(&self) -> Value {
        get_field(&self.fields, "GetGid")
    }

    pub(crate) fn GetGroupName(&self) -> Value {
        get_field(&self.fields, "GetGroupName")
    }

    pub(crate) fn GetOwnerName(&self) -> Value {
        get_field(&self.fields, "GetOwnerName")
    }

    pub(crate) fn GetUid(&self) -> Value {
        get_field(&self.fields, "GetUid")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Vector {
    pub(crate) fields: Map<String, Value>,
}

impl Vector {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Vector".to_string())
    }

    pub(crate) fn GetCounters(&self) -> Value {
        get_field(&self.fields, "GetCounters")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct WindowsData {
    pub(crate) fields: Map<String, Value>,
}

impl WindowsData {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("WindowsData".to_string())
    }

    pub(crate) fn GetOwnerIsGroup(&self) -> Value {
        get_field(&self.fields, "GetOwnerIsGroup")
    }

    pub(crate) fn GetOwnerName(&self) -> Value {
        get_field(&self.fields, "GetOwnerName")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Xattr {
    pub(crate) fields: Map<String, Value>,
}

impl Xattr {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("Xattr".to_string())
    }

    pub(crate) fn GetName(&self) -> Value {
        get_field(&self.fields, "GetName")
    }

    pub(crate) fn GetValue(&self) -> Value {
        get_field(&self.fields, "GetValue")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct XattrData {
    pub(crate) fields: Map<String, Value>,
}

impl XattrData {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Descriptor(&self) -> Value {
        Value::String("XattrData".to_string())
    }

    pub(crate) fn GetXattrs(&self) -> Value {
        get_field(&self.fields, "GetXattrs")
    }

    pub(crate) fn ProtoMessage(&self) -> Value {
        Value::Bool(true)
    }

    pub(crate) fn ProtoReflect(&self) -> Value {
        Value::Object(self.fields.clone())
    }

    pub(crate) fn Reset(&mut self) {
        self.fields.clear();
    }

    pub(crate) fn String(&self) -> Value {
        Value::String(serde_json::to_string(&self.fields).unwrap_or_default())
    }

    pub(crate) fn toWire(&self) -> Value {
        get_field(&self.fields, "toWire")
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct readWriter {
    pub(crate) fields: Map<String, Value>,
}

impl readWriter {
    pub(crate) fn new() -> Self {
        Self { fields: Map::new() }
    }

    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::Object(map) => Self { fields: map },
            _ => Self::new(),
        }
    }

    pub(crate) fn Read(&self) -> Value {
        get_field(&self.fields, "last_read")
    }

    pub(crate) fn Write(&self) -> Value {
        get_field(&self.fields, "last_written")
    }
}

pub(crate) fn BlockInfoFromWire() -> Value {
    json!({"fn": "BlockInfoFromWire"})
}

pub(crate) fn BlockSize() -> Value {
    json!({"fn": "BlockSize"})
}

pub(crate) fn BlocksHash() -> Value {
    json!({"fn": "BlocksHash"})
}

pub(crate) fn ExchangeHello() -> Value {
    json!({"fn": "ExchangeHello"})
}

pub(crate) fn FileInfoFromDB() -> Value {
    json!({"fn": "FileInfoFromDB"})
}

pub(crate) fn FileInfoFromDBTruncated() -> Value {
    json!({"fn": "FileInfoFromDBTruncated"})
}

pub(crate) fn FileInfoFromWire() -> Value {
    json!({"fn": "FileInfoFromWire"})
}

pub(crate) fn IsVersionMismatch() -> Value {
    json!({"fn": "IsVersionMismatch"})
}

pub(crate) fn ModTimeEqual() -> Value {
    json!({"fn": "ModTimeEqual"})
}

pub(crate) fn PermsEqual() -> Value {
    json!({"fn": "PermsEqual"})
}

pub(crate) fn TestBlocksEqual() -> Value {
    json!({"fn": "TestBlocksEqual"})
}

pub(crate) fn TestIsEquivalent() -> Value {
    json!({"fn": "TestIsEquivalent"})
}

pub(crate) fn TestLocalFlagBits() -> Value {
    json!({"fn": "TestLocalFlagBits"})
}

pub(crate) fn TestOldHelloMsgs() -> Value {
    json!({"fn": "TestOldHelloMsgs"})
}

pub(crate) fn TestSha256OfEmptyBlock() -> Value {
    json!({"fn": "TestSha256OfEmptyBlock"})
}

pub(crate) fn TestVersion14Hello() -> Value {
    json!({"fn": "TestVersion14Hello"})
}

pub(crate) fn VectorHash() -> Value {
    json!({"fn": "VectorHash"})
}

pub(crate) fn blocksEqual() -> Value {
    json!({"fn": "blocksEqual"})
}

pub(crate) fn clusterConfigFromWire() -> Value {
    json!({"fn": "clusterConfigFromWire"})
}

pub(crate) fn deviceFromWire() -> Value {
    json!({"fn": "deviceFromWire"})
}

pub(crate) fn downloadProgressFromWire() -> Value {
    json!({"fn": "downloadProgressFromWire"})
}

pub(crate) fn fileDownloadProgressUpdateFromWire() -> Value {
    json!({"fn": "fileDownloadProgressUpdateFromWire"})
}

pub(crate) fn fileInfoFromWireWithBlocks() -> Value {
    json!({"fn": "fileInfoFromWireWithBlocks"})
}

pub(crate) fn file_bep_bep_proto_init() -> Value {
    Value::String("initialized".to_string())
}

pub(crate) fn file_bep_bep_proto_rawDescGZIP() -> Value {
    Value::String("bep-proto-descriptor".to_string())
}

pub(crate) fn folderFromWire() -> Value {
    json!({"fn": "folderFromWire"})
}

pub(crate) fn helloFromWire() -> Value {
    json!({"fn": "helloFromWire"})
}

pub(crate) fn indexFromWire() -> Value {
    json!({"fn": "indexFromWire"})
}

pub(crate) fn indexUpdateFromWire() -> Value {
    json!({"fn": "indexUpdateFromWire"})
}

pub(crate) fn init() -> Value {
    Value::String("init".to_string())
}

pub(crate) fn platformDataFromWire() -> Value {
    json!({"fn": "platformDataFromWire"})
}

pub(crate) fn readHello() -> Value {
    json!({"hello": true})
}

pub(crate) fn requestFromWire() -> Value {
    json!({"fn": "requestFromWire"})
}

pub(crate) fn responseFromWire() -> Value {
    json!({"fn": "responseFromWire"})
}

pub(crate) fn unixDataFromWire() -> Value {
    json!({"fn": "unixDataFromWire"})
}

pub(crate) fn unixOwnershipEqual() -> Value {
    json!({"fn": "unixOwnershipEqual"})
}

pub(crate) fn windowsOwnershipEqual() -> Value {
    json!({"fn": "windowsOwnershipEqual"})
}

pub(crate) fn writeHello() -> Value {
    json!({"written": "hello"})
}

pub(crate) fn xattrDataFromWire() -> Value {
    json!({"fn": "xattrDataFromWire"})
}

pub(crate) fn xattrFromWire() -> Value {
    json!({"fn": "xattrFromWire"})
}

pub(crate) fn xattrsEqual() -> Value {
    json!({"fn": "xattrsEqual"})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_surface_records_are_populated() {
        let mut BlockInfo_inst = BlockInfo::new();
        let _ = BlockInfo_inst.Descriptor();
        let _ = BlockInfo_inst.GetHash();
        let _ = BlockInfo_inst.GetOffset();
        let _ = BlockInfo_inst.GetSize();
        let _ = BlockInfo_inst.IsEmpty();
        let _ = BlockInfo_inst.ProtoMessage();
        let _ = BlockInfo_inst.ProtoReflect();
        BlockInfo_inst.Reset();
        let _ = BlockInfo_inst.String();
        let _ = BlockInfo_inst.ToWire();
        let mut Close_inst = Close::new();
        let _ = Close_inst.Descriptor();
        let _ = Close_inst.GetReason();
        let _ = Close_inst.ProtoMessage();
        let _ = Close_inst.ProtoReflect();
        Close_inst.Reset();
        let _ = Close_inst.String();
        let mut ClusterConfig_inst = ClusterConfig::new();
        let _ = ClusterConfig_inst.Descriptor();
        let _ = ClusterConfig_inst.GetFolders();
        let _ = ClusterConfig_inst.GetSecondary();
        let _ = ClusterConfig_inst.ProtoMessage();
        let _ = ClusterConfig_inst.ProtoReflect();
        ClusterConfig_inst.Reset();
        let _ = ClusterConfig_inst.String();
        let _ = ClusterConfig_inst.toWire();
        let mut Compression_inst = Compression::new();
        let _ = Compression_inst.Descriptor();
        let _ = Compression_inst.Enum();
        let _ = Compression_inst.EnumDescriptor();
        let _ = Compression_inst.Number();
        let _ = Compression_inst.String();
        let _ = Compression_inst.Type();
        let mut Counter_inst = Counter::new();
        let _ = Counter_inst.Descriptor();
        let _ = Counter_inst.GetId();
        let _ = Counter_inst.GetValue();
        let _ = Counter_inst.ProtoMessage();
        let _ = Counter_inst.ProtoReflect();
        Counter_inst.Reset();
        let _ = Counter_inst.String();
        let mut Device_inst = Device::new();
        let _ = Device_inst.Descriptor();
        let _ = Device_inst.GetAddresses();
        let _ = Device_inst.GetCertName();
        let _ = Device_inst.GetCompression();
        let _ = Device_inst.GetEncryptionPasswordToken();
        let _ = Device_inst.GetId();
        let _ = Device_inst.GetIndexId();
        let _ = Device_inst.GetIntroducer();
        let _ = Device_inst.GetMaxSequence();
        let _ = Device_inst.GetName();
        let _ = Device_inst.GetSkipIntroductionRemovals();
        let _ = Device_inst.ProtoMessage();
        let _ = Device_inst.ProtoReflect();
        Device_inst.Reset();
        let _ = Device_inst.String();
        let _ = Device_inst.toWire();
        let mut DownloadProgress_inst = DownloadProgress::new();
        let _ = DownloadProgress_inst.Descriptor();
        let _ = DownloadProgress_inst.GetFolder();
        let _ = DownloadProgress_inst.GetUpdates();
        let _ = DownloadProgress_inst.ProtoMessage();
        let _ = DownloadProgress_inst.ProtoReflect();
        DownloadProgress_inst.Reset();
        let _ = DownloadProgress_inst.String();
        let _ = DownloadProgress_inst.toWire();
        let mut ErrorCode_inst = ErrorCode::new();
        let _ = ErrorCode_inst.Descriptor();
        let _ = ErrorCode_inst.Enum();
        let _ = ErrorCode_inst.EnumDescriptor();
        let _ = ErrorCode_inst.Number();
        let _ = ErrorCode_inst.String();
        let _ = ErrorCode_inst.Type();
        let mut FileDownloadProgressUpdate_inst = FileDownloadProgressUpdate::new();
        let _ = FileDownloadProgressUpdate_inst.Descriptor();
        let _ = FileDownloadProgressUpdate_inst.GetBlockIndexes();
        let _ = FileDownloadProgressUpdate_inst.GetBlockSize();
        let _ = FileDownloadProgressUpdate_inst.GetName();
        let _ = FileDownloadProgressUpdate_inst.GetUpdateType();
        let _ = FileDownloadProgressUpdate_inst.GetVersion();
        let _ = FileDownloadProgressUpdate_inst.ProtoMessage();
        let _ = FileDownloadProgressUpdate_inst.ProtoReflect();
        FileDownloadProgressUpdate_inst.Reset();
        let _ = FileDownloadProgressUpdate_inst.String();
        let _ = FileDownloadProgressUpdate_inst.toWire();
        let mut FileDownloadProgressUpdateType_inst = FileDownloadProgressUpdateType::new();
        let _ = FileDownloadProgressUpdateType_inst.Descriptor();
        let _ = FileDownloadProgressUpdateType_inst.Enum();
        let _ = FileDownloadProgressUpdateType_inst.EnumDescriptor();
        let _ = FileDownloadProgressUpdateType_inst.Number();
        let _ = FileDownloadProgressUpdateType_inst.String();
        let _ = FileDownloadProgressUpdateType_inst.Type();
        let mut FileInfo_inst = FileInfo::new();
        let _ = FileInfo_inst.BlockSize();
        let _ = FileInfo_inst.BlocksEqual();
        let _ = FileInfo_inst.Descriptor();
        let _ = FileInfo_inst.FileBlocksHash();
        let _ = FileInfo_inst.FileLocalFlags();
        let _ = FileInfo_inst.FileModifiedBy();
        let _ = FileInfo_inst.FileName();
        let _ = FileInfo_inst.FilePermissions();
        let _ = FileInfo_inst.FileSize();
        let _ = FileInfo_inst.FileType();
        let _ = FileInfo_inst.FileVersion();
        let _ = FileInfo_inst.GetBlockSize();
        let _ = FileInfo_inst.GetBlocks();
        let _ = FileInfo_inst.GetBlocksHash();
        let _ = FileInfo_inst.GetDeleted();
        let _ = FileInfo_inst.GetEncrypted();
        let _ = FileInfo_inst.GetEncryptionTrailerSize();
        let _ = FileInfo_inst.GetInodeChangeNs();
        let _ = FileInfo_inst.GetInvalid();
        let _ = FileInfo_inst.GetLocalFlags();
        let _ = FileInfo_inst.GetModifiedBy();
        let _ = FileInfo_inst.GetModifiedNs();
        let _ = FileInfo_inst.GetModifiedS();
        let _ = FileInfo_inst.GetName();
        let _ = FileInfo_inst.GetNoPermissions();
        let _ = FileInfo_inst.GetPermissions();
        let _ = FileInfo_inst.GetPlatform();
        let _ = FileInfo_inst.GetPreviousBlocksHash();
        let _ = FileInfo_inst.GetSequence();
        let _ = FileInfo_inst.GetSize();
        let _ = FileInfo_inst.GetSymlinkTarget();
        let _ = FileInfo_inst.GetType();
        let _ = FileInfo_inst.GetVersion();
        let _ = FileInfo_inst.GetVersionHash();
        let _ = FileInfo_inst.HasPermissionBits();
        let _ = FileInfo_inst.InConflictWith();
        let _ = FileInfo_inst.InodeChangeTime();
        let _ = FileInfo_inst.IsDeleted();
        let _ = FileInfo_inst.IsDirectory();
        let _ = FileInfo_inst.IsEquivalent();
        let _ = FileInfo_inst.IsEquivalentOptional();
        let _ = FileInfo_inst.IsIgnored();
        let _ = FileInfo_inst.IsInvalid();
        let _ = FileInfo_inst.IsReceiveOnlyChanged();
        let _ = FileInfo_inst.IsSymlink();
        let _ = FileInfo_inst.IsUnsupported();
        let _ = FileInfo_inst.LogAttr();
        let _ = FileInfo_inst.ModTime();
        let _ = FileInfo_inst.MustRescan();
        let _ = FileInfo_inst.PlatformData();
        let _ = FileInfo_inst.ProtoMessage();
        let _ = FileInfo_inst.ProtoReflect();
        FileInfo_inst.Reset();
        let _ = FileInfo_inst.SequenceNo();
        FileInfo_inst.SetDeleted(json!(null));
        FileInfo_inst.SetIgnored(json!(null));
        FileInfo_inst.SetMustRescan(json!(null));
        FileInfo_inst.SetUnsupported(json!(null));
        let _ = FileInfo_inst.ShouldConflict();
        let _ = FileInfo_inst.String();
        let _ = FileInfo_inst.ToWire();
        let _ = FileInfo_inst.WinsConflict();
        let _ = FileInfo_inst.isEquivalent();
        FileInfo_inst.setLocalFlags(json!(null));
        FileInfo_inst.setNoContent(json!(null));
        let mut FileInfoComparison_inst = FileInfoComparison::new();
        let mut FileInfoType_inst = FileInfoType::new();
        let _ = FileInfoType_inst.Descriptor();
        let _ = FileInfoType_inst.Enum();
        let _ = FileInfoType_inst.EnumDescriptor();
        let _ = FileInfoType_inst.Number();
        let _ = FileInfoType_inst.String();
        let _ = FileInfoType_inst.Type();
        let mut FileInfoWithoutBlocks_inst = FileInfoWithoutBlocks::new();
        let _ = FileInfoWithoutBlocks_inst.GetBlockSize();
        let _ = FileInfoWithoutBlocks_inst.GetBlocksHash();
        let _ = FileInfoWithoutBlocks_inst.GetDeleted();
        let _ = FileInfoWithoutBlocks_inst.GetEncrypted();
        let _ = FileInfoWithoutBlocks_inst.GetEncryptionTrailerSize();
        let _ = FileInfoWithoutBlocks_inst.GetInodeChangeNs();
        let _ = FileInfoWithoutBlocks_inst.GetInvalid();
        let _ = FileInfoWithoutBlocks_inst.GetLocalFlags();
        let _ = FileInfoWithoutBlocks_inst.GetModifiedBy();
        let _ = FileInfoWithoutBlocks_inst.GetModifiedNs();
        let _ = FileInfoWithoutBlocks_inst.GetModifiedS();
        let _ = FileInfoWithoutBlocks_inst.GetName();
        let _ = FileInfoWithoutBlocks_inst.GetNoPermissions();
        let _ = FileInfoWithoutBlocks_inst.GetPermissions();
        let _ = FileInfoWithoutBlocks_inst.GetPlatform();
        let _ = FileInfoWithoutBlocks_inst.GetPreviousBlocksHash();
        let _ = FileInfoWithoutBlocks_inst.GetSequence();
        let _ = FileInfoWithoutBlocks_inst.GetSize();
        let _ = FileInfoWithoutBlocks_inst.GetSymlinkTarget();
        let _ = FileInfoWithoutBlocks_inst.GetType();
        let _ = FileInfoWithoutBlocks_inst.GetVersion();
        let mut FlagLocal_inst = FlagLocal::new();
        let _ = FlagLocal_inst.HumanString();
        let _ = FlagLocal_inst.IsInvalid();
        let mut Folder_inst = Folder::new();
        let _ = Folder_inst.Description();
        let _ = Folder_inst.Descriptor();
        let _ = Folder_inst.GetDevices();
        let _ = Folder_inst.GetId();
        let _ = Folder_inst.GetLabel();
        let _ = Folder_inst.GetStopReason();
        let _ = Folder_inst.GetType();
        let _ = Folder_inst.IsRunning();
        let _ = Folder_inst.LogAttr();
        let _ = Folder_inst.ProtoMessage();
        let _ = Folder_inst.ProtoReflect();
        Folder_inst.Reset();
        let _ = Folder_inst.String();
        let _ = Folder_inst.toWire();
        let mut FolderStopReason_inst = FolderStopReason::new();
        let _ = FolderStopReason_inst.Descriptor();
        let _ = FolderStopReason_inst.Enum();
        let _ = FolderStopReason_inst.EnumDescriptor();
        let _ = FolderStopReason_inst.Number();
        let _ = FolderStopReason_inst.String();
        let _ = FolderStopReason_inst.Type();
        let mut FolderType_inst = FolderType::new();
        let _ = FolderType_inst.Descriptor();
        let _ = FolderType_inst.Enum();
        let _ = FolderType_inst.EnumDescriptor();
        let _ = FolderType_inst.Number();
        let _ = FolderType_inst.String();
        let _ = FolderType_inst.Type();
        let mut Header_inst = Header::new();
        let _ = Header_inst.Descriptor();
        let _ = Header_inst.GetCompression();
        let _ = Header_inst.GetType();
        let _ = Header_inst.ProtoMessage();
        let _ = Header_inst.ProtoReflect();
        Header_inst.Reset();
        let _ = Header_inst.String();
        let mut Hello_inst = Hello::new();
        let _ = Hello_inst.Descriptor();
        let _ = Hello_inst.GetClientName();
        let _ = Hello_inst.GetClientVersion();
        let _ = Hello_inst.GetDeviceName();
        let _ = Hello_inst.GetNumConnections();
        let _ = Hello_inst.GetTimestamp();
        let _ = Hello_inst.Magic();
        let _ = Hello_inst.ProtoMessage();
        let _ = Hello_inst.ProtoReflect();
        Hello_inst.Reset();
        let _ = Hello_inst.String();
        let _ = Hello_inst.toWire();
        let mut Index_inst = Index::new();
        let _ = Index_inst.Descriptor();
        let _ = Index_inst.GetFiles();
        let _ = Index_inst.GetFolder();
        let _ = Index_inst.GetLastSequence();
        let _ = Index_inst.ProtoMessage();
        let _ = Index_inst.ProtoReflect();
        Index_inst.Reset();
        let _ = Index_inst.String();
        let _ = Index_inst.toWire();
        let mut IndexUpdate_inst = IndexUpdate::new();
        let _ = IndexUpdate_inst.Descriptor();
        let _ = IndexUpdate_inst.GetFiles();
        let _ = IndexUpdate_inst.GetFolder();
        let _ = IndexUpdate_inst.GetLastSequence();
        let _ = IndexUpdate_inst.GetPrevSequence();
        let _ = IndexUpdate_inst.ProtoMessage();
        let _ = IndexUpdate_inst.ProtoReflect();
        IndexUpdate_inst.Reset();
        let _ = IndexUpdate_inst.String();
        let _ = IndexUpdate_inst.toWire();
        let mut MessageCompression_inst = MessageCompression::new();
        let _ = MessageCompression_inst.Descriptor();
        let _ = MessageCompression_inst.Enum();
        let _ = MessageCompression_inst.EnumDescriptor();
        let _ = MessageCompression_inst.Number();
        let _ = MessageCompression_inst.String();
        let _ = MessageCompression_inst.Type();
        let mut MessageType_inst = MessageType::new();
        let _ = MessageType_inst.Descriptor();
        let _ = MessageType_inst.Enum();
        let _ = MessageType_inst.EnumDescriptor();
        let _ = MessageType_inst.Number();
        let _ = MessageType_inst.String();
        let _ = MessageType_inst.Type();
        let mut Ping_inst = Ping::new();
        let _ = Ping_inst.Descriptor();
        let _ = Ping_inst.ProtoMessage();
        let _ = Ping_inst.ProtoReflect();
        Ping_inst.Reset();
        let _ = Ping_inst.String();
        let mut PlatformData_inst = PlatformData::new();
        let _ = PlatformData_inst.Descriptor();
        let _ = PlatformData_inst.GetDarwin();
        let _ = PlatformData_inst.GetFreebsd();
        let _ = PlatformData_inst.GetLinux();
        let _ = PlatformData_inst.GetNetbsd();
        let _ = PlatformData_inst.GetUnix();
        let _ = PlatformData_inst.GetWindows();
        let _ = PlatformData_inst.MergeWith();
        let _ = PlatformData_inst.ProtoMessage();
        let _ = PlatformData_inst.ProtoReflect();
        PlatformData_inst.Reset();
        PlatformData_inst.SetXattrs(json!(null));
        let _ = PlatformData_inst.String();
        let _ = PlatformData_inst.Xattrs();
        let _ = PlatformData_inst.toWire();
        let mut Request_inst = Request::new();
        let _ = Request_inst.Descriptor();
        let _ = Request_inst.GetBlockNo();
        let _ = Request_inst.GetFolder();
        let _ = Request_inst.GetFromTemporary();
        let _ = Request_inst.GetHash();
        let _ = Request_inst.GetId();
        let _ = Request_inst.GetName();
        let _ = Request_inst.GetOffset();
        let _ = Request_inst.GetSize();
        let _ = Request_inst.ProtoMessage();
        let _ = Request_inst.ProtoReflect();
        Request_inst.Reset();
        let _ = Request_inst.String();
        let _ = Request_inst.toWire();
        let mut Response_inst = Response::new();
        let _ = Response_inst.Descriptor();
        let _ = Response_inst.GetCode();
        let _ = Response_inst.GetData();
        let _ = Response_inst.GetId();
        let _ = Response_inst.ProtoMessage();
        let _ = Response_inst.ProtoReflect();
        Response_inst.Reset();
        let _ = Response_inst.String();
        let _ = Response_inst.toWire();
        let mut UnixData_inst = UnixData::new();
        let _ = UnixData_inst.Descriptor();
        let _ = UnixData_inst.GetGid();
        let _ = UnixData_inst.GetGroupName();
        let _ = UnixData_inst.GetOwnerName();
        let _ = UnixData_inst.GetUid();
        let _ = UnixData_inst.ProtoMessage();
        let _ = UnixData_inst.ProtoReflect();
        UnixData_inst.Reset();
        let _ = UnixData_inst.String();
        let _ = UnixData_inst.toWire();
        let mut Vector_inst = Vector::new();
        let _ = Vector_inst.Descriptor();
        let _ = Vector_inst.GetCounters();
        let _ = Vector_inst.ProtoMessage();
        let _ = Vector_inst.ProtoReflect();
        Vector_inst.Reset();
        let _ = Vector_inst.String();
        let mut WindowsData_inst = WindowsData::new();
        let _ = WindowsData_inst.Descriptor();
        let _ = WindowsData_inst.GetOwnerIsGroup();
        let _ = WindowsData_inst.GetOwnerName();
        let _ = WindowsData_inst.ProtoMessage();
        let _ = WindowsData_inst.ProtoReflect();
        WindowsData_inst.Reset();
        let _ = WindowsData_inst.String();
        let mut Xattr_inst = Xattr::new();
        let _ = Xattr_inst.Descriptor();
        let _ = Xattr_inst.GetName();
        let _ = Xattr_inst.GetValue();
        let _ = Xattr_inst.ProtoMessage();
        let _ = Xattr_inst.ProtoReflect();
        Xattr_inst.Reset();
        let _ = Xattr_inst.String();
        let _ = Xattr_inst.toWire();
        let mut XattrData_inst = XattrData::new();
        let _ = XattrData_inst.Descriptor();
        let _ = XattrData_inst.GetXattrs();
        let _ = XattrData_inst.ProtoMessage();
        let _ = XattrData_inst.ProtoReflect();
        XattrData_inst.Reset();
        let _ = XattrData_inst.String();
        let _ = XattrData_inst.toWire();
        let mut readWriter_inst = readWriter::new();
        let _ = readWriter_inst.Read();
        let _ = readWriter_inst.Write();
        let _ = BlockInfoFromWire();
        let _ = BlockSize();
        let _ = BlocksHash();
        let _ = ExchangeHello();
        let _ = FileInfoFromDB();
        let _ = FileInfoFromDBTruncated();
        let _ = FileInfoFromWire();
        let _ = IsVersionMismatch();
        let _ = ModTimeEqual();
        let _ = PermsEqual();
        let _ = TestBlocksEqual();
        let _ = TestIsEquivalent();
        let _ = TestLocalFlagBits();
        let _ = TestOldHelloMsgs();
        let _ = TestSha256OfEmptyBlock();
        let _ = TestVersion14Hello();
        let _ = VectorHash();
        let _ = blocksEqual();
        let _ = clusterConfigFromWire();
        let _ = deviceFromWire();
        let _ = downloadProgressFromWire();
        let _ = fileDownloadProgressUpdateFromWire();
        let _ = fileInfoFromWireWithBlocks();
        let _ = file_bep_bep_proto_init();
        let _ = file_bep_bep_proto_rawDescGZIP();
        let _ = folderFromWire();
        let _ = helloFromWire();
        let _ = indexFromWire();
        let _ = indexUpdateFromWire();
        let _ = init();
        let _ = platformDataFromWire();
        let _ = readHello();
        let _ = requestFromWire();
        let _ = responseFromWire();
        let _ = unixDataFromWire();
        let _ = unixOwnershipEqual();
        let _ = windowsOwnershipEqual();
        let _ = writeHello();
        let _ = xattrDataFromWire();
        let _ = xattrFromWire();
        let _ = xattrsEqual();
    }
}
