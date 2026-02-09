use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

const LOG_RECORD_MAX_BYTES: u32 = 32 * 1024 * 1024;
pub(crate) const JOURNAL_FILE_NAME: &str = "events.log";
pub(crate) const MANIFEST_FILE_NAME: &str = "MANIFEST";
const BLOCK_BLOB_FILE_NAME: &str = "blocks.blob";
const BLOCK_BLOB_MARKER_PREFIX: &str = "__stblob__:";
const MANIFEST_TMP_FILE_NAME: &str = "MANIFEST.tmp";
const COMMIT_SEGMENT_PREFIX: &str = "CSEG-";
const COMMIT_SEGMENT_SUFFIX: &str = ".log";
const COMPACT_SEGMENT_TMP_NAME: &str = "CSEG-compact.tmp";
const MANIFEST_SCHEMA_VERSION: u32 = 1;
const ACTIVE_SEGMENT_ROTATE_BYTES: u64 = 8 * 1024 * 1024;
const KEY_SEP: char = '\u{001f}';
const PATH_SEG_SEP: char = '\u{001e}';

#[derive(Clone, Debug)]
pub(crate) struct StoreConfig {
    pub(crate) root: PathBuf,
    pub(crate) max_runtime_memory_mb: usize,
    pub(crate) max_deleted_tombstones: usize,
}

impl StoreConfig {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_runtime_memory_mb: 50,
            max_deleted_tombstones: 1000,
        }
    }

    pub(crate) fn with_memory_cap_mb(mut self, cap_mb: usize) -> Self {
        self.max_runtime_memory_mb = cap_mb;
        self
    }

    pub(crate) fn with_deleted_tombstone_cap(mut self, cap: usize) -> Self {
        self.max_deleted_tombstones = cap;
        self
    }

    fn memory_budget_bytes(&self) -> usize {
        self.max_runtime_memory_mb.saturating_mul(1024 * 1024)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileMetadata {
    pub(crate) folder: String,
    pub(crate) device: String,
    pub(crate) path: String,
    pub(crate) sequence: u64,
    pub(crate) deleted: bool,
    pub(crate) ignored: bool,
    pub(crate) local_flags: u32,
    pub(crate) file_type: String,
    pub(crate) modified_ns: u64,
    pub(crate) size: u64,
    pub(crate) block_hashes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredFileMetadata {
    sequence: u64,
    deleted: bool,
    ignored: bool,
    local_flags: u32,
    file_type: CompactFileType,
    modified_ns: u64,
    size: u64,
    block_hashes: StoredBlockHashes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CompactFileType {
    File,
    Directory,
    Symlink,
    Other(String),
}

impl CompactFileType {
    fn from_str(value: &str) -> Self {
        match value {
            "file" => Self::File,
            "directory" => Self::Directory,
            "symlink" => Self::Symlink,
            other => Self::Other(other.to_string()),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Self::File => "file",
            Self::Directory => "directory",
            Self::Symlink => "symlink",
            Self::Other(other) => other.as_str(),
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::Other(other) => other.len(),
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockHashReference {
    offset: u64,
    len: u32,
    checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StoredBlockHashes {
    None,
    Ref(BlockHashReference),
    Inline(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoreStats {
    pub(crate) file_count: usize,
    pub(crate) deleted_tombstone_count: usize,
    pub(crate) estimated_memory_bytes: usize,
    pub(crate) memory_budget_bytes: usize,
}

type PathStore = BTreeMap<String, StoredFileMetadata>;
type DeviceStore = BTreeMap<String, PathStore>;
type FolderStore = BTreeMap<String, DeviceStore>;

#[derive(Debug)]
pub(crate) struct Store {
    config: StoreConfig,
    manifest_path: PathBuf,
    manifest: StoreManifest,
    journal_path: PathBuf,
    block_blob_path: PathBuf,
    files: FolderStore,
    file_count: usize,
    deleted_tombstones: VecDeque<String>,
    tombstone_set: HashSet<String>,
    approx_memory_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PageCursor {
    pub(crate) folder: String,
    pub(crate) device: String,
    pub(crate) path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FilePage {
    pub(crate) items: Vec<FileMetadata>,
    pub(crate) next_cursor: Option<PageCursor>,
}

#[derive(Clone, Debug)]
enum JournalOp {
    Upsert(FileMetadata),
    Delete {
        folder: String,
        device: String,
        path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoreManifest {
    schema_version: u32,
    segments: Vec<String>,
    active_segment: String,
    next_segment_id: u64,
}

impl Store {
    pub(crate) fn open(config: StoreConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.root)?;
        let manifest_path = config.root.join(MANIFEST_FILE_NAME);
        let manifest = load_or_init_manifest(&config.root, &manifest_path)?;
        for segment in &manifest.segments {
            let path = config.root.join(segment);
            if !path.exists() {
                if segment == &manifest.active_segment
                    && segment == JOURNAL_FILE_NAME
                    && manifest.segments.len() == 1
                {
                    let file = File::create(&path)?;
                    file.sync_all()?;
                } else {
                    return Err(io::Error::new(
                        ErrorKind::NotFound,
                        format!("manifest segment missing: {}", path.display()),
                    ));
                }
            }
        }
        sync_dir(&config.root)?;
        let journal_path = config.root.join(&manifest.active_segment);
        let block_blob_path = config.root.join(BLOCK_BLOB_FILE_NAME);
        // The block-hash blob cache is rebuilt from the authoritative journal at open time.
        let blob = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&block_blob_path)?;
        blob.sync_all()?;

        let (flat_files, deleted_tombstones, approx_memory_bytes) = Self::load_from_segments(
            &config.root,
            &manifest.segments,
            &manifest.active_segment,
            &block_blob_path,
            config.max_deleted_tombstones,
        )?;
        let budget_bytes = config.memory_budget_bytes();
        if budget_bytes > 0 && approx_memory_bytes > budget_bytes {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!(
                    "memory budget exceeded on open: estimated={} budget={}",
                    approx_memory_bytes, budget_bytes
                ),
            ));
        }
        let tombstone_set = deleted_tombstones.iter().cloned().collect();
        let file_count = flat_files.len();
        let files = build_folder_store(flat_files)?;

        Ok(Self {
            config,
            manifest_path,
            manifest,
            journal_path,
            block_blob_path,
            files,
            file_count,
            deleted_tombstones,
            tombstone_set,
            approx_memory_bytes,
        })
    }

    pub(crate) fn upsert_file(&mut self, mut file: FileMetadata) -> io::Result<()> {
        let key = composite_key(&file.folder, &file.device, &file.path);
        let mut budget_file = stored_from_file(&file);
        budget_file.block_hashes = in_memory_block_hash_shape(&budget_file.block_hashes);
        let next_bytes = projected_upsert_memory_bytes(
            self.approx_memory_bytes,
            self.stored_for_key(&key),
            &key,
            &budget_file,
        );
        let budget_bytes = self.config.memory_budget_bytes();
        if budget_bytes > 0 && next_bytes > budget_bytes {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!(
                    "memory budget exceeded on upsert: projected={} budget={}",
                    next_bytes, budget_bytes
                ),
            ));
        }
        self.append_op(&JournalOp::Upsert(file.clone()))?;
        file.block_hashes = spill_block_hashes(&self.block_blob_path, &file.block_hashes)?;
        self.apply_upsert(key, stored_from_file(&file));
        Ok(())
    }

    pub(crate) fn delete_file(&mut self, folder: &str, device: &str, path: &str) -> io::Result<()> {
        self.append_op(&JournalOp::Delete {
            folder: folder.to_string(),
            device: device.to_string(),
            path: path.to_string(),
        })?;

        let key = composite_key(folder, device, path);
        if let Some(prev) = self.remove_stored_by_key(&key) {
            self.approx_memory_bytes = self
                .approx_memory_bytes
                .saturating_sub(estimate_entry_bytes(&key, &prev));
            self.file_count = self.file_count.saturating_sub(1);
        }
        self.record_tombstone(&key);

        Ok(())
    }

    pub(crate) fn get_file(&self, folder: &str, device: &str, path: &str) -> Option<FileMetadata> {
        let key = composite_key(folder, device, path);
        let stored = self.stored_for_key(&key)?;
        file_from_entry(&key, stored)
    }

    pub(crate) fn resolve_file_block_hashes(&self, meta: &FileMetadata) -> io::Result<Vec<String>> {
        load_block_hashes(&self.block_blob_path, &meta.block_hashes)
    }

    pub(crate) fn get_file_with_blocks(
        &self,
        folder: &str,
        device: &str,
        path: &str,
    ) -> io::Result<Option<FileMetadata>> {
        let Some(mut out) = self.get_file(folder, device, path) else {
            return Ok(None);
        };
        out.block_hashes = self.resolve_file_block_hashes(&out)?;
        Ok(Some(out))
    }

    pub(crate) fn all_files_lexicographic(&self) -> Vec<FileMetadata> {
        let mut out = Vec::with_capacity(self.file_count);
        self.iter_all_entries(|folder, device, path_key, value| {
            if let Some(file) = file_from_parts(folder, device, path_key, value) {
                out.push(file);
            }
            true
        });
        out
    }

    pub(crate) fn all_files_in_folder_prefix(
        &self,
        folder: &str,
        prefix: &str,
    ) -> Vec<FileMetadata> {
        let mut out = Vec::new();
        if let Some(device_store) = self.files.get(folder) {
            for (device, path_store) in device_store {
                for (path_key, value) in path_store {
                    if let Some(file) = file_from_parts(folder, device, path_key, value) {
                        if file.path.starts_with(prefix) {
                            out.push(file);
                        }
                    }
                }
            }
        }

        out
    }

    pub(crate) fn for_each_file<F>(&self, mut visit: F)
    where
        F: FnMut(FileMetadata) -> bool,
    {
        self.iter_all_entries(|folder, device, path_key, value| {
            let Some(file) = file_from_parts(folder, device, path_key, value) else {
                return true;
            };
            visit(file)
        });
    }

    pub(crate) fn for_each_file_in_folder<F>(&self, folder: &str, mut visit: F)
    where
        F: FnMut(FileMetadata) -> bool,
    {
        if let Some(device_store) = self.files.get(folder) {
            for (device, path_store) in device_store {
                for (path_key, value) in path_store {
                    let Some(file) = file_from_parts(folder, device, path_key, value) else {
                        continue;
                    };
                    if !visit(file) {
                        return;
                    }
                }
            }
        }
    }

    pub(crate) fn for_each_file_in_folder_device<F>(&self, folder: &str, device: &str, mut visit: F)
    where
        F: FnMut(FileMetadata) -> bool,
    {
        if let Some(path_store) = self
            .files
            .get(folder)
            .and_then(|devices| devices.get(device))
        {
            for (path_key, value) in path_store {
                let Some(file) = file_from_parts(folder, device, path_key, value) else {
                    continue;
                };
                if !visit(file) {
                    return;
                }
            }
        }
    }

    pub(crate) fn devices_in_folder(&self, folder: &str) -> Vec<String> {
        self.files
            .get(folder)
            .map(|devices| devices.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn file_count(&self) -> usize {
        self.file_count
    }

    pub(crate) fn tombstone_count(&self) -> usize {
        self.deleted_tombstones.len()
    }

    pub(crate) fn has_tombstone(&self, folder: &str, device: &str, path: &str) -> bool {
        let key = composite_key(folder, device, path);
        self.tombstone_set.contains(&key)
    }

    pub(crate) fn all_files_ordered_page(
        &self,
        start_after: Option<&PageCursor>,
        limit: usize,
    ) -> FilePage {
        if limit == 0 {
            return FilePage {
                items: Vec::new(),
                next_cursor: None,
            };
        }

        let mut items = Vec::with_capacity(limit.saturating_add(1));
        let start_key =
            start_after.map(|cursor| composite_key(&cursor.folder, &cursor.device, &cursor.path));
        self.iter_all_entries(|folder, device, path_key, value| {
            let entry_key = composite_key_from_path_key(folder, device, path_key);
            if let Some(start) = &start_key {
                if entry_key <= *start {
                    return true;
                }
            }
            let Some(file) = file_from_parts(folder, device, path_key, value) else {
                return true;
            };
            items.push(file);
            items.len() <= limit
        });

        finalize_page(items, limit)
    }

    pub(crate) fn files_in_folder_ordered_page(
        &self,
        folder: &str,
        start_after: Option<&PageCursor>,
        limit: usize,
    ) -> FilePage {
        if limit == 0 {
            return FilePage {
                items: Vec::new(),
                next_cursor: None,
            };
        }

        let mut items = Vec::with_capacity(limit.saturating_add(1));
        let start_key = start_after.and_then(|cursor| {
            if cursor.folder == folder {
                Some(composite_key(&cursor.folder, &cursor.device, &cursor.path))
            } else {
                None
            }
        });
        if let Some(device_store) = self.files.get(folder) {
            for (device, path_store) in device_store {
                for (path_key, value) in path_store {
                    let entry_key = composite_key_from_path_key(folder, device, path_key);
                    if let Some(start) = &start_key {
                        if entry_key <= *start {
                            continue;
                        }
                    }
                    let Some(file) = file_from_parts(folder, device, path_key, value) else {
                        continue;
                    };
                    items.push(file);
                    if items.len() > limit {
                        break;
                    }
                }
                if items.len() > limit {
                    break;
                }
            }
        }

        finalize_page(items, limit)
    }

    pub(crate) fn files_in_folder_device_ordered_page(
        &self,
        folder: &str,
        device: &str,
        start_after_path: Option<&str>,
        limit: usize,
    ) -> FilePage {
        if limit == 0 {
            return FilePage {
                items: Vec::new(),
                next_cursor: None,
            };
        }

        let mut items = Vec::with_capacity(limit.saturating_add(1));
        let start_path_key = start_after_path.map(encode_path_key);
        if let Some(path_store) = self
            .files
            .get(folder)
            .and_then(|devices| devices.get(device))
        {
            match start_path_key {
                Some(path_key) => {
                    for (entry_path_key, value) in
                        path_store.range((Bound::Excluded(path_key), Bound::Unbounded))
                    {
                        let Some(file) = file_from_parts(folder, device, entry_path_key, value)
                        else {
                            continue;
                        };
                        items.push(file);
                        if items.len() > limit {
                            break;
                        }
                    }
                }
                None => {
                    for (entry_path_key, value) in path_store {
                        let Some(file) = file_from_parts(folder, device, entry_path_key, value)
                        else {
                            continue;
                        };
                        items.push(file);
                        if items.len() > limit {
                            break;
                        }
                    }
                }
            }
        }

        finalize_page(items, limit)
    }

    pub(crate) fn files_in_folder_device_prefix_ordered_page(
        &self,
        folder: &str,
        device: &str,
        prefix: &str,
        start_after_path: Option<&str>,
        limit: usize,
    ) -> FilePage {
        if limit == 0 {
            return FilePage {
                items: Vec::new(),
                next_cursor: None,
            };
        }

        let mut items = Vec::with_capacity(limit.saturating_add(1));
        let start_path_key = match start_after_path {
            Some(path) => encode_path_key(path),
            None => encode_path_key(prefix),
        };
        let range_start = match start_after_path {
            Some(_) => Bound::Excluded(start_path_key),
            None => Bound::Included(start_path_key),
        };
        if let Some(path_store) = self
            .files
            .get(folder)
            .and_then(|devices| devices.get(device))
        {
            for (entry_path_key, value) in path_store.range((range_start, Bound::Unbounded)) {
                let Some(file) = file_from_parts(folder, device, entry_path_key, value) else {
                    continue;
                };
                if !file.path.starts_with(prefix) {
                    if compare_path_order(&file.path, prefix) == Ordering::Greater {
                        break;
                    }
                    continue;
                }
                items.push(file);
                if items.len() > limit {
                    break;
                }
            }
        }

        finalize_page(items, limit)
    }

    pub(crate) fn compact(&mut self) -> io::Result<()> {
        let tmp = self.config.root.join(COMPACT_SEGMENT_TMP_NAME);
        let mut file = File::create(&tmp)?;

        for (folder, devices) in &self.files {
            for (device, paths) in devices {
                for (path_key, record) in paths {
                    let Some(mut materialized) = file_from_parts(folder, device, path_key, record)
                    else {
                        continue;
                    };
                    materialized.block_hashes = self.resolve_file_block_hashes(&materialized)?;
                    Self::write_record(&mut file, &JournalOp::Upsert(materialized))?;
                }
            }
        }
        for tombstone in &self.deleted_tombstones {
            if let Some((folder, device, path)) = split_composite_key(tombstone) {
                Self::write_record(
                    &mut file,
                    &JournalOp::Delete {
                        folder,
                        device,
                        path,
                    },
                )?;
            }
        }

        file.sync_all()?;
        sync_dir(&self.config.root)?;

        let compact_segment = self.allocate_next_segment_name();
        let compact_segment_path = self.config.root.join(&compact_segment);
        fs::rename(&tmp, &compact_segment_path)?;
        sync_dir(&self.config.root)?;

        let old_segments = self.manifest.segments.clone();
        self.manifest.segments = vec![compact_segment.clone()];
        self.manifest.active_segment = compact_segment;
        persist_manifest(&self.config.root, &self.manifest_path, &self.manifest)?;
        self.journal_path = compact_segment_path;

        for old in old_segments {
            let old_path = self.config.root.join(old);
            if old_path != self.journal_path {
                let _ = fs::remove_file(old_path);
            }
        }

        Ok(())
    }

    pub(crate) fn stats(&self) -> StoreStats {
        StoreStats {
            file_count: self.file_count,
            deleted_tombstone_count: self.deleted_tombstones.len(),
            estimated_memory_bytes: self.approx_memory_bytes,
            memory_budget_bytes: self.config.memory_budget_bytes(),
        }
    }

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    fn load_from_segments(
        root: &Path,
        segments: &[String],
        active_segment: &str,
        block_blob_path: &Path,
        max_tombstones: usize,
    ) -> io::Result<(
        BTreeMap<String, StoredFileMetadata>,
        VecDeque<String>,
        usize,
    )> {
        let mut block_blob = OpenOptions::new()
            .create(true)
            .append(true)
            .open(block_blob_path)?;
        let mut files: BTreeMap<String, StoredFileMetadata> = BTreeMap::new();
        let mut deleted_tombstones = VecDeque::new();
        let mut tombstone_set = HashSet::new();
        let mut payload_buf: Vec<u8> = Vec::new();

        for segment in segments {
            let is_active_segment = segment == active_segment;
            let path = root.join(segment);
            if !path.exists() {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("manifest segment missing: {}", path.display()),
                ));
            }
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);

            loop {
                let mut hdr = [0_u8; 8];
                let first_read = reader.read(&mut hdr[..1]);
                match first_read {
                    Ok(0) => break,
                    Ok(1) => {}
                    Ok(_) => unreachable!("single-byte read should not exceed one byte"),
                    Err(err) => return Err(err),
                }
                match reader.read_exact(&mut hdr[1..]) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                        if is_active_segment {
                            break;
                        }
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            format!("partial header in archived segment {}", path.display()),
                        ));
                    }
                    Err(err) => return Err(err),
                }

                let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
                let expected_checksum = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);

                if len == 0 || len > LOG_RECORD_MAX_BYTES {
                    if is_active_segment {
                        break;
                    }
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!(
                            "invalid record length in archived segment {}",
                            path.display()
                        ),
                    ));
                }

                payload_buf.resize(len as usize, 0);
                match reader.read_exact(&mut payload_buf) {
                    Ok(()) => {}
                    Err(err) if err.kind() == ErrorKind::UnexpectedEof => {
                        if is_active_segment {
                            break;
                        }
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            format!("truncated record in archived segment {}", path.display()),
                        ));
                    }
                    Err(err) => return Err(err),
                }

                if checksum(&payload_buf) != expected_checksum {
                    if is_active_segment {
                        break;
                    }
                    return Err(io::Error::new(
                        ErrorKind::InvalidData,
                        format!("checksum mismatch in archived segment {}", path.display()),
                    ));
                }

                let op = match decode_op(&payload_buf) {
                    Some(op) => op,
                    None => {
                        if is_active_segment {
                            break;
                        }
                        return Err(io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "invalid record payload in archived segment {}",
                                path.display()
                            ),
                        ));
                    }
                };
                apply_op(
                    &mut files,
                    &mut deleted_tombstones,
                    &mut tombstone_set,
                    max_tombstones,
                    op,
                    &mut block_blob,
                )?;
            }
        }
        block_blob.sync_data()?;

        let approx_bytes = files
            .iter()
            .map(|(key, value)| estimate_entry_bytes(key, value))
            .sum();
        Ok((files, deleted_tombstones, approx_bytes))
    }

    fn append_op(&mut self, op: &JournalOp) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        Self::write_record(&mut file, op)?;
        file.sync_data()?;
        self.rotate_active_segment_if_needed()?;
        Ok(())
    }

    fn rotate_active_segment_if_needed(&mut self) -> io::Result<()> {
        let size = fs::metadata(&self.journal_path)?.len();
        if size < ACTIVE_SEGMENT_ROTATE_BYTES {
            return Ok(());
        }

        let new_segment_name = self.allocate_next_segment_name();
        let new_segment_path = self.config.root.join(&new_segment_name);
        let file = File::create(&new_segment_path)?;
        file.sync_all()?;

        self.manifest.segments.push(new_segment_name.clone());
        self.manifest.active_segment = new_segment_name;
        persist_manifest(&self.config.root, &self.manifest_path, &self.manifest)?;
        self.journal_path = new_segment_path;
        Ok(())
    }

    fn allocate_next_segment_name(&mut self) -> String {
        loop {
            let candidate = format!(
                "{COMMIT_SEGMENT_PREFIX}{:08}{COMMIT_SEGMENT_SUFFIX}",
                self.manifest.next_segment_id
            );
            self.manifest.next_segment_id = self.manifest.next_segment_id.saturating_add(1);
            if !self.manifest.segments.iter().any(|v| v == &candidate) {
                return candidate;
            }
        }
    }

    fn write_record(file: &mut File, op: &JournalOp) -> io::Result<()> {
        let payload = encode_op(op);
        if payload.len() as u32 > LOG_RECORD_MAX_BYTES {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                format!("journal record too large: {} bytes", payload.len()),
            ));
        }

        let len = payload.len() as u32;
        let checksum = checksum(&payload);

        file.write_all(&len.to_le_bytes())?;
        file.write_all(&checksum.to_le_bytes())?;
        file.write_all(&payload)?;
        Ok(())
    }

    fn stored_for_key(&self, key: &str) -> Option<&StoredFileMetadata> {
        let (folder, device, path_key) = split_composite_key_raw(key)?;
        self.files.get(folder)?.get(device)?.get(path_key)
    }

    fn remove_stored_by_key(&mut self, key: &str) -> Option<StoredFileMetadata> {
        let (folder, device, path_key) = split_composite_key_raw(key)?;
        let mut remove_folder = false;
        let removed = if let Some(device_store) = self.files.get_mut(folder) {
            let mut remove_device = false;
            let removed = if let Some(path_store) = device_store.get_mut(device) {
                let removed = path_store.remove(path_key);
                if path_store.is_empty() {
                    remove_device = true;
                }
                removed
            } else {
                None
            };
            if remove_device {
                device_store.remove(device);
            }
            if device_store.is_empty() {
                remove_folder = true;
            }
            removed
        } else {
            None
        };
        if remove_folder {
            self.files.remove(folder);
        }
        removed
    }

    fn insert_stored_by_key(
        &mut self,
        key: &str,
        file: StoredFileMetadata,
    ) -> Option<StoredFileMetadata> {
        let (folder, device, path_key) = split_composite_key_raw(key)?;
        Some(
            self.files
                .entry(folder.to_string())
                .or_default()
                .entry(device.to_string())
                .or_default()
                .insert(path_key.to_string(), file),
        )
        .flatten()
    }

    fn iter_all_entries<F>(&self, mut visit: F)
    where
        F: FnMut(&str, &str, &str, &StoredFileMetadata) -> bool,
    {
        for (folder, devices) in &self.files {
            for (device, paths) in devices {
                for (path_key, value) in paths {
                    if !visit(folder, device, path_key, value) {
                        return;
                    }
                }
            }
        }
    }

    fn apply_upsert(&mut self, key: String, file: StoredFileMetadata) {
        let prev = self.insert_stored_by_key(&key, file.clone());
        if let Some(prev) = prev {
            self.approx_memory_bytes = self
                .approx_memory_bytes
                .saturating_sub(estimate_entry_bytes(&key, &prev));
        } else {
            self.file_count = self.file_count.saturating_add(1);
        }
        self.approx_memory_bytes = self
            .approx_memory_bytes
            .saturating_add(estimate_entry_bytes(&key, &file));
        if self.tombstone_set.remove(&key) {
            self.deleted_tombstones.retain(|k| *k != key);
        }
    }

    fn record_tombstone(&mut self, key: &str) {
        if self.config.max_deleted_tombstones == 0 {
            return;
        }
        if self.tombstone_set.contains(key) {
            self.deleted_tombstones.retain(|k| k != key);
        } else {
            self.tombstone_set.insert(key.to_string());
        }
        self.deleted_tombstones.push_back(key.to_string());
        while self.deleted_tombstones.len() > self.config.max_deleted_tombstones {
            if let Some(removed) = self.deleted_tombstones.pop_front() {
                self.tombstone_set.remove(&removed);
            }
        }
    }
}

fn load_or_init_manifest(root: &Path, manifest_path: &Path) -> io::Result<StoreManifest> {
    let manifest_tmp_path = root.join(MANIFEST_TMP_FILE_NAME);
    if !manifest_path.exists() && manifest_tmp_path.exists() {
        // Recovery path for interrupted manifest swap.
        fs::rename(&manifest_tmp_path, manifest_path)?;
        sync_dir(root)?;
    }

    if manifest_path.exists() {
        let raw = fs::read_to_string(manifest_path)?;
        let mut manifest: StoreManifest = serde_json::from_str(&raw).map_err(|err| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("decode manifest {}: {err}", manifest_path.display()),
            )
        })?;
        normalize_manifest(root, &mut manifest, false)?;
        persist_manifest(root, manifest_path, &manifest)?;
        return Ok(manifest);
    }

    let existing_segments = discover_commit_segments(root);
    let mut manifest = if existing_segments.is_empty() {
        StoreManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            segments: vec![JOURNAL_FILE_NAME.to_string()],
            active_segment: JOURNAL_FILE_NAME.to_string(),
            next_segment_id: 1,
        }
    } else {
        let max_segment_id = existing_segments
            .iter()
            .filter_map(|name| parse_segment_id(name))
            .max()
            .unwrap_or(0);
        StoreManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            active_segment: existing_segments
                .last()
                .cloned()
                .unwrap_or_else(|| JOURNAL_FILE_NAME.to_string()),
            segments: existing_segments,
            next_segment_id: max_segment_id.saturating_add(1),
        }
    };
    normalize_manifest(root, &mut manifest, true)?;
    persist_manifest(root, manifest_path, &manifest)?;
    Ok(manifest)
}

fn normalize_manifest(
    root: &Path,
    manifest: &mut StoreManifest,
    include_discovered: bool,
) -> io::Result<()> {
    manifest.schema_version = MANIFEST_SCHEMA_VERSION;

    if manifest.segments.is_empty() {
        manifest.segments.push(JOURNAL_FILE_NAME.to_string());
    }
    if manifest.active_segment.trim().is_empty() {
        manifest.active_segment = manifest
            .segments
            .last()
            .cloned()
            .unwrap_or_else(|| JOURNAL_FILE_NAME.to_string());
    }
    if !manifest
        .segments
        .iter()
        .any(|v| v == &manifest.active_segment)
    {
        manifest.segments.push(manifest.active_segment.clone());
    }

    let mut seen = HashSet::new();
    manifest
        .segments
        .retain(|segment| seen.insert(segment.clone()));

    let mut max_segment_id = 0_u64;
    for segment in &manifest.segments {
        if let Some(id) = parse_segment_id(segment) {
            max_segment_id = max_segment_id.max(id);
        }
    }
    if manifest.next_segment_id <= max_segment_id {
        manifest.next_segment_id = max_segment_id.saturating_add(1);
    }
    if manifest.next_segment_id == 0 {
        manifest.next_segment_id = 1;
    }

    if include_discovered {
        // Manifest recovery path: if manifest was absent, include extant segment files.
        let discovered = discover_commit_segments(root);
        for segment in discovered {
            if !manifest.segments.iter().any(|v| v == &segment) {
                manifest.segments.push(segment);
            }
        }
    }
    if parse_segment_id(&manifest.active_segment).is_none() {
        let active_exists = root.join(&manifest.active_segment).exists();
        if !active_exists {
            if let Some(last_segment) = manifest
                .segments
                .iter()
                .filter(|name| parse_segment_id(name).is_some())
                .max()
                .cloned()
            {
                manifest.active_segment = last_segment;
            }
        }
    }

    for segment in &manifest.segments {
        if !is_valid_manifest_segment_name(segment) {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid manifest segment name: {segment}"),
            ));
        }
    }
    if !is_valid_manifest_segment_name(&manifest.active_segment) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "invalid manifest active segment name: {}",
                manifest.active_segment
            ),
        ));
    }
    Ok(())
}

fn is_valid_manifest_segment_name(name: &str) -> bool {
    if name == JOURNAL_FILE_NAME {
        return true;
    }
    parse_segment_id(name).is_some()
}

fn discover_commit_segments(root: &Path) -> Vec<String> {
    let mut discovered = fs::read_dir(root)
        .ok()
        .into_iter()
        .flat_map(|it| it.flatten())
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            if parse_segment_id(&name).is_some() {
                Some(name)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    discovered.sort();
    discovered
}

fn parse_segment_id(name: &str) -> Option<u64> {
    if !name.starts_with(COMMIT_SEGMENT_PREFIX) || !name.ends_with(COMMIT_SEGMENT_SUFFIX) {
        return None;
    }
    let raw = &name[COMMIT_SEGMENT_PREFIX.len()..name.len() - COMMIT_SEGMENT_SUFFIX.len()];
    raw.parse::<u64>().ok()
}

fn persist_manifest(root: &Path, manifest_path: &Path, manifest: &StoreManifest) -> io::Result<()> {
    let tmp = root.join(MANIFEST_TMP_FILE_NAME);
    let payload = serde_json::to_vec_pretty(manifest).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("encode manifest {}: {err}", manifest_path.display()),
        )
    })?;

    {
        let mut file = File::create(&tmp)?;
        file.write_all(&payload)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    fs::rename(&tmp, manifest_path)?;
    sync_dir(root)?;
    Ok(())
}

fn apply_op(
    files: &mut BTreeMap<String, StoredFileMetadata>,
    deleted_tombstones: &mut VecDeque<String>,
    tombstone_set: &mut HashSet<String>,
    max_tombstones: usize,
    op: JournalOp,
    block_blob: &mut File,
) -> io::Result<()> {
    match op {
        JournalOp::Upsert(mut file) => {
            file.block_hashes =
                spill_block_hashes_to_writer(block_blob, &file.block_hashes, false)?;
            let key = composite_key(&file.folder, &file.device, &file.path);
            files.insert(key.clone(), stored_from_file(&file));
            if tombstone_set.remove(&key) {
                deleted_tombstones.retain(|k| *k != key);
            }
        }
        JournalOp::Delete {
            folder,
            device,
            path,
        } => {
            let key = composite_key(&folder, &device, &path);
            files.remove(&key);
            if max_tombstones > 0 {
                if tombstone_set.contains(&key) {
                    deleted_tombstones.retain(|k| *k != key);
                } else {
                    tombstone_set.insert(key.clone());
                }
                deleted_tombstones.push_back(key);
                while deleted_tombstones.len() > max_tombstones {
                    if let Some(removed) = deleted_tombstones.pop_front() {
                        tombstone_set.remove(&removed);
                    }
                }
            }
        }
    }
    Ok(())
}

fn build_folder_store(flat_files: BTreeMap<String, StoredFileMetadata>) -> io::Result<FolderStore> {
    let mut out: FolderStore = BTreeMap::new();
    for (key, value) in flat_files {
        let (folder, device, path_key) = split_composite_key_raw(&key).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("invalid composite key in store state: {key}"),
            )
        })?;
        out.entry(folder.to_string())
            .or_default()
            .entry(device.to_string())
            .or_default()
            .insert(path_key.to_string(), value);
    }
    Ok(out)
}

fn encode_op(op: &JournalOp) -> Vec<u8> {
    let line = match op {
        JournalOp::Upsert(file) => {
            let hashes = file
                .block_hashes
                .iter()
                .map(|v| escape(v))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "U\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                escape(&file.folder),
                escape(&file.device),
                escape(&file.path),
                file.sequence,
                if file.deleted { 1 } else { 0 },
                file.modified_ns,
                file.size,
                if file.ignored { 1 } else { 0 },
                file.local_flags,
                escape(&file.file_type),
                hashes
            )
        }
        JournalOp::Delete {
            folder,
            device,
            path,
        } => {
            format!(
                "D\t{}\t{}\t{}\n",
                escape(folder),
                escape(device),
                escape(path)
            )
        }
    };
    line.into_bytes()
}

fn decode_op(payload: &[u8]) -> Option<JournalOp> {
    let line = std::str::from_utf8(payload).ok()?;
    let line = line.strip_suffix('\n').unwrap_or(line);
    let parts = split_escaped_tab(line);
    if parts.is_empty() {
        return None;
    }

    match parts[0].as_str() {
        "U" if parts.len() == 8 => {
            let folder = unescape(&parts[1])?;
            let path = unescape(&parts[2])?;
            let sequence = parts[3].parse().ok()?;
            let deleted = match parts[4].as_str() {
                "0" => false,
                "1" => true,
                _ => return None,
            };
            let modified_ns = parts[5].parse().ok()?;
            let size = parts[6].parse().ok()?;
            let block_hashes = if parts[7].is_empty() {
                Vec::new()
            } else {
                split_escaped_comma(&parts[7])
                    .into_iter()
                    .map(|v| unescape(&v))
                    .collect::<Option<Vec<_>>>()?
            };
            Some(JournalOp::Upsert(FileMetadata {
                folder,
                device: "local".to_string(),
                path,
                sequence,
                deleted,
                ignored: false,
                local_flags: 0,
                file_type: "file".to_string(),
                modified_ns,
                size,
                block_hashes,
            }))
        }
        "U" if parts.len() == 12 => {
            let folder = unescape(&parts[1])?;
            let device = unescape(&parts[2])?;
            let path = unescape(&parts[3])?;
            let sequence = parts[4].parse().ok()?;
            let deleted = match parts[5].as_str() {
                "0" => false,
                "1" => true,
                _ => return None,
            };
            let modified_ns = parts[6].parse().ok()?;
            let size = parts[7].parse().ok()?;
            let ignored = match parts[8].as_str() {
                "0" => false,
                "1" => true,
                _ => return None,
            };
            let local_flags = parts[9].parse().ok()?;
            let file_type = unescape(&parts[10])?;
            let block_hashes = if parts[11].is_empty() {
                Vec::new()
            } else {
                split_escaped_comma(&parts[11])
                    .into_iter()
                    .map(|v| unescape(&v))
                    .collect::<Option<Vec<_>>>()?
            };

            Some(JournalOp::Upsert(FileMetadata {
                folder,
                device,
                path,
                sequence,
                deleted,
                ignored,
                local_flags,
                file_type,
                modified_ns,
                size,
                block_hashes,
            }))
        }
        "D" if parts.len() == 4 => Some(JournalOp::Delete {
            folder: unescape(&parts[1])?,
            device: unescape(&parts[2])?,
            path: unescape(&parts[3])?,
        }),
        _ => None,
    }
}

fn split_escaped_tab(s: &str) -> Vec<String> {
    split_escaped(s, '\t')
}

fn split_escaped_comma(s: &str) -> Vec<String> {
    split_escaped(s, ',')
}

fn split_escaped(s: &str, sep: char) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut escaped = false;

    for ch in s.chars() {
        if escaped {
            cur.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' {
            escaped = true;
            cur.push(ch);
            continue;
        }

        if ch == sep {
            out.push(cur);
            cur = String::new();
        } else {
            cur.push(ch);
        }
    }

    out.push(cur);
    out
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace(',', "\\,")
}

fn unescape(s: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = s.chars();

    while let Some(ch) = chars.next() {
        if ch != '\\' {
            out.push(ch);
            continue;
        }

        let esc = chars.next()?;
        match esc {
            '\\' => out.push('\\'),
            't' => out.push('\t'),
            'n' => out.push('\n'),
            ',' => out.push(','),
            _ => return None,
        }
    }

    Some(out)
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn composite_key(folder: &str, device: &str, path: &str) -> String {
    let path_key = encode_path_key(path);
    composite_key_from_path_key(folder, device, &path_key)
}

fn composite_key_from_path_key(folder: &str, device: &str, path_key: &str) -> String {
    format!("{folder}{KEY_SEP}{device}{KEY_SEP}{path_key}")
}

fn split_composite_key_raw(key: &str) -> Option<(&str, &str, &str)> {
    let (folder, rest) = key.split_once(KEY_SEP)?;
    let (device, path_key) = rest.split_once(KEY_SEP)?;
    Some((folder, device, path_key))
}

fn split_composite_key(key: &str) -> Option<(String, String, String)> {
    let (folder, device, path_key) = split_composite_key_raw(key)?;
    let path = decode_path_key(path_key)?;
    Some((folder.to_string(), device.to_string(), path))
}

pub(crate) fn encode_path_key(path: &str) -> String {
    let normalized = normalize_path(path);
    if normalized.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    for segment in normalized.split('/') {
        if segment.is_empty() {
            continue;
        }
        out.push(PATH_SEG_SEP);
        out.push_str(segment);
    }
    out
}

pub(crate) fn decode_path_key(path_key: &str) -> Option<String> {
    if path_key.is_empty() {
        return Some(String::new());
    }
    let mut segments = Vec::new();
    for part in path_key.split(PATH_SEG_SEP) {
        if part.is_empty() {
            continue;
        }
        if part.contains(PATH_SEG_SEP) {
            return None;
        }
        segments.push(part);
    }
    Some(segments.join("/"))
}

pub(crate) fn normalize_path(path: &str) -> String {
    path.replace('\\', "/")
        .trim_matches('/')
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

pub(crate) fn compare_path_order(a: &str, b: &str) -> Ordering {
    encode_path_key(a).cmp(&encode_path_key(b))
}

fn finalize_page(mut items: Vec<FileMetadata>, limit: usize) -> FilePage {
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    let next_cursor = if has_more {
        items.last().map(|last| PageCursor {
            folder: last.folder.clone(),
            device: last.device.clone(),
            path: last.path.clone(),
        })
    } else {
        None
    };

    FilePage { items, next_cursor }
}

fn in_memory_block_hash_shape(hashes: &StoredBlockHashes) -> StoredBlockHashes {
    match hashes {
        StoredBlockHashes::None => StoredBlockHashes::None,
        _ => StoredBlockHashes::Ref(BlockHashReference {
            offset: 0,
            len: 0,
            checksum: 0,
        }),
    }
}

fn spill_block_hashes(block_blob_path: &Path, hashes: &[String]) -> io::Result<Vec<String>> {
    let mut blob = OpenOptions::new()
        .create(true)
        .append(true)
        .open(block_blob_path)?;
    spill_block_hashes_to_writer(&mut blob, hashes, true)
}

fn spill_block_hashes_to_writer(
    block_blob: &mut File,
    hashes: &[String],
    sync_data: bool,
) -> io::Result<Vec<String>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    if block_hashes_are_marker(hashes) {
        return Ok(hashes.to_vec());
    }

    let payload = serde_json::to_vec(hashes).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("encode block hashes: {err}"),
        )
    })?;
    if payload.len() as u32 > LOG_RECORD_MAX_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("block hash payload too large: {} bytes", payload.len()),
        ));
    }
    let checksum = checksum(&payload);
    let offset = block_blob.seek(SeekFrom::End(0))?;
    let len = payload.len() as u32;
    block_blob.write_all(&len.to_le_bytes())?;
    block_blob.write_all(&checksum.to_le_bytes())?;
    block_blob.write_all(&payload)?;
    if sync_data {
        block_blob.sync_data()?;
    }

    Ok(vec![format!(
        "{BLOCK_BLOB_MARKER_PREFIX}{offset}:{len}:{checksum}"
    )])
}

fn load_block_hashes(block_blob_path: &Path, hashes: &[String]) -> io::Result<Vec<String>> {
    if hashes.is_empty() || !block_hashes_are_marker(hashes) {
        return Ok(hashes.to_vec());
    }

    let marker = &hashes[0];
    let Some((offset, expected_len, expected_checksum)) = parse_block_blob_marker(marker) else {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid block marker format: {marker}"),
        ));
    };

    let mut blob = File::open(block_blob_path)?;
    blob.seek(SeekFrom::Start(offset))?;

    let mut hdr = [0_u8; 8];
    blob.read_exact(&mut hdr)?;
    let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
    let checksum_on_disk = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
    if len != expected_len || checksum_on_disk != expected_checksum {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "block marker does not match blob record header",
        ));
    }
    if len == 0 || len > LOG_RECORD_MAX_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid block payload length {len}"),
        ));
    }

    let mut payload = vec![0_u8; len as usize];
    blob.read_exact(&mut payload)?;
    if checksum(&payload) != checksum_on_disk {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "block payload checksum mismatch",
        ));
    }

    serde_json::from_slice::<Vec<String>>(&payload).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("decode block hashes: {err}"),
        )
    })
}

fn block_hashes_are_marker(hashes: &[String]) -> bool {
    hashes.len() == 1 && hashes[0].starts_with(BLOCK_BLOB_MARKER_PREFIX)
}

fn parse_block_blob_marker(marker: &str) -> Option<(u64, u32, u32)> {
    let raw = marker.strip_prefix(BLOCK_BLOB_MARKER_PREFIX)?;
    let mut parts = raw.split(':');
    let offset = parts.next()?.parse::<u64>().ok()?;
    let len = parts.next()?.parse::<u32>().ok()?;
    let checksum = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((offset, len, checksum))
}

fn marker_from_block_ref(reference: &BlockHashReference) -> String {
    format!(
        "{BLOCK_BLOB_MARKER_PREFIX}{}:{}:{}",
        reference.offset, reference.len, reference.checksum
    )
}

fn stored_block_hashes_from_runtime_hashes(hashes: &[String]) -> StoredBlockHashes {
    if hashes.is_empty() {
        return StoredBlockHashes::None;
    }
    if hashes.len() == 1 {
        if let Some((offset, len, checksum)) = parse_block_blob_marker(&hashes[0]) {
            return StoredBlockHashes::Ref(BlockHashReference {
                offset,
                len,
                checksum,
            });
        }
    }
    StoredBlockHashes::Inline(hashes.to_vec())
}

fn runtime_hashes_from_stored_block_hashes(hashes: &StoredBlockHashes) -> Vec<String> {
    match hashes {
        StoredBlockHashes::None => Vec::new(),
        StoredBlockHashes::Ref(reference) => vec![marker_from_block_ref(reference)],
        StoredBlockHashes::Inline(values) => values.clone(),
    }
}

fn stored_from_file(file: &FileMetadata) -> StoredFileMetadata {
    StoredFileMetadata {
        sequence: file.sequence,
        deleted: file.deleted,
        ignored: file.ignored,
        local_flags: file.local_flags,
        file_type: CompactFileType::from_str(&file.file_type),
        modified_ns: file.modified_ns,
        size: file.size,
        block_hashes: stored_block_hashes_from_runtime_hashes(&file.block_hashes),
    }
}

fn file_from_entry(key: &str, stored: &StoredFileMetadata) -> Option<FileMetadata> {
    let (folder, device, path_key) = split_composite_key_raw(key)?;
    file_from_parts(folder, device, path_key, stored)
}

fn file_from_parts(
    folder: &str,
    device: &str,
    path_key: &str,
    stored: &StoredFileMetadata,
) -> Option<FileMetadata> {
    let path = decode_path_key(path_key)?;
    Some(FileMetadata {
        folder: folder.to_string(),
        device: device.to_string(),
        path,
        sequence: stored.sequence,
        deleted: stored.deleted,
        ignored: stored.ignored,
        local_flags: stored.local_flags,
        file_type: stored.file_type.as_str().to_string(),
        modified_ns: stored.modified_ns,
        size: stored.size,
        block_hashes: runtime_hashes_from_stored_block_hashes(&stored.block_hashes),
    })
}

fn estimate_stored_file_bytes(file: &StoredFileMetadata) -> usize {
    let hash_bytes = match &file.block_hashes {
        StoredBlockHashes::None => 0,
        StoredBlockHashes::Ref(_) => std::mem::size_of::<BlockHashReference>(),
        StoredBlockHashes::Inline(hashes) => hashes.iter().map(String::len).sum(),
    };
    file.file_type.estimated_bytes() + hash_bytes + 64
}

fn estimate_entry_bytes(key: &str, file: &StoredFileMetadata) -> usize {
    key.len().saturating_add(estimate_stored_file_bytes(file))
}

fn projected_upsert_memory_bytes(
    current_bytes: usize,
    current_file: Option<&StoredFileMetadata>,
    key: &str,
    next_file: &StoredFileMetadata,
) -> usize {
    let without_current = current_bytes.saturating_sub(
        current_file
            .map(|file| estimate_entry_bytes(key, file))
            .unwrap_or(0),
    );
    without_current.saturating_add(estimate_entry_bytes(key, next_file))
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!(
            "syncthing-rs-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp root");
        path
    }

    fn meta(folder: &str, path: &str, sequence: u64) -> FileMetadata {
        FileMetadata {
            folder: folder.to_string(),
            device: "local".to_string(),
            path: path.to_string(),
            sequence,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: "file".to_string(),
            modified_ns: sequence,
            size: sequence,
            block_hashes: vec![format!("h-{sequence:08}")],
        }
    }

    fn read_manifest(root: &Path) -> StoreManifest {
        let raw = fs::read_to_string(root.join(MANIFEST_FILE_NAME)).expect("read manifest");
        serde_json::from_str(&raw).expect("parse manifest")
    }

    #[test]
    fn round_trip_recovery_and_lexicographic_prefix_scan() {
        let root = temp_root("roundtrip");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(50);

        {
            let mut store = Store::open(cfg.clone()).expect("open store");
            store
                .upsert_file(meta("alpha", "b.txt", 2))
                .expect("upsert");
            store
                .upsert_file(meta("alpha", "a.txt", 1))
                .expect("upsert");
            store.upsert_file(meta("beta", "z.txt", 7)).expect("upsert");
            store.delete_file("beta", "local", "z.txt").expect("delete");
        }

        let store = Store::open(cfg).expect("reopen store");
        let names: Vec<String> = store
            .all_files_lexicographic()
            .into_iter()
            .map(|f| format!("{}/{}", f.folder, f.path))
            .collect();
        assert_eq!(names, vec!["alpha/a.txt", "alpha/b.txt"]);

        let prefixed: Vec<String> = store
            .all_files_in_folder_prefix("alpha", "a")
            .into_iter()
            .map(|f| f.path)
            .collect();
        assert_eq!(prefixed, vec!["a.txt"]);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn crash_recovery_ignores_truncated_tail_record() {
        let root = temp_root("recovery");
        let cfg = StoreConfig::new(&root);

        {
            let mut store = Store::open(cfg.clone()).expect("open");
            store
                .upsert_file(meta("alpha", "a.txt", 1))
                .expect("upsert");

            let mut file = OpenOptions::new()
                .append(true)
                .open(store.journal_path())
                .expect("open journal append");
            file.write_all(&100_u32.to_le_bytes()).expect("write len");
            file.write_all(&0_u32.to_le_bytes())
                .expect("write checksum");
            file.write_all(b"{").expect("write payload");
            file.sync_all().expect("sync");
        }

        let store = Store::open(cfg).expect("reopen");
        assert_eq!(store.file_count(), 1);
        assert!(store.get_file("alpha", "local", "a.txt").is_some());

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn compact_rewrites_log_and_preserves_state() {
        let root = temp_root("compact");
        let cfg = StoreConfig::new(&root);

        let before_size;
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            for i in 0_u64..200 {
                store
                    .upsert_file(meta("alpha", &format!("{i:04}.dat"), i + 1))
                    .expect("upsert");
            }
            for i in 0_u64..100 {
                store
                    .delete_file("alpha", "local", &format!("{i:04}.dat"))
                    .expect("delete");
            }
            before_size = fs::metadata(store.journal_path())
                .expect("meta before")
                .len();
            store.compact().expect("compact");
        }

        let store = Store::open(cfg).expect("reopen");
        let after_size = fs::metadata(store.journal_path())
            .expect("meta after")
            .len();

        assert_eq!(store.file_count(), 100);
        assert!(after_size <= before_size);
        let manifest = read_manifest(&root);
        assert_eq!(manifest.segments.len(), 1);
        assert!(manifest.active_segment.starts_with(COMMIT_SEGMENT_PREFIX));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn creates_manifest_with_active_segment() {
        let root = temp_root("manifest-init");
        let cfg = StoreConfig::new(&root);
        let _store = Store::open(cfg).expect("open");

        let manifest = read_manifest(&root);
        assert_eq!(manifest.schema_version, MANIFEST_SCHEMA_VERSION);
        assert_eq!(manifest.active_segment, JOURNAL_FILE_NAME);
        assert_eq!(manifest.segments, vec![JOURNAL_FILE_NAME.to_string()]);
        assert_eq!(manifest.next_segment_id, 1);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn rotates_active_segment_after_threshold() {
        let root = temp_root("segment-rotation");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(256);
        let mut store = Store::open(cfg).expect("open");

        let long_path_tail = "x".repeat(50_000);
        let mut rotated = false;
        for i in 0_u64..512 {
            let path = format!("bulk/{i:04}-{long_path_tail}.bin");
            store
                .upsert_file(meta("alpha", &path, i + 1))
                .expect("upsert");
            if let Some(file_name) = store.journal_path.file_name() {
                let file_name = file_name.to_string_lossy();
                if file_name.starts_with(COMMIT_SEGMENT_PREFIX) {
                    rotated = true;
                    break;
                }
            }
        }

        assert!(rotated, "store should rotate into CSEG segment");
        let manifest = read_manifest(&root);
        assert!(manifest.segments.len() >= 2);
        assert!(manifest.active_segment.starts_with(COMMIT_SEGMENT_PREFIX));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn manifest_is_authoritative_over_orphan_segments() {
        let root = temp_root("manifest-authoritative");
        let cfg = StoreConfig::new(&root);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            store
                .upsert_file(meta("alpha", "kept.txt", 1))
                .expect("upsert kept");
        }

        // This segment is not referenced by MANIFEST and must not be replayed.
        let orphan_path = root.join("CSEG-99999999.log");
        let mut orphan = File::create(&orphan_path).expect("create orphan segment");
        Store::write_record(
            &mut orphan,
            &JournalOp::Upsert(meta("alpha", "orphan.txt", 2)),
        )
        .expect("write orphan");
        orphan.sync_all().expect("sync orphan");

        let store = Store::open(cfg).expect("reopen");
        assert!(store.get_file("alpha", "local", "kept.txt").is_some());
        assert!(store.get_file("alpha", "local", "orphan.txt").is_none());

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn recovers_from_manifest_tmp_when_manifest_missing() {
        let root = temp_root("manifest-tmp-recovery");
        let cfg = StoreConfig::new(&root);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            store
                .upsert_file(meta("alpha", "a.txt", 1))
                .expect("upsert");
        }

        let manifest_path = root.join(MANIFEST_FILE_NAME);
        let manifest_tmp_path = root.join(MANIFEST_TMP_FILE_NAME);
        let raw = fs::read_to_string(&manifest_path).expect("read manifest");
        fs::write(&manifest_tmp_path, raw).expect("write tmp manifest");
        fs::remove_file(&manifest_path).expect("remove manifest");

        let store = Store::open(cfg).expect("reopen via manifest tmp");
        assert!(
            manifest_path.exists(),
            "manifest should be restored from tmp"
        );
        assert_eq!(store.file_count(), 1);
        assert!(store.get_file("alpha", "local", "a.txt").is_some());

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn open_rejects_manifest_with_invalid_segment_name() {
        let root = temp_root("manifest-invalid-segment");
        let manifest = serde_json::json!({
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "segments": ["../escape.log"],
            "active_segment": "../escape.log",
            "next_segment_id": 1
        });
        fs::write(
            root.join(MANIFEST_FILE_NAME),
            serde_json::to_vec(&manifest).expect("encode manifest"),
        )
        .expect("write manifest");
        let err = Store::open(StoreConfig::new(&root)).expect_err("must reject manifest");
        assert!(err.to_string().contains("invalid manifest"));
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn open_fails_when_manifest_segment_missing() {
        let root = temp_root("manifest-segment-missing");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(256);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            let long_path_tail = "x".repeat(40_000);
            for i in 0_u64..512 {
                let path = format!("bulk/{i:04}-{long_path_tail}.bin");
                store
                    .upsert_file(meta("alpha", &path, i + 1))
                    .expect("upsert");
                if store
                    .journal_path
                    .file_name()
                    .map(|n| n.to_string_lossy().starts_with(COMMIT_SEGMENT_PREFIX))
                    .unwrap_or(false)
                {
                    break;
                }
            }
        }

        let manifest = read_manifest(&root);
        assert!(manifest.segments.len() >= 2, "must have archived segment");
        let archived = manifest
            .segments
            .iter()
            .find(|segment| *segment != &manifest.active_segment)
            .expect("archived segment")
            .clone();
        fs::remove_file(root.join(&archived)).expect("remove archived segment");

        let err = Store::open(cfg).expect_err("must fail when manifest segment is missing");
        assert!(err.to_string().contains("manifest segment missing"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn open_fails_on_corruption_in_non_active_segment() {
        let root = temp_root("archived-segment-corrupt");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(256);
        {
            let mut store = Store::open(cfg.clone()).expect("open");
            let long_path_tail = "y".repeat(40_000);
            for i in 0_u64..512 {
                let path = format!("bulk/{i:04}-{long_path_tail}.bin");
                store
                    .upsert_file(meta("alpha", &path, i + 1))
                    .expect("upsert");
                if store
                    .journal_path
                    .file_name()
                    .map(|n| n.to_string_lossy().starts_with(COMMIT_SEGMENT_PREFIX))
                    .unwrap_or(false)
                {
                    break;
                }
            }
        }

        let manifest = read_manifest(&root);
        let archived = manifest
            .segments
            .iter()
            .find(|segment| *segment != &manifest.active_segment)
            .expect("archived segment")
            .clone();
        let mut file = OpenOptions::new()
            .append(true)
            .open(root.join(&archived))
            .expect("open archived segment");
        file.write_all(&0_u32.to_le_bytes())
            .expect("write bad length");
        file.write_all(&0_u32.to_le_bytes())
            .expect("write bad checksum");
        file.sync_all().expect("sync");

        let err = Store::open(cfg).expect_err("must fail on archived segment corruption");
        assert!(
            err.to_string().contains("invalid record length")
                || err.to_string().contains("invalid record")
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn tracks_memory_budget_in_stats() {
        let root = temp_root("budget");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(50);

        let mut store = Store::open(cfg).expect("open");
        for i in 0_u64..1024 {
            store
                .upsert_file(meta("alpha", &format!("f-{i:04}.bin"), i + 1))
                .expect("upsert");
        }

        let stats = store.stats();
        assert_eq!(stats.memory_budget_bytes, 50 * 1024 * 1024);
        assert_eq!(stats.deleted_tombstone_count, 0);
        assert!(stats.estimated_memory_bytes < stats.memory_budget_bytes);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn compact_entry_memory_estimate_excludes_duplicate_path_strings() {
        let root = temp_root("compact-entry-estimate");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(50);
        let mut store = Store::open(cfg).expect("open");

        let folder = "very-long-folder-id-for-memory-estimate";
        let device = "remote-device-id-with-long-prefix";
        let path = "deep/nested/path/with/a/very/long/file-name-that-would-hurt-memory.bin";
        store
            .upsert_file(FileMetadata {
                folder: folder.to_string(),
                device: device.to_string(),
                path: path.to_string(),
                sequence: 1,
                deleted: false,
                ignored: false,
                local_flags: 0,
                file_type: "file".to_string(),
                modified_ns: 1,
                size: 1,
                block_hashes: vec!["h-1".to_string()],
            })
            .expect("upsert");

        let key = composite_key(folder, device, path);
        let stored = store
            .stored_for_key(&key)
            .expect("stored entry should exist")
            .clone();
        let expected = key.len() + estimate_stored_file_bytes(&stored);

        assert_eq!(store.stats().estimated_memory_bytes, expected);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn rejects_upsert_when_budget_would_be_exceeded() {
        let root = temp_root("budget-overflow");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(1);
        let mut store = Store::open(cfg).expect("open");

        let mut inserted = 0_u64;
        for i in 0_u64..8_000 {
            let path = format!("f-{i:06}-{}.bin", "x".repeat(1024));
            let result = store.upsert_file(meta("alpha", &path, i + 1));
            if result.is_err() {
                let err = result.expect_err("must fail");
                assert!(err.to_string().contains("memory budget exceeded"));
                break;
            }
            inserted += 1;
        }
        assert!(
            inserted > 0,
            "should insert at least one entry before capping"
        );
        assert!(
            store.stats().estimated_memory_bytes <= store.stats().memory_budget_bytes,
            "store should stay at or under budget"
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn rejects_open_when_journal_exceeds_budget() {
        let root = temp_root("budget-open-reject");
        {
            let cfg = StoreConfig::new(&root).with_memory_cap_mb(64);
            let mut store = Store::open(cfg).expect("open writer");
            for i in 0_u64..1_200 {
                let path = format!("seed-{i:05}-{}.bin", "y".repeat(1024));
                store
                    .upsert_file(meta("alpha", &path, i + 1))
                    .expect("seed upsert");
            }
        }

        let tiny_cfg = StoreConfig::new(&root).with_memory_cap_mb(1);
        let err = Store::open(tiny_cfg).expect_err("must reject over-budget open");
        assert!(err.to_string().contains("memory budget exceeded on open"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn keyset_pagination_returns_stable_order() {
        let root = temp_root("paging");
        let cfg = StoreConfig::new(&root);
        let mut store = Store::open(cfg).expect("open");
        for i in 0_u64..25 {
            store
                .upsert_file(meta("alpha", &format!("{i:04}.dat"), i + 1))
                .expect("upsert");
        }

        let mut cursor: Option<PageCursor> = None;
        let mut out = Vec::new();
        loop {
            let page = store.all_files_ordered_page(cursor.as_ref(), 10);
            for item in &page.items {
                out.push(item.path.clone());
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        let expected = (0_u64..25)
            .map(|i| format!("{i:04}.dat"))
            .collect::<Vec<_>>();
        assert_eq!(out, expected);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn folder_pagination_respects_segment_path_order() {
        let root = temp_root("folder-paging-segment-order");
        let cfg = StoreConfig::new(&root);
        let mut store = Store::open(cfg).expect("open");
        let paths = [
            "a/x.txt",
            "a/z.txt",
            "a.d/x.txt",
            "a.d/y/z.txt",
            "a.d/y.txt",
            "aa.bin",
            "b.txt",
        ];
        for (idx, path) in paths.iter().enumerate() {
            store
                .upsert_file(meta("alpha", path, (idx + 1) as u64))
                .expect("upsert");
        }

        let mut cursor: Option<PageCursor> = None;
        let mut out = Vec::new();
        loop {
            let page = store.files_in_folder_ordered_page("alpha", cursor.as_ref(), 2);
            for item in &page.items {
                out.push(item.path.clone());
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(
            out,
            vec![
                "a/x.txt",
                "a/z.txt",
                "a.d/x.txt",
                "a.d/y/z.txt",
                "a.d/y.txt",
                "aa.bin",
                "b.txt",
            ]
        );

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn folder_pagination_across_devices_keeps_duplicate_paths() {
        let root = temp_root("folder-paging-dup-paths");
        let cfg = StoreConfig::new(&root);
        let mut store = Store::open(cfg).expect("open");
        store
            .upsert_file(FileMetadata {
                device: "local".to_string(),
                ..meta("alpha", "same.txt", 1)
            })
            .expect("upsert local");
        store
            .upsert_file(FileMetadata {
                device: "peer-a".to_string(),
                ..meta("alpha", "same.txt", 2)
            })
            .expect("upsert peer");

        let mut cursor: Option<PageCursor> = None;
        let mut seen = Vec::new();
        loop {
            let page = store.files_in_folder_ordered_page("alpha", cursor.as_ref(), 1);
            for item in page.items {
                seen.push(format!("{}:{}", item.device, item.path));
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(seen.len(), 2);
        assert!(seen.contains(&"local:same.txt".to_string()));
        assert!(seen.contains(&"peer-a:same.txt".to_string()));
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn device_scoped_keyset_pagination_is_stable() {
        let root = temp_root("device-paging");
        let cfg = StoreConfig::new(&root);
        let mut store = Store::open(cfg).expect("open");

        for i in 0_u64..12 {
            store
                .upsert_file(FileMetadata {
                    device: "local".to_string(),
                    ..meta("alpha", &format!("{i:04}.dat"), i + 1)
                })
                .expect("upsert local");
        }
        for i in 0_u64..8 {
            store
                .upsert_file(FileMetadata {
                    device: "remote-a".to_string(),
                    ..meta("alpha", &format!("{i:04}.dat"), i + 1)
                })
                .expect("upsert remote");
        }

        let mut cursor_path: Option<String> = None;
        let mut out = Vec::new();
        loop {
            let page = store.files_in_folder_device_ordered_page(
                "alpha",
                "local",
                cursor_path.as_deref(),
                5,
            );
            for item in &page.items {
                out.push(item.path.clone());
                assert_eq!(item.device, "local");
            }
            match page.next_cursor {
                Some(next) => cursor_path = Some(next.path),
                None => break,
            }
        }

        let expected = (0_u64..12)
            .map(|i| format!("{i:04}.dat"))
            .collect::<Vec<_>>();
        assert_eq!(out, expected);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn device_prefix_pagination_uses_ordered_range_bounds() {
        let root = temp_root("device-prefix-paging");
        let cfg = StoreConfig::new(&root);
        let mut store = Store::open(cfg).expect("open");

        for (idx, path) in ["a/0.dat", "a/1.dat", "a/2.dat", "a.d/0.dat", "b/0.dat"]
            .into_iter()
            .enumerate()
        {
            store
                .upsert_file(meta("alpha", path, (idx + 1) as u64))
                .expect("upsert");
        }

        let mut cursor_path: Option<String> = None;
        let mut out = Vec::new();
        loop {
            let page = store.files_in_folder_device_prefix_ordered_page(
                "alpha",
                "local",
                "a/",
                cursor_path.as_deref(),
                2,
            );
            for item in &page.items {
                out.push(item.path.clone());
            }
            match page.next_cursor {
                Some(next) => cursor_path = Some(next.path),
                None => break,
            }
        }

        assert_eq!(out, vec!["a/0.dat", "a/1.dat", "a/2.dat"]);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn bounded_tombstones_keep_latest_deletes_only() {
        let root = temp_root("tombstones");
        let cfg = StoreConfig::new(&root).with_deleted_tombstone_cap(3);
        let mut store = Store::open(cfg).expect("open");

        for i in 0_u64..5 {
            let path = format!("{i:04}.dat");
            store
                .upsert_file(meta("alpha", &path, i + 1))
                .expect("upsert");
            store.delete_file("alpha", "local", &path).expect("delete");
        }

        assert_eq!(store.tombstone_count(), 3);
        assert!(!store.has_tombstone("alpha", "local", "0000.dat"));
        assert!(!store.has_tombstone("alpha", "local", "0001.dat"));
        assert!(store.has_tombstone("alpha", "local", "0002.dat"));
        assert!(store.has_tombstone("alpha", "local", "0003.dat"));
        assert!(store.has_tombstone("alpha", "local", "0004.dat"));

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn segment_path_ordering_beats_raw_ascii_path_ordering() {
        let root = temp_root("path-order");
        let cfg = StoreConfig::new(&root);
        let mut store = Store::open(cfg).expect("open");

        store
            .upsert_file(meta("alpha", "a.d/x.txt", 1))
            .expect("upsert a.d/x");
        store
            .upsert_file(meta("alpha", "a/x.txt", 2))
            .expect("upsert a/x");
        store
            .upsert_file(meta("alpha", "a/z.txt", 3))
            .expect("upsert a/z");

        let ordered = store
            .all_files_lexicographic()
            .into_iter()
            .map(|f| f.path)
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["a/x.txt", "a/z.txt", "a.d/x.txt"]);

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn block_hashes_spill_to_disk_and_materialize_after_reopen() {
        let root = temp_root("hash-spill");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(50);
        let expected = vec![
            "h1".to_string(),
            "h2".to_string(),
            "h3".to_string(),
            "h4".to_string(),
        ];

        {
            let mut store = Store::open(cfg.clone()).expect("open");
            let mut file = meta("alpha", "a.bin", 1);
            file.block_hashes = expected.clone();
            store.upsert_file(file).expect("upsert");

            let in_mem = store
                .get_file("alpha", "local", "a.bin")
                .expect("stored file");
            assert_eq!(in_mem.block_hashes.len(), 1);
            assert!(in_mem.block_hashes[0].starts_with(BLOCK_BLOB_MARKER_PREFIX));

            let materialized = store
                .get_file_with_blocks("alpha", "local", "a.bin")
                .expect("materialize")
                .expect("file present");
            assert_eq!(materialized.block_hashes, expected);
        }

        {
            let store = Store::open(cfg).expect("reopen");
            let materialized = store
                .get_file_with_blocks("alpha", "local", "a.bin")
                .expect("materialize after reopen")
                .expect("file present");
            assert_eq!(materialized.block_hashes, expected);
        }

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn stored_entry_uses_compact_block_hash_reference() {
        let root = temp_root("compact-block-ref");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(50);
        let mut store = Store::open(cfg).expect("open");

        let mut file = meta("alpha", "blob.bin", 1);
        file.block_hashes = vec!["h1".to_string(), "h2".to_string(), "h3".to_string()];
        store.upsert_file(file).expect("upsert");

        let key = composite_key("alpha", "local", "blob.bin");
        let stored = store.stored_for_key(&key).expect("stored entry");
        match &stored.block_hashes {
            StoredBlockHashes::Ref(reference) => {
                assert!(reference.len > 0);
                assert!(reference.checksum > 0);
            }
            other => panic!("expected compact block ref, got {other:?}"),
        }

        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn compact_file_type_preserves_unknown_values() {
        let root = temp_root("compact-file-type");
        let cfg = StoreConfig::new(&root).with_memory_cap_mb(50);
        let mut store = Store::open(cfg).expect("open");

        store
            .upsert_file(FileMetadata {
                folder: "alpha".to_string(),
                device: "local".to_string(),
                path: "custom.type".to_string(),
                sequence: 1,
                deleted: false,
                ignored: false,
                local_flags: 0,
                file_type: "special".to_string(),
                modified_ns: 1,
                size: 1,
                block_hashes: vec![],
            })
            .expect("upsert");

        let roundtrip = store
            .get_file("alpha", "local", "custom.type")
            .expect("roundtrip file");
        assert_eq!(roundtrip.file_type, "special");

        fs::remove_dir_all(root).expect("cleanup");
    }
}
