use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::ops::Bound;
use std::cmp::Ordering;
use std::path::{Path, PathBuf};

const LOG_RECORD_MAX_BYTES: u32 = 32 * 1024 * 1024;
pub(crate) const JOURNAL_FILE_NAME: &str = "events.log";
const BLOCK_BLOB_FILE_NAME: &str = "blocks.blob";
const BLOCK_BLOB_MARKER_PREFIX: &str = "__stblob__:";
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
pub(crate) struct StoreStats {
    pub(crate) file_count: usize,
    pub(crate) deleted_tombstone_count: usize,
    pub(crate) estimated_memory_bytes: usize,
    pub(crate) memory_budget_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct Store {
    config: StoreConfig,
    journal_path: PathBuf,
    block_blob_path: PathBuf,
    files: BTreeMap<String, FileMetadata>,
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

impl Store {
    pub(crate) fn open(config: StoreConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.root)?;
        let journal_path = config.root.join(JOURNAL_FILE_NAME);
        let block_blob_path = config.root.join(BLOCK_BLOB_FILE_NAME);
        if !journal_path.exists() {
            let file = File::create(&journal_path)?;
            file.sync_all()?;
            sync_dir(&config.root)?;
        }
        // The block-hash blob cache is rebuilt from the authoritative journal at open time.
        let blob = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&block_blob_path)?;
        blob.sync_all()?;

        let (files, deleted_tombstones, approx_memory_bytes) =
            Self::load_from_journal(
                &journal_path,
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

        Ok(Self {
            config,
            journal_path,
            block_blob_path,
            files,
            deleted_tombstones,
            tombstone_set,
            approx_memory_bytes,
        })
    }

    pub(crate) fn upsert_file(&mut self, mut file: FileMetadata) -> io::Result<()> {
        let key = composite_key(&file.folder, &file.device, &file.path);
        let mut budget_file = file.clone();
        budget_file.block_hashes = in_memory_block_hash_shape(&budget_file.block_hashes);
        let next_bytes = projected_upsert_memory_bytes(
            self.approx_memory_bytes,
            self.files.get(&key),
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
        self.apply_upsert(file);
        Ok(())
    }

    pub(crate) fn delete_file(&mut self, folder: &str, device: &str, path: &str) -> io::Result<()> {
        self.append_op(&JournalOp::Delete {
            folder: folder.to_string(),
            device: device.to_string(),
            path: path.to_string(),
        })?;

        let key = composite_key(folder, device, path);
        if let Some(prev) = self.files.remove(&key) {
            self.approx_memory_bytes = self
                .approx_memory_bytes
                .saturating_sub(estimate_file_bytes(&prev));
        }
        self.record_tombstone(&key);

        Ok(())
    }

    pub(crate) fn get_file(&self, folder: &str, device: &str, path: &str) -> Option<&FileMetadata> {
        self.files.get(&composite_key(folder, device, path))
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
        let Some(file) = self.get_file(folder, device, path) else {
            return Ok(None);
        };
        let mut out = file.clone();
        out.block_hashes = self.resolve_file_block_hashes(file)?;
        Ok(Some(out))
    }

    pub(crate) fn all_files_lexicographic(&self) -> Vec<FileMetadata> {
        self.files.values().cloned().collect()
    }

    pub(crate) fn all_files_in_folder_prefix(
        &self,
        folder: &str,
        prefix: &str,
    ) -> Vec<FileMetadata> {
        let mut out = Vec::new();
        let folder_prefix = folder_prefix(folder);

        for (key, value) in self.files.range(folder_prefix.clone()..) {
            if !key.starts_with(&folder_prefix) {
                break;
            }
            if value.path.starts_with(prefix) {
                out.push(value.clone());
            }
        }

        out
    }

    pub(crate) fn for_each_file<F>(&self, mut visit: F)
    where
        F: FnMut(&FileMetadata) -> bool,
    {
        for value in self.files.values() {
            if !visit(value) {
                break;
            }
        }
    }

    pub(crate) fn for_each_file_in_folder<F>(&self, folder: &str, mut visit: F)
    where
        F: FnMut(&FileMetadata) -> bool,
    {
        let prefix = folder_prefix(folder);
        for (key, value) in self.files.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            if !visit(value) {
                break;
            }
        }
    }

    pub(crate) fn for_each_file_in_folder_device<F>(&self, folder: &str, device: &str, mut visit: F)
    where
        F: FnMut(&FileMetadata) -> bool,
    {
        let prefix = folder_device_prefix(folder, device);
        for (key, value) in self.files.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            if !visit(value) {
                break;
            }
        }
    }

    pub(crate) fn file_count(&self) -> usize {
        self.files.len()
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
        match start_after {
            Some(cursor) => {
                let start_key = composite_key(&cursor.folder, &cursor.device, &cursor.path);
                for (_, value) in self
                    .files
                    .range((Bound::Excluded(start_key), Bound::Unbounded))
                    .take(limit.saturating_add(1))
                {
                    items.push(value.clone());
                }
            }
            None => {
                for value in self.files.values().take(limit.saturating_add(1)) {
                    items.push(value.clone());
                }
            }
        }

        finalize_page(items, limit)
    }

    pub(crate) fn files_in_folder_ordered_page(
        &self,
        folder: &str,
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
        let folder_start = folder_prefix(folder);
        for (key, value) in self.files.range(folder_start.clone()..) {
            if !key.starts_with(&folder_start) {
                break;
            }
            if let Some(start_path) = start_after_path {
                if compare_path_order(&value.path, start_path) != Ordering::Greater {
                    continue;
                }
            }
            items.push(value.clone());
            if items.len() > limit {
                break;
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
        let prefix = folder_device_prefix(folder, device);
        let start_key = start_after_path.map(|p| composite_key(folder, device, p));

        match start_key {
            Some(key) => {
                for (entry_key, value) in self
                    .files
                    .range((Bound::Excluded(key), Bound::Unbounded))
                {
                    if !entry_key.starts_with(&prefix) {
                        break;
                    }
                    items.push(value.clone());
                    if items.len() > limit {
                        break;
                    }
                }
            }
            None => {
                for (entry_key, value) in self.files.range(prefix.clone()..) {
                    if !entry_key.starts_with(&prefix) {
                        break;
                    }
                    items.push(value.clone());
                    if items.len() > limit {
                        break;
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
        let device_prefix = folder_device_prefix(folder, device);
        let start_key = match start_after_path {
            Some(path) => composite_key(folder, device, path),
            None => composite_key(folder, device, prefix),
        };
        let range_start = match start_after_path {
            Some(_) => Bound::Excluded(start_key),
            None => Bound::Included(start_key),
        };

        for (entry_key, value) in self.files.range((range_start, Bound::Unbounded)) {
            if !entry_key.starts_with(&device_prefix) {
                break;
            }
            if !value.path.starts_with(prefix) {
                if compare_path_order(&value.path, prefix) == Ordering::Greater {
                    break;
                }
                continue;
            }
            items.push(value.clone());
            if items.len() > limit {
                break;
            }
        }

        finalize_page(items, limit)
    }

    pub(crate) fn compact(&mut self) -> io::Result<()> {
        let tmp = self.config.root.join("events.log.compact.tmp");
        let mut file = File::create(&tmp)?;

        for record in self.files.values() {
            let mut materialized = record.clone();
            materialized.block_hashes = self.resolve_file_block_hashes(record)?;
            Self::write_record(&mut file, &JournalOp::Upsert(materialized))?;
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

        if self.journal_path.exists() {
            fs::remove_file(&self.journal_path)?;
        }
        fs::rename(&tmp, &self.journal_path)?;
        sync_dir(&self.config.root)?;

        Ok(())
    }

    pub(crate) fn stats(&self) -> StoreStats {
        StoreStats {
            file_count: self.files.len(),
            deleted_tombstone_count: self.deleted_tombstones.len(),
            estimated_memory_bytes: self.approx_memory_bytes,
            memory_budget_bytes: self.config.memory_budget_bytes(),
        }
    }

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    fn load_from_journal(
        path: &Path,
        block_blob_path: &Path,
        max_tombstones: usize,
    ) -> io::Result<(BTreeMap<String, FileMetadata>, VecDeque<String>, usize)> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut block_blob = OpenOptions::new()
            .create(true)
            .append(true)
            .open(block_blob_path)?;
        let mut files: BTreeMap<String, FileMetadata> = BTreeMap::new();
        let mut deleted_tombstones = VecDeque::new();
        let mut tombstone_set = HashSet::new();
        let mut payload_buf: Vec<u8> = Vec::new();

        loop {
            let mut hdr = [0_u8; 8];
            match reader.read_exact(&mut hdr) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            }

            let len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
            let expected_checksum = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);

            if len == 0 || len > LOG_RECORD_MAX_BYTES {
                break;
            }

            payload_buf.resize(len as usize, 0);
            match reader.read_exact(&mut payload_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            }

            if checksum(&payload_buf) != expected_checksum {
                break;
            }

            let op = match decode_op(&payload_buf) {
                Some(op) => op,
                None => break,
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

        let approx_bytes = files.values().map(estimate_file_bytes).sum();
        Ok((files, deleted_tombstones, approx_bytes))
    }

    fn append_op(&self, op: &JournalOp) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.journal_path)?;
        Self::write_record(&mut file, op)?;
        file.sync_data()?;
        Ok(())
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

    fn apply_upsert(&mut self, file: FileMetadata) {
        let key = composite_key(&file.folder, &file.device, &file.path);
        if let Some(prev) = self.files.insert(key.clone(), file.clone()) {
            self.approx_memory_bytes = self
                .approx_memory_bytes
                .saturating_sub(estimate_file_bytes(&prev));
        }
        self.approx_memory_bytes = self
            .approx_memory_bytes
            .saturating_add(estimate_file_bytes(&file));
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

fn apply_op(
    files: &mut BTreeMap<String, FileMetadata>,
    deleted_tombstones: &mut VecDeque<String>,
    tombstone_set: &mut HashSet<String>,
    max_tombstones: usize,
    op: JournalOp,
    block_blob: &mut File,
) -> io::Result<()> {
    match op {
        JournalOp::Upsert(mut file) => {
            file.block_hashes = spill_block_hashes_to_writer(block_blob, &file.block_hashes)?;
            let key = composite_key(&file.folder, &file.device, &file.path);
            files.insert(key.clone(), file);
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
    format!("{folder}{KEY_SEP}{device}{KEY_SEP}{path_key}")
}

fn split_composite_key(key: &str) -> Option<(String, String, String)> {
    let (folder, rest) = key.split_once(KEY_SEP)?;
    let (device, path_key) = rest.split_once(KEY_SEP)?;
    let path = decode_path_key(path_key)?;
    Some((folder.to_string(), device.to_string(), path))
}

fn folder_prefix(folder: &str) -> String {
    format!("{folder}{KEY_SEP}")
}

fn folder_device_prefix(folder: &str, device: &str) -> String {
    format!("{folder}{KEY_SEP}{device}{KEY_SEP}")
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

fn in_memory_block_hash_shape(hashes: &[String]) -> Vec<String> {
    if hashes.is_empty() {
        Vec::new()
    } else {
        vec![format!("{BLOCK_BLOB_MARKER_PREFIX}0:0:0")]
    }
}

fn spill_block_hashes(block_blob_path: &Path, hashes: &[String]) -> io::Result<Vec<String>> {
    let mut blob = OpenOptions::new()
        .create(true)
        .append(true)
        .open(block_blob_path)?;
    spill_block_hashes_to_writer(&mut blob, hashes)
}

fn spill_block_hashes_to_writer(block_blob: &mut File, hashes: &[String]) -> io::Result<Vec<String>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    if block_hashes_are_marker(hashes) {
        return Ok(hashes.to_vec());
    }

    let payload = serde_json::to_vec(hashes)
        .map_err(|err| io::Error::new(ErrorKind::InvalidData, format!("encode block hashes: {err}")))?;
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
    block_blob.sync_data()?;

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

    serde_json::from_slice::<Vec<String>>(&payload)
        .map_err(|err| io::Error::new(ErrorKind::InvalidData, format!("decode block hashes: {err}")))
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

fn estimate_file_bytes(file: &FileMetadata) -> usize {
    let hash_bytes: usize = file.block_hashes.iter().map(String::len).sum();
    file.folder.len() + file.path.len() + file.device.len() + file.file_type.len() + hash_bytes + 64
}

fn projected_upsert_memory_bytes(
    current_bytes: usize,
    current_file: Option<&FileMetadata>,
    next_file: &FileMetadata,
) -> usize {
    let without_current = current_bytes.saturating_sub(current_file.map(estimate_file_bytes).unwrap_or(0));
    without_current.saturating_add(estimate_file_bytes(next_file))
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
        assert!(inserted > 0, "should insert at least one entry before capping");
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

        let mut cursor_path: Option<String> = None;
        let mut out = Vec::new();
        loop {
            let page = store.files_in_folder_ordered_page("alpha", cursor_path.as_deref(), 2);
            for item in &page.items {
                out.push(item.path.clone());
            }
            match page.next_cursor {
                Some(next) => cursor_path = Some(next.path),
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
}
