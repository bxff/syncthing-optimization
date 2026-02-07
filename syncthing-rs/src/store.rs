use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Write};
use std::ops::Bound;
use std::path::{Path, PathBuf};

const LOG_RECORD_MAX_BYTES: u32 = 32 * 1024 * 1024;
pub(crate) const JOURNAL_FILE_NAME: &str = "events.log";
const KEY_SEP: char = '\u{001f}';

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
    pub(crate) path: String,
    pub(crate) sequence: u64,
    pub(crate) deleted: bool,
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
    files: BTreeMap<String, FileMetadata>,
    deleted_tombstones: VecDeque<String>,
    tombstone_set: HashSet<String>,
    approx_memory_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PageCursor {
    pub(crate) folder: String,
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
    Delete { folder: String, path: String },
}

impl Store {
    pub(crate) fn open(config: StoreConfig) -> io::Result<Self> {
        fs::create_dir_all(&config.root)?;
        let journal_path = config.root.join(JOURNAL_FILE_NAME);
        if !journal_path.exists() {
            File::create(&journal_path)?;
        }

        let (files, deleted_tombstones, approx_memory_bytes) =
            Self::load_from_journal(&journal_path, config.max_deleted_tombstones)?;
        let tombstone_set = deleted_tombstones.iter().cloned().collect();

        Ok(Self {
            config,
            journal_path,
            files,
            deleted_tombstones,
            tombstone_set,
            approx_memory_bytes,
        })
    }

    pub(crate) fn upsert_file(&mut self, file: FileMetadata) -> io::Result<()> {
        self.append_op(&JournalOp::Upsert(file.clone()))?;
        self.apply_upsert(file);
        Ok(())
    }

    pub(crate) fn delete_file(&mut self, folder: &str, path: &str) -> io::Result<()> {
        self.append_op(&JournalOp::Delete {
            folder: folder.to_string(),
            path: path.to_string(),
        })?;

        let key = composite_key(folder, path);
        if let Some(prev) = self.files.remove(&key) {
            self.approx_memory_bytes = self
                .approx_memory_bytes
                .saturating_sub(estimate_file_bytes(&prev));
        }
        self.record_tombstone(&key);

        Ok(())
    }

    pub(crate) fn get_file(&self, folder: &str, path: &str) -> Option<&FileMetadata> {
        self.files.get(&composite_key(folder, path))
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

    pub(crate) fn file_count(&self) -> usize {
        self.files.len()
    }

    pub(crate) fn tombstone_count(&self) -> usize {
        self.deleted_tombstones.len()
    }

    pub(crate) fn has_tombstone(&self, folder: &str, path: &str) -> bool {
        let key = composite_key(folder, path);
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
                let start_key = composite_key(&cursor.folder, &cursor.path);
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
        let range_start = match start_after_path {
            Some(path) => composite_key(folder, path),
            None => folder_start.clone(),
        };
        let lower = if start_after_path.is_some() {
            Bound::Excluded(range_start)
        } else {
            Bound::Included(range_start)
        };

        for (key, value) in self.files.range((lower, Bound::Unbounded)) {
            if !key.starts_with(&folder_start) {
                break;
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
            Self::write_record(&mut file, &JournalOp::Upsert(record.clone()))?;
        }
        for tombstone in &self.deleted_tombstones {
            if let Some((folder, path)) = split_composite_key(tombstone) {
                Self::write_record(
                    &mut file,
                    &JournalOp::Delete {
                        folder: folder.to_string(),
                        path: path.to_string(),
                    },
                )?;
            }
        }

        file.sync_all()?;

        if self.journal_path.exists() {
            fs::remove_file(&self.journal_path)?;
        }
        fs::rename(&tmp, &self.journal_path)?;

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
        max_tombstones: usize,
    ) -> io::Result<(BTreeMap<String, FileMetadata>, VecDeque<String>, usize)> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
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
            );
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
        let key = composite_key(&file.folder, &file.path);
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
) {
    match op {
        JournalOp::Upsert(file) => {
            let key = composite_key(&file.folder, &file.path);
            files.insert(key.clone(), file);
            if tombstone_set.remove(&key) {
                deleted_tombstones.retain(|k| *k != key);
            }
        }
        JournalOp::Delete { folder, path } => {
            let key = composite_key(&folder, &path);
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
                "U\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                escape(&file.folder),
                escape(&file.path),
                file.sequence,
                if file.deleted { 1 } else { 0 },
                file.modified_ns,
                file.size,
                hashes,
            )
        }
        JournalOp::Delete { folder, path } => {
            format!("D\t{}\t{}\n", escape(folder), escape(path))
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
                path,
                sequence,
                deleted,
                modified_ns,
                size,
                block_hashes,
            }))
        }
        "D" if parts.len() == 3 => Some(JournalOp::Delete {
            folder: unescape(&parts[1])?,
            path: unescape(&parts[2])?,
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

fn composite_key(folder: &str, path: &str) -> String {
    format!("{folder}{KEY_SEP}{path}")
}

fn split_composite_key(key: &str) -> Option<(&str, &str)> {
    key.split_once(KEY_SEP)
}

fn folder_prefix(folder: &str) -> String {
    format!("{folder}{KEY_SEP}")
}

fn finalize_page(mut items: Vec<FileMetadata>, limit: usize) -> FilePage {
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    let next_cursor = if has_more {
        items.last().map(|last| PageCursor {
            folder: last.folder.clone(),
            path: last.path.clone(),
        })
    } else {
        None
    };

    FilePage { items, next_cursor }
}

fn estimate_file_bytes(file: &FileMetadata) -> usize {
    let hash_bytes: usize = file.block_hashes.iter().map(String::len).sum();
    file.folder.len() + file.path.len() + hash_bytes + 64
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
            path: path.to_string(),
            sequence,
            deleted: false,
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
            store.delete_file("beta", "z.txt").expect("delete");
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
        assert!(store.get_file("alpha", "a.txt").is_some());

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
                    .delete_file("alpha", &format!("{i:04}.dat"))
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
    fn bounded_tombstones_keep_latest_deletes_only() {
        let root = temp_root("tombstones");
        let cfg = StoreConfig::new(&root).with_deleted_tombstone_cap(3);
        let mut store = Store::open(cfg).expect("open");

        for i in 0_u64..5 {
            let path = format!("{i:04}.dat");
            store
                .upsert_file(meta("alpha", &path, i + 1))
                .expect("upsert");
            store.delete_file("alpha", &path).expect("delete");
        }

        assert_eq!(store.tombstone_count(), 3);
        assert!(!store.has_tombstone("alpha", "0000.dat"));
        assert!(!store.has_tombstone("alpha", "0001.dat"));
        assert!(store.has_tombstone("alpha", "0002.dat"));
        assert!(store.has_tombstone("alpha", "0003.dat"));
        assert!(store.has_tombstone("alpha", "0004.dat"));

        fs::remove_dir_all(root).expect("cleanup");
    }
}
