use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const LOG_RECORD_MAX_BYTES: u32 = 32 * 1024 * 1024;
pub(crate) const JOURNAL_FILE_NAME: &str = "events.log";
const KEY_SEP: char = '\u{001f}';

#[derive(Clone, Debug)]
pub(crate) struct StoreConfig {
    pub(crate) root: PathBuf,
    pub(crate) max_runtime_memory_mb: usize,
}

impl StoreConfig {
    pub(crate) fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_runtime_memory_mb: 50,
        }
    }

    pub(crate) fn with_memory_cap_mb(mut self, cap_mb: usize) -> Self {
        self.max_runtime_memory_mb = cap_mb;
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
    pub(crate) estimated_memory_bytes: usize,
    pub(crate) memory_budget_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct Store {
    config: StoreConfig,
    journal_path: PathBuf,
    files: BTreeMap<String, FileMetadata>,
    approx_memory_bytes: usize,
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

        let (files, approx_memory_bytes) = Self::load_from_journal(&journal_path)?;

        Ok(Self {
            config,
            journal_path,
            files,
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

    pub(crate) fn compact(&mut self) -> io::Result<()> {
        let tmp = self.config.root.join("events.log.compact.tmp");
        let mut file = File::create(&tmp)?;

        for record in self.files.values() {
            Self::write_record(&mut file, &JournalOp::Upsert(record.clone()))?;
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
            estimated_memory_bytes: self.approx_memory_bytes,
            memory_budget_bytes: self.config.memory_budget_bytes(),
        }
    }

    pub(crate) fn journal_path(&self) -> &Path {
        &self.journal_path
    }

    fn load_from_journal(path: &Path) -> io::Result<(BTreeMap<String, FileMetadata>, usize)> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut files: BTreeMap<String, FileMetadata> = BTreeMap::new();

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

            let mut payload = vec![0_u8; len as usize];
            match reader.read_exact(&mut payload) {
                Ok(()) => {}
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            }

            if checksum(&payload) != expected_checksum {
                break;
            }

            let op = match decode_op(&payload) {
                Some(op) => op,
                None => break,
            };
            apply_op(&mut files, op);
        }

        let approx_bytes = files.values().map(estimate_file_bytes).sum();
        Ok((files, approx_bytes))
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
        if let Some(prev) = self.files.insert(key, file.clone()) {
            self.approx_memory_bytes = self
                .approx_memory_bytes
                .saturating_sub(estimate_file_bytes(&prev));
        }
        self.approx_memory_bytes = self
            .approx_memory_bytes
            .saturating_add(estimate_file_bytes(&file));
    }
}

fn apply_op(files: &mut BTreeMap<String, FileMetadata>, op: JournalOp) {
    match op {
        JournalOp::Upsert(file) => {
            files.insert(composite_key(&file.folder, &file.path), file);
        }
        JournalOp::Delete { folder, path } => {
            files.remove(&composite_key(&folder, &path));
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

fn folder_prefix(folder: &str) -> String {
    format!("{folder}{KEY_SEP}")
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
        assert!(stats.estimated_memory_bytes < stats.memory_budget_bytes);

        fs::remove_dir_all(root).expect("cleanup");
    }
}
