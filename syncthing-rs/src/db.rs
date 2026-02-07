use crate::store::{FileMetadata as StoreFileMetadata, Store, StoreConfig};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) type DeviceId = String;
pub(crate) type IndexId = u64;
pub(crate) type LocalFlags = u32;

pub(crate) const FLAG_LOCAL_RECEIVE_ONLY: LocalFlags = 1 << 0;
pub(crate) const FLAG_LOCAL_INVALID: LocalFlags = 1 << 1;
pub(crate) const FLAG_LOCAL_CONFLICT: LocalFlags = 1 << 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PullOrder {
    Alphabetic,
    NewestFirst,
    OldestFirst,
    LargestFirst,
    SmallestFirst,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileInfoType {
    File,
    Directory,
    Symlink,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileInfo {
    pub(crate) folder: String,
    pub(crate) path: String,
    pub(crate) sequence: i64,
    pub(crate) modified_ns: i64,
    pub(crate) size: i64,
    pub(crate) deleted: bool,
    pub(crate) ignored: bool,
    pub(crate) local_flags: LocalFlags,
    pub(crate) file_type: FileInfoType,
    pub(crate) block_hashes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileMetadata {
    pub(crate) name: String,
    pub(crate) sequence: i64,
    pub(crate) mod_nanos: i64,
    pub(crate) size: i64,
    pub(crate) local_flags: LocalFlags,
    pub(crate) file_type: FileInfoType,
    pub(crate) deleted: bool,
}

impl FileMetadata {
    pub(crate) fn mod_time(&self) -> i64 {
        self.mod_nanos
    }

    pub(crate) fn is_receive_only_changed(&self) -> bool {
        self.local_flags & FLAG_LOCAL_RECEIVE_ONLY != 0
    }

    pub(crate) fn is_directory(&self) -> bool {
        self.file_type == FileInfoType::Directory
    }

    pub(crate) fn should_conflict(&self) -> bool {
        self.local_flags & FLAG_LOCAL_CONFLICT != 0
    }

    pub(crate) fn is_invalid(&self) -> bool {
        self.local_flags & FLAG_LOCAL_INVALID != 0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    pub(crate) files: usize,
    pub(crate) directories: usize,
    pub(crate) deleted: usize,
    pub(crate) bytes: i64,
    pub(crate) receive_only_changed: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BlockMapEntry {
    pub(crate) blocklist_hash: Vec<u8>,
    pub(crate) offset: i64,
    pub(crate) block_index: i32,
    pub(crate) size: i32,
    pub(crate) file_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyValue {
    pub(crate) key: String,
    pub(crate) value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalFilePage {
    pub(crate) items: Vec<FileInfo>,
    pub(crate) next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NeededFilePage {
    pub(crate) items: Vec<FileInfo>,
    pub(crate) next_cursor: Option<String>,
}

const RUNTIME_META_FILE: &str = "runtime-meta.json";
const META_KEY_SEP: char = '\u{001f}';

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct RuntimeMetadata {
    index_ids: BTreeMap<String, IndexId>,
    mtimes: BTreeMap<String, [i64; 2]>,
    kv: BTreeMap<String, Vec<u8>>,
}

pub(crate) trait Kv {
    fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    fn put_kv(&mut self, key: &str, val: &[u8]) -> Result<(), String>;
    fn delete_kv(&mut self, key: &str) -> Result<(), String>;
    fn prefix_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, String>;
}

pub(crate) trait Db {
    fn update(&mut self, folder: &str, device: &str, files: Vec<FileInfo>) -> Result<(), String>;
    fn close(&mut self) -> Result<(), String>;

    fn get_device_file(
        &self,
        folder: &str,
        device: &str,
        file: &str,
    ) -> Result<Option<FileInfo>, String>;
    fn get_global_availability(&self, folder: &str, file: &str) -> Result<Vec<DeviceId>, String>;
    fn get_global_file(&self, folder: &str, file: &str) -> Result<Option<FileInfo>, String>;

    fn all_global_files(&self, folder: &str) -> Result<Vec<FileMetadata>, String>;
    fn all_global_files_prefix(
        &self,
        folder: &str,
        prefix: &str,
    ) -> Result<Vec<FileMetadata>, String>;
    fn all_local_files(&self, folder: &str, device: &str) -> Result<Vec<FileInfo>, String>;
    fn all_local_files_ordered(&self, folder: &str, device: &str) -> Result<Vec<FileInfo>, String>;
    fn all_local_files_by_sequence(
        &self,
        folder: &str,
        device: &str,
        start_seq: i64,
        limit: usize,
    ) -> Result<Vec<FileInfo>, String>;
    fn all_local_files_with_prefix(
        &self,
        folder: &str,
        device: &str,
        prefix: &str,
    ) -> Result<Vec<FileInfo>, String>;
    fn all_local_files_with_blocks_hash(
        &self,
        folder: &str,
        hash: &[u8],
    ) -> Result<Vec<FileMetadata>, String>;
    fn all_needed_global_files(
        &self,
        folder: &str,
        device: &str,
        order: PullOrder,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<FileInfo>, String>;
    fn all_local_blocks_with_hash(
        &self,
        folder: &str,
        hash: &[u8],
    ) -> Result<Vec<BlockMapEntry>, String>;

    fn drop_all_files(&mut self, folder: &str, device: &str) -> Result<(), String>;
    fn drop_device(&mut self, device: &str) -> Result<(), String>;
    fn drop_files_named(
        &mut self,
        folder: &str,
        device: &str,
        names: &[String],
    ) -> Result<(), String>;
    fn drop_folder(&mut self, folder: &str) -> Result<(), String>;

    fn get_device_sequence(&self, folder: &str, device: &str) -> Result<i64, String>;
    fn list_folders(&self) -> Result<Vec<String>, String>;
    fn list_devices_for_folder(&self, folder: &str) -> Result<Vec<DeviceId>, String>;
    fn remote_sequences(&self, folder: &str) -> Result<BTreeMap<DeviceId, i64>, String>;

    fn count_global(&self, folder: &str) -> Result<Counts, String>;
    fn count_local(&self, folder: &str, device: &str) -> Result<Counts, String>;
    fn count_need(&self, folder: &str, device: &str) -> Result<Counts, String>;
    fn count_receive_only_changed(&self, folder: &str) -> Result<Counts, String>;

    fn drop_all_index_ids(&mut self) -> Result<(), String>;
    fn get_index_id(&self, folder: &str, device: &str) -> Result<IndexId, String>;
    fn set_index_id(&mut self, folder: &str, device: &str, id: IndexId) -> Result<(), String>;

    fn delete_mtime(&mut self, folder: &str, name: &str) -> Result<(), String>;
    fn get_mtime(&self, folder: &str, name: &str) -> Result<(i64, i64), String>;
    fn put_mtime(
        &mut self,
        folder: &str,
        name: &str,
        ondisk_ns: i64,
        virtual_ns: i64,
    ) -> Result<(), String>;

    fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    fn put_kv(&mut self, key: &str, val: &[u8]) -> Result<(), String>;
    fn delete_kv(&mut self, key: &str) -> Result<(), String>;
    fn prefix_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, String>;
}

impl Kv for WalFreeDb {
    fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        <Self as Db>::get_kv(self, key)
    }

    fn put_kv(&mut self, key: &str, val: &[u8]) -> Result<(), String> {
        <Self as Db>::put_kv(self, key, val)
    }

    fn delete_kv(&mut self, key: &str) -> Result<(), String> {
        <Self as Db>::delete_kv(self, key)
    }

    fn prefix_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, String> {
        <Self as Db>::prefix_kv(self, prefix)
    }
}

#[derive(Debug)]
pub(crate) struct WalFreeDb {
    store: Store,
    runtime_root: PathBuf,
    index_ids: BTreeMap<(String, DeviceId), IndexId>,
    mtimes: BTreeMap<(String, String), (i64, i64)>,
    kv: BTreeMap<String, Vec<u8>>,
    closed: bool,
}

impl Default for WalFreeDb {
    fn default() -> Self {
        Self::open_default_runtime().expect("open default wal-free runtime store")
    }
}

impl WalFreeDb {
    pub(crate) fn open(config: StoreConfig) -> Result<Self, String> {
        let root = config.root.clone();
        let store = Store::open(config).map_err(|e| format!("open wal-free store: {e}"))?;
        let runtime_meta = load_runtime_metadata(&root)?;
        Ok(Self {
            store,
            runtime_root: root,
            index_ids: runtime_meta_index_ids(&runtime_meta),
            mtimes: runtime_meta_mtimes(&runtime_meta),
            kv: runtime_meta.kv,
            closed: false,
        })
    }

    pub(crate) fn open_runtime(root: impl Into<PathBuf>, memory_cap_mb: usize) -> Result<Self, String> {
        let config = StoreConfig::new(root).with_memory_cap_mb(memory_cap_mb);
        Self::open(config)
    }

    pub(crate) fn runtime_root(&self) -> &PathBuf {
        &self.runtime_root
    }

    pub(crate) fn estimated_memory_bytes(&self) -> usize {
        self.store.stats().estimated_memory_bytes
    }

    pub(crate) fn memory_budget_bytes(&self) -> usize {
        self.store.stats().memory_budget_bytes
    }

    pub(crate) fn all_local_files_ordered_page(
        &self,
        folder: &str,
        device: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<LocalFilePage, String> {
        self.ensure_open()?;
        let page = self
            .store
            .files_in_folder_device_ordered_page(folder, device, start_after, limit);
        Ok(LocalFilePage {
            items: page
                .items
                .into_iter()
                .map(|meta| store_to_file_info(&meta))
                .collect(),
            next_cursor: page.next_cursor.map(|cursor| cursor.path),
        })
    }

    pub(crate) fn all_needed_global_files_ordered_page(
        &self,
        folder: &str,
        device: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<NeededFilePage, String> {
        self.ensure_open()?;
        if limit == 0 {
            return Ok(NeededFilePage {
                items: Vec::new(),
                next_cursor: None,
            });
        }

        let globals = self.global_file_map(folder);
        let mut items = Vec::new();
        let mut has_more = false;

        for (path, global) in globals.iter() {
            if let Some(cursor) = start_after {
                if path.as_str() <= cursor {
                    continue;
                }
            }
            let local = self.get_device_file(folder, device, path)?;
            let requires_update = match local {
                Some(current) => {
                    global.sequence > current.sequence
                        || (global.deleted != current.deleted
                            && global.sequence >= current.sequence)
                }
                None => true,
            };
            if !requires_update {
                continue;
            }
            if items.len() >= limit {
                has_more = true;
                break;
            }
            items.push(global.clone());
        }

        let next_cursor = if has_more {
            items.last().map(|f| f.path.clone())
        } else {
            None
        };
        Ok(NeededFilePage { items, next_cursor })
    }

    fn open_default_runtime() -> Result<Self, String> {
        let mut root = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| format!("clock error: {e}"))?
            .as_nanos();
        root.push(format!(
            "syncthing-rs-runtime-db-{}-{nanos}",
            std::process::id()
        ));
        let config = StoreConfig::new(&root).with_memory_cap_mb(50);
        Self::open(config)
    }

    fn ensure_open(&self) -> Result<(), String> {
        if self.closed {
            return Err("database is closed".to_string());
        }
        Ok(())
    }

    fn persist_runtime_metadata(&self) -> Result<(), String> {
        let path = runtime_metadata_path(&self.runtime_root);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create runtime meta dir: {e}"))?;
        }
        let temp_path = path.with_extension("json.tmp");
        let payload = RuntimeMetadata {
            index_ids: self
                .index_ids
                .iter()
                .map(|((folder, device), id)| (join_meta_key(folder, device), *id))
                .collect(),
            mtimes: self
                .mtimes
                .iter()
                .map(|((folder, name), (ondisk_ns, virtual_ns))| {
                    (join_meta_key(folder, name), [*ondisk_ns, *virtual_ns])
                })
                .collect(),
            kv: self.kv.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&payload)
            .map_err(|e| format!("serialize runtime metadata: {e}"))?;
        fs::write(&temp_path, bytes).map_err(|e| format!("write runtime metadata tmp: {e}"))?;
        fs::File::open(&temp_path)
            .and_then(|f| f.sync_all())
            .map_err(|e| format!("sync runtime metadata tmp: {e}"))?;
        fs::rename(&temp_path, &path).map_err(|e| format!("rename runtime metadata: {e}"))?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)
                .and_then(|f| f.sync_all())
                .map_err(|e| format!("sync runtime metadata dir: {e}"))?;
        }
        Ok(())
    }

    fn all_files_for_folder_device(&self, folder: &str, device: &str) -> Vec<FileInfo> {
        let mut out = Vec::new();
        self.store
            .for_each_file_in_folder_device(folder, device, |meta| {
                out.push(store_to_file_info(meta));
                true
            });
        out
    }

    fn global_file_map(&self, folder: &str) -> BTreeMap<String, FileInfo> {
        let mut out: BTreeMap<String, FileInfo> = BTreeMap::new();
        self.store.for_each_file_in_folder(folder, |meta| {
            if meta.ignored {
                return true;
            }
            let candidate = store_to_file_info(meta);
            let path = candidate.path.clone();
            match out.get(&path) {
                Some(current) if !prefer_global(&candidate, current) => {}
                _ => {
                    out.insert(path, candidate);
                }
            }
            true
        });
        out
    }

    fn global_availability_map(&self, folder: &str) -> BTreeMap<String, BTreeSet<DeviceId>> {
        let mut out: BTreeMap<String, BTreeSet<DeviceId>> = BTreeMap::new();
        self.store.for_each_file_in_folder(folder, |meta| {
            if meta.deleted {
                return true;
            }
            out.entry(meta.path.clone())
                .or_default()
                .insert(meta.device.clone());
            true
        });
        out
    }
}

impl Db for WalFreeDb {
    fn update(&mut self, folder: &str, device: &str, files: Vec<FileInfo>) -> Result<(), String> {
        self.ensure_open()?;
        for mut file in files {
            file.folder = folder.to_string();
            let encoded = file_info_to_store(folder, device, &file);
            self.store
                .upsert_file(encoded)
                .map_err(|e| format!("persist update: {e}"))?;
        }
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.ensure_open()?;
        self.store
            .compact()
            .map_err(|e| format!("compact on close: {e}"))?;
        self.persist_runtime_metadata()?;
        self.closed = true;
        Ok(())
    }

    fn get_device_file(
        &self,
        folder: &str,
        device: &str,
        file: &str,
    ) -> Result<Option<FileInfo>, String> {
        self.ensure_open()?;
        Ok(self
            .store
            .get_file(folder, device, file)
            .map(store_to_file_info))
    }

    fn get_global_availability(&self, folder: &str, file: &str) -> Result<Vec<DeviceId>, String> {
        self.ensure_open()?;
        let mut out = self
            .global_availability_map(folder)
            .remove(file)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        out.sort();
        Ok(out)
    }

    fn get_global_file(&self, folder: &str, file: &str) -> Result<Option<FileInfo>, String> {
        self.ensure_open()?;
        Ok(self.global_file_map(folder).remove(file))
    }

    fn all_global_files(&self, folder: &str) -> Result<Vec<FileMetadata>, String> {
        self.ensure_open()?;
        Ok(self
            .global_file_map(folder)
            .into_values()
            .map(file_metadata_from_info)
            .collect())
    }

    fn all_global_files_prefix(
        &self,
        folder: &str,
        prefix: &str,
    ) -> Result<Vec<FileMetadata>, String> {
        self.ensure_open()?;
        Ok(self
            .global_file_map(folder)
            .into_values()
            .filter(|f| f.path.starts_with(prefix))
            .map(file_metadata_from_info)
            .collect())
    }

    fn all_local_files(&self, folder: &str, device: &str) -> Result<Vec<FileInfo>, String> {
        self.ensure_open()?;
        Ok(self.all_files_for_folder_device(folder, device))
    }

    fn all_local_files_ordered(&self, folder: &str, device: &str) -> Result<Vec<FileInfo>, String> {
        self.ensure_open()?;
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.all_local_files_ordered_page(folder, device, cursor.as_deref(), 2048)?;
            out.extend(page.items);
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(out)
    }

    fn all_local_files_by_sequence(
        &self,
        folder: &str,
        device: &str,
        start_seq: i64,
        limit: usize,
    ) -> Result<Vec<FileInfo>, String> {
        self.ensure_open()?;
        let mut out = self
            .all_local_files(folder, device)?
            .into_iter()
            .filter(|f| f.sequence >= start_seq)
            .collect::<Vec<_>>();
        out.sort_by(|a, b| {
            a.sequence
                .cmp(&b.sequence)
                .then_with(|| crate::store::compare_path_order(&a.path, &b.path))
        });
        if limit > 0 && out.len() > limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    fn all_local_files_with_prefix(
        &self,
        folder: &str,
        device: &str,
        prefix: &str,
    ) -> Result<Vec<FileInfo>, String> {
        self.ensure_open()?;
        Ok(self
            .all_local_files_ordered(folder, device)?
            .into_iter()
            .filter(|f| f.path.starts_with(prefix))
            .collect())
    }

    fn all_local_files_with_blocks_hash(
        &self,
        folder: &str,
        hash: &[u8],
    ) -> Result<Vec<FileMetadata>, String> {
        self.ensure_open()?;
        let target = String::from_utf8_lossy(hash).to_string();
        let mut out = Vec::new();
        self.store.for_each_file_in_folder(folder, |meta| {
            if meta.block_hashes.iter().any(|h| h == &target) {
                out.push(file_metadata_from_info(store_to_file_info(meta)));
            }
            true
        });
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn all_needed_global_files(
        &self,
        folder: &str,
        device: &str,
        order: PullOrder,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<FileInfo>, String> {
        self.ensure_open()?;
        if order == PullOrder::Alphabetic {
            let mut out = Vec::new();
            let mut cursor: Option<String> = None;
            let mut skipped = 0_usize;
            let target = if limit == 0 { usize::MAX } else { limit };
            loop {
                let page = self.all_needed_global_files_ordered_page(
                    folder,
                    device,
                    cursor.as_deref(),
                    2048,
                )?;
                if page.items.is_empty() {
                    break;
                }
                for file in page.items {
                    if skipped < offset {
                        skipped += 1;
                        continue;
                    }
                    if out.len() >= target {
                        return Ok(out);
                    }
                    out.push(file);
                }
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
            }
            return Ok(out);
        }

        let globals = self.global_file_map(folder);
        let mut need = Vec::new();

        for (path, global) in globals {
            let local = self.get_device_file(folder, device, &path)?;
            let requires_update = match local {
                Some(current) => {
                    global.sequence > current.sequence
                        || (global.deleted != current.deleted
                            && global.sequence >= current.sequence)
                }
                None => true,
            };
            if requires_update {
                need.push(global);
            }
        }

        sort_pull_order(&mut need, order);
        let sliced = need
            .into_iter()
            .skip(offset)
            .take(if limit == 0 { usize::MAX } else { limit })
            .collect();
        Ok(sliced)
    }

    fn all_local_blocks_with_hash(
        &self,
        folder: &str,
        hash: &[u8],
    ) -> Result<Vec<BlockMapEntry>, String> {
        self.ensure_open()?;
        let target = String::from_utf8_lossy(hash).to_string();
        let mut out = Vec::new();

        self.store.for_each_file_in_folder(folder, |meta| {
            for (idx, block_hash) in meta.block_hashes.iter().enumerate() {
                if block_hash != &target {
                    continue;
                }
                out.push(BlockMapEntry {
                    blocklist_hash: target.as_bytes().to_vec(),
                    offset: idx as i64 * 128 * 1024,
                    block_index: idx as i32,
                    size: 128 * 1024,
                    file_name: meta.path.clone(),
                });
            }
            true
        });
        out.sort_by(|a, b| a.file_name.cmp(&b.file_name));
        Ok(out)
    }

    fn drop_all_files(&mut self, folder: &str, device: &str) -> Result<(), String> {
        self.ensure_open()?;
        let paths = self
            .all_files_for_folder_device(folder, device)
            .into_iter()
            .map(|f| f.path)
            .collect::<Vec<_>>();
        for path in paths {
            self.store
                .delete_file(folder, device, &path)
                .map_err(|e| format!("drop all files: {e}"))?;
        }
        Ok(())
    }

    fn drop_device(&mut self, device: &str) -> Result<(), String> {
        self.ensure_open()?;
        let mut doomed = Vec::new();
        self.store.for_each_file(|meta| {
            if meta.device == device {
                doomed.push((meta.folder.clone(), meta.path.clone()));
            }
            true
        });
        for (folder, path) in doomed {
            self.store
                .delete_file(&folder, device, &path)
                .map_err(|e| format!("drop device: {e}"))?;
        }
        self.index_ids.retain(|(_, d), _| d != device);
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn drop_files_named(
        &mut self,
        folder: &str,
        device: &str,
        names: &[String],
    ) -> Result<(), String> {
        self.ensure_open()?;
        let name_set = names.iter().cloned().collect::<HashSet<_>>();
        for name in name_set {
            self.store
                .delete_file(folder, device, &name)
                .map_err(|e| format!("drop named file: {e}"))?;
        }
        Ok(())
    }

    fn drop_folder(&mut self, folder: &str) -> Result<(), String> {
        self.ensure_open()?;
        let mut doomed = Vec::new();
        self.store.for_each_file_in_folder(folder, |meta| {
            doomed.push((meta.device.clone(), meta.path.clone()));
            true
        });
        for (device, path) in doomed {
            self.store
                .delete_file(folder, &device, &path)
                .map_err(|e| format!("drop folder: {e}"))?;
        }
        self.index_ids.retain(|(f, _), _| f != folder);
        self.mtimes.retain(|(f, _), _| f != folder);
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn get_device_sequence(&self, folder: &str, device: &str) -> Result<i64, String> {
        self.ensure_open()?;
        let max_seq = self
            .all_files_for_folder_device(folder, device)
            .into_iter()
            .map(|f| f.sequence)
            .max()
            .unwrap_or(0);
        Ok(max_seq)
    }

    fn list_folders(&self) -> Result<Vec<String>, String> {
        self.ensure_open()?;
        let mut folders = BTreeSet::new();
        self.store.for_each_file(|meta| {
            folders.insert(meta.folder.clone());
            true
        });
        Ok(folders.into_iter().collect())
    }

    fn list_devices_for_folder(&self, folder: &str) -> Result<Vec<DeviceId>, String> {
        self.ensure_open()?;
        let mut devices = BTreeSet::new();
        self.store.for_each_file_in_folder(folder, |meta| {
            devices.insert(meta.device.clone());
            true
        });
        Ok(devices.into_iter().collect())
    }

    fn remote_sequences(&self, folder: &str) -> Result<BTreeMap<DeviceId, i64>, String> {
        self.ensure_open()?;
        let mut out = BTreeMap::new();
        self.store.for_each_file_in_folder(folder, |meta| {
            let entry = out.entry(meta.device.clone()).or_insert(0);
            *entry = (*entry).max(u64_to_i64(meta.sequence));
            true
        });
        Ok(out)
    }

    fn count_global(&self, folder: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        Ok(count_files(self.global_file_map(folder).into_values()))
    }

    fn count_local(&self, folder: &str, device: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        let mut counts = Counts::default();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.all_local_files_ordered_page(folder, device, cursor.as_deref(), 2048)?;
            if page.items.is_empty() {
                break;
            }
            accumulate_counts(&mut counts, count_files(page.items.into_iter()));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(counts)
    }

    fn count_need(&self, folder: &str, device: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        let mut counts = Counts::default();
        let mut cursor: Option<String> = None;
        loop {
            let page =
                self.all_needed_global_files_ordered_page(folder, device, cursor.as_deref(), 2048)?;
            if page.items.is_empty() {
                break;
            }
            accumulate_counts(&mut counts, count_files(page.items.into_iter()));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(counts)
    }

    fn count_receive_only_changed(&self, folder: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        let changed = self
            .global_file_map(folder)
            .into_values()
            .filter(|f| f.local_flags & FLAG_LOCAL_RECEIVE_ONLY != 0);
        Ok(count_files(changed))
    }

    fn drop_all_index_ids(&mut self) -> Result<(), String> {
        self.ensure_open()?;
        self.index_ids.clear();
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn get_index_id(&self, folder: &str, device: &str) -> Result<IndexId, String> {
        self.ensure_open()?;
        Ok(*self
            .index_ids
            .get(&(folder.to_string(), device.to_string()))
            .unwrap_or(&0))
    }

    fn set_index_id(&mut self, folder: &str, device: &str, id: IndexId) -> Result<(), String> {
        self.ensure_open()?;
        self.index_ids
            .insert((folder.to_string(), device.to_string()), id);
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn delete_mtime(&mut self, folder: &str, name: &str) -> Result<(), String> {
        self.ensure_open()?;
        self.mtimes.remove(&(folder.to_string(), name.to_string()));
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn get_mtime(&self, folder: &str, name: &str) -> Result<(i64, i64), String> {
        self.ensure_open()?;
        Ok(*self
            .mtimes
            .get(&(folder.to_string(), name.to_string()))
            .unwrap_or(&(0, 0)))
    }

    fn put_mtime(
        &mut self,
        folder: &str,
        name: &str,
        ondisk_ns: i64,
        virtual_ns: i64,
    ) -> Result<(), String> {
        self.ensure_open()?;
        self.mtimes.insert(
            (folder.to_string(), name.to_string()),
            (ondisk_ns, virtual_ns),
        );
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.ensure_open()?;
        Ok(self.kv.get(key).cloned())
    }

    fn put_kv(&mut self, key: &str, val: &[u8]) -> Result<(), String> {
        self.ensure_open()?;
        self.kv.insert(key.to_string(), val.to_vec());
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn delete_kv(&mut self, key: &str) -> Result<(), String> {
        self.ensure_open()?;
        self.kv.remove(key);
        self.persist_runtime_metadata()?;
        Ok(())
    }

    fn prefix_kv(&self, prefix: &str) -> Result<Vec<KeyValue>, String> {
        self.ensure_open()?;
        let mut out = Vec::new();
        for (k, v) in &self.kv {
            if k.starts_with(prefix) {
                out.push(KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                });
            }
        }
        Ok(out)
    }
}

fn file_info_to_store(folder: &str, device: &str, info: &FileInfo) -> StoreFileMetadata {
    StoreFileMetadata {
        folder: folder.to_string(),
        device: device.to_string(),
        path: info.path.clone(),
        sequence: i64_to_u64(info.sequence),
        deleted: info.deleted,
        ignored: info.ignored,
        local_flags: info.local_flags,
        file_type: file_type_to_store(info.file_type),
        modified_ns: i64_to_u64(info.modified_ns),
        size: i64_to_u64(info.size),
        block_hashes: info.block_hashes.clone(),
    }
}

fn store_to_file_info(meta: &StoreFileMetadata) -> FileInfo {
    FileInfo {
        folder: meta.folder.clone(),
        path: meta.path.clone(),
        sequence: u64_to_i64(meta.sequence),
        modified_ns: u64_to_i64(meta.modified_ns),
        size: u64_to_i64(meta.size),
        deleted: meta.deleted,
        ignored: meta.ignored,
        local_flags: meta.local_flags,
        file_type: store_to_file_type(&meta.file_type),
        block_hashes: meta.block_hashes.clone(),
    }
}

fn file_type_to_store(file_type: FileInfoType) -> String {
    match file_type {
        FileInfoType::File => "file".to_string(),
        FileInfoType::Directory => "directory".to_string(),
        FileInfoType::Symlink => "symlink".to_string(),
    }
}

fn store_to_file_type(file_type: &str) -> FileInfoType {
    match file_type {
        "directory" => FileInfoType::Directory,
        "symlink" => FileInfoType::Symlink,
        _ => FileInfoType::File,
    }
}

fn i64_to_u64(v: i64) -> u64 {
    if v < 0 {
        0
    } else {
        v as u64
    }
}

fn u64_to_i64(v: u64) -> i64 {
    v.min(i64::MAX as u64) as i64
}

fn runtime_metadata_path(root: &PathBuf) -> PathBuf {
    root.join(RUNTIME_META_FILE)
}

fn join_meta_key(a: &str, b: &str) -> String {
    format!("{a}{META_KEY_SEP}{b}")
}

fn split_meta_key(key: &str) -> Option<(String, String)> {
    let (a, b) = key.split_once(META_KEY_SEP)?;
    Some((a.to_string(), b.to_string()))
}

fn load_runtime_metadata(root: &PathBuf) -> Result<RuntimeMetadata, String> {
    let path = runtime_metadata_path(root);
    let bytes = match fs::read(&path) {
        Ok(v) => v,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(RuntimeMetadata::default()),
        Err(err) => return Err(format!("read runtime metadata {}: {err}", path.display())),
    };
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("decode runtime metadata {}: {e}", path.display()))
}

fn runtime_meta_index_ids(meta: &RuntimeMetadata) -> BTreeMap<(String, DeviceId), IndexId> {
    let mut out = BTreeMap::new();
    for (k, id) in &meta.index_ids {
        if let Some((folder, device)) = split_meta_key(k) {
            out.insert((folder, device), *id);
        }
    }
    out
}

fn runtime_meta_mtimes(meta: &RuntimeMetadata) -> BTreeMap<(String, String), (i64, i64)> {
    let mut out = BTreeMap::new();
    for (k, pair) in &meta.mtimes {
        if let Some((folder, name)) = split_meta_key(k) {
            out.insert((folder, name), (pair[0], pair[1]));
        }
    }
    out
}

fn file_metadata_from_info(info: FileInfo) -> FileMetadata {
    FileMetadata {
        name: info.path,
        sequence: info.sequence,
        mod_nanos: info.modified_ns,
        size: info.size,
        local_flags: info.local_flags,
        file_type: info.file_type,
        deleted: info.deleted,
    }
}

fn prefer_global(candidate: &FileInfo, current: &FileInfo) -> bool {
    if candidate.sequence != current.sequence {
        return candidate.sequence > current.sequence;
    }
    if candidate.deleted != current.deleted {
        return !candidate.deleted;
    }
    if candidate.modified_ns != current.modified_ns {
        return candidate.modified_ns > current.modified_ns;
    }
    candidate.path > current.path
}

fn sort_pull_order(files: &mut [FileInfo], order: PullOrder) {
    match order {
        PullOrder::Alphabetic => files.sort_by(|a, b| a.path.cmp(&b.path)),
        PullOrder::NewestFirst => files.sort_by(|a, b| {
            b.modified_ns
                .cmp(&a.modified_ns)
                .then_with(|| a.path.cmp(&b.path))
        }),
        PullOrder::OldestFirst => files.sort_by(|a, b| {
            a.modified_ns
                .cmp(&b.modified_ns)
                .then_with(|| a.path.cmp(&b.path))
        }),
        PullOrder::LargestFirst => {
            files.sort_by(|a, b| b.size.cmp(&a.size).then_with(|| a.path.cmp(&b.path)))
        }
        PullOrder::SmallestFirst => {
            files.sort_by(|a, b| a.size.cmp(&b.size).then_with(|| a.path.cmp(&b.path)))
        }
    }
}

fn count_files(iter: impl Iterator<Item = FileInfo>) -> Counts {
    let mut counts = Counts {
        files: 0,
        directories: 0,
        deleted: 0,
        bytes: 0,
        receive_only_changed: 0,
    };

    for file in iter {
        if file.deleted {
            counts.deleted += 1;
        }
        if file.file_type == FileInfoType::Directory {
            counts.directories += 1;
        } else {
            counts.files += 1;
        }
        counts.bytes += file.size;
        if file.local_flags & FLAG_LOCAL_RECEIVE_ONLY != 0 {
            counts.receive_only_changed += 1;
        }
    }

    counts
}

fn accumulate_counts(target: &mut Counts, delta: Counts) {
    target.files += delta.files;
    target.directories += delta.directories;
    target.deleted += delta.deleted;
    target.bytes += delta.bytes;
    target.receive_only_changed += delta.receive_only_changed;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn file(path: &str, seq: i64, size: i64) -> FileInfo {
        FileInfo {
            folder: "default".to_string(),
            path: path.to_string(),
            sequence: seq,
            modified_ns: seq,
            size,
            deleted: false,
            ignored: false,
            local_flags: 0,
            file_type: FileInfoType::File,
            block_hashes: vec!["h1".to_string(), "h2".to_string()],
        }
    }

    fn temp_root(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!(
            "syncthing-rs-db-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp root");
        path
    }

    #[test]
    fn update_and_global_queries() {
        let mut db = WalFreeDb::default();

        db.update(
            "default",
            "dev-a",
            vec![file("a.txt", 1, 100), file("b.txt", 1, 10)],
        )
        .expect("update dev-a");
        db.update(
            "default",
            "dev-b",
            vec![file("a.txt", 2, 101), file("c.txt", 1, 5)],
        )
        .expect("update dev-b");

        let global = db
            .get_global_file("default", "a.txt")
            .expect("global lookup")
            .expect("a exists");
        assert_eq!(global.sequence, 2);

        let avail = db
            .get_global_availability("default", "a.txt")
            .expect("availability");
        assert_eq!(avail, vec!["dev-a".to_string(), "dev-b".to_string()]);

        let global_files = db.all_global_files("default").expect("all global");
        assert_eq!(global_files.len(), 3);
    }

    #[test]
    fn needed_and_counts() {
        let mut db = WalFreeDb::default();
        db.update(
            "default",
            "dev-a",
            vec![file("a.txt", 1, 100), file("b.txt", 1, 50)],
        )
        .expect("update dev-a");
        db.update(
            "default",
            "dev-b",
            vec![file("a.txt", 2, 110), file("c.txt", 1, 25)],
        )
        .expect("update dev-b");

        let need = db
            .all_needed_global_files("default", "dev-a", PullOrder::Alphabetic, 10, 0)
            .expect("need");
        let need_paths = need.into_iter().map(|f| f.path).collect::<Vec<_>>();
        assert_eq!(need_paths, vec!["a.txt", "c.txt"]);

        let count_need = db.count_need("default", "dev-a").expect("need counts");
        assert_eq!(count_need.files, 2);
    }

    #[test]
    fn block_hash_kv_mtime_and_index_id() {
        let mut db = WalFreeDb::default();
        db.update("default", "dev-a", vec![file("a.txt", 1, 100)])
            .expect("update");

        let global = db
            .all_global_files("default")
            .expect("global files")
            .into_iter()
            .next()
            .expect("global file");
        assert_eq!(global.mod_time(), 1);
        assert!(!global.is_receive_only_changed());
        assert!(!global.is_directory());
        assert!(!global.should_conflict());
        assert!(!global.is_invalid());

        let blocks = db
            .all_local_blocks_with_hash("default", b"h1")
            .expect("block query");
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].file_name, "a.txt");

        db.set_index_id("default", "dev-a", 42)
            .expect("set index id");
        assert_eq!(
            db.get_index_id("default", "dev-a").expect("get index id"),
            42
        );

        db.put_mtime("default", "a.txt", 10, 11).expect("put mtime");
        assert_eq!(
            db.get_mtime("default", "a.txt").expect("get mtime"),
            (10, 11)
        );

        <WalFreeDb as Kv>::put_kv(&mut db, "alpha/key", b"value").expect("put kv");
        assert_eq!(
            <WalFreeDb as Kv>::get_kv(&db, "alpha/key").expect("get kv"),
            Some(b"value".to_vec())
        );
        let kv = <WalFreeDb as Kv>::prefix_kv(&db, "alpha/").expect("prefix kv");
        assert_eq!(kv.len(), 1);
        assert_eq!(kv[0].value, b"value");
        <WalFreeDb as Kv>::delete_kv(&mut db, "alpha/key").expect("delete kv");
    }

    #[test]
    fn local_file_pages_follow_segment_path_order() {
        let mut db = WalFreeDb::default();
        db.update(
            "default",
            "local",
            vec![
                file("a.d/x.txt", 1, 1),
                file("a/x.txt", 2, 1),
                file("a/z.txt", 3, 1),
            ],
        )
        .expect("update local");
        db.update(
            "default",
            "remote-a",
            vec![file("a/remote.txt", 4, 1)],
        )
        .expect("update remote");

        let mut cursor: Option<String> = None;
        let mut out = Vec::new();
        loop {
            let page = db
                .all_local_files_ordered_page("default", "local", cursor.as_deref(), 2)
                .expect("page");
            out.extend(page.items.into_iter().map(|f| f.path));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }

        assert_eq!(out, vec!["a/x.txt", "a/z.txt", "a.d/x.txt"]);
    }

    #[test]
    fn cleanup_operations() {
        let mut db = WalFreeDb::default();
        db.update(
            "f1",
            "dev-a",
            vec![file("a.txt", 1, 1), file("b.txt", 1, 1)],
        )
        .expect("update f1");
        db.update("f1", "dev-b", vec![file("a.txt", 2, 1)])
            .expect("update f1 dev-b");
        db.update("f2", "dev-a", vec![file("x.txt", 1, 1)])
            .expect("update f2");

        db.drop_files_named("f1", "dev-a", &[String::from("a.txt")])
            .expect("drop files named");
        assert!(db
            .get_device_file("f1", "dev-a", "a.txt")
            .expect("lookup")
            .is_none());

        db.drop_device("dev-b").expect("drop device");
        assert!(db
            .get_device_file("f1", "dev-b", "a.txt")
            .expect("lookup")
            .is_none());

        db.drop_folder("f2").expect("drop folder");
        assert!(db
            .list_folders()
            .expect("folders")
            .contains(&"f1".to_string()));
        assert!(!db
            .list_folders()
            .expect("folders")
            .contains(&"f2".to_string()));
    }

    #[test]
    fn close_blocks_future_mutation() {
        let mut db = WalFreeDb::default();
        db.close().expect("close");
        let err = <WalFreeDb as Kv>::put_kv(&mut db, "k", b"v").expect_err("must fail after close");
        assert!(err.contains("closed"));
    }

    #[test]
    fn runtime_metadata_persists_across_reopen() {
        let root = temp_root("runtime-meta");
        {
            let mut db = WalFreeDb::open_runtime(&root, 50).expect("open runtime");
            db.set_index_id("default", "local", 77)
                .expect("set index id");
            db.put_mtime("default", "a.txt", 11, 22)
                .expect("put mtime");
            <WalFreeDb as Kv>::put_kv(&mut db, "alpha/key", b"value").expect("put kv");
            db.close().expect("close");
        }
        {
            let db = WalFreeDb::open_runtime(&root, 50).expect("reopen runtime");
            assert_eq!(db.get_index_id("default", "local").expect("get index"), 77);
            assert_eq!(db.get_mtime("default", "a.txt").expect("get mtime"), (11, 22));
            assert_eq!(
                <WalFreeDb as Kv>::get_kv(&db, "alpha/key").expect("get kv"),
                Some(b"value".to_vec())
            );
        }
        let _ = fs::remove_dir_all(root);
    }
}
