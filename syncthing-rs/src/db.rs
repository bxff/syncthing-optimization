use crate::store::{FileMetadata as StoreFileMetadata, Store, StoreConfig};
use std::collections::{BTreeMap, BTreeSet, HashSet};
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

#[derive(Clone, Debug, PartialEq, Eq)]
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
        Ok(Self {
            store,
            runtime_root: root,
            index_ids: BTreeMap::new(),
            mtimes: BTreeMap::new(),
            kv: BTreeMap::new(),
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

    fn all_records(&self) -> Vec<StoredRecord> {
        self.store
            .all_files_lexicographic()
            .into_iter()
            .map(store_to_record)
            .collect()
    }

    fn all_files(&self) -> Vec<FileInfo> {
        self.all_records().into_iter().map(|r| r.file).collect()
    }

    fn all_files_for_folder_device(&self, folder: &str, device: &str) -> Vec<FileInfo> {
        self.all_records()
            .into_iter()
            .filter(|r| r.file.folder == folder && r.device == device)
            .map(|r| r.file)
            .collect()
    }

    fn global_file_map(&self, folder: &str) -> BTreeMap<String, FileInfo> {
        let mut out: BTreeMap<String, FileInfo> = BTreeMap::new();
        for candidate in self
            .all_files()
            .into_iter()
            .filter(|f| f.folder == folder && !f.ignored)
        {
            let path = candidate.path.clone();
            if candidate.folder != folder {
                continue;
            }
            match out.get(&path) {
                Some(current) if !prefer_global(&candidate, current) => {}
                _ => {
                    out.insert(path, candidate);
                }
            }
        }
        out
    }

    fn global_availability_map(&self, folder: &str) -> BTreeMap<String, BTreeSet<DeviceId>> {
        let mut out: BTreeMap<String, BTreeSet<DeviceId>> = BTreeMap::new();
        for record in self
            .all_records()
            .into_iter()
            .filter(|r| r.file.folder == folder && !r.file.deleted)
        {
            if record.file.folder != folder || record.file.deleted {
                continue;
            }
            out.entry(record.file.path.clone())
                .or_default()
                .insert(record.device);
        }
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
        let mut out = self
            .all_files()
            .into_iter()
            .filter(|f| f.folder == folder && f.block_hashes.iter().any(|h| h == &target))
            .map(file_metadata_from_info)
            .collect::<Vec<_>>();
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

        for file in self.all_files().into_iter() {
            if file.folder != folder {
                continue;
            }
            for (idx, block_hash) in file.block_hashes.iter().enumerate() {
                if block_hash != &target {
                    continue;
                }
                out.push(BlockMapEntry {
                    blocklist_hash: target.as_bytes().to_vec(),
                    offset: idx as i64 * 128 * 1024,
                    block_index: idx as i32,
                    size: 128 * 1024,
                    file_name: file.path.clone(),
                });
            }
        }
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
        let doomed = self
            .all_records()
            .into_iter()
            .filter(|r| r.device == device)
            .map(|r| (r.file.folder, r.file.path))
            .collect::<Vec<_>>();
        for (folder, path) in doomed {
            self.store
                .delete_file(&folder, device, &path)
                .map_err(|e| format!("drop device: {e}"))?;
        }
        self.index_ids.retain(|(_, d), _| d != device);
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
        let doomed = self
            .all_records()
            .into_iter()
            .filter(|r| r.file.folder == folder)
            .map(|r| (r.device, r.file.path))
            .collect::<Vec<_>>();
        for (device, path) in doomed {
            self.store
                .delete_file(folder, &device, &path)
                .map_err(|e| format!("drop folder: {e}"))?;
        }
        self.index_ids.retain(|(f, _), _| f != folder);
        self.mtimes.retain(|(f, _), _| f != folder);
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
        for file in self.all_files() {
            folders.insert(file.folder);
        }
        Ok(folders.into_iter().collect())
    }

    fn list_devices_for_folder(&self, folder: &str) -> Result<Vec<DeviceId>, String> {
        self.ensure_open()?;
        let mut devices = BTreeSet::new();
        for record in self
            .all_records()
            .into_iter()
            .filter(|r| r.file.folder == folder)
        {
            devices.insert(record.device);
        }
        Ok(devices.into_iter().collect())
    }

    fn remote_sequences(&self, folder: &str) -> Result<BTreeMap<DeviceId, i64>, String> {
        self.ensure_open()?;
        let mut out = BTreeMap::new();
        for record in self
            .all_records()
            .into_iter()
            .filter(|r| r.file.folder == folder)
        {
            let entry = out.entry(record.device).or_insert(0);
            *entry = (*entry).max(record.file.sequence);
        }
        Ok(out)
    }

    fn count_global(&self, folder: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        Ok(count_files(self.global_file_map(folder).into_values()))
    }

    fn count_local(&self, folder: &str, device: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        Ok(count_files(
            self.all_local_files(folder, device)?.into_iter(),
        ))
    }

    fn count_need(&self, folder: &str, device: &str) -> Result<Counts, String> {
        self.ensure_open()?;
        Ok(count_files(
            self.all_needed_global_files(folder, device, PullOrder::Alphabetic, 0, 0)?
                .into_iter(),
        ))
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
        Ok(())
    }

    fn delete_mtime(&mut self, folder: &str, name: &str) -> Result<(), String> {
        self.ensure_open()?;
        self.mtimes.remove(&(folder.to_string(), name.to_string()));
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
        Ok(())
    }

    fn get_kv(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.ensure_open()?;
        Ok(self.kv.get(key).cloned())
    }

    fn put_kv(&mut self, key: &str, val: &[u8]) -> Result<(), String> {
        self.ensure_open()?;
        self.kv.insert(key.to_string(), val.to_vec());
        Ok(())
    }

    fn delete_kv(&mut self, key: &str) -> Result<(), String> {
        self.ensure_open()?;
        self.kv.remove(key);
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

#[derive(Clone, Debug)]
struct StoredRecord {
    device: String,
    file: FileInfo,
}

fn store_to_record(meta: StoreFileMetadata) -> StoredRecord {
    let device = meta.device.clone();
    let file = store_to_file_info(&meta);
    StoredRecord { device, file }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
