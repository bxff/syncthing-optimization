use crate::store::compare_path_order;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub(crate) struct WalkConfig {
    pub(crate) spill_threshold_entries: usize,
    pub(crate) spill_dir: PathBuf,
}

impl WalkConfig {
    pub(crate) fn new(spill_dir: impl Into<PathBuf>) -> Self {
        Self {
            spill_threshold_entries: 10_000,
            spill_dir: spill_dir.into(),
        }
    }

    pub(crate) fn with_spill_threshold_entries(mut self, threshold: usize) -> Self {
        self.spill_threshold_entries = threshold.max(1);
        self
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WalkStats {
    pub(crate) directories_seen: usize,
    pub(crate) files_emitted: usize,
    pub(crate) spill_files_created: usize,
    pub(crate) spill_bytes_written: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SpillStats {
    files_created: usize,
    bytes_written: u64,
}

pub(crate) fn walk_deterministic(
    root: &Path,
    cfg: &WalkConfig,
    mut emit_file: impl FnMut(String),
) -> io::Result<WalkStats> {
    let mut stack = vec![WorkItem::Dir(PathBuf::new())];
    let mut stats = WalkStats::default();
    fs::create_dir_all(&cfg.spill_dir)?;

    while let Some(item) = stack.pop() {
        match item {
            WorkItem::File(path) => {
                stats.files_emitted += 1;
                emit_file(path);
            }
            WorkItem::Dir(rel_dir) => {
                stats.directories_seen += 1;
                let abs_dir = if rel_dir.as_os_str().is_empty() {
                    root.to_path_buf()
                } else {
                    root.join(&rel_dir)
                };

                let (children, spill_stats) = sorted_child_names(&abs_dir, cfg)?;
                stats.spill_files_created += spill_stats.files_created;
                stats.spill_bytes_written += spill_stats.bytes_written;

                let mut push_items = Vec::with_capacity(children.len());
                for name in children {
                    let rel_path = if rel_dir.as_os_str().is_empty() {
                        PathBuf::from(&name)
                    } else {
                        rel_dir.join(&name)
                    };
                    let abs_path = root.join(&rel_path);
                    let meta = match fs::symlink_metadata(&abs_path) {
                        Ok(meta) => meta,
                        Err(err) => {
                            return Err(io::Error::new(
                                err.kind(),
                                format!("stat {}: {err}", abs_path.display()),
                            ))
                        }
                    };
                    if meta.is_dir() {
                        push_items.push(WorkItem::Dir(rel_path));
                    } else if meta.is_file() || (meta.file_type().is_symlink() && !cfg!(windows)) {
                        push_items.push(WorkItem::File(path_to_slash_string(&rel_path)));
                    }
                }

                // LIFO stack: push reverse so traversal order is ascending.
                for push_item in push_items.into_iter().rev() {
                    stack.push(push_item);
                }
            }
        }
    }

    Ok(stats)
}

fn sorted_child_names(dir: &Path, cfg: &WalkConfig) -> io::Result<(Vec<String>, SpillStats)> {
    let threshold = cfg.spill_threshold_entries.max(1);
    let mut chunk = Vec::with_capacity(threshold);
    let mut spill_paths = Vec::new();
    let mut spill_stats = SpillStats::default();

    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                return Err(io::Error::new(
                    err.kind(),
                    format!("read dir {} entry: {err}", dir.display()),
                ))
            }
        };
        let name = match entry.file_name().into_string() {
            Ok(name) => name,
            Err(raw) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("non-utf8 filename in {}: {:?}", dir.display(), raw),
                ))
            }
        };
        chunk.push(name);
        if chunk.len() >= threshold {
            let spill_path = spill_chunk(&mut chunk, cfg, spill_paths.len())?;
            spill_stats.files_created += 1;
            spill_stats.bytes_written += fs::metadata(&spill_path)?.len();
            spill_paths.push(spill_path);
        }
    }

    if spill_paths.is_empty() {
        chunk.sort_by(|a, b| compare_path_order(a, b));
        return Ok((chunk, spill_stats));
    }

    if !chunk.is_empty() {
        let spill_path = spill_chunk(&mut chunk, cfg, spill_paths.len())?;
        spill_stats.files_created += 1;
        spill_stats.bytes_written += fs::metadata(&spill_path)?.len();
        spill_paths.push(spill_path);
    }

    let merged = merge_spills(&spill_paths)?;
    for path in spill_paths {
        let _ = fs::remove_file(path);
    }
    Ok((merged, spill_stats))
}

fn spill_chunk(chunk: &mut Vec<String>, cfg: &WalkConfig, index: usize) -> io::Result<PathBuf> {
    chunk.sort_by(|a, b| compare_path_order(a, b));
    let path = cfg.spill_dir.join(format!("walk-spill-{index:06}.bin"));
    let file = File::create(&path)?;
    let mut writer = BufWriter::new(file);
    for name in chunk.iter() {
        writer.write_all(name.as_bytes())?;
        writer.write_all(&[0_u8])?;
    }
    writer.flush()?;
    chunk.clear();
    Ok(path)
}

fn merge_spills(spill_paths: &[PathBuf]) -> io::Result<Vec<String>> {
    let mut readers = Vec::with_capacity(spill_paths.len());
    for path in spill_paths {
        readers.push(BufReader::new(File::open(path)?));
    }

    let mut heap = BinaryHeap::new();
    for (idx, reader) in readers.iter_mut().enumerate() {
        if let Some(name) = read_spill_name(reader)? {
            heap.push(HeapItem {
                name,
                source_idx: idx,
            });
        }
    }

    let mut out = Vec::new();
    while let Some(item) = heap.pop() {
        out.push(item.name.clone());
        if let Some(next_name) = read_spill_name(&mut readers[item.source_idx])? {
            heap.push(HeapItem {
                name: next_name,
                source_idx: item.source_idx,
            });
        }
    }
    Ok(out)
}

fn read_spill_name(reader: &mut BufReader<File>) -> io::Result<Option<String>> {
    let mut buf = Vec::new();
    let read = reader.read_until(0_u8, &mut buf)?;
    if read == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&0_u8) {
        buf.pop();
    }
    let name = String::from_utf8(buf)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    Ok(Some(name))
}

fn path_to_slash_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HeapItem {
    name: String,
    source_idx: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse comparison for min-heap behavior on BinaryHeap.
        compare_path_order(&other.name, &self.name)
            .then_with(|| other.source_idx.cmp(&self.source_idx))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum WorkItem {
    Dir(PathBuf),
    File(String),
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
            "syncthing-rs-walker-{name}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp root");
        path
    }

    #[test]
    fn deterministic_walk_uses_segment_order() {
        let root = temp_root("segment-order");
        fs::create_dir_all(root.join("a")).expect("mkdir a");
        fs::create_dir_all(root.join("a.d")).expect("mkdir a.d");
        fs::write(root.join("a").join("x.txt"), b"x").expect("write");
        fs::write(root.join("a").join("z.txt"), b"z").expect("write");
        fs::write(root.join("a.d").join("x.txt"), b"x").expect("write");
        fs::write(root.join("b.txt"), b"b").expect("write");

        let spill = temp_root("spill-segment-order");
        let cfg = WalkConfig::new(&spill).with_spill_threshold_entries(2);
        let mut emitted = Vec::new();
        let stats = walk_deterministic(&root, &cfg, |path| emitted.push(path)).expect("walk");

        assert_eq!(emitted, vec!["a/x.txt", "a/z.txt", "a.d/x.txt", "b.txt"]);
        assert!(stats.directories_seen >= 3);
        assert_eq!(stats.files_emitted, 4);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(spill);
    }

    #[cfg(unix)]
    #[test]
    fn deterministic_walk_emits_symlink_without_descending() {
        use std::os::unix::fs::symlink;

        let root = temp_root("symlink-walk");
        fs::create_dir_all(root.join("dir")).expect("mkdir dir");
        fs::write(root.join("dir").join("a.txt"), b"a").expect("write");
        symlink(root.join("dir"), root.join("link_to_dir")).expect("symlink");

        let spill = temp_root("symlink-walk-spill");
        let cfg = WalkConfig::new(&spill).with_spill_threshold_entries(2);
        let mut emitted = Vec::new();
        walk_deterministic(&root, &cfg, |path| emitted.push(path)).expect("walk");

        assert!(emitted.contains(&"dir/a.txt".to_string()));
        assert!(emitted.contains(&"link_to_dir".to_string()));
        assert!(!emitted.contains(&"link_to_dir/a.txt".to_string()));

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(spill);
    }

    #[test]
    fn spill_sorting_preserves_order_for_large_directory() {
        let root = temp_root("spill");
        fs::create_dir_all(&root).expect("mkdir");
        for name in ["z.bin", "a.bin", "m.bin", "b.bin", "aa.bin", "a.d.bin"] {
            fs::write(root.join(name), b"v").expect("write");
        }

        let spill = temp_root("spill-dir");
        let cfg = WalkConfig::new(&spill).with_spill_threshold_entries(2);
        let mut emitted = Vec::new();
        let stats = walk_deterministic(&root, &cfg, |path| emitted.push(path)).expect("walk");

        assert_eq!(
            emitted,
            vec!["a.bin", "a.d.bin", "aa.bin", "b.bin", "m.bin", "z.bin"]
        );
        assert!(stats.spill_files_created > 0);

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(spill);
    }
}
