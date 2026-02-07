use crate::planner::VersionedFile;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FolderUpdate {
    pub(crate) path: String,
    pub(crate) sequence: u64,
    pub(crate) deleted: bool,
    pub(crate) ignored: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexedFile {
    pub(crate) folder: String,
    pub(crate) file: VersionedFile,
}

#[derive(Default, Debug)]
pub(crate) struct IndexEngine {
    folder_sequence: BTreeMap<String, u64>,
    files: BTreeMap<(String, String), VersionedFile>,
}

impl IndexEngine {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn apply_update(
        &mut self,
        folder: &str,
        update: FolderUpdate,
    ) -> Result<(), String> {
        if update.sequence == 0 {
            return Err("sequence must be greater than zero".to_string());
        }

        let next = self.folder_sequence.entry(folder.to_string()).or_insert(0);
        if update.sequence <= *next {
            return Err(format!(
                "stale sequence {} for folder {} (current {})",
                update.sequence, folder, *next
            ));
        }

        *next = update.sequence;
        self.files.insert(
            (folder.to_string(), update.path.clone()),
            VersionedFile {
                path: update.path,
                sequence: update.sequence,
                deleted: update.deleted,
                ignored: update.ignored,
            },
        );
        Ok(())
    }

    pub(crate) fn folder_sequence(&self, folder: &str) -> u64 {
        self.folder_sequence.get(folder).copied().unwrap_or(0)
    }

    pub(crate) fn ordered_files(&self, folder: Option<&str>) -> Vec<IndexedFile> {
        let mut out = Vec::new();
        match folder {
            Some(folder_id) => {
                for ((f, _), file) in &self.files {
                    if f == folder_id {
                        out.push(IndexedFile {
                            folder: f.clone(),
                            file: file.clone(),
                        });
                    }
                }
            }
            None => {
                for ((folder_id, _), file) in &self.files {
                    out.push(IndexedFile {
                        folder: folder_id.clone(),
                        file: file.clone(),
                    });
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update(path: &str, sequence: u64, deleted: bool) -> FolderUpdate {
        FolderUpdate {
            path: path.to_string(),
            sequence,
            deleted,
            ignored: false,
        }
    }

    #[test]
    fn enforces_monotonic_folder_sequence() {
        let mut idx = IndexEngine::new();
        idx.apply_update("default", update("a.txt", 1, false))
            .expect("apply 1");
        idx.apply_update("default", update("a.txt", 2, false))
            .expect("apply 2");

        let err = idx
            .apply_update("default", update("a.txt", 2, false))
            .expect_err("must reject stale update");
        assert!(err.contains("stale sequence"));
        assert_eq!(idx.folder_sequence("default"), 2);
    }

    #[test]
    fn keeps_lexicographic_order_across_folders() {
        let mut idx = IndexEngine::new();
        idx.apply_update("alpha", update("b.txt", 1, false))
            .expect("alpha b");
        idx.apply_update("alpha", update("a.txt", 2, false))
            .expect("alpha a");
        idx.apply_update("beta", update("a.txt", 1, true))
            .expect("beta a");

        let files = idx.ordered_files(None);
        let tuples = files
            .into_iter()
            .map(|f| format!("{}/{}:{}", f.folder, f.file.path, f.file.sequence))
            .collect::<Vec<_>>();

        assert_eq!(
            tuples,
            vec!["alpha/a.txt:2", "alpha/b.txt:1", "beta/a.txt:1"]
        );
    }
}
