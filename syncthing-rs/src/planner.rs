use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionedFile {
    pub(crate) path: String,
    pub(crate) sequence: u64,
    pub(crate) deleted: bool,
    pub(crate) ignored: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NeedPlan {
    pub(crate) need_paths: Vec<String>,
    pub(crate) stale_deletes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PathClassification {
    pub(crate) ignored: Vec<String>,
    pub(crate) considered: Vec<String>,
    pub(crate) conflicts: Vec<String>,
}

pub(crate) fn compute_need(local: &[VersionedFile], remote: &[VersionedFile]) -> NeedPlan {
    let local_by_path: BTreeMap<&str, &VersionedFile> =
        local.iter().map(|f| (f.path.as_str(), f)).collect();

    let mut need_paths = Vec::new();
    let mut stale_deletes = Vec::new();

    for r in remote {
        if r.ignored {
            continue;
        }

        let local_entry = local_by_path.get(r.path.as_str()).copied();
        let local_sequence = local_entry.map(|f| f.sequence).unwrap_or(0);

        if r.deleted {
            if let Some(local_file) = local_entry {
                if !local_file.deleted {
                    if r.sequence >= local_file.sequence {
                        stale_deletes.push(r.path.clone());
                    }
                }
            }
            continue;
        }

        if r.sequence > local_sequence {
            need_paths.push(r.path.clone());
        }
    }

    need_paths.sort();
    stale_deletes.sort();

    NeedPlan {
        need_paths,
        stale_deletes,
    }
}

pub(crate) fn classify_paths(paths: &[String], ignore_suffixes: &[&str]) -> PathClassification {
    let mut ignored = Vec::new();
    let mut considered = Vec::new();
    let mut conflicts = Vec::new();

    for path in paths {
        let is_ignored = ignore_suffixes.iter().any(|suffix| path.ends_with(suffix));
        if is_ignored {
            ignored.push(path.clone());
            continue;
        }

        if is_conflict_path(path) {
            conflicts.push(path.clone());
        }
        considered.push(path.clone());
    }

    ignored.sort();
    considered.sort();
    conflicts.sort();

    PathClassification {
        ignored,
        considered,
        conflicts,
    }
}

fn is_conflict_path(path: &str) -> bool {
    let base = path.rsplit('/').next().unwrap_or(path);
    base.contains(".sync-conflict-")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(path: &str, sequence: u64, deleted: bool, ignored: bool) -> VersionedFile {
        VersionedFile {
            path: path.to_string(),
            sequence,
            deleted,
            ignored,
        }
    }

    #[test]
    fn computes_need_and_stale_deletes() {
        let local = vec![
            v("a.txt", 2, false, false),
            v("b.txt", 5, false, false),
            v("e.txt", 10, false, false),
        ];

        let remote = vec![
            v("a.txt", 3, false, false),
            v("b.txt", 5, false, false),
            v("d.txt", 1, false, false),
            v("e.txt", 11, true, false),
        ];

        let plan = compute_need(&local, &remote);
        assert_eq!(plan.need_paths, vec!["a.txt", "d.txt"]);
        assert_eq!(plan.stale_deletes, vec!["e.txt"]);
    }

    #[test]
    fn classifies_ignored_and_conflicts() {
        let paths = vec![
            "docs/readme.md".to_string(),
            "docs/.DS_Store".to_string(),
            "tmp/build.tmp".to_string(),
            "docs/readme.sync-conflict-20260207.md".to_string(),
            "sync-conflict-dir/readme.md".to_string(),
            "docs/sync-conflict-not-a-marker.md".to_string(),
        ];

        let c = classify_paths(&paths, &[".tmp", ".DS_Store"]);
        assert_eq!(c.ignored, vec!["docs/.DS_Store", "tmp/build.tmp"]);
        assert_eq!(c.conflicts, vec!["docs/readme.sync-conflict-20260207.md"]);
        assert_eq!(
            c.considered,
            vec![
                "docs/readme.md",
                "docs/readme.sync-conflict-20260207.md",
                "docs/sync-conflict-not-a-marker.md",
                "sync-conflict-dir/readme.md"
            ]
        );
    }
}
