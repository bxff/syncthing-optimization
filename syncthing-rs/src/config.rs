use crate::folder_modes::FolderMode;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FolderType {
    SendReceive,
    ReceiveOnly,
    ReceiveEncrypted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FolderConfig {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) path: String,
    pub(crate) folder_type: FolderType,
    pub(crate) rescan_interval_s: u32,
    pub(crate) fs_watcher_enabled: bool,
    pub(crate) ignore_permissions: bool,
    pub(crate) max_conflicts: i32,
    pub(crate) disable_temp_indexes: bool,
    pub(crate) paused: bool,
}

impl FolderType {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::SendReceive => "sendrecv",
            Self::ReceiveOnly => "recvonly",
            Self::ReceiveEncrypted => "recvenc",
        }
    }

    pub(crate) fn to_mode(&self) -> FolderMode {
        match self {
            Self::SendReceive => FolderMode::SendReceive,
            Self::ReceiveOnly => FolderMode::ReceiveOnly,
            Self::ReceiveEncrypted => FolderMode::ReceiveEncrypted,
        }
    }
}

impl FolderConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.id.trim().is_empty() {
            return Err("folder id must not be empty".to_string());
        }
        if self.path.trim().is_empty() {
            return Err(format!("folder {} path must not be empty", self.id));
        }
        if self.rescan_interval_s == 0 && !self.fs_watcher_enabled {
            return Err(format!(
                "folder {} must have either fs watcher enabled or non-zero rescan interval",
                self.id
            ));
        }
        if self.max_conflicts < -1 {
            return Err(format!(
                "folder {} max_conflicts must be >= -1 (got {})",
                self.id, self.max_conflicts
            ));
        }

        if matches!(self.folder_type, FolderType::ReceiveEncrypted) && self.ignore_permissions {
            return Err(format!(
                "folder {} receive-encrypted mode does not allow ignore_permissions=true",
                self.id
            ));
        }

        Ok(())
    }
}

pub(crate) fn demo_configs() -> Vec<FolderConfig> {
    vec![
        FolderConfig {
            id: "default".to_string(),
            label: "Default".to_string(),
            path: "/data/default".to_string(),
            folder_type: FolderType::SendReceive,
            rescan_interval_s: 3600,
            fs_watcher_enabled: true,
            ignore_permissions: false,
            max_conflicts: 10,
            disable_temp_indexes: false,
            paused: false,
        },
        FolderConfig {
            id: "readonly".to_string(),
            label: "ReadOnly".to_string(),
            path: "/data/readonly".to_string(),
            folder_type: FolderType::ReceiveOnly,
            rescan_interval_s: 3600,
            fs_watcher_enabled: true,
            ignore_permissions: false,
            max_conflicts: 10,
            disable_temp_indexes: false,
            paused: false,
        },
        FolderConfig {
            id: "encrypted".to_string(),
            label: "Encrypted".to_string(),
            path: "/data/encrypted".to_string(),
            folder_type: FolderType::ReceiveEncrypted,
            rescan_interval_s: 3600,
            fs_watcher_enabled: true,
            ignore_permissions: false,
            max_conflicts: 10,
            disable_temp_indexes: false,
            paused: false,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_good_config() {
        let cfg = demo_configs().into_iter().next().expect("default config");
        cfg.validate().expect("must validate");
    }

    #[test]
    fn rejects_zero_rescan_without_watcher() {
        let mut cfg = demo_configs().into_iter().next().expect("default config");
        cfg.rescan_interval_s = 0;
        cfg.fs_watcher_enabled = false;
        let err = cfg.validate().expect_err("must fail");
        assert!(err.contains("non-zero rescan interval"));
    }

    #[test]
    fn rejects_encrypted_ignore_permissions() {
        let mut cfg = demo_configs().into_iter().nth(2).expect("encrypted config");
        cfg.ignore_permissions = true;
        let err = cfg.validate().expect_err("must fail");
        assert!(err.contains("receive-encrypted"));
    }
}
