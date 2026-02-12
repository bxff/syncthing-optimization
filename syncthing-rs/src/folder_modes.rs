#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FolderMode {
    SendReceive,
    SendOnly,
    ReceiveOnly,
    ReceiveEncrypted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FolderModeActions {
    pub(crate) mode: FolderMode,
    pub(crate) pipeline: Vec<&'static str>,
    pub(crate) may_push: bool,
    pub(crate) requires_local_revert: bool,
    pub(crate) encrypted_index: bool,
}

impl FolderMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::SendReceive => "sendreceive",
            Self::SendOnly => "sendonly",
            Self::ReceiveOnly => "receiveonly",
            Self::ReceiveEncrypted => "receiveencrypted",
        }
    }
}

pub(crate) fn mode_actions(mode: FolderMode) -> FolderModeActions {
    match mode {
        FolderMode::SendReceive => FolderModeActions {
            mode,
            pipeline: vec!["scan", "index", "pull", "push"],
            may_push: true,
            requires_local_revert: false,
            encrypted_index: false,
        },
        FolderMode::SendOnly => FolderModeActions {
            mode,
            pipeline: vec!["scan", "index", "pull_metadata", "push"],
            may_push: true,
            requires_local_revert: false,
            encrypted_index: false,
        },
        FolderMode::ReceiveOnly => FolderModeActions {
            mode,
            pipeline: vec!["scan", "index", "pull", "local_revert_required"],
            may_push: false,
            requires_local_revert: true,
            encrypted_index: false,
        },
        FolderMode::ReceiveEncrypted => FolderModeActions {
            mode,
            pipeline: vec![
                "scan",
                "index_encrypted",
                "pull_encrypted",
                "local_revert_required",
            ],
            may_push: false,
            requires_local_revert: true,
            encrypted_index: true,
        },
    }
}

pub(crate) fn all_mode_actions() -> Vec<FolderModeActions> {
    let mut out = vec![
        mode_actions(FolderMode::SendReceive),
        mode_actions(FolderMode::SendOnly),
        mode_actions(FolderMode::ReceiveOnly),
        mode_actions(FolderMode::ReceiveEncrypted),
    ];
    out.sort_by_key(|a| a.mode);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sendrecv_allows_push() {
        let actions = mode_actions(FolderMode::SendReceive);
        assert!(actions.may_push);
        assert!(!actions.requires_local_revert);
        assert_eq!(actions.pipeline, vec!["scan", "index", "pull", "push"]);
    }

    #[test]
    fn receive_encrypted_uses_encrypted_index() {
        let actions = mode_actions(FolderMode::ReceiveEncrypted);
        assert!(actions.encrypted_index);
        assert_eq!(
            actions.pipeline,
            vec![
                "scan",
                "index_encrypted",
                "pull_encrypted",
                "local_revert_required"
            ]
        );
    }

    #[test]
    fn send_only_pipeline_matches_go_contract() {
        let actions = mode_actions(FolderMode::SendOnly);
        assert!(actions.may_push);
        assert!(!actions.requires_local_revert);
        assert_eq!(
            actions.pipeline,
            vec!["scan", "index", "pull_metadata", "push"]
        );
    }

    #[test]
    fn canonical_mode_names_match_go_tokens() {
        assert_eq!(FolderMode::SendReceive.as_str(), "sendreceive");
        assert_eq!(FolderMode::SendOnly.as_str(), "sendonly");
        assert_eq!(FolderMode::ReceiveOnly.as_str(), "receiveonly");
        assert_eq!(FolderMode::ReceiveEncrypted.as_str(), "receiveencrypted");
    }

    #[test]
    fn receive_encrypted_requires_local_revert() {
        let actions = mode_actions(FolderMode::ReceiveEncrypted);
        assert!(actions.requires_local_revert);
    }
}
