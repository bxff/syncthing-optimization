#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolEvent {
    Dial,
    Hello,
    ClusterConfig,
    Index,
    IndexUpdate,
    Request,
    Response,
    DownloadProgress,
    Ping,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProtocolState {
    Start,
    Dialed,
    HelloDone,
    ClusterConfigured,
    IndexSent,
    Live,
    AwaitingResponse,
    Closed,
}

impl ProtocolEvent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Dial => "dial",
            Self::Hello => "hello",
            Self::ClusterConfig => "cluster_config",
            Self::Index => "index",
            Self::IndexUpdate => "index_update",
            Self::Request => "request",
            Self::Response => "response",
            Self::DownloadProgress => "download_progress",
            Self::Ping => "ping",
            Self::Close => "close",
        }
    }
}

pub(crate) fn run_events(events: &[ProtocolEvent]) -> Result<Vec<&'static str>, String> {
    let mut state = ProtocolState::Start;
    let mut trace = Vec::new();

    for event in events {
        state = advance(state, *event)?;
        trace.push(event.as_str());
    }

    Ok(trace)
}

fn advance(state: ProtocolState, event: ProtocolEvent) -> Result<ProtocolState, String> {
    use ProtocolEvent as E;
    use ProtocolState as S;

    match (state, event) {
        (S::Start, E::Dial) => Ok(S::Dialed),
        (S::Dialed, E::Hello) => Ok(S::HelloDone),
        (S::HelloDone, E::ClusterConfig) => Ok(S::ClusterConfigured),
        (S::ClusterConfigured, E::Index) => Ok(S::IndexSent),
        (S::IndexSent, E::IndexUpdate) => Ok(S::Live),
        (S::Live, E::Request) => Ok(S::AwaitingResponse),
        (S::AwaitingResponse, E::Response) => Ok(S::Live),
        (S::Live, E::DownloadProgress) => Ok(S::Live),
        (S::AwaitingResponse, E::DownloadProgress) => Ok(S::AwaitingResponse),
        (S::Live, E::Ping) => Ok(S::Live),
        (S::Live, E::IndexUpdate) => Ok(S::Live),
        (S::AwaitingResponse, E::Ping) => Ok(S::AwaitingResponse),
        (S::AwaitingResponse, E::IndexUpdate) => Ok(S::AwaitingResponse),
        (S::Live, E::Close) => Ok(S::Closed),
        (S::AwaitingResponse, E::Close) => Ok(S::Closed),
        _ => Err(format!(
            "invalid protocol transition: {state:?} + {event:?}"
        )),
    }
}

pub(crate) fn default_sequence() -> [ProtocolEvent; 10] {
    [
        ProtocolEvent::Dial,
        ProtocolEvent::Hello,
        ProtocolEvent::ClusterConfig,
        ProtocolEvent::Index,
        ProtocolEvent::IndexUpdate,
        ProtocolEvent::Request,
        ProtocolEvent::Response,
        ProtocolEvent::DownloadProgress,
        ProtocolEvent::Ping,
        ProtocolEvent::Close,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sequence_is_valid() {
        let trace = run_events(&default_sequence()).expect("valid sequence");
        assert_eq!(
            trace,
            vec![
                "dial",
                "hello",
                "cluster_config",
                "index",
                "index_update",
                "request",
                "response",
                "download_progress",
                "ping",
                "close",
            ]
        );
    }

    #[test]
    fn invalid_transition_fails() {
        let err = run_events(&[ProtocolEvent::Hello]).expect_err("must fail");
        assert!(err.contains("invalid protocol transition"));
    }

    #[test]
    fn response_without_request_fails() {
        let err = run_events(&[
            ProtocolEvent::Dial,
            ProtocolEvent::Hello,
            ProtocolEvent::ClusterConfig,
            ProtocolEvent::Index,
            ProtocolEvent::IndexUpdate,
            ProtocolEvent::Response,
        ])
        .expect_err("must fail");
        assert!(err.contains("invalid protocol transition"));
    }
}
