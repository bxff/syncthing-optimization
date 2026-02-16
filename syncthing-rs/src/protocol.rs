use crate::bep::BepMessage;
use std::collections::BTreeSet;

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
    Ready,
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
        (S::Dialed, E::ClusterConfig) => Ok(S::Ready),
        (S::Ready, E::ClusterConfig) => Ok(S::Ready),
        (S::Ready, E::Index) => Ok(S::Ready),
        (S::Ready, E::IndexUpdate) => Ok(S::Ready),
        (S::Ready, E::Request) => Ok(S::Ready),
        (S::Ready, E::Response) => Ok(S::Ready),
        (S::Ready, E::DownloadProgress) => Ok(S::Ready),
        (S::Ready, E::Ping) => Ok(S::Ready),
        (S::Start, E::Close) | (S::Dialed, E::Close) | (S::Ready, E::Close) => Ok(S::Closed),
        _ => Err(format!(
            "invalid protocol transition: {state:?} + {event:?}"
        )),
    }
}

pub(crate) fn default_sequence() -> [ProtocolEvent; 9] {
    [
        ProtocolEvent::Dial,
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

pub(crate) fn event_from_message(message: &BepMessage) -> ProtocolEvent {
    match message {
        BepMessage::Hello { .. } => ProtocolEvent::Hello,
        BepMessage::ClusterConfig { .. } => ProtocolEvent::ClusterConfig,
        BepMessage::Index { .. } => ProtocolEvent::Index,
        BepMessage::IndexUpdate { .. } => ProtocolEvent::IndexUpdate,
        BepMessage::Request { .. } => ProtocolEvent::Request,
        BepMessage::Response { .. } => ProtocolEvent::Response,
        BepMessage::DownloadProgress { .. } => ProtocolEvent::DownloadProgress,
        BepMessage::Ping { .. } => ProtocolEvent::Ping,
        BepMessage::Close { .. } => ProtocolEvent::Close,
    }
}

pub(crate) fn run_message_exchange(messages: &[BepMessage]) -> Result<Vec<&'static str>, String> {
    let mut state = ProtocolState::Dialed;
    let mut trace = vec![ProtocolEvent::Dial.as_str()];
    let mut pending_requests: BTreeSet<i32> = BTreeSet::new();

    for message in messages {
        if state == ProtocolState::Closed {
            return Err("message received after close".to_string());
        }
        if matches!(message, BepMessage::Hello { .. }) {
            trace.push(ProtocolEvent::Hello.as_str());
            continue;
        }
        match message {
            BepMessage::Request { id, .. } => {
                // 11.4: Track outbound request IDs for correlation.
                pending_requests.insert(*id);
            }
            BepMessage::Response { id, .. } => {
                // Go only correlates responses that match an in-flight request ID.
                // Orphan/mismatched responses do not satisfy any request.
                let _ = pending_requests.remove(id);
            }
            _ => {}
        }
        let event = event_from_message(message);
        state = advance(state, event)?;
        trace.push(event.as_str());
    }

    if state == ProtocolState::Closed && !pending_requests.is_empty() {
        // 11.4: Go tolerates leaked pending requests — log but don't error.
        eprintln!("WARN: connection closed with pending requests: {pending_requests:?}");
    }

    Ok(trace)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bep::default_exchange;

    #[test]
    fn default_sequence_is_valid() {
        let trace = run_events(&default_sequence()).expect("valid sequence");
        assert_eq!(
            trace,
            vec![
                "dial",
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
    fn cluster_config_after_dial_is_allowed() {
        let trace =
            run_events(&[ProtocolEvent::Dial, ProtocolEvent::ClusterConfig]).expect("must succeed");
        assert_eq!(trace, vec!["dial", "cluster_config"]);
    }

    #[test]
    fn default_exchange_is_valid() {
        let trace = run_message_exchange(&default_exchange()).expect("valid exchange");
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
    fn response_id_mismatch_is_tolerated_without_local_pending() {
        let mut messages = default_exchange();
        for msg in &mut messages {
            if let BepMessage::Response { id, .. } = msg {
                *id = 999;
            }
        }
        let trace = run_message_exchange(&messages).expect("must succeed");
        assert!(trace.contains(&"response"));
    }

    #[test]
    fn response_without_pending_request_is_tolerated() {
        let mut messages = default_exchange();
        messages.retain(|message| !matches!(message, BepMessage::Request { .. }));
        let trace = run_message_exchange(&messages).expect("orphan response is tolerated");
        assert!(trace.contains(&"response"));
    }

    #[test]
    fn unresolved_request_is_tolerated_without_connection_close_signal() {
        let mut messages = default_exchange();
        messages.retain(|message| !matches!(message, BepMessage::Response { .. }));
        let trace = run_message_exchange(&messages).expect("must succeed");
        assert!(trace.contains(&"request"));
    }

    #[test]
    fn second_request_while_pending_is_tolerated_without_local_tracking() {
        let mut messages = default_exchange();
        let response_idx = messages
            .iter()
            .position(|message| matches!(message, BepMessage::Response { .. }))
            .expect("default exchange includes a response");
        messages.insert(
            response_idx,
            BepMessage::Request {
                id: 2,
                folder: "default".to_string(),
                name: "b.txt".to_string(),
                offset: 0,
                size: 1,
                hash: Vec::new(),
                from_temporary: false,
                block_no: 0,
            },
        );
        let trace = run_message_exchange(&messages).expect("must succeed");
        assert!(trace.iter().filter(|event| **event == "request").count() >= 2);
    }

    #[test]
    fn close_is_allowed_before_ready_state() {
        let trace =
            run_events(&[ProtocolEvent::Dial, ProtocolEvent::Close]).expect("close allowed");
        assert_eq!(trace, vec!["dial", "close"]);
    }
}
