use crate::bep::BepMessage;

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
    let mut pending_request: Option<u32> = None;

    for message in messages {
        let event = event_from_message(message);
        match message {
            BepMessage::Request { id, .. } => {
                if let Some(existing) = pending_request {
                    return Err(format!(
                        "request {id} received while request {existing} is still pending"
                    ));
                }
                pending_request = Some(*id);
            }
            BepMessage::Response { id, .. } => {
                let expected = pending_request
                    .take()
                    .ok_or_else(|| format!("response {id} received without pending request"))?;
                if *id != expected {
                    return Err(format!(
                        "response id mismatch: expected={expected} received={id}"
                    ));
                }
            }
            _ => {}
        }
        state = advance(state, event)?;
        trace.push(event.as_str());
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
    fn response_id_mismatch_fails() {
        let mut messages = default_exchange();
        for msg in &mut messages {
            if let BepMessage::Response { id, .. } = msg {
                *id = 999;
            }
        }
        let err = run_message_exchange(&messages).expect_err("must fail");
        assert!(err.contains("response id mismatch"));
    }

    #[test]
    fn response_without_pending_request_fails_with_specific_error() {
        let mut messages = default_exchange();
        messages.retain(|message| !matches!(message, BepMessage::Request { .. }));
        let err = run_message_exchange(&messages).expect_err("must fail");
        assert!(err.contains("received without pending request"));
    }

    #[test]
    fn second_request_while_pending_fails() {
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
                hash: "h3".to_string(),
            },
        );
        let err = run_message_exchange(&messages).expect_err("must fail");
        assert!(err.contains("is still pending"));
    }
}
