#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProtocolEvent {
    Dial,
    Hello,
    ClusterConfig,
    Index,
    IndexUpdate,
    Ping,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProtocolState {
    Start,
    Dialed,
    HelloDone,
    ClusterConfigured,
    IndexSent,
    Live,
}

impl ProtocolEvent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Dial => "dial",
            Self::Hello => "hello",
            Self::ClusterConfig => "cluster_config",
            Self::Index => "index",
            Self::IndexUpdate => "index_update",
            Self::Ping => "ping",
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
        (S::Live, E::Ping) => Ok(S::Live),
        (S::Live, E::IndexUpdate) => Ok(S::Live),
        _ => Err(format!(
            "invalid protocol transition: {state:?} + {event:?}"
        )),
    }
}

pub(crate) fn default_sequence() -> [ProtocolEvent; 6] {
    [
        ProtocolEvent::Dial,
        ProtocolEvent::Hello,
        ProtocolEvent::ClusterConfig,
        ProtocolEvent::Index,
        ProtocolEvent::IndexUpdate,
        ProtocolEvent::Ping,
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
                "ping"
            ]
        );
    }

    #[test]
    fn invalid_transition_fails() {
        let err = run_events(&[ProtocolEvent::Hello]).expect_err("must fail");
        assert!(err.contains("invalid protocol transition"));
    }
}
