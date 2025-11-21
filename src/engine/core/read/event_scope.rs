use crate::engine::schema::registry::SchemaRegistry;

/// Captures which event types a query spans so downstream planners/selectors
/// don’t need to repeatedly infer wildcard semantics.
#[derive(Debug, Clone)]
pub enum EventScope {
    /// Query targets a single event type. UID may be missing if schema is unknown.
    Specific {
        event_type: String,
        uid: Option<String>,
    },
    /// Query targets all event types (`*`). Holds `(event_type, uid)` pairs.
    Wildcard { pairs: Vec<(String, String)> },
}

impl EventScope {
    pub fn from_command(event_type: &str, registry: &SchemaRegistry) -> Self {
        if event_type == "*" {
            let pairs = registry
                .get_all()
                .keys()
                .filter_map(|name| registry.get_uid(name).map(|uid| (name.clone(), uid)))
                .collect();
            EventScope::Wildcard { pairs }
        } else {
            EventScope::Specific {
                event_type: event_type.to_string(),
                uid: registry.get_uid(event_type),
            }
        }
    }

    pub fn is_wildcard(&self) -> bool {
        matches!(self, EventScope::Wildcard { .. })
    }

    pub fn primary_uid(&self) -> Option<&str> {
        match self {
            EventScope::Specific { uid: Some(uid), .. } => Some(uid.as_str()),
            _ => None,
        }
    }

    pub fn uid_for(&self, event_type: &str) -> Option<&str> {
        match self {
            EventScope::Specific {
                event_type: scoped,
                uid: Some(uid),
            } if scoped == event_type => Some(uid.as_str()),
            EventScope::Wildcard { pairs } => pairs
                .iter()
                .find(|(et, _)| et == event_type)
                .map(|(_, uid)| uid.as_str()),
            _ => None,
        }
    }
}
