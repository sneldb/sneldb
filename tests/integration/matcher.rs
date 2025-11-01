use regex::Regex;
use serde::Deserialize;
use tracing::{debug, error};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatcherKind {
    Include,
    NotInclude,
    Eq,
    Match,
    IncludeAll,
    IncludeNone, // Added for the opposite of IncludeAll
    Regex,
    RegexAll,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MatcherValue {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Deserialize)]
pub struct Matcher {
    pub kind: MatcherKind,
    pub value: MatcherValue,
}

impl Matcher {
    pub fn matches(&self, actual: &str) -> bool {
        debug!("Matching against actual output:\n{}", actual);
        match (&self.kind, &self.value) {
            (MatcherKind::Include, MatcherValue::Single(s)) => {
                debug!("Checking if output contains: {}", s);
                let result = actual.contains(s);
                debug!("Include match result: {}", result);
                result
            }
            (MatcherKind::NotInclude, MatcherValue::Single(s)) => {
                debug!("Checking if output does not contain: {}", s);
                let result = !actual.contains(s);
                debug!("NotInclude match result: {}", result);
                result
            }
            (MatcherKind::Eq, MatcherValue::Single(s)) => {
                debug!("Checking exact equality with: {}", s);
                let result = actual.trim() == s.trim();
                debug!("Eq match result: {}", result);
                result
            }
            (MatcherKind::Match, MatcherValue::Single(s))
            | (MatcherKind::Regex, MatcherValue::Single(s)) => {
                debug!("Checking regex match with pattern: {}", s);
                match Regex::new(s) {
                    Ok(re) => {
                        let result = re.is_match(actual);
                        debug!("Regex match result: {}", result);
                        result
                    }
                    Err(e) => {
                        error!("Invalid regex: {}", e);
                        false
                    }
                }
            }
            (MatcherKind::IncludeAll, MatcherValue::Multiple(vec)) => {
                debug!("Checking if output contains all patterns: {:?}", vec);
                let result = vec.iter().all(|frag| {
                    let contains = actual.contains(frag);
                    debug!("Checking pattern '{}': {}", frag, contains);
                    contains
                });
                debug!("IncludeAll match result: {}", result);
                result
            }
            (MatcherKind::IncludeNone, MatcherValue::Multiple(vec)) => {
                debug!(
                    "Checking if output contains none of the patterns: {:?}",
                    vec
                );
                let result = vec.iter().all(|frag| {
                    let contains = !actual.contains(frag);
                    debug!("Checking pattern '{}': not present? {}", frag, contains);
                    contains
                });
                debug!("IncludeNone match result: {}", result);
                result
            }
            (MatcherKind::RegexAll, MatcherValue::Multiple(vec)) => {
                debug!("Checking if output matches all regex patterns: {:?}", vec);
                vec.iter().all(|pattern| match Regex::new(pattern) {
                    Ok(re) => {
                        let any_match = actual.lines().any(|line| re.is_match(line));
                        debug!("Pattern '{}' match result: {}", pattern, any_match);
                        any_match
                    }
                    Err(e) => {
                        error!("Invalid regex '{}': {}", pattern, e);
                        false
                    }
                })
            }
            _ => {
                error!("Invalid matcher kind/value combination");
                false
            }
        }
    }
}
