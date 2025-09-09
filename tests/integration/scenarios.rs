// tests/integration/scenarios.rs
use crate::integration::matcher::Matcher;
use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct TestScenario {
    pub name: String,
    pub input_commands: Vec<String>,
    /// Single matcher (legacy, for backward compatibility)
    pub matcher: Option<Matcher>,
    /// Multiple matchers (new, preferred)
    pub matchers: Option<Vec<Matcher>>,
}

pub fn load_scenarios_from_json(path: &str) -> Vec<TestScenario> {
    let content = fs::read_to_string(path).expect("Failed to read scenario file");
    serde_json::from_str(&content).expect("Invalid JSON format")
}
