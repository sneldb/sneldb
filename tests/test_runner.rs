mod integration;

use crate::integration::runner::run_scenario;
use crate::integration::scenarios::load_scenarios_from_json;

use rand::seq::SliceRandom;
use rand::thread_rng;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize tracing with debug level
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive("debug".parse().unwrap())
                .add_directive("snel_db=debug".parse().unwrap())
                .add_directive("test_runner=debug".parse().unwrap()),
        )
        .init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut all_scenarios = load_scenarios_from_json("tests/integration/scenarios.json");

    // Shuffle all scenarios
    let mut rng = thread_rng();
    all_scenarios.shuffle(&mut rng);

    if args.is_empty() {
        for scenario in all_scenarios.iter() {
            run_scenario(scenario);
        }
    } else {
        for name in args {
            match all_scenarios.iter().find(|s| s.name == name) {
                Some(scenario) => run_scenario(scenario),
                None => {
                    eprintln!("Scenario '{}' not found", name);
                    std::process::exit(1);
                }
            }
        }
    }
}
