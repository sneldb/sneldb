use once_cell::sync::Lazy;
use std::sync::Arc;

use crate::shared::config::model::{Settings, load_settings};

pub static CONFIG: Lazy<Arc<Settings>> =
    Lazy::new(|| Arc::new(load_settings().expect("Failed to load configuration")));
