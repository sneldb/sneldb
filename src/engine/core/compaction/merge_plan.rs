#[derive(Debug, Clone)]
pub struct MergePlan {
    /// Level we are compacting from (e.g., 0 for L0)
    pub level_from: u32,
    /// Level we are compacting to (e.g., 1 for L1)
    pub level_to: u32,
    /// Event type UID this plan targets
    pub uid: String,
    /// Input segment directory labels (zero-padded numeric strings)
    pub input_segment_labels: Vec<String>,
    /// Output numeric segment id to allocate/write
    pub output_segment_id: u32,
}


