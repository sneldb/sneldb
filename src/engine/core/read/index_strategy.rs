#[derive(Debug, Clone)]
pub enum IndexStrategy {
    TemporalEq { field: String },
    TemporalRange { field: String },
    EnumBitmap { field: String },
    ZoneSuRF { field: String },
    ZoneXorIndex { field: String },
    XorPresence { field: String },
    FullScan,
}
