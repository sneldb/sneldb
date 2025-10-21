use serde::{Deserialize, Serialize};

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    pub struct IndexKind: u32 {
        const ZONE_INDEX        = 0b0000_0001;
        const ENUM_BITMAP       = 0b0000_0010;
        const XOR_FIELD_FILTER  = 0b0000_0100; // .xf
        const ZONE_XOR_INDEX    = 0b0000_1000; // .zxf
        const ZONE_SURF         = 0b0001_0000; // .zsf
        const TS_CALENDAR       = 0b0010_0000; // {uid}.cal dir/file
        const TS_ZTI            = 0b0100_0000; // {uid}_{zone}.tfi
        const FIELD_CALENDAR    = 0b1000_0000; // {uid}_{field}.cal
        const FIELD_ZTI         = 0b0001_0000_0000; // {uid}_{field}_{zone}.tfi
        const RLTE              = 0b0010_0000_0000; // {uid}.rlte
    }
}
