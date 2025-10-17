#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalType {
    VarBytes = 0,
    I64 = 1,
    U64 = 2,
    F64 = 3,
    Bool = 4,
    I32Date = 5,
}

impl From<u8> for PhysicalType {
    fn from(v: u8) -> Self {
        match v {
            1 => PhysicalType::I64,
            2 => PhysicalType::U64,
            3 => PhysicalType::F64,
            4 => PhysicalType::Bool,
            5 => PhysicalType::I32Date,
            _ => PhysicalType::VarBytes,
        }
    }
}

impl From<PhysicalType> for u8 {
    fn from(p: PhysicalType) -> u8 {
        p as u8
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ColumnBlockHeader {
    pub phys: u8,
    pub flags: u8,
    pub reserved: u16,
    pub row_count: u32,
    pub aux_len: u32,
}

impl ColumnBlockHeader {
    pub const FLAG_HAS_NULLS: u8 = 0b0000_0001;

    pub const LEN: usize = 1 + 1 + 2 + 4 + 4; // repr(C) layout

    pub fn new(phys: PhysicalType, has_nulls: bool, row_count: u32, aux_len: u32) -> Self {
        let mut flags = 0u8;
        if has_nulls {
            flags |= Self::FLAG_HAS_NULLS;
        }
        Self {
            phys: phys.into(),
            flags,
            reserved: 0,
            row_count,
            aux_len,
        }
    }

    pub fn write_to(&self, buf: &mut Vec<u8>) {
        buf.push(self.phys);
        buf.push(self.flags);
        buf.extend_from_slice(&self.reserved.to_le_bytes());
        buf.extend_from_slice(&self.row_count.to_le_bytes());
        buf.extend_from_slice(&self.aux_len.to_le_bytes());
    }

    pub fn read_from(slice: &[u8]) -> Option<Self> {
        if slice.len() < Self::LEN {
            return None;
        }
        let phys = slice[0];
        let flags = slice[1];
        let mut r = [0u8; 2];
        r.copy_from_slice(&slice[2..4]);
        let reserved = u16::from_le_bytes(r);
        let mut c = [0u8; 4];
        c.copy_from_slice(&slice[4..8]);
        let row_count = u32::from_le_bytes(c);
        c.copy_from_slice(&slice[8..12]);
        let aux_len = u32::from_le_bytes(c);
        Some(Self {
            phys,
            flags,
            reserved,
            row_count,
            aux_len,
        })
    }
}
