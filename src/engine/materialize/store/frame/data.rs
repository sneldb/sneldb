use super::header::FrameHeader;

#[derive(Debug, Clone)]
pub struct FrameData {
    pub header: FrameHeader,
    pub compressed: Vec<u8>,
}
