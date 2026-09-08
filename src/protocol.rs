#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Get,
    Set,
    Delete,
}

impl Operation {
    #[inline(always)]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Operation::Get),
            1 => Some(Operation::Set),
            2 => Some(Operation::Delete),
            _ => None,
        }
    }
    #[inline(always)]
    pub fn to_u8(&self) -> u8 {
        match self {
            Operation::Get => 0,
            Operation::Set => 1,
            Operation::Delete => 2,
        }
    }
}
