use std::fmt::Formatter;

use crate::debug_util::format_bytes_dbg;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn as_vec(&self) -> &Vec<u8> {
        &self.0
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, fmtr: &mut Formatter) -> Result<(), std::fmt::Error> {
        fmtr.write_str(&format_bytes_dbg(&self.0))?;
        Ok(())
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<&[u8]> for Bytes {
    fn from(v: &[u8]) -> Self {
        Self(Vec::from(v))
    }
}

impl From<&str> for Bytes {
    fn from(s: &str) -> Self {
        Self(Vec::from(s.as_bytes()))
    }
}
