use anyhow::Result;

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

    pub fn to_string(&self) -> Result<String> {
        Ok(String::from_utf8(self.0.clone())?)
    }

    #[allow(dead_code)]
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

/*
impl std::fmt::Debug for Bytes {
    fn fmt(&self, fmtr: &mut Formatter) -> Result<(), std::fmt::Error> {
        fmtr.write_str(&format_bytes_dbg(&self.0))?;
        Ok(())
    }
}
*/

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.len();
        let mut ret = String::with_capacity(len * 4);
        for byte in &self.0 {
            match byte {
                b'\n' => ret.push_str("\\n"),
                b'\r' => ret.push_str("\\r"),
                0 => ret.push_str("[^A]"),
                1..=31 => ret.push_str(&format!("[0x{:02x}]", byte)),
                32..=127 => ret.push(*byte as char),
                128..=255 => ret.push_str(&format!("[0x{:02x}]", byte)),
            }
        }
        write!(f, "{}", ret)
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
