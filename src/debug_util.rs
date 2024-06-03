
pub fn format_bytes_dbg(bytes: &[u8]) -> String {
    let len = bytes.len();
    let mut ret = String::with_capacity(len * 4);
    for &byte in bytes {
        match byte {
            0..=31 => ret.push_str(&format!("[0x{:02x}]", byte)),
            32..=127 => ret.push(byte as char),
            128..=255 => ret.push_str(&format!("[0x{:02x}]", byte))
        }
    }

    ret
}