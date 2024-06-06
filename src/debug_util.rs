use tokio::net::TcpStream;

pub fn format_bytes_dbg(bytes: &[u8]) -> String {
    let len = bytes.len();
    let mut ret = String::with_capacity(len * 4);
    for &byte in bytes {
        match byte {
            b'\n' => ret.push_str("\n"),
            b'\r' => ret.push_str("\r"),
            0 => ret.push_str("[^A]"),
            1..=31 => ret.push_str(&format!("[0x{:02x}]", byte)),
            32..=127 => ret.push(byte as char),
            128..=255 => ret.push_str(&format!("[0x{:02x}]", byte)),
        }
    }
    ret
}

pub fn peer_addr_str(stream: &TcpStream) -> String {
    stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("<undefined>".to_string())
}
