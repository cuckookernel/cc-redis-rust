use std::time::{SystemTime, UNIX_EPOCH};

use tokio::{io::BufStream, net::TcpStream};

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("WTF?")
        .as_millis() as u64
}

const A_LARGE_PRIME: u64 = 2147483647;

pub fn make_replication_id(seed: u64) -> String {
    let rep_id_u64_p1 = seed.wrapping_mul(A_LARGE_PRIME);
    let rep_id_u64_p2 = rep_id_u64_p1.wrapping_add(A_LARGE_PRIME);
    let rep_id_hex = format!("{rep_id_u64_p1:x}{rep_id_u64_p2:x}");
    let replication_id = &rep_id_hex.as_bytes()[..20];
    String::from_utf8(replication_id.into()).unwrap()
}

pub fn peer_addr_str_v2(stream: &BufStream<TcpStream>) -> String {
    stream
        .get_ref()
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or("<undefined>".to_string())
}

#[derive(Debug)]
pub struct InvalidDigit;

pub fn hex_decode(input: &str) -> Result<Vec<u8>, InvalidDigit> {
    let input_bytes = input.as_bytes();
    let n_bytes = input_bytes.len();
    assert!(n_bytes % 2 == 0);

    let mut output = Vec::with_capacity(n_bytes / 2);

    for i in (0..n_bytes).step_by(2) {
        let digit0_val = hex_decode_1_digit(input_bytes[i])?;
        let digit1_val = hex_decode_1_digit(input_bytes[i + 1])?;

        output.push(digit0_val * 16 + digit1_val)
    }

    Ok(output)
}

#[inline]
fn hex_decode_1_digit(digit: u8) -> Result<u8, InvalidDigit> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok((digit - b'a') + 10),
        b'A'..=b'B' => Ok((digit - b'A') + 10),
        _ => Err(InvalidDigit),
    }
}
