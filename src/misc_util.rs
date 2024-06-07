use std::time::{SystemTime, UNIX_EPOCH};

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

