use super::common::Bytes;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(Bytes),
    Get(Bytes),
    // Coming soon:
    // Set(Vec<u8>, Vec<u8>)
}
