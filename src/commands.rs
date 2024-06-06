#[derive(Debug)]
pub enum Command {
    Ping,
    Echo(Vec<u8>),
    Get(Vec<u8>),
    // Coming soon:
    // Set(Vec<u8>, Vec<u8>)
}
