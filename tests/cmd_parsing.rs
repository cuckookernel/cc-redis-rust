use commands::{parse_cmd, Command};
use redis_starter_rust::*;
use resp::Value::*;

#[test]
fn parse_echo() {
    let val = Array(vec![BulkString("ECHO".into()), BulkString("teo".into())]);
    let expected = Command::Echo("teo".into());
    let cmd = parse_cmd(&val).unwrap();

    assert_eq!(cmd, expected);
}

#[test]
fn parse_simple_ping() {
    let val = SimpleString("PING".into());
    let expected = Command::Ping;
    let cmd = parse_cmd(&val).unwrap();

    assert_eq!(cmd, expected);
}

#[test]
fn parse_bulk_ping() {
    let val = Array(vec![BulkString("PING".into())]);
    let expected = Command::Ping;
    let cmd = parse_cmd(&val).unwrap();

    assert_eq!(cmd, expected);
}
