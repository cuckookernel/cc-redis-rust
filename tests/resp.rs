use anyhow::Result;

use redis_starter_rust::*;

use resp::Value;
use resp::Value::*;
use resp::{b_str, s_str, RespDeserializer};

pub fn deser_str(data: &str) -> Result<Value> {
    let mut deser = RespDeserializer::new(Vec::from(data.as_bytes()));
    deser.deserialize()
}

#[test]
fn parse_simple_str() {
    let input = "+teo\r\n";
    let expected = s_str("teo");

    assert_eq!(deser_str(input).unwrap(), expected);
}

#[test]
fn parse_array_2() {
    let input = "*2\r\n$4\r\nECHO\r\n$5\r\npears\r\n";
    let expected = Array(vec![b_str("ECHO"), b_str("pears")]);

    assert_eq!(deser_str(input).unwrap(), expected);
}

#[test]
fn parse_array_ping() {
    let input = "*1\r\n$4\r\nPING\r\n";
    let expected = Array(vec![b_str("PING")]);

    assert_eq!(deser_str(input).unwrap(), expected);
}
