use anyhow::{format_err, Result};

use crate::common::Bytes;
use crate::resp::{Value, Value::*};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(Bytes),
    Get(Bytes),
    SetKV(Bytes, Bytes, Option<u64>),
}

pub fn parse_cmd(val: &Value) -> Result<Command> {
    use Value::*;

    match val {
        Array(elems) if !elems.is_empty() => {
            let n_elems = elems.len();
            match (&elems[0], n_elems) {
                (BulkString(v), 1) => {
                    // all  commands witH one argument parsed here
                    let word0 = String::from_utf8(v.as_vec().clone())?;
                    match word0.as_str() {
                        "PING" => Ok(Command::Ping),
                        _ => Err(format_err!("Invalid one argument command: `{word0}`")),
                    }
                }

                (BulkString(v), 2) => {
                    // all  commands witH one argument parsed here
                    let word0 = String::from_utf8(v.as_vec().clone())?;
                    match word0.as_str() {
                        "ECHO" => parse_echo(elems),
                        "GET" => parse_get(elems),
                        _ => Err(format_err!("Invalid one argument command: `{word0}`")),
                    }
                }

                (BulkString(v), 3) => {
                    // all  commands witH one argument parsed here
                    let word0 = String::from_utf8(v.as_vec().clone())?;
                    match word0.as_str() {
                        "SET" => parse_set(elems),
                        _ => Err(format_err!("Invalid 2 argument command: `{word0}`")),
                    }
                }

                (BulkString(v), 5) => {
                    // all  commands witH one argument parsed here
                    let word0 = String::from_utf8(v.as_vec().clone())?;
                    match (word0.as_str(), &elems[1], &elems[2], &elems[3], &elems[4]) {
                        ("SET", BulkString(k), BulkString(v), BulkString(arg3), Int(ex_int))
                            if arg3 == &Bytes::from("ex") =>
                        {
                            Ok(Command::SetKV(k.clone(), v.clone(), Some(*ex_int as u64)))
                        }
                        _ => Err(format_err!("Invalid 4 argument command: `{word0}`")),
                    }
                }

                _ => Err(format_err!("Unexpected elems[0]={e:?}", e = elems[0])),
            }
        }

        SimpleString(v) => {
            let word0 = String::from_utf8(v.as_vec().clone())?;
            match word0.as_str() {
                "PING" => Ok(Command::Ping),
                _ => Err(format_err!("Invalid SimpleString={word0}")),
            }
        }
        _ => Err(format_err!("Could not parse command from value = {val:?}")),
    }
}

fn parse_echo(elems: &[Value]) -> Result<Command> {
    match &elems[1] {
        BulkString(bs) => Ok(Command::Echo(bs.clone())),
        _ => Err(format_err!("Invalid argument for echo {e:?}", e = elems[1])),
    }
}

fn parse_get(elems: &[Value]) -> Result<Command> {
    match &elems[1] {
        Value::BulkString(bs) => Ok(Command::Get(bs.clone())),
        _ => Err(format_err!("Invalid argument for GET: {e:?}", e = elems[1])),
    }
}

fn parse_set(elems: &[Value]) -> Result<Command> {
    if let (BulkString(k), BulkString(v)) = (&elems[1], &elems[2]) {
        Ok(Command::SetKV(k.clone(), v.clone(), None))
    } else {
        Err(format_err!(
            "Expected two bulkstrings as arguments for Set: {e:?}",
            e = &elems[1..]
        ))
    }
}
