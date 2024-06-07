use anyhow::{format_err, Result};

use crate::common::Bytes;
use crate::resp::{Value, Value::*};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(Bytes),
    Get(Bytes),
    SetKV(Bytes, Bytes, Option<u64>),
    Info(String),
    ReplConf(String, String),
}

impl Command {
    pub fn to_bulk_array(&self) -> Value {
        match self {
            Self::Ping => vec![Value::from("PING")].into(),
            Self::Echo(bs) => vec!["ECHO".into(), bs.into()].into(),
            Self::Get(bs) => vec!["GET".into(), bs.into()].into(),
            Self::SetKV(kbs, vbs, ex) => {
                match ex {
                    None => vec!["SET".into(), kbs.into(), vbs.into()].into(),
                    Some(ex) => {
                        let ex_str = format!("{}", ex);
                        vec!["SET".into(), kbs.into(), vbs.into(), "px".into(), ex_str.as_str().into()].into()
                    }
                }
            }
            Self::Info(s) => vec!["INFO".into(), s.as_str().into()].into(),
            Self::ReplConf(key, val) => vec!["REPLCONF".into(), key.as_str().into(), val.as_str().into()].into(),
        }
    }
}

pub fn parse_cmd(val: &Value) -> Result<Command> {
    use Value::*;

    match val {
        Array(elems) if !elems.is_empty() => {
            let n_elems = elems.len();

            if let BulkString(bs) = &elems[0] {
                let word0 = bs.to_string()?;

                match n_elems {
                    1 => {
                        // all  commands witH one argument parsed here
                        match word0.as_str() {
                            "PING" => Ok(Command::Ping),
                            _ => Err(format_err!("Invalid one argument command: `{word0}`")),
                        }
                    }
                    2 => {
                        // all  commands witH one argument parsed here
                        match word0.as_str() {
                            "ECHO" => parse_echo(elems),
                            "GET" => parse_get(elems),
                            "INFO" => parse_info(elems),
                            _ => Err(format_err!("Invalid one argument command: `{word0}`")),
                        }
                    }
                    3 => {
                        // all  commands witH one argument parsed here
                        match word0.as_str() {
                            "SET" => parse_set(elems),
                            _ => Err(format_err!("Invalid 2 argument command: `{word0}`")),
                        }
                    }
                    5 => {
                        // all  commands witH one argument parsed here
                        match (word0.as_str(), &elems[1], &elems[2], &elems[3], &elems[4]) {
                            (
                                "SET",
                                BulkString(k),
                                BulkString(v),
                                BulkString(arg3),
                                BulkString(ex_str),
                            ) if arg3 == &Bytes::from("px") => {
                                let ex_int = ex_str.to_string()?.parse::<i64>()?;

                                Ok(Command::SetKV(k.clone(), v.clone(), Some(ex_int as u64)))
                            }
                            _ => Err(format_err!(
                                "Invalid 4 argument command: `{word0}`\nelems={elems:?}"
                            )),
                        }
                    }
                    _ => Err(format_err!("Unexpected number of elems: {n_elems}, {elems:?}")),
                }

            } else {
                Err(format_err!("Unexpected elems[0]={e:?}", e = elems[0]))
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
        _ => Err(format_err!("Invalid argument for ECHO {e:?}", e = elems[1])),
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

fn parse_info(elems: &[Value]) -> Result<Command> {
    match &elems[1] {
        BulkString(bs) => {
            let arg1 = bs.to_string()?;
            Ok(Command::Info(arg1))
        }
        _ => Err(format_err!(
            "Invalid argument for INFO command {e:?}",
            e = elems[1]
        )),
    }
}
