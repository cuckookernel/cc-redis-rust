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
    ReplConfGetAck(String),
    Psync(String, i64),
    Wait(i64, i64)
}

pub fn bad_num_of_arguments_err(cmd: &str, args: &[Value]) -> Result<Command> {
    Err(format_err!(
        "Invalid number for arguments for `{cmd}`: {n}\nargs={args:?}",
        n = args.len()
    ))
}

impl Command {
    pub fn to_bulk_array(&self) -> Value {
        match self {
            Self::Ping => vec![Value::from("PING")].into(),
            Self::Echo(bs) => vec!["ECHO".into(), bs.into()].into(),
            Self::Get(bs) => vec!["GET".into(), bs.into()].into(),
            Self::SetKV(kbs, vbs, ex) => match ex {
                None => vec!["SET".into(), kbs.into(), vbs.into()].into(),
                Some(ex) => {
                    let ex_str = format!("{}", ex);
                    vec![
                        "SET".into(),
                        kbs.into(),
                        vbs.into(),
                        "px".into(),
                        ex_str.as_str().into(),
                    ]
                    .into()
                }
            },
            Self::Info(s) => vec!["INFO".into(), s.as_str().into()].into(),
            Self::ReplConf(key, val) => {
                vec!["REPLCONF".into(), key.as_str().into(), val.as_str().into()].into()
            }
            Self::ReplConfGetAck(arg) => {
                vec!["REPLCONF".into(), "GETACK".into(), arg.as_str().into()].into()
            }
            Self::Psync(key, val) => vec![
                "PSYNC".into(),
                key.as_str().into(),
                val.to_string().as_str().into(),
            ]
            .into(),
            Self::Wait(n_repls, timeout) => vec![
                "WAIT".into(),
                n_repls.to_string().as_str().into(),
                timeout.to_string().as_str().into()
            ]
            .into()
        }
    }
}

pub fn parse_cmd(val: &Value) -> Result<Command> {
    use Value::*;

    match val {
        Array(elems) if !elems.is_empty() => {
            if let BulkString(bs) = &elems[0] {
                let word0 = bs.to_string()?;
                let args = &elems[1..];
                match word0.as_str() {
                    "PING" => Ok(Command::Ping),
                    "ECHO" => parse_echo(args),
                    "GET" => parse_get(args),
                    "INFO" => parse_info(args),
                    "SET" => parse_set(args),
                    "REPLCONF" => {
                        let arg0 = args[0].try_to_string()?;
                        let arg1 = args[1].try_to_string()?;

                        Ok(if arg0 == "GETACK" {
                            Command::ReplConfGetAck(arg1)
                        } else {
                            Command::ReplConf(arg0, arg1)
                        })
                    }
                    "PSYNC" => parse_psync(args),
                    "WAIT" => {
                        if args.len() != 2 {
                            return Err(format_err!("Invalid number of arguments for WAIT {n}, {args:?}", n=args.len()))
                        }
                        Ok(Command::Wait(args[0].try_to_int()?, args[1].try_to_int()?))
                    }
                    _ => {
                        panic!("Don't know about command: `{word0}`")
                    }
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

fn parse_echo(args: &[Value]) -> Result<Command> {
    if args.len() != 1 {
        bad_num_of_arguments_err("ECHO", args)
    } else {
        match &args[0] {
            BulkString(bs) => Ok(Command::Echo(bs.clone())),
            _ => Err(format_err!("Invalid argument for ECHO {e:?}", e = args[0])),
        }
    }
}

fn parse_get(args: &[Value]) -> Result<Command> {
    if args.len() != 1 {
        bad_num_of_arguments_err("GET", args)
    } else {
        match &args[0] {
            Value::BulkString(bs) => Ok(Command::Get(bs.clone())),
            _ => Err(format_err!("Invalid argument for GET: {e:?}", e = args[1])),
        }
    }
}

fn parse_set(args: &[Value]) -> Result<Command> {
    match args.len() {
        2 => {
            if let (BulkString(k), BulkString(v)) = (&args[0], &args[1]) {
                Ok(Command::SetKV(k.clone(), v.clone(), None))
            } else {
                Err(format_err!(
                    "Expected two bulkstrings as arguments for Set: {args:?}"
                ))
            }
        }
        4 => match (&args[0], &args[1], &args[2], &args[3]) {
            (BulkString(k), BulkString(v), BulkString(arg3), BulkString(ex_str))
                if arg3 == &Bytes::from("px") =>
            {
                let ex_int = ex_str.to_string()?.parse::<i64>()?;
                Ok(Command::SetKV(k.clone(), v.clone(), Some(ex_int as u64)))
            }
            _ => Err(format_err!(
                "Invalid 4 argument command: `SET`\nargs={args:?}"
            )),
        },
        _ => bad_num_of_arguments_err("SET", args),
    }
}

fn parse_info(args: &[Value]) -> Result<Command> {
    match &args[0] {
        BulkString(bs) => {
            let arg1 = bs.to_string()?;
            Ok(Command::Info(arg1))
        }
        _ => Err(format_err!(
            "Invalid argument for INFO command: arg0={arg0:?}",
            arg0 = &args[0]
        )),
    }
}

fn parse_psync(args: &[Value]) -> Result<Command> {
    let repl_id = args[0].try_to_string()?;
    let offset = args[1].try_to_string()?.parse::<i64>()?;
    Ok(Command::Psync(repl_id, offset))
}
