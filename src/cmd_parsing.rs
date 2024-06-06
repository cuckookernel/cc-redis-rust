use crate::{commands::Command, resp::Value};
use anyhow::{format_err, Result};

pub fn parse_cmd(val: &Value) -> Result<Command> {
    use Value::*;

    match val {
        Array(elems) if elems.len() > 0 => {
            let n_elems = elems.len();
            match (&elems[0], n_elems) {
                (BulkString(v), 2) => {
                    // all  commands witH one argument parsed here
                    let word0 = String::from_utf8(v.clone())?;
                    match word0.as_str() {
                        "ECHO" => parse_echo(elems),
                        "GET" => parse_get(elems),
                        _ => Err(format_err!("Invalid one argument command: `{word0}`")),
                    }
                }

                _ => Err(format_err!("Unexpected elems[0]={e:?}", e = elems[0])),
            }
        }

        SimpleString(v) => {
            let word0 = String::from_utf8(v.clone())?;
            match word0.as_str() {
                "PING" => Ok(Command::Ping),
                _ => Err(format_err!("Invalid SimpleString={word0}")),
            }
        }
        _ => Err(format_err!("Could not parse command from value = {val:?}")),
    }
}

fn parse_echo(elems: &Vec<Value>) -> Result<Command> {
    match &elems[1] {
        Value::BulkString(v) => Ok(Command::Echo(v.clone())),
        _ => Err(format_err!("Invalid argument for echo {e:?}", e = elems[1])),
    }
}


fn parse_get(elems: &Vec<Value>) -> Result<Command> {
    match &elems[1] {
        Value::BulkString(v) => Ok(Command::Get(v.clone())),
        _ => Err(format_err!("Invalid argument for GET: {e:?}", e = elems[1])),
    }
}
