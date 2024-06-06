use anyhow::Result;
use crate::commands::Command;
use crate::resp;

pub fn process_cmd(cmd_res: &Result<Command>) -> resp::Value {
    use Command::*;
    use resp::Value::*;

    match cmd_res {
        Err(e) => {
            SimpleError(e.to_string())
        }
        Ok(cmd) => match &cmd {
            Ping => SimpleString(Vec::from("PONG".as_bytes())),
            Echo(a) => BulkString(a.clone()),
            _ => SimpleError(format!("Cannot handle cmd yet: {cmd:?}"))
        }
    }

}
