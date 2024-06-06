use crate::commands::Command;
use crate::resp;
use anyhow::Result;

pub fn process_cmd(cmd_res: &Result<Command>) -> resp::Value {
    use resp::Value::*;
    use Command::*;

    match cmd_res {
        Err(e) => SimpleError(e.to_string()),
        Ok(cmd) => match &cmd {
            Ping => SimpleString("PONG".into()),
            Echo(a) => BulkString(a.clone()),
            _ => SimpleError(format!("Cannot handle cmd yet: {cmd:?}")),
        },
    }
}
