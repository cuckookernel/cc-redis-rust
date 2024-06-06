use crate::commands::Command;
use crate::resp;
use std::collections::HashMap;

use crate::{resp::Value, Bytes};

pub struct Db {
    h: HashMap<Bytes, Bytes>,
}

impl Db {
    pub fn new() -> Self {
        Db { h: HashMap::new() }
    }

    pub fn execute(&mut self, cmd: &Command) -> Value {
        use resp::Value::*;
        use Command::*;

        match cmd {
            Ping => SimpleString("PONG".into()),
            Echo(a) => BulkString(a.clone()),
            SetKV(key, val) => {
                self.h.insert(key.clone(), val.clone());
                Value::ok()
            }
            Get(key) => {
                match self.h.get(&key) {
                    Some(val) => BulkString(val.clone()),
                    None => SimpleString("".into())
                }
            }
            /* _ => {
                // SimpleError(format!("Cannot handle cmd yet: {cmd:?}"))
                panic!("Cannot handle cmd yet: {cmd:?}")
            } */
        }
    }
}
