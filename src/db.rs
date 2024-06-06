use crate::commands::Command;
use crate::resp;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{resp::Value, Bytes};

struct ValAndExpiry {
    val: Bytes,
    ex: u64
};

pub fn non_expiring(val : Bytes) -> ValAndExpiry {
    ValAndExpiry{val, ex: i64::MAX}
}

pub struct Db {
    h: HashMap<Bytes, ValAndExpiry>,
}

pub fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("WTF?").as_millis() as u64
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
                self.h.insert(key.clone(), non_expiring(val.clone()) );
                Value::ok()
            }
            Get(key) => {
                match self.h.get(&key) {
                    Some(val_ex) => {
                        if val_ex.ex > now_millis() { // not yet expired
                            BulkString(val_ex.val.clone())
                        } else {
                            NullBulkString
                        }
                    }
                    None => NullBulkString
                }
            }
            /* _ => {
                // SimpleError(format!("Cannot handle cmd yet: {cmd:?}"))
                panic!("Cannot handle cmd yet: {cmd:?}")
            } */
        }
    }
}
