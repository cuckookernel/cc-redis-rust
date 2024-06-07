use crate::commands::Command;
use crate::config::InstanceConfig;
use crate::resp;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{resp::Value, Bytes};

pub struct ValAndExpiry {
    val: Bytes,
    ex: u64, // absolute expiry time in millis since epoch
}

impl ValAndExpiry {
    pub fn new(val: Bytes, ex_interv: Option<u64>) -> Self {
        match ex_interv {
            Some(interv) => ValAndExpiry {
                val,
                ex: now_millis() + interv,
            },
            None => ValAndExpiry { val, ex: u64::MAX },
        }
    }
}

pub struct Db {
    h: HashMap<Bytes, ValAndExpiry>,
    cfg: InstanceConfig,
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("WTF?")
        .as_millis() as u64
}

impl Db {
    pub fn new(cfg: InstanceConfig) -> Self {
        Db { h: HashMap::new(), cfg}
    }

    pub fn execute(&mut self, cmd: &Command) -> Value {
        use resp::Value::*;
        use Command::*;

        match cmd {
            Ping => SimpleString("PONG".into()),
            Echo(a) => BulkString(a.clone()),
            SetKV(key, val, ex) => {
                self.h
                    .insert(key.clone(), ValAndExpiry::new(val.clone(), *ex));
                Value::ok()
            }
            Get(key) => {
                match self.h.get(key) {
                    Some(val_ex) => {
                        if val_ex.ex > now_millis() {
                            // not yet expired
                            BulkString(val_ex.val.clone())
                        } else {
                            NullBulkString
                        }
                    }
                    None => NullBulkString,
                }
            }
            Info(arg) => {
                match arg.as_str() {
                    "replication" => {
                        BulkString(format!("role:{role}", role=self.cfg.role().to_string()).as_str().into())
                    },
                    _ => NullBulkString
                }
            }
            /* _ => {
                    // SimpleError(format!("Cannot handle cmd yet: {cmd:?}"))
                    panic!("Cannot handle cmd yet: {cmd:?}")
              } */
        }
    }
}
