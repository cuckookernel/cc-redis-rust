use crate::commands::Command;
use crate::config::InstanceConfig;
use crate::{resp, Bytes, CmdAndSender};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use mpsc::Receiver;
use resp::Value;
use tokio::sync::mpsc;

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
    replication_id: String,
    replication_offset: usize,
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("WTF?")
        .as_millis() as u64
}

const A_LARGE_PRIME: u64 = 2147483647;

pub fn make_replication_id(seed: u64) -> String {
    let rep_id_u64_p1 = seed.wrapping_mul(A_LARGE_PRIME);
    let rep_id_u64_p2 = rep_id_u64_p1.wrapping_add(A_LARGE_PRIME);
    let rep_id_hex = format!("{rep_id_u64_p1:x}{rep_id_u64_p2:x}");
    let replication_id = &rep_id_hex.as_bytes()[..20];
    String::from_utf8(replication_id.into()).unwrap()
}

impl Db {
    pub fn new(cfg: InstanceConfig) -> Self {
        Db {
            h: HashMap::new(),
            cfg,
            replication_id: make_replication_id(now_millis()),
            replication_offset: 0,
        }
    }

    pub async fn run(mut self, mut rx: Receiver<CmdAndSender>) {
        // long running co-routine that gets commands from only channel and executes them on the Db
        println!("handle_db_commands: Starting loop");
        loop {
            match rx.recv().await {
                Some((cmd, sx)) => {
                    let resp_val = self.execute(&cmd);
                    sx.send(resp_val).await.unwrap()
                }
                None => {
                    println!("handle_commands: Incomming command channel closed. STOPPING");
                    break;
                }
            }
        }
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
            Info(arg) => match arg.as_str() {
                "replication" => {
                    let parts = [
                        format!("role:{role}", role = self.cfg.role()),
                        format!("master_replid:{replid}", replid = self.replication_id),
                        format!(
                            "master_repl_offset:{offset}",
                            offset = self.replication_offset
                        ),
                    ];

                    BulkString(parts.join("\r\n").as_str().into())
                }
                _ => NullBulkString,
            }, /* _ => {
                     // SimpleError(format!("Cannot handle cmd yet: {cmd:?}"))
                     panic!("Cannot handle cmd yet: {cmd:?}")
               } */
        }
    }
}
