use std::collections::HashMap;

use anyhow::Result;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;

use crate::commands::Command;
use crate::config::InstanceConfig;
use crate::misc_util::{make_replication_id, now_millis};
use crate::resp::{get_value_from_stream, s_str, serialize, Value};
use crate::{Bytes, CmdAndSender};

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

        if let Some(master_host_port) = self.cfg.replicaof.clone() {
            println!("Db::run: running replication handshake");
            self.run_replication_handshake(&master_host_port)
                .await
                .unwrap();
        }

        println!("Db::run: Starting loop");
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

    pub async fn run_replication_handshake(&mut self, master_host_port: &str) -> Result<()> {
        let mut proxy = ProxyToMaster::new(master_host_port).await;
        let ping_resp = proxy.send_command(Command::Ping).await?;
        println!("master's response to ping: {ping_resp:?}");

        let repl_conf_1 = Command::ReplConf("listening-port".into(), format!("{}", self.cfg.port));
        let repl_conf_1_resp = proxy.send_command(repl_conf_1).await?;
        println!("master's response to repl_conf_1: {repl_conf_1_resp:?}");

        let repl_conf_2 = Command::ReplConf("capa".into(), "psync2".into());
        let repl_conf_2_resp = proxy.send_command(repl_conf_2).await?;
        println!("master's response to repl_conf_2: {repl_conf_2_resp:?}");

        let psync = Command::Psync("?".into(), -1);
        let psync_resp = proxy.send_command(psync).await?;
        println!("master's response to psync: {psync_resp:?}");

        Ok(())
    }

    pub fn execute(&mut self, cmd: &Command) -> Value {
        use Command::*;

        match cmd {
            Ping => s_str("PONG"),
            Echo(a) => Value::BulkString(a.clone()),
            SetKV(key, val, ex) => self.exec_set(key, val, ex),
            Get(key) => self.exec_get(key),
            Info(arg) => self.exec_info(arg),
            ReplConf(_, _) => Value::ok(),
            Psync(id, offset) if id == "?" && *offset == -1 => {
                let reply_str = format!("FULLRESYNC {repl_id} 0", repl_id = self.replication_id);
                s_str(&reply_str)
            }
            Psync(_, _) => {
                panic!("Can't reply to {cmd:?} yet")
            }
        }
    }

    pub fn exec_set(&mut self, key: &Bytes, val: &Bytes, ex: &Option<u64>) -> Value {
        self.h
            .insert(key.clone(), ValAndExpiry::new(val.clone(), *ex));
        Value::ok()
    }

    pub fn exec_get(&self, key: &Bytes) -> Value {
        match self.h.get(key) {
            Some(val_ex) => {
                if val_ex.ex > now_millis() {
                    // not yet expired
                    Value::BulkString(val_ex.val.clone())
                } else {
                    Value::NullBulkString
                }
            }
            None => Value::NullBulkString,
        }
    }

    pub fn exec_info(&self, arg: &str) -> Value {
        match arg {
            "replication" => {
                let parts = [
                    format!("role:{role}", role = self.cfg.role()),
                    format!("master_replid:{replid}", replid = self.replication_id),
                    format!(
                        "master_repl_offset:{offset}",
                        offset = self.replication_offset
                    ),
                ];

                Value::BulkString(parts.join("\r\n").as_str().into())
            }
            _ => Value::NullBulkString,
        }
    }
}

struct ProxyToMaster {
    stream: TcpStream,
}

impl ProxyToMaster {
    async fn new(master_host_port: &str) -> Self {
        let host_port = master_host_port.replace(' ', ":");
        let stream = TcpStream::connect(host_port).await.unwrap();
        Self { stream }
    }

    async fn send_command(&mut self, cmd: Command) -> Result<Value> {
        println!("ProxyToMaster::send_command: {cmd:?}");

        let cmd_as_value = cmd.to_bulk_array();
        let serialized = serialize(&cmd_as_value)?;
        self.stream.write_all(serialized.as_bytes()).await?;
        get_value_from_stream(&mut self.stream).await
    }
}
