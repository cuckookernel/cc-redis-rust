use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use crate::async_deser::RespDeserializer;
use crate::commands::Command;
use crate::common::Bytes;
use crate::config::InstanceConfig;
// use crate::io_util::debug_peek;
use crate::svc::{handle_stream_async, Query, ToDb};
use crate::misc_util::hex_decode;
// use crate::misc_util::peer_addr_str;
use crate::async_deser::deserialize;
use crate::misc_util::peer_addr_str_v2;
use crate::misc_util::{make_replication_id, now_millis};
use crate::resp::QueryResult;
use crate::resp::{s_str, serialize, Value};
// use crate::async_deser::receive_value_from_stream;

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

struct ReplicaInfo {
    host_port: String,
    bstream: BufStream<TcpStream>,
}

pub struct Db {
    h: HashMap<Bytes, ValAndExpiry>,
    cfg: InstanceConfig,
    // Used by replicas
    repl_byte_cnt: usize,
    // Used by Master
    replicas: HashMap<String, ReplicaInfo>,
    replication_id: String,
    replication_offset: usize,
}

impl Db {
    pub fn new(cfg: InstanceConfig) -> Self {
        Db {
            h: HashMap::new(),
            cfg,
            repl_byte_cnt: 0,
            replicas: HashMap::new(),
            replication_id: make_replication_id(now_millis()),
            replication_offset: 0,
        }
    }

    pub async fn run(mut self, repl_tx: Sender<ToDb>, mut rx: Receiver<ToDb>) {
        // spawn replication coroutine
        if let Some(master_host_port) = self.cfg.replicaof.clone() {
            let mut proxy = ProxyToMaster::new(master_host_port.as_str()).await;

            println!("Db::run: running replication handshake");
            self.run_replication_handshake(&mut proxy).await.unwrap();

            println!("Db::run: replication handshake FINISHED");
            let bstream = proxy.bstream;
            tokio::spawn(handle_stream_async(bstream, repl_tx, true));
            // handle_stream_async(stream, repl_tx, true).await
        }

        tokio::time::sleep(Duration::from_millis(500)).await;

        // long running co-routine that gets commands from only channel and executes them on the Db
        println!("Db::run: Starting Query Loop");
        loop {
            match rx.recv().await {
                Some(ToDb::QueryAndSender(qry, sx)) => {
                    println!("Query loop received: {qry:?}");
                    let resp_val = self.execute(&qry).await;
                    let repl_byte_cnt_inc = resp_val.repl_byte_cnt_inc;
                    sx.send(resp_val).await.unwrap();
                    self.repl_byte_cnt += repl_byte_cnt_inc;
                }
                Some(ToDb::PassedReplStream(bstream)) => {
                    let replica_addr = peer_addr_str_v2(&bstream).replace(' ', ":");

                    println!("Query loop received ReplStream({replica_addr})");
                    if !self.replicas.contains_key(&replica_addr) {
                        self.replicas.insert(
                            replica_addr.clone(),
                            ReplicaInfo {
                                host_port: replica_addr,
                                bstream,
                            },
                        );
                    }
                }
                None => {
                    println!("handle_commands: Incomming command channel closed. STOPPING");
                    break;
                }
            }
        }
    }

    pub async fn run_replication_handshake(&mut self, proxy: &mut ProxyToMaster) -> Result<()> {
        // Run by replica side
        let ping_resp = proxy.send_command(Command::Ping).await?;
        println!("master's response to ping: {ping_resp:?}");

        let repl_conf_1 = Command::ReplConf("listening-port".into(), format!("{}", self.cfg.port));
        let repl_conf_1_resp = proxy.send_command(repl_conf_1).await?;
        println!("master's response to repl_conf_1: {repl_conf_1_resp:?}");

        let repl_conf_2 = Command::ReplConf("capa".into(), "psync2".into());
        let repl_conf_2_resp = proxy.send_command(repl_conf_2).await?;
        println!("master's response to repl_conf_2: {repl_conf_2_resp:?}");
        // debug_peek(&proxy.stream, 64).await;

        let psync = Command::Psync("?".into(), -1);
        let psync_resp = proxy.send_command(psync).await.unwrap();
        println!("master's response to psync: {psync_resp:?}");

        // debug_peek("Before getting rdb_file", &proxy.bstream, 64).await;
        // waiting for RDBFILE now
        let rdb_file = proxy.receive_file().await?;
        println!("rdb_file received: {rdb_file:?}");

        // handle_stream_async(proxy.bstream, repl_tx, true).await;
        Ok(())
    }

    pub async fn execute(&mut self, query: &Query) -> QueryResult {
        use Command::*;

        // self.repl_byte_cnt += query.deser_byte_cnt;

        let result: Vec<Value> = match &query.cmd {
            Ping => vec![s_str("PONG")],
            Echo(a) => vec![Value::BulkString(a.clone())],
            SetKV(key, val, ex) => vec![self.exec_set(key, val, ex).await],
            Get(key) => vec![self.exec_get(key)],
            Info(arg) => vec![self.exec_info(arg)],
            Psync(id, offset) if id == "?" && *offset == -1 => {
                let reply_str = format!("FULLRESYNC {repl_id} 0", repl_id = self.replication_id);

                return QueryResult {
                    vals: vec![
                        s_str(&reply_str),
                        Value::FileContents(get_empty_rdb_bytes().into()),
                    ],
                    repl_byte_cnt_inc: 0,
                    pass_stream: true,
                };
            }
            Psync(_, _) => {
                panic!("Can't reply to {cmd:?} yet", cmd = query.cmd)
            }
            ReplConf(key, val) => {
                vec![self.exec_repl_conf(key, val)]
            }
            ReplConfGetAck(_) => {
                vec![self.exec_repl_conf_get_ack()]
            }
            Wait(_, _) => {
                vec![Value::Int(0)]
            }
        };

        QueryResult {
            vals: result,
            repl_byte_cnt_inc: query.deser_byte_cnt,
            pass_stream: false
        }
    }

    pub async fn exec_set(&mut self, key: &Bytes, val: &Bytes, ex: &Option<u64>) -> Value {
        self.h
            .insert(key.clone(), ValAndExpiry::new(val.clone(), *ex));

        if !self.replicas.is_empty() {
            let cmd = Command::SetKV(key.clone(), val.clone(), *ex);
            let value = cmd.to_bulk_array();
            let bytes = serialize(&value).unwrap().into_inner();

            for (repl_key, replica) in self.replicas.iter_mut() {
                println!("attempting replication to: {repl_key}");

                replica.bstream.write_all(&bytes).await.unwrap_or_else(|e| {
                    println!(
                        "ERROR when attempting to replicate to {host_port}, err={e:?}",
                        host_port = replica.host_port
                    )
                });

                replica
                    .bstream
                    .flush()
                    .await
                    .unwrap_or_else(|e| println!("ERROR: when flushhing, err={e:?}"));
            }
        }

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
            None => {
                println!("Key not found: `{key:?}`");
                Value::NullBulkString
            }
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

    fn exec_repl_conf(&mut self, key: &String, val: &String) -> Value {
        match key.as_str() {
            "listening-port" => {
                /*
                let replica_addr = format!("{host}:{port}", host = host, port = val);

                if !self.replicas.contains_key(&replica_addr) {
                    self.replicas.insert(
                        replica_addr.clone(),
                        ReplicaInfo {
                            host_port: replica_addr,
                            stream: None,
                        },
                    );
                }
                */
                Value::ok()
            }
            "capa" => {
                if val == "psync2" {
                    Value::ok()
                } else {
                    Value::BulkError(format!("Can't handle capa=`{val}`"))
                }
            }
            _ => Value::BulkError(format!("Can't handle repl_conf key=`{key}`")),
        }
    }
    fn exec_repl_conf_get_ack(&mut self) -> Value {
        let repl_byte_cnt_str = self.repl_byte_cnt.to_string();
        vec!["REPLCONF".into(), "ACK".into(), repl_byte_cnt_str.as_str().into()].into()
    }
}

pub struct ProxyToMaster {
    bstream: BufStream<TcpStream>,
}

impl ProxyToMaster {
    async fn new(master_host_port: &str) -> Self {
        let host_port = master_host_port.replace(' ', ":");
        let bstream = BufStream::new(TcpStream::connect(host_port).await.unwrap());
        Self { bstream }
    }

    async fn send_command(&mut self, cmd: Command) -> Result<Value> {
        println!("ProxyToMaster::send_command: {cmd:?}");

        let cmd_as_value = cmd.to_bulk_array();
        let serialized = serialize(&cmd_as_value)?;
        self.bstream.write_all(serialized.as_bytes()).await?;
        self.bstream.flush().await?;

        let (val, _) = deserialize(&mut self.bstream).await?;
        Ok(val)
    }

    /* async fn receive_value(&mut self) -> Result<Value> {
        let val = deserialize(&mut self.bstream).await?;
        Ok(val)
    } */

    async fn receive_file(&mut self) -> Result<Value> {
        let mut deser = RespDeserializer::from_reader(&mut self.bstream);

        let val = deser.deserialize_file().await?;
        Ok(val)
    }
}

const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

fn get_empty_rdb_bytes() -> Vec<u8> {
    hex_decode(EMPTY_RDB_FILE_HEX).unwrap()
}
