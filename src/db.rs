use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use tokio::io::AsyncWriteExt;
use tokio::io::BufStream;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::async_deser::RespDeserializer;
use crate::commands::Command;
use crate::common::Bytes;
use crate::config::InstanceConfig;
// use crate::io_util::debug_peek;
use crate::misc_util::hex_decode;
use crate::replica_handler::handle_replica;
use crate::svc::ClientInfo;
use crate::svc::ToReplica;
use crate::svc::{handle_stream_async, Query, ToDb};
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

#[derive(Debug)]
struct ReplicaInfo {
    // host_port: String,
    sender: Sender<ToReplica>,
    acked_byte_cnt: u64,
}

pub struct Db {
    h: HashMap<Bytes, ValAndExpiry>,
    cfg: InstanceConfig,
    tx: Sender<ToDb>,
    // Used by replicas
    repl_byte_cnt: usize,
    // Used by Master
    replicas: HashMap<String, ReplicaInfo>,
    replication_id: String,
    replication_offset: u64,
}

impl Db {
    pub fn new(cfg: InstanceConfig, tx: Sender<ToDb>) -> Self {
        Db {
            h: HashMap::new(),
            cfg,
            tx,
            repl_byte_cnt: 0,
            replicas: HashMap::new(),
            replication_id: make_replication_id(now_millis()),
            replication_offset: 0,
        }
    }

    pub async fn run(mut self, mut rx: Receiver<ToDb>) {
        // spawn replication coroutine
        if let Some(master_host_port) = self.cfg.replicaof.clone() {
            let mut proxy = ProxyToMaster::new(master_host_port.as_str()).await;

            println!("Db::run: running replication handshake");
            self.run_replication_handshake(&mut proxy).await.unwrap();

            println!("Db::run: replication handshake FINISHED");
            let bstream = proxy.bstream;
            let repl_tx = self.tx.clone();
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
                    let sx1 = sx.clone();
                    let resp_val = self.execute(&qry, sx1).await;
                    let repl_byte_cnt_inc = resp_val.repl_byte_cnt_inc;
                    if !resp_val.vals.is_empty() {
                        sx.send(resp_val).await.unwrap();
                    } else {
                        println!("Not sending resp_val via channel as there are no values...");
                    }
                    self.repl_byte_cnt += repl_byte_cnt_inc;
                }
                Some(ToDb::PassedReplStream(bstream)) => {
                    let replica_addr = peer_addr_str_v2(&bstream).replace(' ', ":");

                    println!("Query loop received ReplStream({replica_addr})");

                    let (to_replica, repl_receiver) = channel::<ToReplica>(100);

                    tokio::spawn(handle_replica(bstream, repl_receiver, self.tx.clone()));

                    if !self.replicas.contains_key(&replica_addr) {
                        self.replicas.insert(
                            replica_addr.clone(),
                            ReplicaInfo {
                                // host_port: replica_addr,
                                acked_byte_cnt: 0,
                                sender: to_replica,
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

    pub async fn execute(&mut self, query: &Query, sx1: Sender<QueryResult>) -> QueryResult {
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
            ReplConfAck(byte_cnt) => {
                self.exec_repl_conf_ack(*byte_cnt as u64, &query.client_info);
                vec![]
            }
            Wait(n_repls, timeout) => {
                let maybe_val = self.exec_wait(*n_repls as usize, true, *timeout, query, sx1).await;
                match maybe_val {
                    Some(val) => vec![val],
                    None => vec![],
                }
            }
            WaitInternal(n_repls, timeout) => {
                let maybe_val = self.exec_wait(*n_repls as usize, false, *timeout, query, sx1).await;
                match maybe_val {
                    Some(val) => vec![val],
                    None => vec![],
                }
            }
        };

        QueryResult {
            vals: result,
            repl_byte_cnt_inc: query.deser_byte_cnt,
            pass_stream: false,
        }
    }

    async fn exec_set(&mut self, key: &Bytes, val: &Bytes, ex: &Option<u64>) -> Value {
        self.h
            .insert(key.clone(), ValAndExpiry::new(val.clone(), *ex));

        if !self.replicas.is_empty() {
            let cmd = Command::SetKV(key.clone(), val.clone(), *ex);
            let value = cmd.to_bulk_array();
            let bytes = serialize(&value).unwrap().into_inner();
            self.replication_offset += bytes.len() as u64;

            println!("Db::exec_set: attempting replication to {n} replicas.", n=self.replicas.len());

            for (repl_key, replica) in self.replicas.iter() {
                // send_bytes_to_replica(replica, &bytes, "attempting replication").await

                let msg_to_replica = ToReplica::Bytes(
                    bytes.clone(),
                    format!("attempting replication to {repl_key} -- {cmd:?}"),
                );

                replica
                    .sender
                    .send(msg_to_replica)
                    .await
                    .unwrap_or_else(|e| {
                        println!("Unable to send msg to replica via channel, e:{e:?} ")
                    });
            }

        }

        Value::ok()
    }

    fn exec_get(&self, key: &Bytes) -> Value {
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

    fn exec_info(&self, arg: &str) -> Value {
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
        vec![
            "REPLCONF".into(),
            "ACK".into(),
            repl_byte_cnt_str.as_str().into(),
        ]
        .into()
    }

    fn exec_repl_conf_ack(&mut self, byte_cnt: u64, client_info: &ClientInfo) {
        println!("!!! exec_repl_conf_ack: byte_cnt={byte_cnt} client_info={client_info:?}");
        let repl_key = format!(
            "{host}:{port}",
            host = client_info.host,
            port = client_info.port
        );

        let replica = self.replicas.get_mut(&repl_key);
        if let Some(rep) = replica {
            rep.acked_byte_cnt = byte_cnt;
        } else {
            let valid_keys: Vec<&String> = self.replicas.keys().into_iter().collect();
            println!("No replica for key=`{repl_key}`, valid keys are={valid_keys:?}")
        }
    }

    async fn exec_wait(
        &self,
        n_repls: usize,
        req_acks: bool,
        timeout: i64,
        qry: &Query,
        rsx: Sender<QueryResult>,
    ) -> Option<Value> {

        if req_acks {
            // Request acks from replicas
            let get_ack_cmd = Command::ReplConfGetAck("*".to_string());
            let cmd_bytes = serialize(&get_ack_cmd.to_bulk_array())
                .unwrap()
                .into_inner();

            println!("Db::exec_set: requesting acks from {n} replicas", n=self.replicas.len());
            for (_repl_key, replica) in self.replicas.iter() {
                replica
                    .sender
                    .send(ToReplica::Bytes(
                        cmd_bytes.clone(),
                        "requesting getack".into(),
                    ))
                    .await
                    .unwrap();
            }
        }

        let acked_repl_cnt = self
            .replicas
            .iter()
            .map(|(_rkey, ri)| {
                /* println!(
                    "rkey: {rkey} ri.acked_byte_cnt: {rac:?}  my_offset={o}",
                    rac = ri.acked_byte_cnt,
                    o = self.replication_offset
                );*/
                if ri.acked_byte_cnt >= self.replication_offset {
                    1
                } else {
                    0
                }
            })
            .sum::<usize>();

        if acked_repl_cnt >= n_repls || timeout < 0 {
            Some(Value::Int(acked_repl_cnt as i64))
        } else {
            let lapse = 100u64;
            let new_cmd = Command::WaitInternal(n_repls as i64, timeout - (lapse as i64));
            let mut new_qry = qry.clone();
            new_qry.cmd = new_cmd;
            let tx1 = self.tx.clone();
            tokio::spawn(wait_again(lapse, new_qry, tx1, rsx));
            None
        }
    }
}

async fn wait_again(for_millis: u64, new_qry: Query, tx: Sender<ToDb>, rsx: Sender<QueryResult>) {
    let dur = Duration::from_millis(for_millis);
    tokio::time::sleep(dur).await;

    let to_db = ToDb::QueryAndSender(new_qry, rsx);
    tx.send(to_db).await.unwrap()
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
