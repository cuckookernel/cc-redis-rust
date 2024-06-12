use std::time::Duration;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};

use crate::async_deser;
use crate::common::Bytes;
use crate::misc_util::peer_addr_str;

use crate::commands::{parse_cmd, Command};
use crate::resp::{self, serialize_many, QueryResult};

#[derive(Debug)]
pub enum ToDb {
    QueryAndSender(Query, Sender<QueryResult>),
    PassedReplStream(BufStream<TcpStream>),
}

#[derive(Debug)]
pub struct ClientInfo {
    pub host: String,
}

#[derive(Debug)]
pub struct Query {
    pub cmd: Command,
    // pub is_repl_update: bool,
    pub client_info: ClientInfo,
}

impl Query {
    pub fn new(cmd: Command, /* is_repl_update: bool, */ addr: String) -> Self {
        // let addr = peer_addr_str(stream);

        #[allow(clippy::single_char_pattern)]
        let addr_parts: Vec<&str> = addr.split(":").collect();
        Query {
            cmd,
            // is_repl_update,
            client_info: ClientInfo {
                host: addr_parts[0].to_string(),
            },
        }
    }
}

pub async fn handle_stream_async(
    mut bstream: BufStream<TcpStream>,
    tx: Sender<ToDb>,
    is_replication: bool,
) {
    println!("Starting handle_stream_async(replication={is_replication})\n");

    // debug_peek(format!("before loop (replication={is_replication})").as_str(), &bstream, 64).await;
    loop {
        bstream.get_ref().readable().await.unwrap();

        //let n_peeked = debug_peek("before process_input", &mut bstream, 1).await;
        // if n_peeked == 0 {
        //    tokio::time::sleep(Duration::from_millis(100)).await;
        //    continue
        // }

        let deser_res = async_deser::deserialize(&mut bstream).await;
        match deser_res {
            Ok(input_value) => {
                let addr = peer_addr_str(bstream.get_ref());
                println!(
                    "handle_stream_async(replication={is_replication}): processing_input from:{addr}, value: {input_value:?}"
                );

                let query_result: QueryResult = process_input_async(input_value, addr, &tx).await;

                // Send result, but NOT if we are in replica mode
                if !is_replication {
                    if query_result.vals.len() == 0 {
                        println!("handle_stream_async: 0 output vals; {query_result:?}")
                    }
                    let serialized = serialize_many(&query_result.vals).unwrap();

                    let write_result = bstream.write_all(serialized.as_bytes()).await;
                    if let Err(err) = write_result {
                        println!("handle_stream_async: Error when writing: {err:?}");
                    }
                    let flush_result = bstream.flush().await;
                    if let Err(err) = flush_result {
                        println!("handle_stream_async: Error when flushing: {err:?}");
                    }
                }

                if query_result.pass_stream {
                    tx.send(ToDb::PassedReplStream(bstream)).await.unwrap();
                    break;
                }
            }
            Err(err) => {
                println!("EERRRORR: Failed to deserialize value. err:{err:?}");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }

    println!("END of handle_stream_async(replication={is_replication})\n");
}

async fn process_input_async(
    input_val: resp::Value,
    addr: String,
    // bstream: &mut BufStream<TcpStream>,
    send_to_db: &Sender<ToDb>,
) -> QueryResult {
    // debug_peek("before calling deserialize", &mut bstream, 64).await;

    let query = make_query(&input_val, addr).await.unwrap();
    let (val_s, mut val_r) = mpsc::channel(1);

    send_to_db
        .send(ToDb::QueryAndSender(query, val_s))
        .await
        .unwrap();

    let output_res = val_r.recv().await.unwrap();
    // output_res.unwrap_or_else(|e| vec![resp::s_err(&e.to_string())].into())
    output_res
}

async fn make_query(input_val: &resp::Value, addr: String) -> Result<Query> {
    let cmd_res = parse_cmd(input_val);
    match cmd_res {
        Ok(cmd) => {
            println!("Command parsed: {cmd:?} (from: {addr})", addr = addr);
            let query = Query::new(cmd, addr);
            Ok(query)
        }
        Err(e) => {
            panic!("parse_cmd failed: {e}");
            // Err(anyhow::format_err!("parse_cmd failed: {e}")),
        }
    }
}

pub async fn debug_peek(msg: &str, bstream: &mut BufStream<TcpStream>, n: usize) {

    let output = peek(bstream, n).await;
    println!("{msg} PEEKED ({n}): `{output:?}`", n = output.len());
}

#[allow(dead_code)]
pub async fn peek(bstream: &mut BufStream<TcpStream>, n: usize) -> Bytes {
    // let mut vec = vec![0u8; n]; // Vec::<u8>::with_capacity(n);
    // let mut buf = ReadBuf::new(&mut vec);

    /* let vec = poll_fn( async |cx|
         {
            bstream.get_ref().poll_peek(cx, &mut buf)
        }).await.unwrap();

    */
    let buf = bstream.fill_buf().await.unwrap();
    let n_showed = buf.len().min(n);

    Bytes::from(&buf[..n_showed])
}
