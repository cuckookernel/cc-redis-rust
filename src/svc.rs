use std::io;

use anyhow::Result;

use tokio::{
    io::{AsyncWriteExt, BufStream},
    net::TcpStream,
    sync::mpsc::{self, Sender},
};

use crate::{
    async_deser,
    commands::{parse_cmd, Command},
    misc_util::peer_addr_str_v2,
    resp::{self, b_str, serialize_many, QueryResult, Value},
};

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

// long running coroutine that gets requests directly from the buffered stream
// and replies to them.
pub async fn handle_stream_async(
    mut bstream: BufStream<TcpStream>,
    tx: Sender<ToDb>,
    is_replication: bool,
) {
    println!("Starting handle_stream_async(replication={is_replication})\n");

    let mut _eof_cnt = 0usize;

    // debug_peek(format!("before loop (replication={is_replication})").as_str(), &bstream, 64).await;
    loop {
        // bstream.get_ref().readable().await.unwrap();
        //let n_peeked = debug_peek("before process_input", &mut bstream, 1).await;
        // if n_peeked == 0 {
        //    tokio::time::sleep(Duration::from_millis(100)).await;
        //    continue
        // }

        let deser_res = async_deser::deserialize(&mut bstream).await;

        match deser_res {
            Ok(input_value) => {
                let addr = peer_addr_str_v2(&bstream);
                println!(
                    "handle_stream_async(replication={is_replication}): processing_input from:{addr}, value: {input_value:?}"
                );

                let query_result: QueryResult = process_input_async(input_value, addr, &tx).await;

                // Send result, but NOT if we are in replica mode
                if should_reply(is_replication, &query_result) {
                    do_reply(&mut bstream, &query_result).await;
                }

                if query_result.pass_stream {
                    tx.send(ToDb::PassedReplStream(bstream)).await.unwrap();
                    break;
                }
            }
            Err(err) => {
                if let Some(io_err) = err.downcast_ref::<io::Error>() {
                    if io_err.kind() == io::ErrorKind::UnexpectedEof {
                        _eof_cnt += 1;
                    }
                } else {
                    println!("EERRRORR: Failed to deserialize value. err:{err:?}");
                }
                // tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } // match deser_res
    } // loop
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

fn should_reply(is_replication: bool, query_result: &QueryResult) -> bool {
    if !is_replication {
        return true;
    }

    // True if value is `REPLCONF ACK anything``
    if let Value::Array(parts) = &query_result.vals[0] {
        parts.len() >= 2 && parts[0] == b_str("REPLCONF") && parts[1] == b_str("ACK")
    } else {
        false
    }
}

async fn do_reply(bstream: &mut BufStream<TcpStream>, query_result: &QueryResult) {
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
