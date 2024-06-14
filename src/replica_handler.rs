use std::io;

use anyhow::Result;
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::async_deser;
use crate::misc_util::peer_addr_str_v2;
use crate::resp::QueryResult;
use crate::resp::Value;
use crate::svc::{do_reply, process_input_async, ToDb, ToReplica};

pub async fn handle_replica(
    mut bstream: BufStream<TcpStream>,
    mut repl_recv: Receiver<ToReplica>,
    tx: Sender<ToDb>,
) {
    let addr = peer_addr_str_v2(&bstream);
    println!("\n\nStarting handle_replica from: {addr}\n");

    let mut eof_cnt = 0usize;

    loop {
        let value_from_replica = async_deser::deserialize(&mut bstream);
        let msg_to_replica = repl_recv.recv();

        tokio::select! {
            deser_res = value_from_replica => {
                eof_cnt = handle_incoming_val_from_replica(deser_res, &mut bstream, &tx, eof_cnt).await;
                if eof_cnt > 10000 {
                    // probably disconnected
                    // break
                }
            }
            opt_msg = msg_to_replica => {
                match opt_msg {
                    Some(ToReplica::Bytes(bytes, action)) => {
                        send_bytes_to_replica(&mut bstream, &bytes, &action).await;
                    }
                    None => {

                    }
                }
            }
        };
    } // loop
    // println!("\n\nEND of handle_replica -- from: {addr}\n\n");
}

async fn handle_incoming_val_from_replica(
    deser_res: Result<(Value, usize)>,
    bstream: &mut BufStream<TcpStream>,
    tx: &Sender<ToDb>,
    eof_cnt: usize,
) -> usize {
    let addr = peer_addr_str_v2(&bstream);
    match deser_res {
        Ok((input_value, deser_byte_cnt)) => {
            println!("handle_replica: processing_input from:{addr}, value: {input_value:?}");

            let query_result: QueryResult =
                process_input_async(input_value, deser_byte_cnt, &addr, &tx).await;

            // if should_reply(is_replication, &query_result) {
            do_reply(bstream, &query_result).await;
            // }
        }
        Err(err) => {
            if let Some(io_err) = err.downcast_ref::<io::Error>() {
                if io_err.kind() == io::ErrorKind::UnexpectedEof {
                    return eof_cnt + 1;
                }
            } else {
                println!("EERRRORR: Failed to deserialize value. err:{err:?}");
            }
        }
    } // match deser_res
    eof_cnt
}

async fn send_bytes_to_replica(bstream: &mut BufStream<TcpStream>, bytes: &[u8], action: &str) {
    bstream.write_all(bytes).await.unwrap_or_else(|e| {
        let addr = peer_addr_str_v2(bstream);
        println!("ERROR when {action} to {addr}, err={e:?}")
    });

    bstream
        .flush()
        .await
        .unwrap_or_else(|e| println!("ERROR: when flushing after {action}, err={e:?}"));
}
