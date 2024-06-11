use std::io;

use anyhow::Result;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};

use crate::misc_util::peer_addr_str;

use crate::commands::{parse_cmd, Command};
use crate::common::Bytes;
use crate::resp::{self, serialize_many, QueryResult};

#[derive(Debug)]
pub enum ToDb {
    QueryAndSender(Query, Sender<QueryResult>),
    PassedReplStream(TcpStream),
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
    pub fn new(cmd: Command, /* is_repl_update: bool, */ stream: &TcpStream) -> Self {
        let addr = peer_addr_str(stream);

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

pub async fn handle_stream(stream: TcpStream, tx: Sender<ToDb>, is_replication: bool) {
    loop {
        stream.readable().await.unwrap();
        let mut input_buffer = Vec::with_capacity(4096);

        // Try to read data, this may still fail with `WouldBlock`
        // if the readiness event is a false positive.
        match stream.try_read_buf(&mut input_buffer) {
            Ok(0) => break,
            Ok(_) => {
                println!(
                    "handle_client: received input (from {addr}) {n_bytes} bytes:\n{msg:?}",
                    addr = peer_addr_str(&stream),
                    n_bytes = input_buffer.len(),
                    msg = format!("{:?}", Bytes::from(input_buffer.as_slice()))
                );

                let query_result: QueryResult = process_input(&stream, &input_buffer, &tx).await;

                // Send result, but NOT if we in replica mode
                if !is_replication {
                    let serialized = serialize_many(&query_result.vals).unwrap();

                    let write_result = stream.try_write(serialized.as_bytes());
                    if let Err(err) = write_result {
                        println!("handle_client: Error when writing: {err:?}");
                        return;
                    }
                }

                if query_result.pass_stream {
                    tx.send(ToDb::PassedReplStream(stream)).await.unwrap();
                    break;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(err) => {
                println!("handle_client: Error when reading from buffer: {err:?}");
                return; //  Err(e.into());
            }
        };
    }
}

async fn process_input(
    stream: &TcpStream,
    input_buffer: &[u8],
    send_to_db: &Sender<ToDb>,
) -> QueryResult {
    let input_val = resp::deserialize(input_buffer);

    let output_res = match input_val {
        Ok(val) => {
            let query = make_query(stream, &val).await.unwrap();
            let (val_s, mut val_r) = mpsc::channel(1);
            send_to_db
                .send(ToDb::QueryAndSender(query, val_s))
                .await
                .unwrap();
            Ok(val_r.recv().await.unwrap())
        }
        Err(e) => Err(e),
    };
    output_res.unwrap_or_else(|e| vec![resp::s_err(&e.to_string())].into())
}

async fn make_query(
    stream: &TcpStream,
    input_val: &resp::Value,
    // is_repl_update: bool,
) -> Result<Query> {
    let cmd_res = parse_cmd(input_val);
    match cmd_res {
        Ok(cmd) => {
            println!("Command parsed: {cmd:?}");
            let query = Query::new(cmd, stream);
            Ok(query)
        }
        Err(e) => {
            panic!("parse_cmd failed: {e}");
            // Err(anyhow::format_err!("parse_cmd failed: {e}")),
        }
    }
}
