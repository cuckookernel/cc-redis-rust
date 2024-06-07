// Uncomment this block to pass the first stage
use anyhow::Result;
use mpsc::{Receiver, Sender};
use std::error::Error;
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

mod commands;
mod common;
mod config;
mod db;
mod debug_util;
mod resp;

use commands::{parse_cmd, Command};
use common::Bytes;
use config::InstanceConfig;
use db::Db;
use debug_util::{self as dbgu, peer_addr_str};

type CmdAndSender = (Command, Sender<resp::Value>);



#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = InstanceConfig::from_command_args();
    let port = config.port();

    println!("Logs from your program will appear here!");

    // Channel for commands directed at Db
    let (tx, rx): (Sender<CmdAndSender>, Receiver<CmdAndSender>) = mpsc::channel(100);

    tokio::spawn(async move { handle_db_commands(rx, config).await });

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    println!("\nOpened Listener");
    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("Accepted new client: {:?}", addr);
                let tx1 = tx.clone();
                tokio::spawn(async move { handle_client(&mut stream, tx1).await });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_db_commands(
    mut rx: Receiver<CmdAndSender>,
    cfg: InstanceConfig
) {
    // long running co-routine that gets commands from only channel and executes them on the Db
    println!("handle_db_commands: Setting up Db object.");
    let mut db = Db::new(cfg);

    println!("handle_db_commands: Starting loop");
    loop {
        match rx.recv().await {
            Some((cmd, sx)) => {
                let resp_val = db.execute(&cmd);
                sx.send(resp_val).await.unwrap()
            }
            None => {
                println!("handle_commands: Incomming command channel closed. STOPPING");
                break;
            }
        }
    }
}

async fn handle_client(stream: &mut TcpStream, tx: Sender<CmdAndSender>) {
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
                    addr = peer_addr_str(stream),
                    n_bytes = input_buffer.len(),
                    msg = dbgu::format_bytes_dbg(&input_buffer)
                );

                let output_val = proc_input(&input_buffer, &tx).await;

                let mut ser = resp::RespSerializer::new();
                ser.serialize(&output_val).unwrap();

                let write_result = stream.try_write(ser.get());
                if let Err(err) = write_result {
                    println!("handle_client: Error when writing: {err:?}");
                    return;
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

async fn proc_input(input_buffer: &[u8], tx: &Sender<CmdAndSender>) -> resp::Value {
    let input_val = resp::deserialize(input_buffer);
    let output_res = match input_val {
        Ok(val) => cmd_to_db(&val, tx).await,
        Err(e) => Err(e),
    };
    output_res.unwrap_or_else(|e| resp::Value::BulkError(e.to_string()))
}

async fn cmd_to_db(input_val: &resp::Value, sender: &Sender<CmdAndSender>) -> Result<resp::Value> {
    let cmd_res = parse_cmd(input_val);
    match cmd_res {
        Ok(cmd) => {
            println!("Command parsed: {cmd:?}");
            let (val_s, mut val_r) = mpsc::channel(1);
            sender.send((cmd, val_s)).await?;
            Ok(val_r.recv().await.unwrap())
        }
        Err(e) => {
            panic!("parse_cmd failed: {e}");
            // Err(anyhow::format_err!("parse_cmd failed: {e}")),
        }
    }
}
