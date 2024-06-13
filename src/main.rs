// Uncomment this block to pass the first stage
use anyhow::Result;

mod async_deser;
mod commands;
mod common;
mod config;
mod db;
mod io_util;
mod misc_util;
mod resp;
mod svc;

use config::InstanceConfig;
use db::Db;
use log::info;
use mpsc::{Receiver, Sender};
use std::error::Error;
use std::time::Duration;
use tokio::io::BufStream;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use svc::ToDb;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = InstanceConfig::from_command_args();
    info!("Config from args: {config:?}");
    let port = config.port();

    info!("Logs from your program will appear here!");

    // Channel for commands directed at Db
    let (tx, rx): (Sender<ToDb>, Receiver<ToDb>) = mpsc::channel(100);

    println!("main: Setting up Db object.");
    let db = Db::new(config);
    tokio::spawn(db.run(tx.clone(), rx));

    tokio::time::sleep(Duration::from_millis(3000)).await;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    println!("\nOpened Listener (on port: {port})");
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted new client (on {port}): peer={addr:?}");
                let tx1 = tx.clone();
                let bstream = BufStream::new(stream);
                tokio::spawn(async move { svc::handle_stream_async(bstream, tx1, false).await });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}
