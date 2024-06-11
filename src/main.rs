// Uncomment this block to pass the first stage
use anyhow::Result;

use io_util::{handle_stream, ToDb};
use mpsc::{Receiver, Sender};
use std::error::Error;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

mod commands;
mod common;
mod config;
mod db;
mod io_util;
mod misc_util;
mod resp;

use config::InstanceConfig;
use db::Db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = InstanceConfig::from_command_args();
    println!("Config from args: {config:?}");
    let port = config.port();

    println!("Logs from your program will appear here!");

    // Channel for commands directed at Db
    let (tx, rx): (Sender<ToDb>, Receiver<ToDb>) = mpsc::channel(100);

    println!("main: Setting up Db object.");
    let db = Db::new(config);
    tokio::spawn(db.run(tx.clone(), rx));

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    println!("\nOpened Listener");
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted new client: {:?}", addr);
                let tx1 = tx.clone();
                tokio::spawn(async move { handle_stream(stream, tx1, false).await });
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}
