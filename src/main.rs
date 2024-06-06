// Uncomment this block to pass the first stage

use cmd_parsing::parse_cmd;
use std::error::Error;
use std::io;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};

mod common;
mod cmd_parsing;
mod cmd_proc;
mod commands;
mod debug_util;
mod resp;

use debug_util::{self as dbgu, peer_addr_str};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    println!("\nOpened Listener");

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("Accepted new client: {:?}", addr);
                tokio::spawn(async move { handle_client(&mut stream).await });
                println!("After join")
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_client(stream: &mut TcpStream) {
    // -> Result<(), Box<dyn Error>> {
    // let mut input_buffer = Vec::<u8>::with_capacity(128);
    // let mut empty_cnt = 0;

    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        stream.readable().await.unwrap();

        // let read_result = read_until_exhausted(&mut stream);

        let mut input_buffer = Vec::with_capacity(4096);

        // Try to read data, this may still fail with `WouldBlock`
        // if the readiness event is a false positive.
        match stream.try_read_buf(&mut input_buffer) {
            Ok(0) => break,
            Ok(_) => {
                println!(
                    "received (from {addr}) {n_bytes}: {msg:?}",
                    addr = peer_addr_str(stream),
                    n_bytes = input_buffer.len(),
                    msg = dbgu::format_bytes_dbg(&input_buffer)
                );

                let input_val = resp::deserialize(&input_buffer);

                let output_val = match input_val {
                    Ok(val) => {
                        let cmd = parse_cmd(&val);
                        cmd_proc::process_cmd(&cmd)
                    }
                    Err(_e) => {
                        eprintln!(
                            "Error interpreting input as utf8, input=`{msg}`",
                            msg = dbgu::format_bytes_dbg(&input_buffer)
                        );
                        return;
                    }
                };

                let mut ser = resp::RespSerializer::new();
                ser.serialize(&output_val).unwrap();

                let write_result = stream.try_write(ser.get());
                if let Err(err) = write_result {
                    println!("Error when writing: {err:?}");
                    return; // Err(err.into())
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(_e) => {
                return; //  Err(e.into());
            }
        };
    }
    // Ok(())
}
