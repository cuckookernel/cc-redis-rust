// Uncomment this block to pass the first stage

use std::io;
use tokio::net::{TcpListener, TcpStream};
use std::error::Error;
use std::time::Duration;


pub mod debug_util;

use debug_util as dbgu;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    println!("\nOpened Listener");

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("Accepted new client: {:?}", addr);
                tokio::spawn(async move {handle_client(&mut stream).await});
                println!("After join")
            },
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}


async fn handle_client(stream: &mut TcpStream) { // -> Result<(), Box<dyn Error>> {
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
                println!("received (from {addr}) {n_bytes}: {msg:?}",
                         addr=stream.peer_addr().map(|a| a.to_string()).unwrap_or("<undefined>".to_string()),
                         n_bytes=input_buffer.len(),
                         msg=dbgu::format_bytes_dbg(&input_buffer));

                let write_result = stream.try_write("+PONG\r\n".as_bytes());
                if let Err(err) = write_result {
                    println!("Error when writing: {err:?}");
                    return // Err(err.into())
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

