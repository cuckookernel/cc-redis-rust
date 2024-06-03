// Uncomment this block to pass the first stage
use std::{io::Read, io::Write, net::TcpListener};

fn main() {
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut input_buffer = Vec::<u8>::new();
                let read_result = stream.read(input_buffer.as_mut_slice());
                if let Ok(n_bytes_read) = read_result {
                    println!("received {n_bytes_read}: {msg:?}", msg=input_buffer);
                    let write_result = stream.write("+PONG\r\n".as_bytes());
                    if let Err(err) = write_result {
                        println!("Error when writing: {err:?}")
                    }
                } else {
                    println!("Error when receiving {:?}", read_result)
                }
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
