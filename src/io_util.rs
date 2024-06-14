// use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufStream};
use tokio::net::TcpStream;

use crate::common::Bytes;

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
