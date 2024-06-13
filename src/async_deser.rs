use std::future::Future;
use std::pin::Pin;

use anyhow::Result;

use crate::io_util::debug_peek;
use crate::misc_util::peer_addr_str_v2;
use crate::resp::{parse_len, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio::{io::BufStream, net::TcpStream};

pub async fn deserialize(bstream: &mut BufStream<TcpStream>) -> Result<(Value, usize)> {
    let deser = RespDeserializer::from_reader(bstream);

    let (val, deser_byte_cnt, _) = deser.deserialize().await?;
    Ok((val, deser_byte_cnt))
}

pub struct RespDeserializer<'a> {
    bstream: &'a mut BufStream<TcpStream>,
    #[allow(dead_code)]
    addr: String,
}

const LF: u8 = b'\n';

impl<'a> RespDeserializer<'a> {
    pub fn from_reader(bstream: &'a mut BufStream<TcpStream>) -> Self {
        let addr = peer_addr_str_v2(&bstream);
        Self { bstream, addr }
    }

    pub async fn deserialize_file(&mut self) -> Result<Value> {
        let first_byte = self.bstream.read_u8().await?;

        let mut bytes: Vec<u8> = Vec::with_capacity(64);
        assert_eq!(first_byte, b'$');

        self.bstream.read_until(LF, &mut bytes).await?;
        let len = parse_len(&bytes)?;
        println!("Reading FILE of length: {len}");
        let mut bytes_ = vec![0u8; len];
        self.bstream.read_exact(bytes_.as_mut_slice()).await?;
        debug_peek("After reading file: ", self.bstream, 128).await;

        Ok(Value::FileContents(bytes_.into()))
    }

    pub fn deserialize(
        mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(Value, usize, Self)>> + Send + 'a>> {
        let mut bytes: Vec<u8> = Vec::with_capacity(64);

        Box::pin(async {
            // debug_peek("deserialize starts: ", self.bstream, 128).await;

            let first_byte = self.bstream.read_u8().await?;
            // println!("{addr}: first_byte:`{ch}`", addr=self.addr, ch=first_byte as char);
            let mut deser_byte_cnt = 1;
            match first_byte {
                b'+' => {
                    // SimpleString
                    deser_byte_cnt += self.bstream.read_until(LF, &mut bytes).await?;
                    let len = bytes.len() - 2; // leave out "\r\n"
                    Ok((Value::SimpleString((&bytes[..len]).into()), deser_byte_cnt, self))
                }
                b':' => {
                    deser_byte_cnt += self.bstream.read_until(LF, &mut bytes).await?;
                    let i = String::from_utf8(bytes)?.trim_end().parse::<i64>()?;
                    Ok((Value::Int(i), deser_byte_cnt, self))
                }
                b'$' => {
                    deser_byte_cnt += self.bstream.read_until(LF, &mut bytes).await?;
                    let len = parse_len(&bytes)?;
                    // println!("Reading bulkstring of length: {len}");

                    let mut bytes_ = vec![0u8; len];
                    // println!("After reading bulkstring: bytes_ has {n}", n=bytes_.len());
                    deser_byte_cnt += self.bstream.read_exact(bytes_.as_mut_slice()).await?;
                    // debug_peek("PEEEEKING:", self.bstream, 128).await;
                    deser_byte_cnt += self.bstream.read_until(LF, &mut bytes).await?;

                    Ok((Value::BulkString(bytes_.into()), deser_byte_cnt, self))
                }
                b'*' => {
                    self.bstream.read_until(LF, &mut bytes).await?;
                    let array_len = parse_len(&bytes)?;

                    let mut elems: Vec<Value> = Vec::new();
                    for _i in 0..array_len {
                        // debug_peek(format!("reading array elem: {i}").as_str(), &self.bstream, 16).await;
                        let (elem, cnt_inc, deser_) = self.deserialize().await?;
                        deser_byte_cnt += cnt_inc;
                        self = deser_;
                        elems.push(elem);
                        // println!("elems has: {n}: {elems:?}", n=elems.len());
                    }
                    Ok((Value::Array(elems), deser_byte_cnt, self))
                }
                _ => Err(anyhow::format_err!(
                    "Invalid starting byte = `{first_byte}`"
                )),
            }
        })
    }
}
