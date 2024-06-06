use anyhow::{format_err, Result};
use std::io;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};

use super::common::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    NullBulkString,
    SimpleString(Bytes),
    Array(Vec<Value>),
    Int(i64),
    BulkString(Bytes),
    SimpleError(String),
    BulkError(String),
}

impl Value {
    pub fn ok() -> Self {
        Self::SimpleString("OK".into())
    }
}

#[allow(dead_code)]
pub fn s_str(s: &str) -> Value {
    Value::SimpleString(s.into())
}

#[allow(dead_code)]
pub fn b_str(s: &str) -> Value {
    Value::BulkString(s.into())
}

#[allow(dead_code)]
pub fn s_err(s: &str) -> Value {
    Value::SimpleError(s.to_string())
}

pub struct RespSerializer {
    // buf: Vec<u8>,
    writer: BufWriter<Vec<u8>>,
}

impl RespSerializer {
    pub fn new() -> Self {
        Self {
            writer: BufWriter::new(Vec::new()),
        }
    }

    pub fn serialize(&mut self, value: &Value) -> Result<usize> {
        use self::Value::*;

        let mut cnt = 0usize; // accumulator count of bytes written

        match &value {
            NullBulkString => {
                cnt += self.write(b"$")?;
                cnt += self.writeln("-1".as_bytes())?;
            }
            SimpleString(v) => {
                cnt += self.write(b"+")?;
                cnt += self.writeln(v.as_bytes())?;
            }
            Int(i) => {
                cnt += self.write(b":")?;
                cnt += self.writeln(format!("{i}").as_bytes())?;
            }
            Array(elems) => {
                cnt += self.write_len_line(b'*', elems.len())?;
                cnt += elems.iter().try_fold(0usize, |acc, elem| {
                    self.serialize(elem).map(|cnt| acc + cnt)
                })?;
            }
            BulkString(v) => {
                cnt += self.write_len_line(b'$', v.len())?;
                cnt += self.writeln(v.as_bytes())?
            }
            SimpleError(msg) => {
                cnt += self.write(b"-")?;
                cnt += self.writeln(msg.replace('\r', "\\r").replace('\n', "\\n").as_bytes())?
            }
            BulkError(msg) => {
                cnt += self.write_len_line(b'!', msg.len())?;
                cnt += self.writeln(msg.as_bytes())?;
            }
        };
        Ok(cnt)
    }

    pub fn get(&self) -> &[u8] {
        return self.writer.buffer();
    }

    pub fn writeln(&mut self, data: &[u8]) -> io::Result<usize> {
        Ok(self.writer.write(data)? + self.writer.write(b"\r\n")?)
    }

    pub fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.writer.write(data)
    }

    pub fn write_len_line(&mut self, c: u8, len: usize) -> io::Result<usize> {
        self.write(&[c])?;
        self.writeln(format!("{len}").as_bytes())
    }
}

impl Default for RespSerializer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RespDeserializer {
    reader: BufReader<Cursor<Vec<u8>>>,
}

impl RespDeserializer {
    pub fn new(v: Vec<u8>) -> Self {
        Self {
            reader: BufReader::new(Cursor::new(v)),
        }
    }

    pub fn deserialize(&mut self) -> Result<Value> {
        let mut bytes: Vec<u8> = Vec::with_capacity(64);

        let one_byte = self.read_one_byte()?;
        match one_byte {
            b'+' => { // SimpleString
                self.read_until(b'\n', &mut bytes)?;
                let len = bytes.len() - 2; // leave out "\r\n"
                Ok(Value::SimpleString((&bytes[..len]).into()))
            }
            b':' => {
                self.read_until(b'\n', &mut bytes)?;
                let i = String::from_utf8(Vec::from(bytes))?.trim_end().parse::<i64>()?;
                Ok(Value::Int(i))
            }
            b'$' => {
                self.read_until(b'\n', &mut bytes)?;
                let len = parse_len(&bytes)?;

                let mut bytes_ = vec![0u8; len];
                self.reader.read_exact(bytes_.as_mut_slice())?;
                self.read_until(b'\n', &mut bytes)?;
                Ok(Value::BulkString(bytes_.into()))
            }
            b'*' => {
                self.read_until(b'\n', &mut bytes)?;
                let array_len = parse_len(&bytes)?;

                let mut elems: Vec<Value> = Vec::new();
                for _ in 0..array_len {
                    let elem = self.deserialize()?;
                    elems.push(elem)
                }
                Ok(Value::Array(elems))
            }
            _ => Err(anyhow::format_err!("Invalid starting byte = {one_byte}")),
        }
    }

    pub fn read_until(&mut self, c: u8, buf: &mut Vec<u8>) -> Result<usize> {
        buf.clear();
        let mut read: usize = 0;
        loop {
            let byte = self.read_one_byte()?;
            read += 1;
            buf.push(byte);
            if byte == c {
                break;
            }
        }

        Ok(read)
    }

    pub fn read_one_byte(&mut self) -> Result<u8> {
        let mut one_byte: [u8; 1] = [0u8; 1];
        _ = self.reader.read(&mut one_byte)?;
        Ok(one_byte[0])
    }
}

pub fn parse_len(bytes: &[u8]) -> Result<usize> {
    String::from_utf8(Vec::from(bytes))?
        .trim_end()
        .parse::<usize>()
        .map_err(|e| {
            println!(
                "parse_len: bytes=`{s}`",
                s = String::from_utf8(Vec::from(bytes)).unwrap_or("???".to_string())
            );
            e.into()
        })
}

pub fn deserialize(data: &[u8]) -> Result<Value> {
    let mut deser = RespDeserializer::new(Vec::from(data));
    deser.deserialize().map_err(|err| {
        format_err!(
            "Error interpretting byte as Value bytes=`{bytes:?}`, err={err}",
            bytes = Bytes::from(data)
        )
    })
}
