use anyhow::{format_err, Result};
use std::io;
use std::io::{BufWriter, Write};

use crate::common::Bytes;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    NullBulkString,
    SimpleString(Bytes),
    Array(Vec<Value>),
    Int(i64),
    BulkString(Bytes),
    FileContents(Bytes),
    SimpleError(String),
    BulkError(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub vals: Vec<Value>,
    pub pass_stream: bool,
}

impl From<Vec<Value>> for QueryResult {
    fn from(v: Vec<Value>) -> Self {
        QueryResult {
            vals: v,
            pass_stream: false,
        }
    }
}

impl Value {
    pub fn ok() -> Self {
        Self::SimpleString("OK".into())
    }

    pub fn try_to_string(&self) -> Result<String> {
        match self {
            Self::NullBulkString => Ok("".into()),
            Self::SimpleString(bs) => bs.to_string(),
            Self::BulkString(bs) => bs.to_string(),
            _ => Err(format_err!(
                "Value is not convertable to string, self={self:?}"
            )),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::BulkString(s.into())
    }
}

impl From<&Bytes> for Value {
    fn from(s: &Bytes) -> Self {
        Value::BulkString(s.clone())
    }
}

impl From<&[Value]> for Value {
    fn from(v: &[Value]) -> Value {
        let v1: Vec<Value> = v.into();
        Value::Array(v1)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Value {
        Value::Array(v)
    }
}

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

// Serialization

pub fn serialize(value: &Value) -> Result<Bytes> {
    let mut serializer = RespSerializer::default();
    serializer.serialize(value)?;
    // Ok(serializer.get().into())
    Ok(serializer.writer.into_inner()?.into())
}

pub fn serialize_many(values: &[Value]) -> Result<Bytes> {
    let mut serializer = RespSerializer::default();

    for value in values {
        serializer.serialize(value)?;
    }

    // Ok(serializer.get().into())
    Ok(serializer.writer.into_inner()?.into())
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
            FileContents(bs) => {
                cnt += self.write_len_line(b'$', bs.len())?;
                cnt += self.write(bs.as_bytes())?
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
