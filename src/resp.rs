use anyhow::Result;
use std::io;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    SimpleString(Vec<u8>),
    Array(Vec<Value>),
    BulkString(Vec<u8>),
    SimpleError(String),
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

    pub fn serialize(&mut self, value: &Value) -> Result<()> {
        use self::Value::*;
        match &value {
            SimpleString(v) => {
                self.write(b"+")?;
                self.writeln(v)?
            }
            Array(elems) => {
                self.write_len_line(b'*', elems.len())?;
                elems
                    .iter()
                    .fold(Ok(()), |acc, elem| acc.and(self.serialize(elem)))?;
                self.newl()?
            }
            BulkString(v) => {
                self.write_len_line(b'$', v.len())?;
                self.writeln(&v)?
            }
            SimpleError(msg) => {
                self.write(b"-")?;
                self.writeln(msg.replace('\r', "\\r").replace('\n', "\\n").as_bytes())?            }
        };
        Ok(())
    }

    pub fn get(&self) -> &[u8] {
        return &self.writer.buffer();
    }

    pub fn newl(&mut self) -> io::Result<usize> {
        self.write(b"\r\n")
    }

    pub fn writeln(&mut self, data: &[u8]) -> io::Result<usize> {
        self.writer.write(data)?;
        self.writer.write(b"\r\n")
    }

    pub fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.writer.write(data)
    }

    pub fn write_len_line(&mut self, c: u8, len: usize) -> io::Result<usize> {
        self.write(&vec![c])?;
        self.writeln(format!("{len}").as_bytes())
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
            b'+' => {
                self.read_until(b'\n', &mut bytes)?;
                let len = bytes.len() - 2; // leave out "\r\n"
                let mut string_bytes = Vec::<u8>::with_capacity(len);
                string_bytes.extend_from_slice(&bytes[..len]);
                Ok(Value::SimpleString(string_bytes))
            }
            b'$' => {
                self.read_until(b'\n', &mut bytes)?;
                let len = String::from_utf8(bytes.clone())?.parse::<usize>()?;
                println!("Reading Bulk string of length: {len}");

                let mut bytes_ = vec![0u8; len];
                self.reader.read_exact(bytes_.as_mut_slice())?;
                self.read_until(b'\n', &mut bytes)?;
                Ok(Value::BulkString(bytes))
            }
            b'*' => {
                self.read_until(b'\n', &mut bytes)?;
                let array_len = String::from_utf8(bytes.clone())?.parse::<usize>()?;

                let mut elems: Vec<Value> = Vec::new();
                for _ in 0..array_len {
                    let elem = self.deserialize()?;
                    elems.push(elem)
                }

                self.read_until(b'\n', &mut bytes)?;
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
            buf.push(c);
            if byte == c {
                break;
            }
        }

        Ok(read)
    }

    pub fn read_one_byte(&mut self) -> Result<u8> {
        let mut one_byte: [u8; 1] = [0u8; 1];
        self.reader.read(&mut one_byte)?;
        Ok(one_byte[0])
    }
}
