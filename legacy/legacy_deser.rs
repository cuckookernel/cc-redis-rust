use std::io::Read;

pub async fn handle_stream_buffer(stream: TcpStream, tx: Sender<ToDb>, is_replication: bool) {
    println!("starting handle_stream(replication={is_replication})\n");

    loop {
        stream.readable().await.unwrap();
        let mut input_buffer = Vec::with_capacity(4096);

        // Try to read data, this may still fail with `WouldBlock`
        // if the readiness event is a false positive.
        match stream.try_read_buf(&mut input_buffer) {
            Ok(0) => break,
            Ok(_) => {
                println!(
                    "handle_stream(replication={is_replication}): received input (from {addr}) {n_bytes} bytes:\n{msg:?}",
                    addr = peer_addr_str(&stream),
                    n_bytes = input_buffer.len(),
                    msg = format!("{:?}", Bytes::from(input_buffer.as_slice()))
                );

                let query_result: QueryResult = process_input_buffer(&stream, &input_buffer, &tx).await;

                // Send result, but NOT if we are in replica mode
                if !is_replication {
                    let serialized = serialize_many(&query_result.vals).unwrap();

                    let write_result = stream.try_write(serialized.as_bytes());
                    if let Err(err) = write_result {
                        println!("handle_stream: Error when writing: {err:?}");
                        return;
                    }
                }

                if query_result.pass_stream {
                    tx.send(ToDb::PassedReplStream(stream)).await.unwrap();
                    break;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(err) => {
                println!("handle_client: Error when reading from buffer: {err:?}");
                return; //  Err(e.into());
            }
        };
    }
}


async fn process_input_buffer(
    stream: &TcpStream,
    input_buffer: &[u8],
    send_to_db: &Sender<ToDb>,
) -> QueryResult {
    let input_val = resp::deserialize_v0(input_buffer);

    let output_res = match input_val {
        Ok(val) => {
            let query = make_query(stream, &val).await.unwrap();
            let (val_s, mut val_r) = mpsc::channel(1);
            send_to_db
                .send(ToDb::QueryAndSender(query, val_s))
                .await
                .unwrap();
            Ok(val_r.recv().await.unwrap())
        }
        Err(e) => Err(e),
    };
    output_res.unwrap_or_else(|e| vec![resp::s_err(&e.to_string())].into())
}


pub struct RespDeserializerV0 {
    stream: std::io::BufStream<Cursor<Vec<u8>>>,
}

impl RespDeserializerV0 {
    pub fn new(v: Vec<u8>) -> Self {
        Self {
            reader: std::io::BufStream::new(Cursor::new(v)),
        }
    }

    pub fn deserialize(&mut self) -> Result<Value> {
        let mut bytes: Vec<u8> = Vec::with_capacity(64);

        let one_byte = self.read_one_byte()?;
        match one_byte {
            b'+' => {
                // SimpleString
                self.read_until(b'\n', &mut bytes)?;
                let len = bytes.len() - 2; // leave out "\r\n"
                Ok(Value::SimpleString((&bytes[..len]).into()))
            }
            b':' => {
                self.read_until(b'\n', &mut bytes)?;
                let i = String::from_utf8(bytes)?.trim_end().parse::<i64>()?;
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

pub fn deserialize_v0(data: &[u8]) -> Result<Value> {
    let mut deser = RespDeserializerV0::new(Vec::from(data));
    deser.deserialize().map_err(|err| {
        format_err!(
            "Error interpretting byte as Value bytes=`{bytes:?}`, err={err}",
            bytes = Bytes::from(data)
        )
    })
}

pub async fn get_value_from_stream_v0(stream: &mut TcpStream) -> Result<Value> {
    stream.readable().await?;

    let mut input_buffer = Vec::with_capacity(4096);
    let bytes_read = stream.read_buf(&mut input_buffer).await?;

    println!(
        "get_value_from_stream: received (from {addr}) {n_bytes} (={bytes_read}?) bytes:\n{msg:?}",
        addr = peer_addr_str(stream),
        n_bytes = input_buffer.len(),
        msg = format!("{:?}", Bytes::from(input_buffer.as_slice()))
    );

    let mut deser = RespDeserializerV0::new(input_buffer);
    deser.deserialize()
}

pub fn deserialize_v0(
    mut self,
) -> Pin<Box<dyn Future<Output = Result<(Value, usize, Self)>> + Send + 'a>> {
    let mut bytes: Vec<u8> = Vec::with_capacity(64);

    Box::pin(async {
        // debug_peek("deserialize starts: ", self.bstream, 128).await;
        let first_byte = self.bstream.read_u8().await?;
        // println!("{addr}: first_byte:`{ch}`", addr=self.addr, ch=first_byte as char);
        let mut deser_byte_cnt = 1;
        let output = match first_byte {
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
                deser_byte_cnt += self.bstream.read_until(LF, &mut bytes).await?;
                let array_len = parse_len(&bytes)?;

                let mut elems: Vec<Value> = Vec::new();
                for _i in 0..array_len {
                    // debug_peek(format!("reading array elem: {i}").as_str(), &self.bstream, 16).await;
                    let (elem, cnt_inc, deser_) = self.deserialize_v0().await?;
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
        }; // match
        // println!("deser: RESULT  deser_byte_cnt={deser_byte_cnt}");
        // Ok(result)
        output
    } // async
    ) // pin
}
