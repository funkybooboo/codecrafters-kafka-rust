#![allow(unused_imports)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use bytes::{Buf, BufMut};

fn handle_connection(mut stream: TcpStream) {
    let mut len = [0; 4];
    stream.read_exact(&mut len).unwrap();
    let len = i32::from_be_bytes(len) as usize;

    let mut request = vec![0; len];
    stream.read_exact(&mut request).unwrap();

    let mut request = request.as_slice();
    let _request_api_key = request.get_i16();
    let _request_api_version = request.get_i16();
    let correlation_id = request.get_i32();

    let mut response = Vec::with_capacity(8);
    response.put_i32(0);
    response.put_i32(correlation_id);
    stream.write_all(&response).unwrap();
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection");
                handle_connection(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
