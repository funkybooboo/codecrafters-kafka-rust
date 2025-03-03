use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use bytes::{Buf, BufMut};

fn handle_connection(mut stream: TcpStream) {
    let mut request = read_request(&mut stream);

    let request_api_key = request.get_i16();
    let request_api_version = request.get_i16();
    let correlation_id = request.get_i32();

    let mut response = Vec::with_capacity(8);
    response.put_i32(0);
    response.put_i32(correlation_id);

    stream.write_all(&response).unwrap();
}

fn read_request(stream: &mut TcpStream) -> &[u8] {
    let request_length = read_request_length(stream);

    let mut request_buffer = vec![0; request_length];
    stream.read_exact(&mut request_buffer).unwrap();

    request_buffer.as_slice()
}

fn read_request_length(stream: &mut TcpStream) -> usize {
    let mut length_bytes = [0; 4];
    stream.read_exact(&mut length_bytes).unwrap();

    i32::from_be_bytes(length_bytes) as usize
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    println!("Server listening on 127.0.0.1:9092...");

    for connection in listener.incoming() {
        match connection {
            Ok(stream) => {
                println!("Accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                eprintln!("Connection error: {}", e);
            }
        }
    }
}
