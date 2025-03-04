use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use bytes::{Buf, BufMut, BytesMut};

const API_VERSIONS_KEY: i16 = 18;
const API_VERSIONS_MIN_VERSION: i16 = 0;
const API_VERSIONS_MAX_VERSION: i16 = 4;
const ERROR_CODE: i16 = 0;

fn handle_connection(mut stream: TcpStream) {
    let request = read_request(&mut stream);
    let mut request = request.as_slice();

    let _request_api_key = request.get_i16();
    let _request_api_version = request.get_i16();
    let correlation_id = request.get_i32();

    // Create response buffer
    let mut response = BytesMut::new();

    // Reserve space for message length (to be filled later)
    response.put_i32(0); // Placeholder for length
    response.put_i32(correlation_id); // Correlation ID
    response.put_i16(ERROR_CODE); // Error code

    // API_VERSIONS response body
    response.put_i32(1); // Number of API keys (at least one entry)
    response.put_i16(API_VERSIONS_KEY); // API key 18 (API_VERSIONS)
    response.put_i16(API_VERSIONS_MIN_VERSION); // Min Version
    response.put_i16(API_VERSIONS_MAX_VERSION); // Max Version

    // Compute and update message length
    let message_length = (response.len() - 4) as i32; // Excluding the length field itself
    response[0..4].copy_from_slice(&message_length.to_be_bytes());

    // Send response
    stream.write_all(&response).unwrap();
}

fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let request_length = read_request_length(stream);

    let mut request_buffer = vec![0; request_length];
    stream.read_exact(&mut request_buffer).unwrap();

    request_buffer
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
