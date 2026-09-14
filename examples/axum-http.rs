//! Serves an axum `Router` over plain HTTP with an explicit accept loop.
//!
//! Run with `cargo run --example axum-http`, then request `http://127.0.0.1:3000/` to see the
//! peer address of the connection.

use axum::{Router, extract::Extension, routing::get};
use hserver::{ConnectionBuilder, ConnectionInfo, Server};
use std::io::Error as IoError;
use tokio::net::TcpListener;

/// Serves the router on every accepted TCP stream.
#[tokio::main]
async fn main() -> Result<(), IoError> {
    let router: Router = Router::new().route("/", get(hello));

    let listener = TcpListener::bind(("127.0.0.1", 3000)).await?;
    println!("listening on http://127.0.0.1:3000/");
    let mut server = Server::new(ConnectionBuilder::new(), router);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let connection = server.handle(stream, peer_addr);

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("connection error: {error}");
            }
        });
    }
}

/// Greets the peer by reading the connection address attached by the server.
async fn hello(Extension(info): Extension<ConnectionInfo>) -> String {
    format!("hello from {}\n", info.peer_addr())
}
