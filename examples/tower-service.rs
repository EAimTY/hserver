//! Serves a hand-written `tower::Service` over plain HTTP with an explicit accept loop.
//!
//! Run with `cargo run --example tower-service`, then request `http://127.0.0.1:3000/` to see
//! the peer address of the connection.

use hserver::{ConnectionBuilder, ConnectionInfo, Incoming, Server};
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Bytes;
use std::{
    convert::Infallible,
    future::{self, Ready},
    io::Error as IoError,
    task::{Context, Poll},
};
use tokio::net::TcpListener;
use tower_service::Service;

/// Greets the peer by reading the connection address attached by the server.
#[derive(Clone)]
struct Hello;

/// Serves the service on every accepted TCP stream.
#[tokio::main]
async fn main() -> Result<(), IoError> {
    let listener = TcpListener::bind(("127.0.0.1", 3000)).await?;
    println!("listening on http://127.0.0.1:3000/");
    let mut server = Server::new(ConnectionBuilder::new(), Hello);

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

impl Service<Request<Incoming>> for Hello {
    /// Plain text response body.
    type Response = Response<Full<Bytes>>;

    /// The service never fails.
    type Error = Infallible;

    /// Immediately ready response future.
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        let body = request.extensions().get::<ConnectionInfo>().map_or_else(
            || "missing connection info".to_owned(),
            |info| info.peer_addr().to_string(),
        );
        future::ready(Ok(Response::new(Full::from(Bytes::from(body)))))
    }
}
