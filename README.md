# hserver

[![Version](https://img.shields.io/crates/v/hserver.svg?style=flat)](https://crates.io/crates/hserver)
[![Documentation](https://img.shields.io/badge/docs-release-brightgreen.svg?style=flat)](https://docs.rs/hserver)
[![License](https://img.shields.io/crates/l/hserver.svg?style=flat)](https://github.com/EAimTY/hserver/blob/master/LICENSE)

`hserver` is a configurable HTTP server adaptor built on top of `hyper` and `tower`. It is designed for applications that want Hyper's connection engine without giving up control of the listener, accept loop, stream source, or task spawning strategy.

It handles the repetitive integration work of:

- adapting a `tower::Service` into Hyper's service model,
- configuring HTTP/1 and HTTP/2 through a shared connection builder,
- attaching peer metadata to each request, and
- optionally performing a Rustls TLS handshake before serving the connection,
- exposing separate regular and upgrade-capable connection futures.

## Why hserver

`hserver` sits between raw Hyper plumbing and full web frameworks.

- Unlike higher-level frameworks, it does not take ownership of your listener, connection acceptance policy, or task lifecycle.
- Unlike hand-rolled Hyper integration, it packages the repetitive parts of service adaptation, protocol negotiation, peer metadata propagation, and TLS handshakes into one reusable layer.
- Unlike framework-specific server types, it works directly with a cloned `tower_service::Service`, so you can keep your own service stack and runtime architecture.

This crate is a good fit when you need a server layer that is explicit, composable, and highly configurable rather than opinionated.

## Feature flags

- `http1` (default): enables [`ConnectionBuilder::http1`](https://docs.rs/hserver/latest/hserver/struct.ConnectionBuilder.html#method.http1).
- `http2` (default): enables [`ConnectionBuilder::http2`](https://docs.rs/hserver/latest/hserver/struct.ConnectionBuilder.html#method.http2).
- `tls`: enables Rustls support and changes [`Server::new`](https://docs.rs/hserver/latest/hserver/struct.Server.html#method.new), [`Server::handle`](https://docs.rs/hserver/latest/hserver/struct.Server.html#method.handle), and [`Server::handle_upgradable`](https://docs.rs/hserver/latest/hserver/struct.Server.html#method.handle_upgradable) to perform a TLS handshake before building the HTTP connection.

At least one of `http1` or `http2` must be enabled.

## Example

The example below works with and without the `tls` feature. With `tls` enabled, `handle` returns a [`TlsHandshake`](https://docs.rs/hserver/latest/hserver/struct.TlsHandshake.html), which is then awaited to obtain the HTTP [`Connection`](https://docs.rs/hserver/latest/hserver/struct.Connection.html).

```rust,no_run
use std::{
    convert::Infallible,
    future::{ready, Ready},
    task::{Context, Poll},
};

use hserver::{ConnectionBuilder, ConnectionInfo, Incoming, Server};
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Bytes;
use tokio::net::TcpListener;
use tower_service::Service;

#[derive(Clone)]
struct Hello;

impl Service<Request<Incoming>> for Hello {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        let peer = request
            .extensions()
            .get::<ConnectionInfo>()
            .map(ConnectionInfo::peer_addr);

        ready(Ok(Response::new(Full::from(Bytes::from(format!(
            "hello from {peer:?}\n"
        ))))))
    }
}

async fn run() -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", 3000)).await?;
    let connection_builder = ConnectionBuilder::new();

    let mut server = {
        #[cfg(not(feature = "tls"))]
        {
            Server::new(connection_builder, Hello)
        }

        #[cfg(feature = "tls")]
        {
            let tls_config: std::sync::Arc<rustls::ServerConfig> =
                unimplemented!("load a rustls::ServerConfig here");
            Server::new(tls_config, connection_builder, Hello)
        }
    };

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let accepted = server.handle(stream, peer_addr);

        #[cfg(not(feature = "tls"))]
        let connection = accepted;

        #[cfg(feature = "tls")]
        let connection = accepted.await?;

        tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("connection error: {error}");
            }
        });
    }
}
```

If you need HTTP/1 upgrades such as WebSocket or `CONNECT`, call [`Server::handle_upgradable`](https://docs.rs/hserver/latest/hserver/struct.Server.html#method.handle_upgradable) instead. That returns an [`UpgradableConnection`](https://docs.rs/hserver/latest/hserver/struct.UpgradableConnection.html), or with `tls`, a [`TlsHandshake`](https://docs.rs/hserver/latest/hserver/struct.TlsHandshake.html) that resolves into one.

## Request metadata

Each request gets a [`ConnectionInfo`](https://docs.rs/hserver/latest/hserver/struct.ConnectionInfo.html) value in its extensions. Handlers can read it to inspect the peer socket address that accepted the connection.

## Shutdown behavior

[`Connection`](https://docs.rs/hserver/latest/hserver/struct.Connection.html) and [`UpgradableConnection`](https://docs.rs/hserver/latest/hserver/struct.UpgradableConnection.html) implement `Future` and must keep being polled to drive I/O. To begin a graceful shutdown, pin the connection and call [`Connection::graceful_shutdown`](https://docs.rs/hserver/latest/hserver/struct.Connection.html#method.graceful_shutdown) or [`UpgradableConnection::graceful_shutdown`](https://docs.rs/hserver/latest/hserver/struct.UpgradableConnection.html#method.graceful_shutdown); the future should then continue running until in-flight work finishes.

## License

Licensed under either of these, at your option:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))
