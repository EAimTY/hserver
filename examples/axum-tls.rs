//! Serves an axum `Router` over TLS with an explicit accept loop, generating a self-signed
//! `localhost` certificate at startup.
//!
//! Run with `cargo run --example axum-tls`, then request `https://localhost:3443/` with
//! `curl --insecure https://localhost:3443/`.

use axum::{Router, routing::get};
use hserver::{ConnectionBuilder, Server, Tls};
use rustls::{
    ServerConfig,
    crypto::ring,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
};
use std::{error::Error, sync::Arc};
use tokio::net::TcpListener;

/// Serves the router over TLS on every accepted TCP stream.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router: Router = Router::new().route("/", get(hello));
    let tls_config = self_signed_tls_config()?;

    let listener = TcpListener::bind(("127.0.0.1", 3443)).await?;
    println!("listening on https://localhost:3443/");
    let mut server = Server::new(ConnectionBuilder::new(), router);

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let handshake = server.handle(Tls(tls_config.clone(), stream), peer_addr);

        tokio::spawn(async move {
            match handshake.await {
                Ok(connection) => {
                    if let Err(error) = connection.await {
                        eprintln!("connection error: {error}");
                    }
                }
                Err(error) => eprintln!("tls handshake error: {error}"),
            }
        });
    }
}

/// Greets the caller.
async fn hello() -> &'static str {
    "hello\n"
}

/// Builds a TLS server configuration around a freshly generated self-signed `localhost`
/// certificate.
fn self_signed_tls_config() -> Result<Arc<ServerConfig>, Box<dyn Error>> {
    let provider = Arc::new(ring::default_provider());
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;

    let server_config = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            vec![certified_key.cert.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                certified_key.signing_key.serialize_der(),
            )),
        )?;

    Ok(Arc::new(server_config))
}
