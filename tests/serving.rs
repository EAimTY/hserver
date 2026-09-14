//! End-to-end tests for the public serving surface of `hserver`.
//!
//! Every test drives the real Hyper connection engine over in-memory duplex streams, asserting the
//! crate's own invariants: peer metadata propagation, transport flexibility, connection builder
//! plumbing, TLS handshake outcome handling, service error propagation, HTTP/1 upgrades, and
//! graceful shutdown semantics. Upstream behavior of Hyper and Rustls is exercised only as far as
//! the crate's wiring depends on it.

use futures::future::{self as futures_future, Either};
use h2::client;
use hserver::{ConnectionBuilder, ConnectionInfo, Incoming, Server, Tls};
use http::{
    Request, Response, StatusCode,
    header::{CONNECTION, HeaderValue, UPGRADE},
};
use http_body_util::Full;
use hyper::{body::Bytes, upgrade};
use hyper_util::rt::TokioIo;
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    crypto::ring,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};
use std::{
    convert::Infallible,
    error::Error,
    future::{self, Future, Ready},
    io::Error as IoError,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{self, AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    sync::{Notify, oneshot},
    time,
};
use tokio_rustls::TlsConnector;
use tower_service::Service;

/// Peer address the tests hand to [`Server::handle`], taken from the reserved TEST-NET-3 range so
/// an address merely carried through the crate can never collide with a real local address.
const TEST_PEER_ADDR: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7)), 43110);

/// Wall-clock budget for a single awaited interaction, so a broken path fails the test instead of
/// hanging it.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Payload echoed across the upgraded transport by the upgrade test.
const ECHO_PAYLOAD: &[u8] = b"ping";

/// Minimal HTTP/1.1 GET request head with keep-alive semantics.
const REQUEST_HEAD: &str = "GET / HTTP/1.1\r\nhost: hserver.test\r\n\r\n";

/// Minimal HTTP/1.1 GET request head asking the server to upgrade the protocol to `test`.
const UPGRADE_REQUEST_HEAD: &str =
    "GET / HTTP/1.1\r\nhost: hserver.test\r\nconnection: upgrade\r\nupgrade: test\r\n\r\n";

/// Shared result type for every test body, boxing over any error.
type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

/// Echo service returning the attached peer address as the response body.
#[derive(Clone)]
struct EchoPeer;

/// Service that always fails, exercising the connection error path for service failures.
#[derive(Clone)]
struct Failing;

/// Service that models an in-flight request: it signals entry, then completes its response only
/// once a permit is stored on the release notifier.
#[derive(Clone)]
struct Gated {
    /// Notified once the service has been entered for a request.
    entered: Arc<Notify>,

    /// The response future waits for a permit on this notifier before completing.
    release: Arc<Notify>,
}

/// Service completing an HTTP/1 upgrade and echoing the first bytes received on the upgraded
/// transport back to the client.
#[derive(Clone)]
struct UpgradeEcho;

/// An HTTP/1.x response split into its textual head and framed body.
struct H1Response {
    /// Response head, including the status line and every header line.
    head: String,

    /// Message body as framed by the content-length header.
    body: Vec<u8>,
}

impl Gated {
    /// Creates the service together with the notifier handles controlling it.
    fn new() -> (Self, Arc<Notify>, Arc<Notify>) {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        (
            Self {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            },
            entered,
            release,
        )
    }
}

impl Service<Request<Incoming>> for EchoPeer {
    /// Response body carrying the peer address of the serving connection.
    type Response = Response<Full<Bytes>>;

    /// The echo service never fails.
    type Error = Infallible;

    /// Immediately ready response future.
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        future::ready(Ok(peer_response(&request)))
    }
}

impl Service<Request<Incoming>> for Failing {
    /// Response type is never produced.
    type Response = Response<Full<Bytes>>;

    /// The service always fails with this error.
    type Error = IoError;

    /// Immediately ready failing future.
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Request<Incoming>) -> Self::Future {
        future::ready(Err(IoError::other("service failed")))
    }
}

impl Service<Request<Incoming>> for Gated {
    /// Response body carrying the peer address of the serving connection.
    type Response = Response<Full<Bytes>>;

    /// The gated service never fails.
    type Error = Infallible;

    /// Asynchronous future completing after the release permit is stored.
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            Ok(peer_response(&request))
        })
    }
}

impl Service<Request<Incoming>> for UpgradeEcho {
    /// Empty-bodied switching-protocols response.
    type Response = Response<Full<Bytes>>;

    /// The service never fails.
    type Error = Infallible;

    /// Immediately ready response future.
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: Request<Incoming>) -> Self::Future {
        let upgrade = upgrade::on(&mut request);

        // The upgrade future must not be awaited inside the service future: Hyper only hands the
        // transport over after the switching-protocols response has been sent, so the echo runs on
        // a separate task. Its outcome is observed from the client side of the test.
        tokio::spawn(async move {
            let mut upgraded = match upgrade.await {
                Ok(upgraded) => TokioIo::new(upgraded),
                Err(_) => return,
            };

            let mut received = vec![0_u8; ECHO_PAYLOAD.len()];

            if upgraded.read_exact(&mut received).await.is_err() {
                return;
            }

            let _ = upgraded.write_all(&received).await;
        });

        let mut response = Response::new(Full::new(Bytes::new()));
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        response
            .headers_mut()
            .insert(CONNECTION, HeaderValue::from_static("upgrade"));
        response
            .headers_mut()
            .insert(UPGRADE, HeaderValue::from_static("test"));
        future::ready(Ok(response))
    }
}

/// Builds a response whose body is the peer address attached by the server, letting tests observe
/// peer metadata propagation through the response payload. A missing `ConnectionInfo` yields a
/// distinctive body so the corresponding assertion failure names its cause.
fn peer_response(request: &Request<Incoming>) -> Response<Full<Bytes>> {
    let body = request.extensions().get::<ConnectionInfo>().map_or_else(
        || "missing connection info".to_owned(),
        |info| info.peer_addr().to_string(),
    );
    Response::new(Full::new(Bytes::from(body)))
}

/// Writes `request_head` to `stream` and reads back a complete response.
async fn h1_roundtrip<S>(
    stream: &mut S,
    request_head: &str,
) -> Result<H1Response, Box<dyn Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.write_all(request_head.as_bytes()).await?;
    h1_response(stream).await
}

/// Reads a complete content-length-framed HTTP/1.x response from `stream`.
///
/// A `101 Switching Protocols` response is returned without a body, at which point the caller owns
/// the transport.
async fn h1_response<S>(stream: &mut S) -> Result<H1Response, Box<dyn Error + Send + Sync>>
where
    S: AsyncRead + Unpin,
{
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];

    loop {
        if stream.read(&mut byte).await? == 0 {
            return Err(
                IoError::other("connection closed before the response head completed").into(),
            );
        }

        head.push(byte[0]);

        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }

    let head = String::from_utf8(head)?;

    if head.starts_with("HTTP/1.1 101") {
        return Ok(H1Response {
            head,
            body: Vec::new(),
        });
    }

    let content_length =
        content_length(&head).ok_or("response head has no content-length header")?;
    let mut body = vec![0_u8; content_length];
    stream.read_exact(&mut body).await?;

    Ok(H1Response { head, body })
}

/// Extracts the content-length value from a response head, if one is present.
fn content_length(head: &str) -> Option<usize> {
    head.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim_end()
            .eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    })
}

/// Checks whether `error` or any of its sources mentions `needle` in its display.
fn error_chain_contains(error: &(dyn Error + 'static), needle: &str) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.to_string().contains(needle) {
            return true;
        }
        current = error.source();
    }
    false
}

/// Creates a matched pair of a rustls server configuration and a client connector trusting the
/// generated self-signed `localhost` certificate.
fn tls_endpoints() -> Result<(Arc<ServerConfig>, TlsConnector), Box<dyn Error + Send + Sync>> {
    let provider = Arc::new(ring::default_provider());
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;

    let server_config = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            vec![certified_key.cert.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                certified_key.signing_key.serialize_der(),
            )),
        )?;

    let mut root_certificates = RootCertStore::empty();
    root_certificates.add(certified_key.cert.der().clone())?;

    let client_config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(root_certificates)
        .with_no_client_auth();

    Ok((
        Arc::new(server_config),
        TlsConnector::from(Arc::new(client_config)),
    ))
}

/// Body expected from the echo services: the peer address handed to the server, as a string.
fn expected_body() -> Vec<u8> {
    TEST_PEER_ADDR.to_string().into_bytes()
}

/// Server name presented by the test TLS client, matching the generated certificate.
fn server_name() -> Result<ServerName<'static>, Box<dyn Error + Send + Sync>> {
    Ok(ServerName::try_from("localhost".to_owned())?)
}

/// Awaits `future` with the shared test timeout so a stalled interaction fails instead of hanging.
async fn with_timeout<F>(future: F) -> Result<F::Output, Box<dyn Error + Send + Sync>>
where
    F: Future,
{
    time::timeout(TEST_TIMEOUT, future)
        .await
        .map_err(Into::into)
}

/// Two sequential HTTP/1.1 requests on one connection must both be served by the auto-detected
/// HTTP/1 stack, carry the peer address handed to [`Server::handle`], and leave the connection
/// with a clean result when the client closes its side.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn http1_serves_keep_alive_requests_with_peer_info() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let mut server = Server::new(ConnectionBuilder::new(), EchoPeer);
    let connection = server.handle(server_stream, TEST_PEER_ADDR);

    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let connection = pin!(connection);
        let _ = connection_result_tx.send(connection.await);
    });

    let first = with_timeout(h1_roundtrip(&mut client, REQUEST_HEAD)).await??;
    assert!(first.head.starts_with("HTTP/1.1 200 OK\r\n"));
    assert_eq!(first.body, expected_body());

    let second = with_timeout(h1_roundtrip(&mut client, REQUEST_HEAD)).await??;
    assert_eq!(second.body, expected_body());

    client.shutdown().await?;
    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}

/// Configuration made through [`ConnectionBuilder::http1`] must apply to connections served by the
/// server, observable here as title-cased response headers on the wire.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn http1_applies_connection_builder_configuration() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let mut connection_builder = ConnectionBuilder::new();
    connection_builder.http1().title_case_headers(true);
    let mut server = Server::new(connection_builder, EchoPeer);
    let connection = server.handle(server_stream, TEST_PEER_ADDR);

    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let connection = pin!(connection);
        let _ = connection_result_tx.send(connection.await);
    });

    let response = with_timeout(h1_roundtrip(&mut client, REQUEST_HEAD)).await??;
    assert!(response.head.contains("Content-Length"));
    assert!(!response.head.contains("content-length"));

    client.shutdown().await?;
    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}

/// A failing service must surface its error through the connection future and reach the client
/// with no synthesized HTTP response.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn http1_propagates_service_error_to_connection() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let mut server = Server::new(ConnectionBuilder::new(), Failing);
    let connection = server.handle(server_stream, TEST_PEER_ADDR);

    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let connection = pin!(connection);
        let _ = connection_result_tx.send(connection.await);
    });

    client.write_all(REQUEST_HEAD.as_bytes()).await?;
    let connection_result = with_timeout(connection_result_rx).await??;

    let error = match connection_result {
        Ok(()) => return Err("connection future unexpectedly resolved successfully".into()),
        Err(error) => error,
    };
    assert!(error_chain_contains(error.as_ref(), "service failed"));

    let mut received = Vec::new();
    with_timeout(client.read_to_end(&mut received)).await??;
    assert!(received.is_empty());
    Ok(())
}

/// An HTTP/2 prior-knowledge request must be served by the auto-detected HTTP/2 stack with the
/// peer address handed to [`Server::handle`] attached, including configuration made through
/// [`ConnectionBuilder::http2`].
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn http2_serves_request_with_peer_info() -> TestResult {
    let (client, server_stream) = io::duplex(64 * 1024);
    let mut connection_builder = ConnectionBuilder::new();
    connection_builder.http2().max_frame_size(32 * 1024);
    let mut server = Server::new(connection_builder, EchoPeer);
    let connection = server.handle(server_stream, TEST_PEER_ADDR);

    tokio::spawn(async move {
        let connection = pin!(connection);
        let _ = connection.await;
    });

    let (request_sender, h2_connection) = with_timeout(client::handshake(client)).await??;

    tokio::spawn(async move {
        let _ = h2_connection.await;
    });

    let request = Request::builder().uri("http://hserver.test/").body(())?;

    let response = {
        let mut sender = with_timeout(request_sender.ready()).await??;
        let (response, _stream) = sender.send_request(request, true)?;
        with_timeout(response).await??
    };
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body();
    let mut received = Vec::new();

    while let Some(chunk) = with_timeout(body.data()).await? {
        let chunk = chunk?;
        body.flow_control().release_capacity(chunk.len())?;
        received.extend_from_slice(&chunk);
    }

    assert_eq!(received, expected_body());
    Ok(())
}

/// [`Server::handle_upgradable`] must serve an HTTP/1 upgrade: the switching-protocols response is
/// delivered, the transport is handed over, and the upgraded echo works end to end.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn http1_upgrade_serves_echo_after_switching_protocols() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let mut server = Server::new(ConnectionBuilder::new(), UpgradeEcho);
    let connection = server.handle_upgradable(server_stream, TEST_PEER_ADDR);

    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let connection = pin!(connection);
        let _ = connection_result_tx.send(connection.await);
    });

    let response = with_timeout(h1_roundtrip(&mut client, UPGRADE_REQUEST_HEAD)).await??;
    assert!(response.head.starts_with("HTTP/1.1 101"));

    client.write_all(ECHO_PAYLOAD).await?;
    let mut echoed = vec![0_u8; ECHO_PAYLOAD.len()];
    with_timeout(client.read_exact(&mut echoed)).await??;
    assert_eq!(echoed, ECHO_PAYLOAD);

    client.shutdown().await?;
    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}

/// [`hserver::Connection::graceful_shutdown`] must let the in-flight request complete before the
/// connection ends, while refusing any further request on the same connection.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn graceful_shutdown_completes_in_flight_request() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let (service, entered, release) = Gated::new();
    let mut server = Server::new(ConnectionBuilder::new(), service);
    let connection = server.handle(server_stream, TEST_PEER_ADDR);

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut connection = pin!(connection);
        let mut shutdown_wait = pin!(shutdown_signal.notified());

        // The connection must keep being polled while the shutdown is pending, otherwise neither
        // the request nor the shutdown signal would ever make progress.
        let result = match futures_future::select(&mut shutdown_wait, &mut connection).await {
            Either::Left(((), remaining)) => {
                remaining.as_mut().graceful_shutdown();
                remaining.await
            }
            Either::Right((result, _shutdown_wait)) => result,
        };

        let _ = connection_result_tx.send(result);
    });

    client.write_all(REQUEST_HEAD.as_bytes()).await?;
    with_timeout(entered.notified()).await?;
    shutdown.notify_one();
    release.notify_one();

    let response = with_timeout(h1_response(&mut client)).await??;
    assert_eq!(response.body, expected_body());

    let mut received = Vec::new();
    with_timeout(client.read_to_end(&mut received)).await??;
    assert!(received.is_empty());

    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}

/// [`hserver::UpgradableConnection::graceful_shutdown`] must promptly end an otherwise idle
/// keep-alive connection with a clean result.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn graceful_shutdown_closes_idle_upgradable_connection() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let mut server = Server::new(ConnectionBuilder::new(), EchoPeer);
    let connection = server.handle_upgradable(server_stream, TEST_PEER_ADDR);

    let shutdown = Arc::new(Notify::new());
    let shutdown_signal = Arc::clone(&shutdown);
    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut connection = pin!(connection);
        let mut shutdown_wait = pin!(shutdown_signal.notified());

        // The connection must keep being polled while the shutdown is pending, otherwise neither
        // the request nor the shutdown signal would ever make progress.
        let result = match futures_future::select(&mut shutdown_wait, &mut connection).await {
            Either::Left(((), remaining)) => {
                remaining.as_mut().graceful_shutdown();
                remaining.await
            }
            Either::Right((result, _shutdown_wait)) => result,
        };

        let _ = connection_result_tx.send(result);
    });

    let response = with_timeout(h1_roundtrip(&mut client, REQUEST_HEAD)).await??;
    assert_eq!(response.body, expected_body());

    shutdown.notify_one();

    let mut received = Vec::new();
    with_timeout(client.read_to_end(&mut received)).await??;
    assert!(received.is_empty());

    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}

/// A [`Tls`] transport handed to [`Server::handle`] must complete the Rustls handshake, then serve
/// HTTP/1 with the peer address handed to the server attached to the request.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn tls_serves_request_after_handshake() -> TestResult {
    let (client, server_stream) = io::duplex(64 * 1024);
    let (server_config, client_connector) = tls_endpoints()?;
    let mut server = Server::new(ConnectionBuilder::new(), EchoPeer);
    let handshake = server.handle(Tls(server_config, server_stream), TEST_PEER_ADDR);

    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let handshake = pin!(handshake);

        let connection = match handshake.await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = connection_result_tx.send(Err(error.into()));
                return;
            }
        };

        let connection = pin!(connection);
        let _ = connection_result_tx.send(connection.await);
    });

    let mut client = with_timeout(client_connector.connect(server_name()?, client)).await??;
    let response = with_timeout(h1_roundtrip(&mut client, REQUEST_HEAD)).await??;
    assert_eq!(response.body, expected_body());

    client.shutdown().await?;
    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}

/// Handing non-TLS bytes to a [`Tls`] transport must fail the handle future with an I/O error
/// instead of reaching the HTTP connection layer.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn tls_handshake_failure_fails_handle_future() -> TestResult {
    let (mut client, server_stream) = io::duplex(64 * 1024);
    let (server_config, _client_connector) = tls_endpoints()?;
    let mut server = Server::new(ConnectionBuilder::new(), EchoPeer);
    let handshake = server.handle(Tls(server_config, server_stream), TEST_PEER_ADDR);

    let (handshake_result_tx, handshake_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let handshake = pin!(handshake);
        let _ = handshake_result_tx.send(handshake.await.map(|_| ()));
    });

    client.write_all(b"not a tls handshake").await?;
    drop(client);

    let handshake_result = with_timeout(handshake_result_rx).await??;
    assert!(handshake_result.is_err());
    Ok(())
}

/// A [`Tls`] transport handed to [`Server::handle_upgradable`] must resolve into a working
/// upgrade-capable connection serving HTTP/1 over TLS.
///
/// # Panics
///
/// Panics if any of the asserted invariants does not hold.
#[tokio::test]
async fn tls_upgradable_serves_request_after_handshake() -> TestResult {
    let (client, server_stream) = io::duplex(64 * 1024);
    let (server_config, client_connector) = tls_endpoints()?;
    let mut server = Server::new(ConnectionBuilder::new(), EchoPeer);
    let handshake = server.handle_upgradable(Tls(server_config, server_stream), TEST_PEER_ADDR);

    let (connection_result_tx, connection_result_rx) = oneshot::channel();

    tokio::spawn(async move {
        let handshake = pin!(handshake);

        let connection = match handshake.await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = connection_result_tx.send(Err(error.into()));
                return;
            }
        };

        let connection = pin!(connection);
        let _ = connection_result_tx.send(connection.await);
    });

    let mut client = with_timeout(client_connector.connect(server_name()?, client)).await??;
    let request_head = "GET / HTTP/1.1\r\nhost: hserver.test\r\nconnection: close\r\n\r\n";
    let response = with_timeout(h1_roundtrip(&mut client, request_head)).await??;
    assert_eq!(response.body, expected_body());

    let connection_result = with_timeout(connection_result_rx).await??;
    assert!(connection_result.is_ok());
    Ok(())
}
