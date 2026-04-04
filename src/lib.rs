#![doc = include_str!("../README.md")]

#[cfg(not(any(feature = "http1", feature = "http2")))]
compile_error!("At least one of the features `http1` or `http2` must be enabled");

use http::{Request, Response};
use http_body::Body;
use hyper::service::Service as HyperService;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::{
        Builder as HyperConnectionBuilder, Connection as HyperConnection,
        UpgradeableConnection as HyperUpgradeableConnection,
    },
};
use pin_project::pin_project;
use std::{
    error::Error,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tower_service::Service;
#[cfg(feature = "tls")]
use {
    rustls::ServerConfig,
    std::{io::Error as IoError, marker::PhantomData, task},
    tokio_rustls::{Accept, TlsAcceptor, server::TlsStream},
};

mod sealed {
    pub trait Sealed {}
}

#[rustfmt::skip]
#[cfg(feature = "http1")]
/// Re-export of Hyper's HTTP/1 connection configuration builder.
pub use hyper_util::server::conn::auto::Http1Builder;

#[rustfmt::skip]
#[cfg(feature = "http2")]
/// Re-export of Hyper's HTTP/2 connection configuration builder.
pub use hyper_util::server::conn::auto::Http2Builder;

/// Re-export of Hyper's default request body type.
pub use hyper::body::Incoming;

/// A configurable HTTP server that keeps listener ownership and accept-loop
/// policy in the caller's code.
///
/// `Server` does not own the listening socket. Instead, it is given accepted
/// streams and peer addresses so the caller keeps control of the accept loop,
/// backpressure, and task spawning policy.
pub struct Server<S> {
    // wrap the `ConnectionBuilder` in an `Arc` to avoid cloning it
    connection_builder: Arc<ConnectionBuilder>,

    #[cfg(feature = "tls")]
    tls_acceptor: TlsAcceptor,

    // `tower::Service`
    service: S,
}

/// Configures how accepted streams are served over HTTP.
///
/// This wraps Hyper's auto protocol connection builder and exposes the
/// protocol-specific tuning builders through [`ConnectionBuilder::http1`] and
/// [`ConnectionBuilder::http2`], allowing protocol behavior to be customized
/// without giving up the crate's accept-loop model.
pub struct ConnectionBuilder(HyperConnectionBuilder<TokioExecutor>);

/// A running HTTP connection future.
///
/// The connection must be polled to make progress. Call
/// [`Connection::graceful_shutdown`] to stop accepting new work while allowing
/// in-flight requests to complete.
#[pin_project]
pub struct Connection<T, S, B>(
    #[pin] HyperConnection<'static, TokioIo<Stream<T>>, HyperServiceAdaptor<S>, TokioExecutor>,
)
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body;

/// A running HTTP connection future with HTTP/1 upgrade support enabled.
///
/// The connection must be polled to make progress. Call
/// [`UpgradableConnection::graceful_shutdown`] to stop accepting new work
/// while allowing in-flight requests to complete.
#[pin_project]
pub struct UpgradableConnection<T, S, B>(
    #[pin]
    HyperUpgradeableConnection<'static, TokioIo<Stream<T>>, HyperServiceAdaptor<S>, TokioExecutor>,
)
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body;

/// Constructs a running connection future from an accepted transport.
pub trait ConnectionMode<S, B>:
    sealed::Sealed + Future<Output = Result<(), Box<dyn Error + Send + Sync>>> + Sized
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// The accepted transport type consumed by this connection.
    type Transport;

    /// Constructs a running connection future for the accepted transport.
    fn construct(
        stream: Self::Transport,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self;
}

/// A pending TLS handshake that resolves into a connection future.
///
/// This type is only available when the `tls` feature is enabled. It is
/// returned by [`Server::handle`] before the HTTP connection is constructed.
/// By default it resolves into a [`Connection`].
#[cfg(feature = "tls")]
#[pin_project::pin_project]
pub struct TlsHandshake<T, S, B, C>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
    C: ConnectionMode<S, B, Transport = TlsStream<T>>,
{
    #[pin]
    handshake: Accept<T>,
    from: SocketAddr,
    connection_builder: Option<Arc<ConnectionBuilder>>,
    service: Option<S>,
    _marker: PhantomData<(B, C)>,
}

/// Peer information attached to each incoming request.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionInfo {
    peer_addr: SocketAddr,
}

/// An adaptor converting a `tower::Service` into a `hyper::service::Service`.
struct HyperServiceAdaptor<S> {
    service: S,
    peer_addr: SocketAddr,
}

#[cfg(not(feature = "tls"))]
type Stream<T> = T;

#[cfg(feature = "tls")]
type Stream<T> = TlsStream<T>;

#[cfg(not(feature = "tls"))]
type HandleOk<T, S, B> = Connection<T, S, B>;

#[cfg(feature = "tls")]
type HandleOk<T, S, B> = TlsHandshake<T, S, B, Connection<T, S, B>>;

#[cfg(not(feature = "tls"))]
type HandleUpgradableOk<T, S, B> = UpgradableConnection<T, S, B>;

#[cfg(feature = "tls")]
type HandleUpgradableOk<T, S, B> = TlsHandshake<T, S, B, UpgradableConnection<T, S, B>>;

impl<S, B> Server<S>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// Creates a new server from a connection builder and a clonable service.
    ///
    /// When the `tls` feature is enabled, the server also stores the supplied
    /// Rustls configuration and applies it to newly accepted streams.
    pub fn new(
        #[cfg(feature = "tls")] tls_config: Arc<ServerConfig>,
        connection_builder: ConnectionBuilder,
        service: S,
    ) -> Self {
        Self {
            #[cfg(feature = "tls")]
            tls_acceptor: TlsAcceptor::from(tls_config),
            connection_builder: Arc::new(connection_builder),
            service,
        }
    }

    /// Handles an accepted stream and prepares it for HTTP serving.
    ///
    /// The caller provides the accepted stream and peer socket address, which
    /// usually come from a listener such as
    /// [`tokio::net::TcpListener::accept`]. The socket address is also inserted
    /// into each request as [`ConnectionInfo`].
    ///
    /// Without the `tls` feature, the returned value is a ready-to-poll
    /// [`Connection`]. With `tls` enabled, the returned value is a
    /// [`TlsHandshake`] future that must be awaited before the HTTP connection
    /// starts.
    pub fn handle<T>(&mut self, stream: T, from: SocketAddr) -> HandleOk<T, S, B>
    where
        T: AsyncRead + AsyncWrite + Unpin + 'static,
    {
        #[cfg(not(feature = "tls"))]
        {
            <Connection<T, S, B> as ConnectionMode<S, B>>::construct(
                stream,
                from,
                self.connection_builder.clone(),
                self.service.clone(),
            )
        }

        #[cfg(feature = "tls")]
        {
            TlsHandshake {
                handshake: self.tls_acceptor.accept(stream),
                from,
                connection_builder: Some(self.connection_builder.clone()),
                service: Some(self.service.clone()),
                _marker: PhantomData,
            }
        }
    }

    /// Handles an accepted stream and prepares it for HTTP serving with
    /// HTTP/1 upgrade support enabled.
    ///
    /// This behaves like [`Server::handle`], but returns an
    /// [`UpgradableConnection`] or, with `tls`, a [`TlsHandshake`] that
    /// resolves into one, built with Hyper's upgrade-capable connection
    /// future.
    pub fn handle_upgradable<T>(
        &mut self,
        stream: T,
        from: SocketAddr,
    ) -> HandleUpgradableOk<T, S, B>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        #[cfg(not(feature = "tls"))]
        {
            <UpgradableConnection<T, S, B> as ConnectionMode<S, B>>::construct(
                stream,
                from,
                self.connection_builder.clone(),
                self.service.clone(),
            )
        }

        #[cfg(feature = "tls")]
        {
            TlsHandshake::<T, S, B, UpgradableConnection<T, S, B>> {
                handshake: self.tls_acceptor.accept(stream),
                from,
                connection_builder: Some(self.connection_builder.clone()),
                service: Some(self.service.clone()),
                _marker: PhantomData,
            }
        }
    }
}

impl<T, S, B> Connection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// Begins a graceful shutdown for this connection.
    ///
    /// The connection future must continue to be polled after this call so the
    /// shutdown can complete.
    pub fn graceful_shutdown(self: Pin<&mut Self>) {
        let this = self.project();
        this.0.graceful_shutdown();
    }
}

impl<T, S, B> sealed::Sealed for Connection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body,
{
}

impl<T, S, B> ConnectionMode<S, B> for Connection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    #[cfg(not(feature = "tls"))]
    type Transport = T;
    #[cfg(feature = "tls")]
    type Transport = TlsStream<T>;

    fn construct(
        stream: Self::Transport,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self {
        let service = HyperServiceAdaptor {
            service,
            peer_addr: from,
        };

        let connection = connection_builder
            .0
            .serve_connection(TokioIo::new(stream), service)
            .into_owned();

        Self(connection)
    }
}

impl<T, S, B> UpgradableConnection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// Begins a graceful shutdown for this connection.
    ///
    /// The connection future must continue to be polled after this call so the
    /// shutdown can complete.
    pub fn graceful_shutdown(self: Pin<&mut Self>) {
        let this = self.project();
        this.0.graceful_shutdown();
    }
}

impl<T, S, B> sealed::Sealed for UpgradableConnection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body,
{
}

impl<T, S, B> ConnectionMode<S, B> for UpgradableConnection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    #[cfg(not(feature = "tls"))]
    type Transport = T;
    #[cfg(feature = "tls")]
    type Transport = TlsStream<T>;

    fn construct(
        stream: Self::Transport,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self {
        let service = HyperServiceAdaptor {
            service,
            peer_addr: from,
        };

        let connection = connection_builder
            .0
            .serve_connection_with_upgrades(TokioIo::new(stream), service)
            .into_owned();

        Self(connection)
    }
}

impl ConnectionBuilder {
    /// Creates a connection builder with Hyper's default settings and a Tokio
    /// executor.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "http1")]
    /// Returns Hyper's HTTP/1 configuration builder.
    ///
    /// Changes made through the returned builder are applied to future
    /// connections accepted by the server.
    pub fn http1(&mut self) -> Http1Builder<'_, TokioExecutor> {
        self.0.http1()
    }

    #[cfg(feature = "http2")]
    /// Returns Hyper's HTTP/2 configuration builder.
    ///
    /// Changes made through the returned builder are applied to future
    /// connections accepted by the server.
    pub fn http2(&mut self) -> Http2Builder<'_, TokioExecutor> {
        self.0.http2()
    }
}

impl ConnectionInfo {
    /// Returns the peer socket address for the connection that carried the
    /// request.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }
}

impl<S, B> HyperService<Request<B>> for HyperServiceAdaptor<S>
where
    S: Service<Request<B>> + Clone,
{
    type Error = S::Error;
    type Future = S::Future;
    type Response = S::Response;

    fn call(&self, mut req: Request<B>) -> Self::Future {
        let connection_info = ConnectionInfo {
            peer_addr: self.peer_addr,
        };

        req.extensions_mut().insert(connection_info);
        self.service.clone().call(req)
    }
}

impl<T, S, B> Future for Connection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Output = Result<(), Box<dyn Error + Send + Sync>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.0.poll(cx)
    }
}

impl<T, S, B> Future for UpgradableConnection<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Output = Result<(), Box<dyn Error + Send + Sync>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        this.0.poll(cx)
    }
}

#[cfg(feature = "tls")]
impl<T, S, B, C> Future for TlsHandshake<T, S, B, C>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
    C: ConnectionMode<S, B, Transport = TlsStream<T>>,
{
    type Output = Result<C, IoError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let stream = task::ready!(this.handshake.poll(cx))?;

        let connection = C::construct(
            stream,
            *this.from,
            this.connection_builder.take().unwrap(),
            this.service.take().unwrap(),
        );

        Poll::Ready(Ok(connection))
    }
}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self(HyperConnectionBuilder::new(TokioExecutor::new()))
    }
}
