#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(any(feature = "http1", feature = "http2")))]
compile_error!("At least one of the features `http1` or `http2` must be enabled");

use crate::sealed::Sealed;
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

/// Namespace sealing the public transport traits of this crate against external implementations.
mod sealed {
    /// Marker trait implemented only for the transport types this crate is able to serve.
    pub trait Sealed {}
}

/// Re-export of Hyper's default request body type.
pub use hyper::body::Incoming;
/// Re-export of Hyper's HTTP/1 connection configuration builder.
#[cfg(feature = "http1")]
#[cfg_attr(docsrs, doc(cfg(feature = "http1")))]
pub use hyper_util::server::conn::auto::Http1Builder;
/// Re-export of Hyper's HTTP/2 connection configuration builder.
#[cfg(feature = "http2")]
#[cfg_attr(docsrs, doc(cfg(feature = "http2")))]
pub use hyper_util::server::conn::auto::Http2Builder;

/// A configurable HTTP server that keeps listener ownership and accept-loop policy in the caller's
/// code.
///
/// `Server` does not own the listening socket. Instead, it is given accepted transports and peer
/// addresses, so the caller keeps control of the accept loop, backpressure, and task spawning
/// policy.
///
/// The server is transport-agnostic: any stream that is `AsyncRead + AsyncWrite + Unpin + 'static`
/// can be served directly, and the `tls` feature only adds an additional `Tls` transport without
/// changing any signature.
pub struct Server<S> {
    /// Connection builder wrapped in an `Arc` and shared by every handled connection, so handling
    /// a transport clones only the `Arc`.
    connection_builder: Arc<ConnectionBuilder>,

    /// `tower::Service` cloned for every accepted transport.
    service: S,
}

/// Configures how accepted transports are served over HTTP.
///
/// This wraps Hyper's auto protocol connection builder and exposes the protocol-specific tuning
/// builders through [`ConnectionBuilder::http1`] and [`ConnectionBuilder::http2`], allowing
/// protocol behavior to be customized without giving up the crate's accept-loop model.
pub struct ConnectionBuilder(
    /// Wrapped Hyper auto protocol connection builder.
    HyperConnectionBuilder<TokioExecutor>,
);

/// A running HTTP connection future.
///
/// The connection must be polled to make progress. Call [`Connection::graceful_shutdown`] to stop
/// accepting new work while allowing in-flight requests to complete.
#[pin_project]
pub struct Connection<T, S, B>(
    /// Wrapped Hyper connection future driving the transport.
    #[pin]
    HyperConnection<'static, TokioIo<T>, HyperServiceAdaptor<S>, TokioExecutor>,
)
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body;

/// A running HTTP connection future with HTTP/1 upgrade support enabled.
///
/// The connection must be polled to make progress. Call [`UpgradableConnection::graceful_shutdown`]
/// to stop accepting new work while allowing in-flight requests to complete.
#[pin_project]
pub struct UpgradableConnection<T, S, B>(
    /// Wrapped Hyper upgrade-capable connection future driving the transport.
    #[pin]
    HyperUpgradeableConnection<'static, TokioIo<T>, HyperServiceAdaptor<S>, TokioExecutor>,
)
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body;

/// An accepted transport that can be served by [`Server::handle`].
///
/// This trait is sealed; hserver decides which types can be served. It is implemented for every
/// stream that is `AsyncRead + AsyncWrite + Unpin + 'static`, in which case [`Server::handle`]
/// returns a ready-to-poll [`Connection`]. With the `tls` feature, it is also implemented for the
/// `Tls` transport, in which case [`Server::handle`] returns a `TlsHandshake` future that resolves
/// into a `Connection` once the Rustls handshake completes.
pub trait Transport<S, B>: Sealed + Sized {
    /// The future returned by [`Server::handle`] for this transport.
    type Handle;

    /// Constructs the connection future for the accepted transport.
    fn construct(
        self,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self::Handle;
}

/// An accepted transport that can be served by [`Server::handle_upgradable`].
///
/// This trait is sealed; hserver decides which types can be served. It is implemented for every
/// stream that is `AsyncRead + AsyncWrite + Unpin + Send + 'static`, in which case
/// [`Server::handle_upgradable`] returns a ready-to-poll [`UpgradableConnection`]. With the `tls`
/// feature, it is also implemented for the `Tls` transport, in which case
/// [`Server::handle_upgradable`] returns an `UpgradableTlsHandshake` future that resolves into an
/// `UpgradableConnection` once the Rustls handshake completes.
///
/// Unlike [`Transport`], implementing this trait requires the transport to be sendable between
/// threads because HTTP/1 upgrades require it.
pub trait UpgradableTransport<S, B>: Sealed + Sized {
    /// The future returned by [`Server::handle_upgradable`] for this transport.
    type Handle;

    /// Constructs the upgrade-capable connection future for the accepted transport.
    fn construct(
        self,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self::Handle;
}

/// A TLS transport: Rustls configuration together with an accepted raw stream.
///
/// Pass it to [`Server::handle`] or [`Server::handle_upgradable`] to serve the connection over TLS.
/// The Rustls handshake runs first; on success the HTTP connection is constructed over the
/// established TLS stream.
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub struct Tls<T>(
    /// Rustls server configuration driving the TLS handshake.
    pub Arc<ServerConfig>,
    /// Raw accepted stream promoted into a TLS stream after the handshake.
    pub T,
);

/// A pending TLS handshake that resolves into a [`Connection`].
///
/// This type is only available when the `tls` feature is enabled. It is returned by
/// [`Server::handle`] when a [`Tls`] transport is passed.
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
#[pin_project]
pub struct TlsHandshake<T, S, B>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// Pending Rustls handshake driving the TLS establishment.
    #[pin]
    handshake: Accept<T>,

    /// Peer socket address carried into the constructed connection.
    from: SocketAddr,

    /// Connection builder consumed when the connection is constructed.
    connection_builder: Option<Arc<ConnectionBuilder>>,

    /// Service consumed when the connection is constructed.
    service: Option<S>,

    /// Marker keeping the body type parameter present in this future.
    _marker: PhantomData<B>,
}

/// A pending TLS handshake that resolves into an [`UpgradableConnection`].
///
/// This type is only available when the `tls` feature is enabled. It is returned by
/// [`Server::handle_upgradable`] when a [`Tls`] transport is passed.
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
#[pin_project]
pub struct UpgradableTlsHandshake<T, S, B>
where
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    /// Pending Rustls handshake driving the TLS establishment.
    #[pin]
    handshake: Accept<T>,

    /// Peer socket address carried into the constructed connection.
    from: SocketAddr,

    /// Connection builder consumed when the connection is constructed.
    connection_builder: Option<Arc<ConnectionBuilder>>,

    /// Service consumed when the connection is constructed.
    service: Option<S>,

    /// Marker keeping the body type parameter present in this future.
    _marker: PhantomData<B>,
}

/// Peer information attached to each incoming request.
///
/// Every request served by [`Server`] carries a `ConnectionInfo` value in its extensions, letting
/// handlers inspect the peer socket address of the connection that carried the request.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionInfo {
    /// Peer socket address of the accepted connection that carried the request.
    peer_addr: SocketAddr,
}

/// Adaptor presenting a `tower::Service` as a Hyper service.
///
/// Every call attaches the peer address of the serving connection as [`ConnectionInfo`] before
/// delegating the request to a cloned inner service.
struct HyperServiceAdaptor<S> {
    /// Inner `tower::Service` cloned for every request.
    service: S,

    /// Peer socket address attached to every request as `ConnectionInfo`.
    peer_addr: SocketAddr,
}

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
    pub fn new(connection_builder: ConnectionBuilder, service: S) -> Self {
        Self {
            connection_builder: Arc::new(connection_builder),
            service,
        }
    }

    /// Handles an accepted transport and returns its connection future.
    ///
    /// The caller provides the accepted transport together with the peer socket address, which
    /// usually come from a listener such as `tokio::net::TcpListener::accept`. The address is
    /// inserted into every request served over the transport as [`ConnectionInfo`].
    ///
    /// For a stream that is `AsyncRead + AsyncWrite + Unpin + 'static` the returned future is a
    /// ready-to-poll [`Connection`]. With the `tls` feature, passing a `Tls` transport instead
    /// returns a `TlsHandshake` future that must be awaited before the HTTP connection starts.
    pub fn handle<T>(&mut self, stream: T, from: SocketAddr) -> T::Handle
    where
        T: Transport<S, B>,
        T::Handle: Future,
    {
        T::construct(
            stream,
            from,
            self.connection_builder.clone(),
            self.service.clone(),
        )
    }

    /// Handles an accepted transport and returns its connection future with HTTP/1 upgrade support
    /// enabled.
    ///
    /// This behaves like [`Server::handle`], but builds the connection with Hyper's upgrade-capable
    /// connection future, returning an [`UpgradableConnection`], or an `UpgradableTlsHandshake`
    /// resolving into one when a `Tls` transport is passed. Unlike [`Transport`], the transport
    /// must be sendable between threads.
    pub fn handle_upgradable<T>(&mut self, stream: T, from: SocketAddr) -> T::Handle
    where
        T: UpgradableTransport<S, B>,
        T::Handle: Future,
    {
        T::construct(
            stream,
            from,
            self.connection_builder.clone(),
            self.service.clone(),
        )
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
    /// The connection future must continue to be polled after this call so the shutdown can
    /// complete.
    pub fn graceful_shutdown(self: Pin<&mut Self>) {
        let this = self.project();
        this.0.graceful_shutdown();
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
    /// The connection future must continue to be polled after this call so the shutdown can
    /// complete.
    pub fn graceful_shutdown(self: Pin<&mut Self>) {
        let this = self.project();
        this.0.graceful_shutdown();
    }
}

impl ConnectionBuilder {
    /// Creates a connection builder with Hyper's default settings and a Tokio executor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns Hyper's HTTP/1 configuration builder.
    ///
    /// Changes made through the returned builder are applied to future connections served by the
    /// server.
    #[cfg(feature = "http1")]
    pub fn http1(&mut self) -> Http1Builder<'_, TokioExecutor> {
        self.0.http1()
    }

    /// Returns Hyper's HTTP/2 configuration builder.
    ///
    /// Changes made through the returned builder are applied to future connections served by the
    /// server.
    #[cfg(feature = "http2")]
    pub fn http2(&mut self) -> Http2Builder<'_, TokioExecutor> {
        self.0.http2()
    }
}

impl ConnectionInfo {
    /// Returns the peer socket address of the connection that carried the request.
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

    fn call(&self, mut request: Request<B>) -> Self::Future {
        request.extensions_mut().insert(ConnectionInfo {
            peer_addr: self.peer_addr,
        });

        self.service.clone().call(request)
    }
}

impl<T, S, B> Transport<S, B> for T
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Handle = Connection<T, S, B>;

    fn construct(
        self,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self::Handle {
        let service = HyperServiceAdaptor {
            service,
            peer_addr: from,
        };

        let connection = connection_builder
            .0
            .serve_connection(TokioIo::new(self), service)
            .into_owned();

        Connection(connection)
    }
}

impl<T, S, B> UpgradableTransport<S, B> for T
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Handle = UpgradableConnection<T, S, B>;

    fn construct(
        self,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self::Handle {
        let service = HyperServiceAdaptor {
            service,
            peer_addr: from,
        };

        let connection = connection_builder
            .0
            .serve_connection_with_upgrades(TokioIo::new(self), service)
            .into_owned();

        UpgradableConnection(connection)
    }
}

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
impl<T, S, B> Transport<S, B> for Tls<T>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Handle = TlsHandshake<T, S, B>;

    fn construct(
        self,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self::Handle {
        TlsHandshake {
            handshake: TlsAcceptor::from(self.0).accept(self.1),
            from,
            connection_builder: Some(connection_builder),
            service: Some(service),
            _marker: PhantomData,
        }
    }
}

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
impl<T, S, B> UpgradableTransport<S, B> for Tls<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Handle = UpgradableTlsHandshake<T, S, B>;

    fn construct(
        self,
        from: SocketAddr,
        connection_builder: Arc<ConnectionBuilder>,
        service: S,
    ) -> Self::Handle {
        UpgradableTlsHandshake {
            handshake: TlsAcceptor::from(self.0).accept(self.1),
            from,
            connection_builder: Some(connection_builder),
            service: Some(service),
            _marker: PhantomData,
        }
    }
}

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
impl<T, S, B> Future for TlsHandshake<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Output = Result<Connection<TlsStream<T>, S, B>, IoError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let stream = task::ready!(this.handshake.poll(cx))?;

        #[expect(
            clippy::unwrap_used,
            reason = "taken exactly once on completion; a ready future is never polled again"
        )]
        let connection = <TlsStream<T> as Transport<S, B>>::construct(
            stream,
            *this.from,
            this.connection_builder.take().unwrap(),
            this.service.take().unwrap(),
        );

        Poll::Ready(Ok(connection))
    }
}

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
impl<T, S, B> Future for UpgradableTlsHandshake<T, S, B>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Service<Request<Incoming>, Response = Response<B>> + Clone + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    type Output = Result<UpgradableConnection<TlsStream<T>, S, B>, IoError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let stream = task::ready!(this.handshake.poll(cx))?;

        #[expect(
            clippy::unwrap_used,
            reason = "taken exactly once on completion; a ready future is never polled again"
        )]
        let connection = <TlsStream<T> as UpgradableTransport<S, B>>::construct(
            stream,
            *this.from,
            this.connection_builder.take().unwrap(),
            this.service.take().unwrap(),
        );

        Poll::Ready(Ok(connection))
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.0.poll(cx)
    }
}

impl<T> Sealed for T where T: AsyncRead + AsyncWrite + Unpin + 'static {}

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
impl<T> Sealed for Tls<T> where T: AsyncRead + AsyncWrite + Unpin + 'static {}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self(HyperConnectionBuilder::new(TokioExecutor::new()))
    }
}
