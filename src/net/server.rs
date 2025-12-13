use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt,
    BufReader as AsyncBufReader, ReadHalf, WriteHalf,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::spawn_blocking;
use tokio::time::{timeout, Instant};
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig as TlsServerConfig};
use tokio_rustls::TlsAcceptor;

use crate::facade::{SimpleStoreFacade, StoreFacade};
use crate::types::{StoreKey as Key, Value, MAX_VALUE_BYTES};

use super::BasicAuthConfig;

type BufferedReader<S> = AsyncBufReader<ReadHalf<S>>;
type StreamWriter<S> = WriteHalf<S>;

const KEY_BYTES_FIELD_WIDTH: usize = 2;
const VALUE_LEN_WIDTH: usize = 2;
const MAX_KEYS_PER_REQUEST: usize = 255;
const MAX_RESPONSE_BYTES: usize = MAX_KEYS_PER_REQUEST * (VALUE_LEN_WIDTH + MAX_VALUE_BYTES);
const MAX_AUTH_HEADER_LEN: usize = 512;

const AUTH_READY: u8 = 0;
const ERR_INVALID_HEADER: u8 = 1;
const ERR_INVALID_PAYLOAD: u8 = 2;
const ERR_STORE_FAILURE: u8 = 3;
const ERR_UNAUTHORIZED: u8 = 4;
const ERR_TIMEOUT: u8 = 5;

/// Authenticated remote server (TLS optional) that exposes the store through the binary protocol.
pub struct RemoteStoreServer {
    bind_address: SocketAddr,
    security: ServerSecurityMode,
    limits: ServerLimits,
    state: Arc<ServerState>,
}

/// Handle that exposes server metrics without holding a mutable reference to the server.
#[derive(Clone)]
pub struct RemoteServerHandle {
    metrics: Arc<ServerMetrics>,
}

/// Snapshot of server metrics.
#[derive(Debug, Clone)]
pub struct ServerMetricsSnapshot {
    pub active_connections: usize,
    pub total_connections: usize,
    pub total_requests: u64,
    pub failed_requests: u64,
    pub average_request_latency_micros: u64,
}

/// Configuration for the remote server.
#[derive(Clone, Debug)]
pub struct RemoteServerConfig {
    pub bind_address: SocketAddr,
    pub security: RemoteServerSecurity,
    pub auth: BasicAuthConfig,
    pub max_connections: usize,
    pub request_timeout: Duration,
    pub client_idle_timeout: Duration,
}

#[derive(Clone, Debug)]
pub enum RemoteServerSecurity {
    Tls {
        certificate_path: PathBuf,
        private_key_path: PathBuf,
    },
    Plain,
}

impl RemoteStoreServer {
    /// Creates a new server instance bound to the provided store.
    pub fn new(store: SimpleStoreFacade, config: RemoteServerConfig) -> Result<Self, ServerError> {
        if config.max_connections == 0 {
            return Err(ServerError::InvalidConfig(
                "max_connections must be greater than zero".into(),
            ));
        }

        let security = match &config.security {
            RemoteServerSecurity::Tls {
                certificate_path,
                private_key_path,
            } => ServerSecurityMode::Tls(build_tls_config(certificate_path, private_key_path)?),
            RemoteServerSecurity::Plain => ServerSecurityMode::Plain,
        };
        let expected_header = config.auth.authorization_header();

        let metrics = Arc::new(ServerMetrics::new());
        let state = Arc::new(ServerState {
            store,
            expected_auth_header: expected_header,
            request_timeout: config.request_timeout,
            idle_timeout: config.client_idle_timeout,
            metrics: metrics.clone(),
        });

        Ok(Self {
            bind_address: config.bind_address,
            security,
            limits: ServerLimits {
                max_connections: config.max_connections,
            },
            state,
        })
    }

    /// Binds the configured socket address and returns the listener.
    pub(crate) async fn bind_listener(&self) -> Result<TcpListener, ServerError> {
        let listener = TcpListener::bind(self.bind_address).await?;
        Ok(listener)
    }

    /// Starts listening for connections until `shutdown` resolves.
    pub async fn run_until_shutdown<F>(&self, shutdown: F) -> Result<(), ServerError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let listener = self.bind_listener().await?;
        self.run_until_shutdown_with_listener(listener, shutdown)
            .await
    }

    pub(crate) async fn run_until_shutdown_with_listener<F>(
        &self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), ServerError>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        tracing::info!(address = %self.bind_address, "remote store server listening");

        let semaphore = Arc::new(Semaphore::new(self.limits.max_connections));
        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("shutdown signal received; stopping remote server");
                    break;
                }
                accept_res = listener.accept() => {
                    let (socket, peer_addr) = match accept_res {
                        Ok(inner) => inner,
                        Err(err) => {
                            tracing::error!(?err, "failed to accept incoming connection");
                            continue;
                        }
                    };

                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tracing::warn!(?peer_addr, "maximum concurrent clients reached; dropping connection");
                            drop(socket);
                            continue;
                        }
                    };

                    let state = Arc::clone(&self.state);
                    let security = self.security.clone();

                    tokio::spawn(async move {
                        let _permit = permit;
                        state.metrics.connection_opened();

                        let result =
                            handle_socket(state.clone(), security, socket, peer_addr).await;

                        if let Err(err) = result {
                            tracing::debug!(?peer_addr, ?err, "connection terminated");
                        }

                        state.metrics.connection_closed();
                    });
                }
            }
        }

        Ok(())
    }

    /// Returns a handle that can be used to inspect metrics.
    pub fn handle(&self) -> RemoteServerHandle {
        RemoteServerHandle {
            metrics: Arc::clone(&self.state.metrics),
        }
    }
}

impl RemoteServerHandle {
    pub fn snapshot(&self) -> ServerMetricsSnapshot {
        self.metrics.snapshot()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ServerError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("io error at {path:?}: {source}")]
    IoPath {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("tls configuration error: {0}")]
    TlsConfig(String),
    #[error("invalid server configuration: {0}")]
    InvalidConfig(String),
}

#[derive(Debug)]
struct ServerLimits {
    max_connections: usize,
}

#[derive(Clone)]
struct ServerState {
    store: SimpleStoreFacade,
    expected_auth_header: String,
    request_timeout: Duration,
    idle_timeout: Duration,
    metrics: Arc<ServerMetrics>,
}

#[derive(Debug)]
struct ServerMetrics {
    active_connections: AtomicUsize,
    total_connections: AtomicUsize,
    total_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_request_latency_ns: AtomicU64,
}

impl ServerMetrics {
    fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            total_connections: AtomicUsize::new(0),
            total_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_request_latency_ns: AtomicU64::new(0),
        }
    }

    fn connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::AcqRel);
        self.total_connections.fetch_add(1, Ordering::AcqRel);
    }

    fn connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::AcqRel);
    }

    fn request_completed(&self, latency: Duration) {
        self.total_requests.fetch_add(1, Ordering::AcqRel);
        let nanos = latency.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.total_request_latency_ns
            .fetch_add(nanos, Ordering::AcqRel);
    }

    fn record_failure(&self) {
        self.failed_requests.fetch_add(1, Ordering::AcqRel);
    }

    fn snapshot(&self) -> ServerMetricsSnapshot {
        let total_requests = self.total_requests.load(Ordering::Acquire);
        let total_latency = self.total_request_latency_ns.load(Ordering::Acquire);
        ServerMetricsSnapshot {
            active_connections: self.active_connections.load(Ordering::Acquire),
            total_connections: self.total_connections.load(Ordering::Acquire),
            total_requests,
            failed_requests: self.failed_requests.load(Ordering::Acquire),
            average_request_latency_micros: if total_requests == 0 {
                0
            } else {
                (total_latency / total_requests) / 1_000
            },
        }
    }
}

async fn handle_socket(
    state: Arc<ServerState>,
    security: ServerSecurityMode,
    socket: TcpStream,
    peer_addr: SocketAddr,
) -> Result<(), ConnectionError> {
    match security {
        ServerSecurityMode::Tls(config) => {
            let acceptor = TlsAcceptor::from(config);
            let tls_stream = match acceptor.accept(socket).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(?peer_addr, ?err, "tls handshake failed");
                    return Err(ConnectionError::Io);
                }
            };

            serve_connection(state, tls_stream, peer_addr).await
        }
        ServerSecurityMode::Plain => serve_connection(state, socket, peer_addr).await,
    }
}

async fn serve_connection<S>(
    state: Arc<ServerState>,
    stream: S,
    peer_addr: SocketAddr,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (read_half, mut write_half) = tokio::io::split(stream);
    let mut reader = AsyncBufReader::new(read_half);

    if let Err(err) = authenticate(&mut reader, &mut write_half, &state).await {
        state.metrics.record_failure();
        return Err(ConnectionError::Protocol(err));
    }

    let mut request_buf = Vec::with_capacity(Key::BYTES * MAX_KEYS_PER_REQUEST);

    loop {
        let key_count = match read_key_count(&mut reader, &state).await {
            Ok(Some(count)) => count,
            Ok(None) => return Ok(()),
            Err(ConnectionError::Protocol(err)) => {
                send_code(&mut write_half, err.code()).await.ok();
                state.metrics.record_failure();
                return Err(ConnectionError::Protocol(err));
            }
            Err(err) => return Err(err),
        };

        let payload_len = key_count as usize * Key::BYTES;
        request_buf.resize(payload_len, 0);

        if let Err(err) =
            read_exact_with_timeout(&mut reader, &mut request_buf, state.request_timeout).await
        {
            match err {
                ConnectionError::Protocol(proto) => {
                    send_code(&mut write_half, proto.code()).await.ok();
                    state.metrics.record_failure();
                    return Err(ConnectionError::Protocol(proto));
                }
                other => return Err(other),
            }
        }

        let start = Instant::now();

        let keys: Vec<Key> = request_buf
            .chunks_exact(Key::BYTES)
            .map(|chunk| {
                let mut key = [0u8; Key::BYTES];
                key.copy_from_slice(chunk);
                Key::from(key)
            })
            .collect();

        let store = state.store.clone();
        let values = match spawn_blocking(move || {
            store.multi_get(&keys).map(|values| {
                values
                    .into_iter()
                    .map(Value::into_inner)
                    .collect::<Vec<Arc<[u8]>>>()
            })
        })
        .await
        {
            Ok(Ok(values)) => values,
            Ok(Err(err)) => {
                tracing::error!(?err, "store get failed");
                send_code(&mut write_half, ERR_STORE_FAILURE).await.ok();
                state.metrics.record_failure();
                return Err(ConnectionError::Store);
            }
            Err(join_err) => {
                tracing::error!(?join_err, "blocking store read panicked");
                send_code(&mut write_half, ERR_STORE_FAILURE).await.ok();
                state.metrics.record_failure();
                return Err(ConnectionError::Store);
            }
        };

        if values.len() != key_count as usize {
            tracing::error!(
                returned = values.len(),
                requested = key_count,
                "store returned mismatched value count"
            );
            send_code(&mut write_half, ERR_STORE_FAILURE).await.ok();
            state.metrics.record_failure();
            return Err(ConnectionError::Store);
        }

        let mut total_response_bytes = 0usize;
        for value in &values {
            let len = value.len();
            if len > MAX_VALUE_BYTES {
                tracing::warn!(
                    value_len = len,
                    "value length exceeds MAX_VALUE_BYTES in remote response"
                );
                send_code(&mut write_half, ERR_INVALID_PAYLOAD).await.ok();
                state.metrics.record_failure();
                return Err(ConnectionError::Protocol(ProtocolError::InvalidPayload));
            }
            total_response_bytes = match total_response_bytes.checked_add(VALUE_LEN_WIDTH + len) {
                Some(total) => total,
                None => {
                    send_code(&mut write_half, ERR_INVALID_PAYLOAD).await.ok();
                    state.metrics.record_failure();
                    return Err(ConnectionError::Protocol(ProtocolError::InvalidPayload));
                }
            };
            if total_response_bytes > MAX_RESPONSE_BYTES {
                send_code(&mut write_half, ERR_INVALID_PAYLOAD).await.ok();
                state.metrics.record_failure();
                return Err(ConnectionError::Protocol(ProtocolError::InvalidPayload));
            }
        }

        for value in values {
            let len = value.len() as u16;
            if let Err(err) = write_half.write_all(&len.to_le_bytes()).await {
                tracing::warn!(?err, "failed to write value length");
                return Err(ConnectionError::Io);
            }
            if !value.is_empty() {
                if let Err(err) = write_half.write_all(value.as_ref()).await {
                    tracing::warn!(?err, "failed to write value payload");
                    return Err(ConnectionError::Io);
                }
            }
        }
        if let Err(err) = write_half.flush().await {
            tracing::warn!(?err, "failed to flush remote response");
            return Err(ConnectionError::Io);
        }

        state.metrics.request_completed(start.elapsed());
        tracing::debug!(
            ?peer_addr,
            request_keys = key_count,
            "served remote read request"
        );
    }
}

async fn authenticate<S>(
    reader: &mut BufferedReader<S>,
    writer: &mut StreamWriter<S>,
    state: &ServerState,
) -> Result<(), ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut line = match read_limited_auth_line(reader, state.idle_timeout).await {
        Ok(bytes) => bytes,
        Err(ProtocolError::InvalidHeader) => {
            let _ = send_code(writer, ERR_INVALID_HEADER).await;
            return Err(ProtocolError::InvalidHeader);
        }
        Err(err) => return Err(err),
    };

    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }

    if line.is_empty() {
        return Err(ProtocolError::Unauthorized);
    }

    let received = String::from_utf8_lossy(&line);
    let trimmed = received.trim();
    if trimmed != state.expected_auth_header {
        let _ = send_code(writer, ERR_UNAUTHORIZED).await;
        return Err(ProtocolError::Unauthorized);
    }

    send_handshake(writer)
        .await
        .map_err(|_| ProtocolError::Timeout)?;
    Ok(())
}

async fn read_limited_auth_line<S>(
    reader: &mut BufferedReader<S>,
    idle_timeout: Duration,
) -> Result<Vec<u8>, ProtocolError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut line = Vec::with_capacity(128);
    let deadline = Instant::now() + idle_timeout;

    loop {
        if line.len() >= MAX_AUTH_HEADER_LEN {
            return Err(ProtocolError::InvalidHeader);
        }

        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(ProtocolError::Timeout)?;

        let (bytes_to_consume, finished) = {
            let buf = match timeout(remaining, reader.fill_buf()).await {
                Ok(Ok(chunk)) => chunk,
                Ok(Err(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Err(ProtocolError::Unauthorized)
                }
                Ok(Err(_)) => return Err(ProtocolError::Unauthorized),
                Err(_) => return Err(ProtocolError::Timeout),
            };

            if buf.is_empty() {
                return Err(ProtocolError::Unauthorized);
            }

            let newline_pos = buf.iter().position(|byte| *byte == b'\n');
            let current_len = line.len();
            if let Some(idx) = newline_pos {
                if current_len + idx > MAX_AUTH_HEADER_LEN {
                    return Err(ProtocolError::InvalidHeader);
                }
                line.extend_from_slice(&buf[..=idx]);
                (idx + 1, true)
            } else {
                let remaining_capacity = MAX_AUTH_HEADER_LEN - current_len;
                let bytes_to_take = remaining_capacity.min(buf.len());
                line.extend_from_slice(&buf[..bytes_to_take]);
                (bytes_to_take, false)
            }
        };

        reader.consume(bytes_to_consume);

        if finished {
            return Ok(line);
        }
    }
}

async fn read_key_count<S>(
    reader: &mut BufferedReader<S>,
    state: &ServerState,
) -> Result<Option<u8>, ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut header = [0u8; 1];
    match timeout(state.idle_timeout, reader.read_exact(&mut header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Ok(Err(err)) => {
            tracing::debug!(?err, "failed to read request header");
            return Err(ConnectionError::Io);
        }
        Err(_) => return Err(ConnectionError::Protocol(ProtocolError::Timeout)),
    }

    let count = header[0];
    if count == 0 || count as usize > MAX_KEYS_PER_REQUEST {
        return Err(ConnectionError::Protocol(ProtocolError::InvalidHeader));
    }

    Ok(Some(count))
}

async fn read_exact_with_timeout<S>(
    reader: &mut BufferedReader<S>,
    buffer: &mut [u8],
    timeout_duration: Duration,
) -> Result<(), ConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(timeout_duration, reader.read_exact(buffer)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(ConnectionError::Protocol(ProtocolError::InvalidPayload))
        }
        Ok(Err(err)) => {
            tracing::debug!(?err, "failed while reading request payload");
            Err(ConnectionError::Io)
        }
        Err(_) => Err(ConnectionError::Protocol(ProtocolError::Timeout)),
    }
}

async fn send_code<S>(writer: &mut StreamWriter<S>, code: u8) -> Result<(), std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    writer.write_all(&[code]).await?;
    writer.flush().await
}

async fn send_handshake<S>(writer: &mut StreamWriter<S>) -> Result<(), std::io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let key_bytes = Key::BYTES as u16;
    let mut payload = [0u8; 1 + KEY_BYTES_FIELD_WIDTH];
    payload[0] = AUTH_READY;
    payload[1..].copy_from_slice(&key_bytes.to_le_bytes());
    writer.write_all(&payload).await?;
    writer.flush().await
}

fn build_tls_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<Arc<TlsServerConfig>, ServerError> {
    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = TlsServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|err| ServerError::TlsConfig(format!("invalid certificate/key pair: {err}")))?;

    Ok(Arc::new(config))
}

fn load_certs(path: &Path) -> Result<Vec<Certificate>, ServerError> {
    let file = File::open(path).map_err(|source| ServerError::IoPath {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|err| ServerError::TlsConfig(format!("failed to parse {path:?}: {err}")))?;
    if certs.is_empty() {
        return Err(ServerError::TlsConfig(format!(
            "no certificates found in {:?}",
            path
        )));
    }
    Ok(certs.into_iter().map(Certificate).collect())
}

fn load_private_key(path: &Path) -> Result<PrivateKey, ServerError> {
    let file = File::open(path).map_err(|source| ServerError::IoPath {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);

    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .map_err(|err| ServerError::TlsConfig(format!("failed to parse {path:?}: {err}")))?;
    if let Some(key) = keys.pop() {
        return Ok(PrivateKey(key));
    }

    let file = File::open(path).map_err(|source| ServerError::IoPath {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);

    let mut rsa_keys = rustls_pemfile::rsa_private_keys(&mut reader)
        .map_err(|err| ServerError::TlsConfig(format!("failed to parse {path:?}: {err}")))?;
    if let Some(key) = rsa_keys.pop() {
        return Ok(PrivateKey(key));
    }

    Err(ServerError::TlsConfig(format!(
        "no usable private key found in {:?}",
        path
    )))
}

#[derive(Debug)]
enum ConnectionError {
    Io,
    Protocol(ProtocolError),
    Store,
}

#[derive(Debug)]
enum ProtocolError {
    InvalidHeader,
    InvalidPayload,
    Unauthorized,
    Timeout,
}

impl ProtocolError {
    fn code(&self) -> u8 {
        match self {
            ProtocolError::InvalidHeader => ERR_INVALID_HEADER,
            ProtocolError::InvalidPayload => ERR_INVALID_PAYLOAD,
            ProtocolError::Unauthorized => ERR_UNAUTHORIZED,
            ProtocolError::Timeout => ERR_TIMEOUT,
        }
    }
}

#[derive(Clone)]
enum ServerSecurityMode {
    Tls(Arc<TlsServerConfig>),
    Plain,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StoreResult;
    use crate::types::{BlockId, Operation, StoreKey as Key, Value};
    use crate::BlockOrchestrator;
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(Default)]
    struct CountingOrchestrator {
        values: Mutex<HashMap<Key, Value>>,
        single_fetches: AtomicUsize,
        batch_fetches: AtomicUsize,
    }

    impl CountingOrchestrator {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn put(&self, key: Key, value: Value) {
            self.values.lock().unwrap().insert(key, value);
        }

        fn batch_fetches(&self) -> usize {
            self.batch_fetches.load(Ordering::Acquire)
        }

        fn single_fetches(&self) -> usize {
            self.single_fetches.load(Ordering::Acquire)
        }
    }

    impl BlockOrchestrator for CountingOrchestrator {
        fn apply_operations(&self, _block_height: BlockId, ops: Vec<Operation>) -> StoreResult<()> {
            let mut state = self.values.lock().unwrap();
            for op in ops {
                if op.value.is_delete() {
                    state.remove(&op.key);
                } else {
                    state.insert(op.key, op.value);
                }
            }
            Ok(())
        }

        fn revert_to(&self, _block: BlockId) -> StoreResult<()> {
            Ok(())
        }

        fn fetch(&self, key: Key) -> StoreResult<Value> {
            self.single_fetches.fetch_add(1, Ordering::AcqRel);
            Ok(self
                .values
                .lock()
                .unwrap()
                .get(&key)
                .cloned()
                .unwrap_or_else(Value::empty))
        }

        fn fetch_many(&self, keys: &[Key]) -> StoreResult<Vec<Value>> {
            self.batch_fetches.fetch_add(1, Ordering::AcqRel);
            let state = self.values.lock().unwrap();
            Ok(keys
                .iter()
                .map(|key| state.get(key).cloned().unwrap_or_else(Value::empty))
                .collect())
        }

        fn pop(&self, _block_height: BlockId, key: Key) -> StoreResult<Value> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .remove(&key)
                .unwrap_or_else(Value::empty))
        }

        fn metrics(&self) -> Option<&crate::metrics::StoreMetrics> {
            None
        }

        fn current_block(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn applied_block_height(&self) -> BlockId {
            0
        }

        fn durable_block_height(&self) -> StoreResult<BlockId> {
            Ok(0)
        }

        fn shutdown(&self) -> StoreResult<()> {
            Ok(())
        }

        fn ensure_healthy(&self) -> StoreResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn serve_connection_batches_remote_reads() {
        let orchestrator = CountingOrchestrator::new();
        orchestrator.put([1u8; Key::BYTES].into(), 7.into());
        orchestrator.put([2u8; Key::BYTES].into(), 19.into());

        let facade = SimpleStoreFacade::new_for_testing(
            orchestrator.clone(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicUsize::new(1)),
        );

        let state = Arc::new(ServerState {
            store: facade,
            expected_auth_header: "proto proto".into(),
            request_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(1),
            metrics: Arc::new(ServerMetrics::new()),
        });

        let (mut client, server) = tokio::io::duplex(1024);
        let server_state = Arc::clone(&state);
        let server_task = tokio::spawn(async move {
            serve_connection(
                server_state,
                server,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            )
            .await
        });

        client.write_all(b"proto proto\n").await.unwrap();
        let mut handshake = [0u8; 1 + KEY_BYTES_FIELD_WIDTH];
        client.read_exact(&mut handshake).await.unwrap();
        assert_eq!(handshake[0], AUTH_READY);
        assert_eq!(
            u16::from_le_bytes([handshake[1], handshake[2]]) as usize,
            Key::BYTES
        );

        let keys = [[1u8; Key::BYTES], [2u8; Key::BYTES]];
        client.write_all(&[keys.len() as u8]).await.unwrap();
        for key in keys {
            client.write_all(&key).await.unwrap();
        }

        let mut values = Vec::new();
        for _ in 0..keys.len() {
            let mut len_buf = [0u8; VALUE_LEN_WIDTH];
            client.read_exact(&mut len_buf).await.unwrap();
            let value_len = u16::from_le_bytes(len_buf) as usize;
            let mut value_bytes = vec![0u8; value_len];
            if value_len > 0 {
                client.read_exact(&mut value_bytes).await.unwrap();
            }
            let mut buf = [0u8; 8];
            buf[..value_len].copy_from_slice(&value_bytes);
            values.push(u64::from_le_bytes(buf));
        }
        drop(client);

        server_task.await.unwrap().unwrap();

        assert_eq!(values, vec![7, 19]);
        assert_eq!(orchestrator.batch_fetches(), 1);
        assert_eq!(orchestrator.single_fetches(), 0);
    }
}
