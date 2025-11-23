use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustls::client::ServerName;
use rustls::{ClientConfig as TlsClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls_pemfile::certs;

use crate::net::BasicAuthConfig;
use crate::types::{Key, MAX_VALUE_BYTES};

const KEY_WIDTH: usize = 8;
const VALUE_LEN_WIDTH: usize = 2;
const MAX_KEYS_PER_REQUEST: usize = 255;
const AUTH_READY: u8 = 0;

/// Configuration required to establish a remote client connection.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub security: ClientSecurity,
    pub auth: BasicAuthConfig,
    pub timeout: Option<Duration>,
}

#[derive(Clone, Debug)]
pub enum ClientSecurity {
    Tls {
        server_name: String,
        ca_certificate: PathBuf,
    },
    Plain,
}

impl ClientConfig {
    pub fn new(
        server_name: impl Into<String>,
        ca_certificate: impl Into<PathBuf>,
        auth: BasicAuthConfig,
    ) -> Self {
        Self::with_tls(server_name, ca_certificate, auth)
    }

    pub fn with_tls(
        server_name: impl Into<String>,
        ca_certificate: impl Into<PathBuf>,
        auth: BasicAuthConfig,
    ) -> Self {
        Self {
            security: ClientSecurity::Tls {
                server_name: server_name.into(),
                ca_certificate: ca_certificate.into(),
            },
            auth,
            timeout: None,
        }
    }

    pub fn without_tls(auth: BasicAuthConfig) -> Self {
        Self {
            security: ClientSecurity::Plain,
            auth,
            timeout: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

/// Blocking client for the remote protocol (supports TLS or plaintext).
pub struct RemoteStoreClient {
    stream: ClientStream,
    request_buf: Vec<u8>,
    response_buf: Vec<u8>,
}

impl RemoteStoreClient {
    /// Establishes a connection to the remote store.
    pub fn connect<A: ToSocketAddrs>(addr: A, config: ClientConfig) -> Result<Self, ClientError> {
        let ClientConfig {
            security,
            auth,
            timeout,
        } = config;

        let stream = TcpStream::connect(addr).map_err(map_io)?;
        stream.set_nodelay(true).ok();

        if let Some(timeout) = timeout {
            stream.set_read_timeout(Some(timeout)).map_err(map_io)?;
            stream.set_write_timeout(Some(timeout)).map_err(map_io)?;
        }

        let mut stream = match security {
            ClientSecurity::Tls {
                server_name,
                ca_certificate,
            } => {
                let tls_config = build_tls_client(&ca_certificate)?;
                let server_name = ServerName::try_from(server_name.as_str())
                    .map_err(|_| ClientError::InvalidDnsName(server_name.clone()))?;
                let connection = ClientConnection::new(tls_config, server_name)?;
                ClientStream::Tls(Box::new(StreamOwned::new(connection, stream)))
            }
            ClientSecurity::Plain => ClientStream::Plain(stream),
        };

        let mut header_line = auth.authorization_header();
        header_line.push('\n');
        stream.write_all(header_line.as_bytes()).map_err(map_io)?;
        stream.flush().map_err(map_io)?;

        let mut ack = [0u8; 1];
        let received = stream.read(&mut ack).map_err(map_io)?;
        if received == 0 {
            return Err(ClientError::ConnectionClosed {
                received: 0,
                expected: 1,
            });
        }
        if ack[0] != AUTH_READY {
            return Err(ClientError::Server { code: ack[0] });
        }

        Ok(Self {
            stream,
            request_buf: Vec::with_capacity(1 + KEY_WIDTH * MAX_KEYS_PER_REQUEST),
            response_buf: Vec::with_capacity(1024),
        })
    }

    /// Executes a batch read for the provided keys.
    ///
    /// Returned values preserve the remote payload verbatim. Missing keys are
    /// encoded as empty byte vectors.
    pub fn get(&mut self, keys: &[Key]) -> Result<Vec<Vec<u8>>, ClientError> {
        if keys.is_empty() {
            return Err(ClientError::InvalidRequest(
                "at least one key must be provided",
            ));
        }
        if keys.len() > MAX_KEYS_PER_REQUEST {
            return Err(ClientError::InvalidRequest(
                "too many keys in a single request",
            ));
        }

        self.request_buf.clear();
        self.request_buf.push(keys.len() as u8);
        for key in keys {
            self.request_buf.extend_from_slice(key);
        }

        self.stream.write_all(&self.request_buf).map_err(map_io)?;
        self.stream.flush().map_err(map_io)?;

        let mut values = Vec::with_capacity(keys.len());
        let mut len_buf = [0u8; VALUE_LEN_WIDTH];
        for (index, _) in keys.iter().enumerate() {
            read_response_chunk(&mut self.stream, &mut len_buf, index == 0)?;
            let value_len = u16::from_le_bytes(len_buf) as usize;
            if value_len > MAX_VALUE_BYTES {
                return Err(ClientError::InvalidResponse(
                    "value length exceeds MAX_VALUE_BYTES",
                ));
            }
            if self.response_buf.len() < value_len {
                self.response_buf.resize(value_len, 0);
            }
            if value_len > 0 {
                let buffer = &mut self.response_buf[..value_len];
                read_response_chunk(&mut self.stream, buffer, false)?;
            }
            values.push(self.response_buf[..value_len].to_vec());
        }

        Ok(values)
    }

    /// Reads a single key from the remote store.
    ///
    /// Missing keys return an empty vector.
    pub fn get_one(&mut self, key: Key) -> Result<Vec<u8>, ClientError> {
        let values = self.get(std::slice::from_ref(&key))?;
        Ok(values.into_iter().next().unwrap_or_default())
    }

    /// Closes the TLS session gracefully.
    pub fn close(self) -> Result<(), ClientError> {
        self.stream.close().map_err(map_io)
    }
}

fn read_response_chunk(
    stream: &mut ClientStream,
    buffer: &mut [u8],
    allow_error_code: bool,
) -> Result<(), ClientError> {
    if buffer.is_empty() {
        return Ok(());
    }

    let mut read = 0;
    while read < buffer.len() {
        let n = stream.read(&mut buffer[read..]).map_err(map_io)?;
        if n == 0 {
            if allow_error_code && read == 1 {
                return Err(ClientError::Server { code: buffer[0] });
            }
            return Err(ClientError::ConnectionClosed {
                received: read,
                expected: buffer.len(),
            });
        }
        read += n;
    }

    Ok(())
}

fn build_tls_client(path: &Path) -> Result<Arc<TlsClientConfig>, ClientError> {
    let root_store = load_root_store(path)?;

    let tls_config = TlsClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(tls_config))
}

fn load_root_store(path: &Path) -> Result<RootCertStore, ClientError> {
    let file = File::open(path).map_err(|source| ClientError::IoPath {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = BufReader::new(file);
    let certs = certs(&mut reader)
        .map_err(|err| ClientError::TlsConfig(format!("failed to parse {path:?}: {err}")))?;

    if certs.is_empty() {
        return Err(ClientError::TlsConfig(format!(
            "no certificates found in {:?}",
            path
        )));
    }

    let mut store = RootCertStore::empty();
    let (added, _) = store.add_parsable_certificates(&certs);
    if added == 0 {
        return Err(ClientError::TlsConfig(format!(
            "no valid certificates in {:?}",
            path
        )));
    }
    Ok(store)
}

fn map_io(err: std::io::Error) -> ClientError {
    match err.kind() {
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock => ClientError::Timeout,
        _ => ClientError::Io(err),
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("io error: {0}")]
    Io(std::io::Error),
    #[error("network operation timed out")]
    Timeout,
    #[error("tls error: {0}")]
    Tls(#[from] rustls::Error),
    #[error("invalid dns name: {0}")]
    InvalidDnsName(String),
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("invalid response from server: {0}")]
    InvalidResponse(&'static str),
    #[error("server returned error code {code}")]
    Server { code: u8 },
    #[error("connection closed before response (received {received} of {expected} bytes)")]
    ConnectionClosed { received: usize, expected: usize },
    #[error("io error at {path:?}: {source}")]
    IoPath {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to load certificates: {0}")]
    TlsConfig(String),
}

enum ClientStream {
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
    Plain(TcpStream),
}

impl ClientStream {
    fn close(self) -> std::io::Result<()> {
        match self {
            ClientStream::Tls(mut stream) => {
                stream.conn.send_close_notify();
                stream.flush()
            }
            ClientStream::Plain(stream) => stream.shutdown(Shutdown::Both),
        }
    }
}

impl Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            ClientStream::Tls(stream) => stream.read(buf),
            ClientStream::Plain(stream) => stream.read(buf),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            ClientStream::Tls(stream) => stream.write(buf),
            ClientStream::Plain(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            ClientStream::Tls(stream) => stream.flush(),
            ClientStream::Plain(stream) => stream.flush(),
        }
    }
}
