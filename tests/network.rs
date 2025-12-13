use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use rollblock::client::{ClientConfig, ClientError, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;
use rollblock::types::{Operation, StoreKey as Key};
use rollblock::{RemoteServerSettings, SimpleStoreFacade, StoreConfig, StoreError, StoreFacade};

fn bytes_to_u64_le(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let len = bytes.len().min(buf.len());
    buf[..len].copy_from_slice(&bytes[..len]);
    u64::from_le_bytes(buf)
}

fn workspace_tmp() -> PathBuf {
    let path = std::env::current_dir().unwrap().join("target/testdata");
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn generate_tls_material(base: &Path) -> (PathBuf, PathBuf) {
    let cert = generate_simple_self_signed(["localhost".to_string()]).unwrap();
    let cert_path = base.join("server.crt");
    let key_path = base.join("server.key");
    std::fs::write(&cert_path, cert.serialize_pem().unwrap()).unwrap();
    std::fs::write(&key_path, cert.serialize_private_key_pem()).unwrap();
    (cert_path, key_path)
}

fn allocate_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn network_round_trip_tls_auto_server() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let key_a: Key = [1u8; Key::BYTES].into();
    let key_b: Key = [2u8; Key::BYTES].into();
    let (cert_path, key_path) = generate_tls_material(temp.path());
    let port = allocate_port();
    let auth = BasicAuthConfig::new("tester", "secret");

    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("tester", "secret")
        .with_tls(cert_path.clone(), key_path);

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false)
        .expect("valid config")
        .with_remote_server(settings);
    let store = SimpleStoreFacade::new(config).unwrap();
    store
        .set(
            1,
            vec![
                Operation {
                    key: key_a,
                    value: 42.into(),
                },
                Operation {
                    key: key_b,
                    value: 7.into(),
                },
            ],
        )
        .unwrap();

    let addr = format!("127.0.0.1:{port}");
    let client_config = ClientConfig::new("localhost", cert_path.clone(), auth.clone())
        .with_timeout(Duration::from_secs(2));
    let mut client = RemoteStoreClient::connect(&addr, client_config).unwrap();
    let single = client.get_one(key_a).unwrap();
    let batch = client.get(&[key_a, key_b]).unwrap();
    client.close().unwrap();

    assert_eq!(bytes_to_u64_le(&single), 42);
    let numeric: Vec<_> = batch.into_iter().map(|v| bytes_to_u64_le(&v)).collect();
    assert_eq!(numeric, vec![42, 7]);
    store.close().unwrap();
}

#[test]
fn network_rejects_bad_credentials() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let key: Key = [9u8; Key::BYTES].into();
    let (cert_path, key_path) = generate_tls_material(temp.path());
    let port = allocate_port();
    let bad_auth = BasicAuthConfig::new("user", "wrong-pass");

    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("user", "valid-pass")
        .with_tls(cert_path.clone(), key_path);

    let config = StoreConfig::new(&data_dir, 2, 16, 1, false)
        .expect("valid config")
        .with_remote_server(settings);
    let store = SimpleStoreFacade::new(config).unwrap();
    store
        .set(
            1,
            vec![Operation {
                key,
                value: 99.into(),
            }],
        )
        .unwrap();

    let addr = format!("127.0.0.1:{port}");
    let client_config = ClientConfig::new("localhost", cert_path.clone(), bad_auth)
        .with_timeout(Duration::from_secs(1));
    match RemoteStoreClient::connect(&addr, client_config) {
        Err(ClientError::Server { code }) => assert_eq!(code, 4),
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("connection with bad credentials should fail"),
    }

    store.close().unwrap();
}

#[test]
fn network_round_trip_without_tls() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let key: Key = [3u8; Key::BYTES].into();
    let port = allocate_port();
    let auth = BasicAuthConfig::new("tester", "secret");

    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("tester", "secret")
        .without_tls();

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false)
        .expect("valid config")
        .with_remote_server(settings);
    let store = SimpleStoreFacade::new(config).unwrap();
    store
        .set(
            1,
            vec![Operation {
                key,
                value: 123.into(),
            }],
        )
        .unwrap();

    let addr = format!("127.0.0.1:{port}");
    let client_config =
        ClientConfig::without_tls(auth.clone()).with_timeout(Duration::from_secs(1));
    let mut client = RemoteStoreClient::connect(&addr, client_config).unwrap();
    let value = client.get_one(key).unwrap();
    client.close().unwrap();

    assert_eq!(bytes_to_u64_le(&value), 123);
    store.close().unwrap();
}

#[test]
fn network_rejects_key_width_mismatch() {
    let port = allocate_port();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

    let listener = TcpListener::bind(addr).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut header = Vec::new();
        reader.read_until(b'\n', &mut header).unwrap();

        let bad_width = (Key::BYTES as u16) + 1;
        let mut payload = [0u8; 3];
        payload[0] = 0; // AUTH_READY
        payload[1..].copy_from_slice(&bad_width.to_le_bytes());
        stream.write_all(&payload).unwrap();
        stream.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
    });

    let client_config = ClientConfig::without_tls(BasicAuthConfig::new("user", "pass"))
        .with_timeout(Duration::from_secs(1));
    let err = RemoteStoreClient::connect(addr, client_config).unwrap_err();
    match err {
        ClientError::KeyWidthMismatch { server, client } => {
            assert_eq!(server, Key::BYTES + 1);
            assert_eq!(client, Key::BYTES);
        }
        other => panic!("expected KeyWidthMismatch, got {other}"),
    }

    server.join().unwrap();
}

#[test]
fn default_remote_server_settings_require_custom_credentials() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let port = allocate_port();
    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port));

    let config = StoreConfig::new(&data_dir, 2, 16, 1, false)
        .expect("valid config")
        .with_remote_server(settings);

    let err = SimpleStoreFacade::new(config)
        .err()
        .expect("placeholder credentials rejected");
    assert!(matches!(err, StoreError::RemoteServerCredentialsMissing));
}

#[test]
fn enable_remote_server_without_custom_credentials_fails() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false)
        .expect("valid config")
        .enable_remote_server()
        .expect("default remote server settings exist");

    let err = SimpleStoreFacade::new(config)
        .err()
        .expect("server should refuse placeholder credentials");
    assert!(matches!(err, StoreError::RemoteServerCredentialsMissing));
}

#[test]
fn network_server_disabled_by_default() {
    let default_addr = "127.0.0.1:9443";
    let probe = match std::net::TcpListener::bind(default_addr) {
        Ok(listener) => listener,
        Err(_) => {
            eprintln!("skipping disablement check because {default_addr} is already in use");
            return;
        }
    };
    drop(probe);

    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false).expect("valid config");
    let store = SimpleStoreFacade::new(config).unwrap();

    match std::net::TcpListener::bind(default_addr) {
        Ok(listener) => drop(listener),
        Err(err) => {
            store.close().unwrap();
            panic!("default remote server should stay disabled: {err}");
        }
    }

    store.close().unwrap();
}
