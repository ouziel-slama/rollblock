use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use rollblock::client::{ClientConfig, ClientError, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;
use rollblock::types::Operation;
use rollblock::{MhinStoreFacade, RemoteServerSettings, StoreConfig, StoreFacade};

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

    let key_a = [1u8; 8];
    let key_b = [2u8; 8];
    let (cert_path, key_path) = generate_tls_material(temp.path());
    let port = allocate_port();
    let auth = BasicAuthConfig::new("tester", "secret");

    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("tester", "secret")
        .with_tls(cert_path.clone(), key_path);

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false).with_remote_server(settings);
    let store = MhinStoreFacade::new(config).unwrap();
    store
        .set(
            1,
            vec![
                Operation {
                    key: key_a,
                    value: 42,
                },
                Operation {
                    key: key_b,
                    value: 7,
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

    assert_eq!(single, 42);
    assert_eq!(batch, vec![42, 7]);
    store.close().unwrap();
}

#[test]
fn network_rejects_bad_credentials() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let key = [9u8; 8];
    let (cert_path, key_path) = generate_tls_material(temp.path());
    let port = allocate_port();
    let bad_auth = BasicAuthConfig::new("user", "wrong-pass");

    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("user", "valid-pass")
        .with_tls(cert_path.clone(), key_path);

    let config = StoreConfig::new(&data_dir, 2, 16, 1, false).with_remote_server(settings);
    let store = MhinStoreFacade::new(config).unwrap();
    store.set(1, vec![Operation { key, value: 99 }]).unwrap();

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

    let key = [3u8; 8];
    let port = allocate_port();
    let auth = BasicAuthConfig::new("tester", "secret");

    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("tester", "secret")
        .without_tls();

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false).with_remote_server(settings);
    let store = MhinStoreFacade::new(config).unwrap();
    store.set(1, vec![Operation { key, value: 123 }]).unwrap();

    let addr = format!("127.0.0.1:{port}");
    let client_config =
        ClientConfig::without_tls(auth.clone()).with_timeout(Duration::from_secs(1));
    let mut client = RemoteStoreClient::connect(&addr, client_config).unwrap();
    let value = client.get_one(key).unwrap();
    client.close().unwrap();

    assert_eq!(value, 123);
    store.close().unwrap();
}

#[test]
fn default_plaintext_server_uses_proto_credentials() {
    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");

    let key = [4u8; 8];
    let port = allocate_port();
    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port));

    let config = StoreConfig::new(&data_dir, 2, 16, 1, false).with_remote_server(settings);
    let store = MhinStoreFacade::new(config).unwrap();
    store.set(1, vec![Operation { key, value: 77 }]).unwrap();

    let addr = format!("127.0.0.1:{port}");
    let client_config = ClientConfig::without_tls(BasicAuthConfig::new("proto", "proto"))
        .with_timeout(Duration::from_secs(1));
    let mut client = RemoteStoreClient::connect(&addr, client_config).unwrap();
    let value = client.get_one(key).unwrap();
    client.close().unwrap();

    assert_eq!(value, 77);

    let metrics = store.remote_server_metrics().expect("metrics available");
    assert!(metrics.total_connections >= 1);

    store.close().unwrap();
}

#[test]
fn network_round_trip_default_plaintext_config() {
    // The default configuration binds to 127.0.0.1:9443. If that address is already
    // occupied (for example when running tests alongside a developer instance),
    // skip the test instead of flaking.
    let default_addr = "127.0.0.1:9443";
    if std::net::TcpListener::bind(default_addr).is_err() {
        eprintln!("skipping default config test because {default_addr} is in use");
        return;
    }

    let workspace = workspace_tmp();
    let temp = tempfile::tempdir_in(&workspace).unwrap();
    let data_dir = temp.path().join("store");
    let key = [5u8; 8];

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false)
        .enable_remote_server()
        .expect("default remote server settings exist");
    let store = MhinStoreFacade::new(config).unwrap();
    store.set(1, vec![Operation { key, value: 11 }]).unwrap();

    let client_config = ClientConfig::without_tls(BasicAuthConfig::new("proto", "proto"))
        .with_timeout(Duration::from_secs(2));
    let mut client = RemoteStoreClient::connect(default_addr, client_config).unwrap();
    let value = client.get_one(key).unwrap();
    client.close().unwrap();

    assert_eq!(value, 11);
    store.close().unwrap();
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

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false);
    let store = MhinStoreFacade::new(config).unwrap();

    match std::net::TcpListener::bind(default_addr) {
        Ok(listener) => drop(listener),
        Err(err) => {
            store.close().unwrap();
            panic!("default remote server should stay disabled: {err}");
        }
    }

    store.close().unwrap();
}
