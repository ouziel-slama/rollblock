use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::thread;
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use rollblock::client::{ClientConfig, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;
use rollblock::types::Operation;
use rollblock::{MhinStoreFacade, RemoteServerSettings, StoreConfig, StoreFacade};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("store");

    let hot_key = [0xABu8; 8];
    let tls_dir = temp.path().join("tls");
    std::fs::create_dir_all(&tls_dir)?;
    let cert = generate_simple_self_signed(["localhost".to_string()])?;
    let cert_path = tls_dir.join("server.crt");
    let key_path = tls_dir.join("server.key");
    std::fs::write(&cert_path, cert.serialize_pem()?)?;
    std::fs::write(&key_path, cert.serialize_private_key_pem())?;

    let port = pick_free_port();
    let auth = BasicAuthConfig::new("demo-user", "demo-pass");
    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("demo-user", "demo-pass")
        .with_tls(cert_path.clone(), key_path);

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false).with_remote_server(settings);
    let store = MhinStoreFacade::new(config)?;
    store.set(
        1,
        vec![Operation {
            key: hot_key,
            value: 1_337.into(),
        }],
    )?;

    // Give the embedded runtime a moment to bind the socket.
    thread::sleep(Duration::from_millis(200));

    let addr = format!("127.0.0.1:{port}");
    let client_config =
        ClientConfig::new("localhost", cert_path, auth).with_timeout(Duration::from_secs(2));
    let mut client = RemoteStoreClient::connect(&addr, client_config)?;
    let value = client.get_one(hot_key)?;
    client.close()?;

    println!("Remote read returned {} bytes", value.len());

    if let Some(metrics) = store.remote_server_metrics() {
        println!(
            "Active connections: {}, total requests: {}",
            metrics.active_connections, metrics.total_requests
        );
    }

    store.close()?;
    Ok(())
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
