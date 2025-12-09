//! Network client example demonstrating remote store access
//!
//! This example shows how to:
//! - Configure a store with an embedded gRPC server (TLS + Basic Auth)
//! - Connect a remote client to read data
//! - Access server metrics
//!
//! Run with: cargo run --example network_client

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::thread;
use std::time::Duration;

use rcgen::generate_simple_self_signed;
use rollblock::client::{ClientConfig, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;
use rollblock::types::{Operation, StoreKey as Key};
use rollblock::{MhinStoreFacade, RemoteServerSettings, StoreConfig, StoreFacade};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 Network Client Example\n");
    println!("Demonstrating remote store access via gRPC with TLS and Basic Auth.\n");

    // Create temporary directories for data and TLS certificates
    let temp = tempfile::tempdir()?;
    let data_dir = temp.path().join("store");

    // Generate a self-signed TLS certificate for localhost
    println!("🔐 Generating self-signed TLS certificate...");
    let hot_key: Key = [0xABu8; Key::BYTES].into();
    let tls_dir = temp.path().join("tls");
    std::fs::create_dir_all(&tls_dir)?;
    let cert = generate_simple_self_signed(["localhost".to_string()])?;
    let cert_path = tls_dir.join("server.crt");
    let key_path = tls_dir.join("server.key");
    std::fs::write(&cert_path, cert.serialize_pem()?)?;
    std::fs::write(&key_path, cert.serialize_private_key_pem())?;
    println!("   ✓ Certificate generated\n");

    // Configure the embedded gRPC server with TLS and Basic Auth
    println!("📦 Configuring store with embedded gRPC server...");
    let port = pick_free_port();
    let auth = BasicAuthConfig::new("demo-user", "demo-pass");
    let settings = RemoteServerSettings::default()
        .with_bind_address(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
        .with_basic_auth("demo-user", "demo-pass")
        .with_tls(cert_path.clone(), key_path);

    let config = StoreConfig::new(&data_dir, 2, 32, 1, false)?.with_remote_server(settings);
    let store = MhinStoreFacade::new(config)?;
    println!("   ✓ Store created with server on port {}\n", port);

    // Write some data to the store
    println!("✏️  Writing test data...");
    store.set(
        1,
        vec![Operation {
            key: hot_key,
            value: 1_337.into(),
        }],
    )?;
    println!("   ✓ Data written\n");

    // Give the embedded runtime a moment to bind the socket
    thread::sleep(Duration::from_millis(200));

    // Connect a remote client with TLS and Basic Auth
    println!("🔗 Connecting remote client...");
    let addr = format!("127.0.0.1:{port}");
    let client_config =
        ClientConfig::new("localhost", cert_path, auth).with_timeout(Duration::from_secs(2));
    let mut client = RemoteStoreClient::connect(&addr, client_config)?;
    println!("   ✓ Client connected\n");

    // Read data remotely
    println!("🔍 Reading data via remote client...");
    let value = client.get_one(hot_key)?;
    client.close()?;
    println!("   ✓ Remote read returned {} bytes\n", value.len());

    // Display server metrics
    println!("📊 Server Metrics:");
    if let Some(metrics) = store.remote_server_metrics() {
        println!(
            "   • Active connections: {}\n   • Total requests: {}",
            metrics.active_connections, metrics.total_requests
        );
    }

    store.close()?;
    println!("\n✅ Network client example completed!");
    Ok(())
}

fn pick_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
