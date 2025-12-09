# Examples

## Quick Start

```shell
cargo add rollblock
```

```rust
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};
use rollblock::types::{Operation, StoreKey as Key};

fn main() -> rollblock::StoreResult<()> {
    let config = StoreConfig::new("./data", 4, 1000, 1, false)?;
    let store = MhinStoreFacade::new(config)?;

    let key: Key = [1, 2, 3, 4, 5, 6, 7, 8].into();
    store.set(1, vec![Operation { key, value: 42u64.into() }])?;

    let value = store.get(key)?;
    println!("Value: {}", value.to_u64().unwrap());

    let removed = store.pop(2, key)?;
    println!("Removed: {}", removed.to_u64().unwrap_or(0));

    store.rollback(0)?;
    assert!(store.get(key)?.is_delete());

    Ok(())
}
```

> The remote protocol advertises the compile-time key width during the handshake; clients compiled with a different width abort early.

---

## 1. MhinStoreFacade — Basic Key-Value Store

The simplest facade for batch operations at block granularity.

```rust
use rollblock::{MhinStoreFacade, StoreConfig, StoreFacade};
use rollblock::types::{Operation, StoreKey as Key, Value};

fn main() -> rollblock::StoreResult<()> {
    let config = StoreConfig::new("./data", 4, 1000, 1, false)?;
    let store = MhinStoreFacade::new(config)?;

    let key_a: Key = [0xAA; Key::BYTES].into();
    let key_b: Key = [0xBB; Key::BYTES].into();

    // Block 1: insert two keys
    store.set(1, vec![
        Operation { key: key_a, value: 100u64.into() },
        Operation { key: key_b, value: 200u64.into() },
    ])?;

    // Block 2: update key_a, delete key_b
    store.set(2, vec![
        Operation { key: key_a, value: 150u64.into() },
        Operation { key: key_b, value: Value::empty() }, // delete
    ])?;

    // Block 3: remove key_a while returning its previous value
    let previous = store.pop(3, key_a)?;
    println!("key_a used to be {}", previous.to_u64().unwrap_or(0));

    // Read values after the pop
    assert!(store.get(key_a)?.is_delete());
    assert!(store.get(key_b)?.is_delete());

    // Batch read
    let values = store.multi_get(&[key_a, key_b])?;
    println!("key_a = {:?}, key_b = {:?}", values[0], values[1]);

    // Rollback to block 1
    store.rollback(1)?;
    assert_eq!(store.get(key_a)?.to_u64(), Some(100));
    assert_eq!(store.get(key_b)?.to_u64(), Some(200));

    // Graceful shutdown (captures snapshot)
    store.close()?;
    Ok(())
}
```

---

## 2. MhinStoreBlockFacade — Staged Block Transactions

Buffer operations before committing. Intermediate reads reflect pending changes.

```rust
use rollblock::{MhinStoreBlockFacade, StoreConfig};
use rollblock::types::{Operation, StoreKey as Key};

fn main() -> rollblock::StoreResult<()> {
    let config = StoreConfig::new("./data", 4, 1000, 1, false)?;
    let store = MhinStoreBlockFacade::new(config)?;

    let key: Key = [0x01; Key::BYTES].into();

    // Start staging block 10
    store.start_block(10)?;

    // Stage operations one at a time
    store.set(Operation { key, value: 42u64.into() })?;

    // Read reflects staged value (not yet committed)
    assert_eq!(store.get(key)?.to_u64(), Some(42));

    // Pop deletes the staged key and returns the buffered value
    let staged = store.pop(key)?;
    assert_eq!(staged.to_u64(), Some(42));
    assert!(store.get(key)?.is_delete());

    // Commit the block
    store.end_block()?;

    // Now committed (pop applied the delete)
    assert_eq!(store.current_block()?, 10);
    assert!(store.get(key)?.is_delete());

    // Rollback requires no block in progress
    store.rollback(0)?;
    assert!(store.get(key)?.is_delete());

    Ok(())
}
```

---

## 3. RemoteStoreClient — TCP/TLS Client

Connect to a running store over the network. Requires the embedded server.

```rust
use std::time::Duration;

use rollblock::{MhinStoreFacade, RemoteServerSettings, StoreConfig, StoreFacade};
use rollblock::client::{ClientConfig, RemoteStoreClient};
use rollblock::net::BasicAuthConfig;
use rollblock::types::{Operation, StoreKey as Key};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── Server side ──────────────────────────────────────────────────────────
    let server_settings = RemoteServerSettings::default()
        .with_bind_address("127.0.0.1:9443".parse()?)
        .with_basic_auth("myuser", "mypass")
        .with_tls("./certs/server.crt", "./certs/server.key");

    let config = StoreConfig::new("./data", 4, 1000, 1, false)?
        .with_remote_server(server_settings);

    let store = MhinStoreFacade::new(config)?;
    store.set(
        1,
        vec![Operation {
            key: Key::from([0xAB; Key::BYTES]),
            value: 1337u64.into(),
        }],
    )?;

    // ── Client side ──────────────────────────────────────────────────────────
    let client_cfg = ClientConfig::new(
        "localhost",                       // server name for TLS
        "./certs/server.crt",              // CA certificate
        BasicAuthConfig::new("myuser", "mypass"),
    ).with_timeout(Duration::from_secs(2));

    let mut client = RemoteStoreClient::connect("127.0.0.1:9443", client_cfg)?;

    // Single read
    let value = client.get_one(Key::from([0xAB; Key::BYTES]))?;
    println!("Remote value: {} bytes", value.len());

    // Batch read (up to 255 keys)
    let values = client.get(&[Key::from([0xAB; Key::BYTES]), Key::from([0x00; Key::BYTES])])?;
    println!("Got {} values", values.len());

    client.close()?;
    Ok(())
}
```

**Plain-text mode (no TLS):**

```rust
let client_cfg = ClientConfig::without_tls(BasicAuthConfig::new("user", "pass"));
```

---

## 4. Reopening an Existing Store

```rust
use rollblock::{MhinStoreFacade, StoreConfig};

fn main() -> rollblock::StoreResult<()> {
    // Loads shards, capacity, durability from persisted metadata
    let config = StoreConfig::existing("./data");
    let store = MhinStoreFacade::new(config)?;

    println!("Resumed at block {}", store.current_block()?);
    Ok(())
}
```

---

## See Also

- [Configuration](configuration.md) — all options and tuning guidance
- [Architecture](architecture.md) — internal design
- [Observability](observability.md) — metrics and tracing

