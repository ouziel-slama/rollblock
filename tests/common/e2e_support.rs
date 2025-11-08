use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};

use rollblock::types::Operation;
use rollblock::{DurabilityMode, MhinStoreFacade, StoreConfig, StoreFacade, StoreResult};
use tempfile::{Builder, TempDir};

const POLL_INTERVAL: Duration = Duration::from_millis(5);

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

static INIT_TRACING: Once = Once::new();

pub fn init_tracing() {
    INIT_TRACING.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    });
}

fn testdata_root() -> PathBuf {
    let workspace_tmp = std::env::current_dir().unwrap().join("target/testdata");
    fs::create_dir_all(&workspace_tmp).unwrap();
    workspace_tmp
}

pub struct StoreHarness {
    #[allow(unused)]
    tempdir: TempDir,
    data_dir: PathBuf,
    config: StoreConfig,
}

impl StoreHarness {
    pub fn builder(name: &str) -> StoreHarnessBuilder {
        StoreHarnessBuilder::new(name)
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn config(&self) -> StoreConfig {
        self.config.clone()
    }

    pub fn open(&self) -> StoreResult<MhinStoreFacade> {
        MhinStoreFacade::new(self.config.clone())
    }

    pub fn reopen(&self) -> StoreResult<MhinStoreFacade> {
        let mut config = StoreConfig::existing(&self.data_dir);
        config.thread_count = self.config.thread_count;
        config.durability_mode = self.config.durability_mode.clone();
        config.snapshot_interval = self.config.snapshot_interval;
        config.compress_journal = self.config.compress_journal;
        config.journal_compression_level = self.config.journal_compression_level;
        config.mode = self.config.mode;
        MhinStoreFacade::new(config)
    }
}

pub struct StoreHarnessBuilder {
    tempdir: TempDir,
    initial_capacity: usize,
    thread_count: usize,
    durability_mode: DurabilityMode,
    snapshot_interval: Duration,
    compress_journal: bool,
}

impl StoreHarnessBuilder {
    pub fn new(name: &str) -> Self {
        let base = testdata_root();
        let tempdir = Builder::new()
            .prefix(&format!("e2e-{name}-"))
            .tempdir_in(base)
            .expect("failed to create tempdir");

        Self {
            tempdir,
            initial_capacity: 64,
            thread_count: 1,
            durability_mode: DurabilityMode::default(),
            snapshot_interval: Duration::from_secs(3600),
            compress_journal: false,
        }
    }

    pub fn initial_capacity(mut self, capacity: usize) -> Self {
        self.initial_capacity = capacity;
        self
    }

    pub fn thread_count(mut self, threads: usize) -> Self {
        self.thread_count = threads;
        self
    }

    pub fn durability_mode(mut self, mode: DurabilityMode) -> Self {
        self.durability_mode = mode;
        self
    }

    pub fn snapshot_interval(mut self, interval: Duration) -> Self {
        self.snapshot_interval = interval;
        self
    }

    pub fn compress_journal(mut self, enabled: bool) -> Self {
        self.compress_journal = enabled;
        self
    }

    pub fn build(self) -> StoreHarness {
        let data_dir = self.tempdir.path().to_path_buf();

        let mut config = StoreConfig::new(
            &data_dir,
            4,
            self.initial_capacity,
            self.thread_count,
            false,
        );
        config.durability_mode = self.durability_mode.clone();
        config.snapshot_interval = self.snapshot_interval;
        config.compress_journal = self.compress_journal;

        StoreHarness {
            tempdir: self.tempdir,
            data_dir,
            config,
        }
    }
}

pub fn apply_block<F>(store: &F, block_height: u64, operations: Vec<Operation>) -> StoreResult<()>
where
    F: StoreFacade + ?Sized,
{
    store.set(block_height, operations)
}

pub fn wait_for_durable<F>(store: &F, target_block: u64, timeout: Duration) -> StoreResult<()>
where
    F: StoreFacade + ?Sized,
{
    wait_for_block_condition("durable_block", store, target_block, timeout, |s| {
        s.durable_block()
    })
}

fn wait_for_block_condition<F, G>(
    label: &'static str,
    store: &F,
    target_block: u64,
    timeout: Duration,
    mut current_block: G,
) -> StoreResult<()>
where
    F: StoreFacade + ?Sized,
    G: FnMut(&F) -> StoreResult<u64>,
{
    let deadline = Instant::now() + timeout;

    loop {
        store.ensure_healthy()?;

        if current_block(store)? >= target_block {
            return Ok(());
        }

        if Instant::now() >= deadline {
            panic!("timed out waiting for {} to reach {}", label, target_block);
        }

        thread::sleep(POLL_INTERVAL);
    }
}
