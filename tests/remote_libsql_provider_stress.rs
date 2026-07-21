#![cfg(feature = "native-libsql")]

//! Remote stress smoke against self-hosted `sqld`.
//!
//! Opt-in via `LIBSQL_REMOTE_URL`. Tuned lighter than local stress for older
//! hardware and HTTP round-trips:
//!
//! ```sh
//! ./scripts/run-remote-stress.sh
//! ```

use std::sync::Arc;

use duroxide::provider_stress_tests::StressTestConfig;
use duroxide::provider_stress_tests::large_payload::{
    LargePayloadConfig, run_large_payload_test_with_config,
};
use duroxide::provider_stress_tests::parallel_orchestrations::{
    ProviderStressFactory, run_parallel_orchestrations_test_with_config,
};
use duroxide::providers::Provider;
use libsql_durable::{LibsqlDatabaseConfig, LibsqlProvider};

fn remote_url() -> Option<String> {
    std::env::var("LIBSQL_REMOTE_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn auth_token() -> String {
    std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_default()
}

macro_rules! skip_if_no_remote {
    () => {
        if remote_url().is_none() {
            eprintln!("skipping remote stress: set LIBSQL_REMOTE_URL to enable");
            return;
        }
    };
}

struct RemoteLibsqlStressFactory {
    url: String,
    auth_token: String,
}

impl RemoteLibsqlStressFactory {
    fn from_env() -> Option<Self> {
        Some(Self {
            url: remote_url()?,
            auth_token: auth_token(),
        })
    }

    async fn prepare_clean_provider(&self) -> Result<LibsqlProvider, String> {
        let provider = LibsqlProvider::new(LibsqlDatabaseConfig::remote(self.url.clone(), self.auth_token.clone()))
        .await
        .map_err(|e| format!("remote provider init failed: {e}"))?;
        provider
            .clear_runtime_data()
            .await
            .map_err(|e| format!("clear_runtime_data failed: {e}"))?;
        Ok(provider)
    }
}

#[async_trait::async_trait]
impl ProviderStressFactory for RemoteLibsqlStressFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        // Stress harness calls create_provider once per run; wipe first so a
        // shared primary is not polluted by prior remote validation suites.
        let provider = self
            .prepare_clean_provider()
            .await
            .expect("failed to create remote stress provider");
        Arc::new(provider)
    }
}

/// Lighter than local smoke: fewer concurrent orchs, short duration, longer wait
/// budget for HTTP/Hrana latency on older machines.
fn remote_parallel_config() -> StressTestConfig {
    let max_concurrent = std::env::var("REMOTE_STRESS_MAX_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let duration_secs = std::env::var("REMOTE_STRESS_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let wait_timeout_secs = std::env::var("REMOTE_STRESS_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120);

    StressTestConfig {
        max_concurrent,
        duration_secs,
        tasks_per_instance: 2,
        activity_delay_ms: 10,
        orch_concurrency: 1,
        worker_concurrency: 2,
        wait_timeout_secs,
    }
}

fn remote_large_payload_config() -> LargePayloadConfig {
    let wait_timeout_secs = std::env::var("REMOTE_STRESS_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120);

    LargePayloadConfig {
        base: StressTestConfig {
            max_concurrent: 1,
            duration_secs: 1,
            tasks_per_instance: 1,
            activity_delay_ms: 10,
            orch_concurrency: 1,
            worker_concurrency: 1,
            wait_timeout_secs,
        },
        // Keep payloads modest over HTTP to avoid thrashing a 2015-class host.
        small_payload_kb: 2,
        medium_payload_kb: 4,
        large_payload_kb: 8,
        activity_count: 4,
        sub_orch_count: 1,
    }
}

#[tokio::test]
async fn remote_parallel_orchestrations_stress_smoke() {
    skip_if_no_remote!();
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_test_writer()
        .try_init();

    let Some(factory) = RemoteLibsqlStressFactory::from_env() else {
        return;
    };

    // Fail fast with a clear message if primary is down.
    if let Err(err) = factory.prepare_clean_provider().await {
        eprintln!("skipping remote parallel stress: {err}");
        return;
    }

    let config = remote_parallel_config();
    eprintln!(
        "remote parallel stress: concurrent={} duration={}s wait={}s against {}",
        config.max_concurrent, config.duration_secs, config.wait_timeout_secs, factory.url
    );

    let result = run_parallel_orchestrations_test_with_config(&factory, config)
        .await
        .expect("remote parallel orchestration stress smoke failed");

    eprintln!(
        "remote parallel stress result: completed={} failed={} success_rate={:.2}%",
        result.completed,
        result.failed,
        result.success_rate()
    );

    assert_eq!(result.failed, 0, "remote stress failures: {result:?}");
    assert!(
        result.success_rate() >= 100.0,
        "remote success rate was {:.2}%",
        result.success_rate()
    );
    assert!(
        result.completed > 0,
        "expected at least one completed orchestration over remote"
    );
}

#[tokio::test]
async fn remote_large_payload_stress_smoke() {
    skip_if_no_remote!();
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_test_writer()
        .try_init();

    let Some(factory) = RemoteLibsqlStressFactory::from_env() else {
        return;
    };

    if let Err(err) = factory.prepare_clean_provider().await {
        eprintln!("skipping remote large-payload stress: {err}");
        return;
    }

    let config = remote_large_payload_config();
    eprintln!(
        "remote large-payload stress: activities={} large_kb={} against {}",
        config.activity_count, config.large_payload_kb, factory.url
    );

    let result = run_large_payload_test_with_config(&factory, config)
        .await
        .expect("remote large payload stress smoke failed");

    eprintln!(
        "remote large-payload stress result: completed={} failed={} success_rate={:.2}%",
        result.completed,
        result.failed,
        result.success_rate()
    );

    assert_eq!(result.failed, 0, "remote large-payload failures: {result:?}");
    assert!(
        result.success_rate() >= 100.0,
        "remote large-payload success rate was {:.2}%",
        result.success_rate()
    );
}
