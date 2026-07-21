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
use tempfile::TempDir;

struct LocalLibsqlFactory {
    config: LibsqlDatabaseConfig,
    _temp_dir: TempDir,
}

impl LocalLibsqlFactory {
    fn new(db_name: &str) -> Self {
        let temp_dir = tempfile::tempdir().expect("failed to create temp dir");
        let path = temp_dir.path().join(db_name);
        Self {
            config: LibsqlDatabaseConfig::local(path),
            _temp_dir: temp_dir,
        }
    }
}

#[async_trait::async_trait]
impl ProviderStressFactory for LocalLibsqlFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            LibsqlProvider::new(self.config.clone())
                .await
                .expect("failed to create LibsqlProvider"),
        )
    }
}

#[tokio::test]
async fn libsql_parallel_orchestrations_stress_smoke() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_test_writer()
        .try_init();

    let config = StressTestConfig {
        max_concurrent: 4,
        duration_secs: 2,
        tasks_per_instance: 3,
        activity_delay_ms: 5,
        orch_concurrency: 2,
        worker_concurrency: 2,
        wait_timeout_secs: 60,
    };

    let factory = LocalLibsqlFactory::new("parallel-stress.db");
    let result = run_parallel_orchestrations_test_with_config(&factory, config)
        .await
        .expect("parallel orchestration stress smoke failed");

    assert_eq!(result.failed, 0, "stress failures: {result:?}");
    assert!(
        result.success_rate() >= 100.0,
        "success rate was {:.2}%",
        result.success_rate()
    );
}

#[tokio::test]
async fn libsql_large_payload_stress_smoke() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("error")
        .with_test_writer()
        .try_init();

    let config = LargePayloadConfig {
        base: StressTestConfig {
            max_concurrent: 2,
            duration_secs: 1,
            tasks_per_instance: 1,
            activity_delay_ms: 5,
            orch_concurrency: 1,
            worker_concurrency: 1,
            wait_timeout_secs: 60,
        },
        small_payload_kb: 5,
        medium_payload_kb: 10,
        large_payload_kb: 20,
        activity_count: 10,
        sub_orch_count: 2,
    };

    let factory = LocalLibsqlFactory::new("large-payload-stress.db");
    let result = run_large_payload_test_with_config(&factory, config)
        .await
        .expect("large payload stress smoke failed");

    assert_eq!(result.failed, 0, "stress failures: {result:?}");
    assert!(
        result.success_rate() >= 100.0,
        "success rate was {:.2}%",
        result.success_rate()
    );
}
