#![cfg(feature = "compat-sqlite")]

use std::sync::Arc;
use std::time::Duration;

use duroxide::provider_validations::bulk_deletion::test_delete_instance_bulk_safety_and_limits;
use duroxide::provider_validations::capability_filtering::test_fetch_with_compatible_filter_returns_item;
use duroxide::provider_validations::prune::test_prune_safety;
use duroxide::provider_validations::sessions::test_session_affinity_same_worker;
use duroxide::provider_validations::tag_filtering::test_tag_round_trip_preservation;
use duroxide::provider_validations::{
    ProviderFactory, test_exclusive_instance_lock, test_instance_creation_via_metadata,
    test_list_instances, test_multi_operation_atomic_ack, test_timer_delayed_visibility,
    test_worker_peek_lock_semantics,
};
use duroxide::providers::Provider;
use libsql_durable::LibsqlProvider;

const TEST_LOCK_TIMEOUT: Duration = Duration::from_millis(1000);

struct LibsqlTestFactory;

#[async_trait::async_trait]
impl ProviderFactory for LibsqlTestFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(
            LibsqlProvider::new_in_memory()
                .await
                .expect("failed to create in-memory LibsqlProvider"),
        )
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }
}

#[tokio::test]
async fn libsql_provider_core_validations() {
    let factory = LibsqlTestFactory;

    test_instance_creation_via_metadata(&factory).await;
    test_exclusive_instance_lock(&factory).await;
    test_multi_operation_atomic_ack(&factory).await;
    test_worker_peek_lock_semantics(&factory).await;
    test_timer_delayed_visibility(&factory).await;
    test_list_instances(&factory).await;
}

#[tokio::test]
async fn libsql_provider_extended_contract_validations() {
    let factory = LibsqlTestFactory;

    test_fetch_with_compatible_filter_returns_item(&factory).await;
    test_tag_round_trip_preservation(&factory).await;
    test_session_affinity_same_worker(&factory).await;
    test_prune_safety(&factory).await;
    test_delete_instance_bulk_safety_and_limits(&factory).await;
}
