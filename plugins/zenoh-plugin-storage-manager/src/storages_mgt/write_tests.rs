// Copyright (c) 2026
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

use std::sync::{Arc, Mutex as StdMutex};

use super::*;
use zenoh_backend_traits::{config::PluginConfig, Persistence, Storage};
use zenoh_ext::StorageWriteDurability;

#[derive(Default)]
struct Calls {
    puts: usize,
    deletes: usize,
    syncs: usize,
    fail_write: bool,
    fail_sync: bool,
}
struct Backend(Arc<StdMutex<Calls>>);
#[async_trait]
impl Storage for Backend {
    fn get_admin_status(&self) -> zenoh_util::ffi::JsonValue {
        serde_json::json!({}).into()
    }
    async fn put(
        &mut self,
        _: Option<OwnedKeyExpr>,
        _: ZBytes,
        _: Encoding,
        _: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let mut calls = self.0.lock().unwrap();
        calls.puts += 1;
        if calls.fail_write {
            bail!("injected write failure");
        }
        Ok(StorageInsertionResult::Inserted)
    }
    async fn delete(
        &mut self,
        _: Option<OwnedKeyExpr>,
        _: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        self.0.lock().unwrap().deletes += 1;
        Ok(StorageInsertionResult::Deleted)
    }
    fn supports_local_sync(&self) -> bool {
        true
    }
    async fn sync(&mut self) -> ZResult<()> {
        let mut calls = self.0.lock().unwrap();
        calls.syncs += 1;
        if calls.fail_sync {
            bail!("injected synchronization failure");
        }
        Ok(())
    }
    async fn get(&mut self, _: Option<OwnedKeyExpr>, _: &str) -> ZResult<Vec<StoredData>> {
        Ok(vec![])
    }
    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        Ok(vec![])
    }
}

async fn service(calls: Arc<StdMutex<Calls>>) -> StorageService {
    let config = zenoh::Config::from_json5(r#"{mode:"peer",listen:{endpoints:[]},scouting:{multicast:{enabled:false},gossip:{enabled:false}}}"#).unwrap();
    let session = Arc::new(zenoh::open(config).await.unwrap());
    let mut configuration = PluginConfig::try_from((
        "test",
        &serde_json::json!({"storages":{"test":{"key_expr":"test/**","volume":"memory"}}}),
    ))
    .unwrap();
    StorageService::new(
        session,
        configuration.storages.remove(0),
        "test",
        Arc::new(Mutex::new(Box::new(Backend(calls)))),
        Capability {
            persistence: Persistence::Durable,
            history: History::Latest,
        },
        CacheLatest::new(Arc::new(RwLock::new(Default::default())), None),
        Readiness::new(true),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_is_required_before_a_local_sync_write_can_succeed() {
    let calls = Arc::new(StdMutex::new(Calls::default()));
    let service = service(calls.clone()).await;
    let sample = SampleBuilder::put(OwnedKeyExpr::new("test/key").unwrap(), "value")
        .timestamp(service.session.new_timestamp())
        .into();
    service
        .process_sample_with_result(sample, Some(StorageWriteDurability::LocalSync))
        .await
        .unwrap();
    assert_eq!(calls.lock().unwrap().puts, 1);
    assert_eq!(calls.lock().unwrap().syncs, 1);
    let sample = SampleBuilder::delete(OwnedKeyExpr::new("test/key").unwrap())
        .timestamp(service.session.new_timestamp())
        .into();
    service
        .process_sample_with_result(sample, Some(StorageWriteDurability::LocalSync))
        .await
        .unwrap();
    assert_eq!(calls.lock().unwrap().deletes, 1);
    assert_eq!(calls.lock().unwrap().syncs, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_and_sync_failures_propagate_instead_of_acknowledging_success() {
    for fail_write in [false, true] {
        let calls = Arc::new(StdMutex::new(Calls {
            fail_write,
            fail_sync: !fail_write,
            ..Default::default()
        }));
        let service = service(calls.clone()).await;
        let sample = SampleBuilder::put(OwnedKeyExpr::new("test/key").unwrap(), "value")
            .timestamp(service.session.new_timestamp())
            .into();
        assert!(service
            .process_sample_with_result(sample, Some(StorageWriteDurability::LocalSync))
            .await
            .is_err());
        assert_eq!(calls.lock().unwrap().puts, 1);
        assert_eq!(calls.lock().unwrap().syncs, usize::from(!fail_write));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processed_writes_do_not_claim_or_perform_synchronization() {
    let calls = Arc::new(StdMutex::new(Calls::default()));
    let service = service(calls.clone()).await;
    let sample = SampleBuilder::put(OwnedKeyExpr::new("test/key").unwrap(), "value")
        .timestamp(service.session.new_timestamp())
        .into();
    service
        .process_sample_with_result(sample, Some(StorageWriteDurability::Processed))
        .await
        .unwrap();
    assert_eq!(calls.lock().unwrap().syncs, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_replies_report_sync_failure_without_a_success_receipt() {
    use std::time::Duration;
    use zenoh_ext::{
        acknowledged_storage_write, StorageWriteError, StorageWriteOperation, StorageWriteRequest,
    };
    for fail_sync in [false, true] {
        let calls = Arc::new(StdMutex::new(Calls {
            fail_sync,
            ..Default::default()
        }));
        let service = Arc::new(service(calls).await);
        let endpoint = service.write_endpoint.clone();
        let queryable = service
            .session
            .declare_queryable(endpoint.clone())
            .await
            .unwrap();
        let handler = service.clone();
        let server = tokio::spawn(async move {
            handler
                .reply_write(queryable.recv_async().await.unwrap())
                .await;
        });
        let result = acknowledged_storage_write(
            &service.session,
            &endpoint,
            StorageWriteRequest {
                protocol: 1,
                operation_id: "test-operation".into(),
                operation: StorageWriteOperation::Put,
                key: "test/key".into(),
                durability: StorageWriteDurability::LocalSync,
            },
            Some("value".into()),
            Encoding::TEXT_PLAIN,
            Duration::from_secs(2),
        )
        .await;
        if fail_sync {
            match result {
                Err(StorageWriteError::Rejected(rejection)) => assert!(rejection.may_have_applied),
                _ => panic!("Synchronization failure must report a potentially applied write"),
            }
        } else {
            assert_eq!(
                result.unwrap().durability,
                StorageWriteDurability::LocalSync
            );
        }
        server.await.unwrap();
        service.session.close().await.unwrap();
    }
}
