// Copyright (c) 2026
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

use std::{fmt, time::Duration};

use serde::{Deserialize, Serialize};
use zenoh::{
    bytes::{Encoding, ZBytes},
    key_expr::OwnedKeyExpr,
    query::QueryTarget,
    Session,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageWriteOperation {
    Put,
    Delete,
}

/// `Processed` confirms a backend call. `LocalSync` additionally requires the backend's explicit
/// stable-storage synchronization. Neither level confirms replication or exactly-once execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageWriteDurability {
    Processed,
    LocalSync,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageWriteRequest {
    pub protocol: u8,
    /// Correlation ID, not an idempotency or deduplication guarantee.
    pub operation_id: String,
    pub operation: StorageWriteOperation,
    pub key: String,
    pub durability: StorageWriteDurability,
}

impl StorageWriteRequest {
    pub fn validate(&self) -> Result<OwnedKeyExpr, String> {
        if self.protocol != 1
            || self.operation_id.is_empty()
            || self.operation_id.len() > 128
            || !self
                .operation_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        {
            return Err("Unsupported protocol or invalid operation ID".into());
        }
        let key = OwnedKeyExpr::new(self.key.clone()).map_err(|e| e.to_string())?;
        if key.is_wild() {
            return Err("Acknowledged writes require one concrete key".into());
        }
        Ok(key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageWriteResult {
    Inserted,
    Replaced,
    Outdated,
    Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageWriteAck {
    pub protocol: u8,
    pub operation_id: String,
    pub key: String,
    pub service: String,
    pub durability: StorageWriteDurability,
    pub result: StorageWriteResult,
    pub timestamp: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageWriteRejection {
    pub error: String,
    pub may_have_applied: bool,
}

#[derive(Debug)]
pub enum StorageWriteError {
    InvalidRequest(String),
    /// The server rejected the request or reported a backend failure. An operation may have
    /// applied before a backend error, such as a failure synchronizing its WAL.
    Rejected(StorageWriteRejection),
    /// A response was absent, ambiguous, malformed, or lost. Inspect state before retrying.
    UnknownOutcome(String),
}
impl fmt::Display for StorageWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(message) => write!(f, "Invalid storage write: {message}"),
            Self::Rejected(rejection) => write!(
                f,
                "Storage write rejected (may_have_applied={}): {}",
                rejection.may_have_applied, rejection.error
            ),
            Self::UnknownOutcome(message) => write!(f, "Storage write outcome unknown: {message}"),
        }
    }
}
impl std::error::Error for StorageWriteError {}

/// Write through a specific opt-in storage command endpoint, separate from data query permissions.
/// The endpoint must identify one service. A timeout never proves that the write did not occur.
pub async fn acknowledged_storage_write(
    session: &Session,
    endpoint: &str,
    request: StorageWriteRequest,
    payload: Option<ZBytes>,
    encoding: Encoding,
    timeout: Duration,
) -> Result<StorageWriteAck, StorageWriteError> {
    use StorageWriteError::*;
    request.validate().map_err(InvalidRequest)?;
    let endpoint_key =
        OwnedKeyExpr::new(endpoint.to_owned()).map_err(|e| InvalidRequest(e.to_string()))?;
    if endpoint_key.is_wild() || !endpoint.starts_with("@storage/") || timeout.is_zero() {
        return Err(InvalidRequest(
            "Use a concrete @storage endpoint and positive deadline".into(),
        ));
    }
    if (request.operation == StorageWriteOperation::Put) != payload.is_some() {
        return Err(InvalidRequest(
            "Put requires a payload; delete must not carry one".into(),
        ));
    }
    let metadata = serde_json::to_vec(&request).map_err(|e| InvalidRequest(e.to_string()))?;
    if metadata.len() > 4096 {
        return Err(InvalidRequest("Metadata exceeds 4096 bytes".into()));
    }
    let mut query = session
        .get(endpoint_key.clone())
        .target(QueryTarget::All)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(timeout)
        .attachment(metadata)
        .encoding(encoding);
    if let Some(payload) = payload {
        query = query.payload(payload);
    }
    let replies = query.await.map_err(|e| UnknownOutcome(e.to_string()))?;
    let mut responses = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        responses.push(reply);
    }
    if responses.len() != 1 {
        return Err(UnknownOutcome(format!(
            "Expected one responder, received {}",
            responses.len()
        )));
    }
    let response = match responses.pop().unwrap().into_result() {
        Ok(response) => response,
        Err(error) => {
            let rejection =
                serde_json::from_slice::<StorageWriteRejection>(&error.payload().to_bytes())
                    .map_err(|error| UnknownOutcome(format!("Malformed rejection: {error}")))?;
            return Err(Rejected(rejection));
        }
    };
    let ack: StorageWriteAck = serde_json::from_slice(&response.payload().to_bytes())
        .map_err(|e| UnknownOutcome(e.to_string()))?;
    if response.key_expr().as_str() != endpoint_key.as_str()
        || ack.protocol != 1
        || ack.operation_id != request.operation_id
        || ack.key != request.key
        || ack.service != endpoint
        || ack.durability != request.durability
    {
        return Err(UnknownOutcome(
            "Reply does not match the requested write".into(),
        ));
    }
    Ok(ack)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> StorageWriteRequest {
        StorageWriteRequest {
            protocol: 1,
            operation_id: "operation-1".into(),
            operation: StorageWriteOperation::Put,
            key: "test/key".into(),
            durability: StorageWriteDurability::LocalSync,
        }
    }

    #[test]
    fn storage_writes_require_literal_keys_and_bounded_correlation_ids() {
        assert!(request().validate().is_ok());
        for key in ["test/**", "test/*", "test/key?x=y", "/test/key"] {
            let mut request = request();
            request.key = key.into();
            assert!(request.validate().is_err());
        }
        let mut value = request();
        value.protocol = 2;
        assert!(value.validate().is_err());
        value = request();
        value.operation_id = "a".repeat(129);
        assert!(value.validate().is_err());
        value.operation_id.clear();
        assert!(value.validate().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unavailable_service_reports_unknown_not_failed_write() {
        let config = zenoh::Config::from_json5(r#"{mode:"peer",listen:{endpoints:[]},scouting:{multicast:{enabled:false},gossip:{enabled:false}}}"#).unwrap();
        let session = zenoh::open(config).await.unwrap();
        let error = acknowledged_storage_write(
            &session,
            "@storage/absent/write",
            request(),
            Some("value".into()),
            Encoding::TEXT_PLAIN,
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, StorageWriteError::UnknownOutcome(_)));
        session.close().await.unwrap();
    }
}

#[cfg(test)]
mod reply_loss_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_reply_can_follow_an_applied_operation() {
        let config = zenoh::Config::from_json5(r#"{mode:"peer",listen:{endpoints:[]},scouting:{multicast:{enabled:false},gossip:{enabled:false}}}"#).unwrap();
        let session = zenoh::open(config).await.unwrap();
        let queryable = session
            .declare_queryable("@storage/test/lost-reply")
            .await
            .unwrap();
        let applied = Arc::new(AtomicBool::new(false));
        let marker = applied.clone();
        let server = tokio::spawn(async move {
            let query = queryable.recv_async().await.unwrap();
            marker.store(true, Ordering::SeqCst);
            drop(query); // Simulate an applied operation whose acknowledgement is lost.
        });
        let result = acknowledged_storage_write(
            &session,
            "@storage/test/lost-reply",
            StorageWriteRequest {
                protocol: 1,
                operation_id: "operation-1".into(),
                operation: StorageWriteOperation::Put,
                key: "test/key".into(),
                durability: StorageWriteDurability::LocalSync,
            },
            Some("value".into()),
            Encoding::TEXT_PLAIN,
            Duration::from_millis(200),
        )
        .await;
        server.await.unwrap();
        assert!(applied.load(Ordering::SeqCst));
        assert!(matches!(result, Err(StorageWriteError::UnknownOutcome(_))));
        session.close().await.unwrap();
    }
}
