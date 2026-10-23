// Copyright (c) 2026
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0

use std::sync::{Arc, Mutex};

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum StorageState {
    Initializing,
    Ready,
    Failed,
    Stopped,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct StorageStatus {
    pub required: bool,
    pub state: StorageState,
    pub error: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Readiness(Arc<Mutex<StorageStatus>>);

impl Readiness {
    pub fn new(required: bool) -> Self {
        Self(Arc::new(Mutex::new(StorageStatus {
            required,
            state: StorageState::Initializing,
            error: None,
        })))
    }

    pub fn snapshot(&self) -> StorageStatus {
        self.0.lock().unwrap().clone()
    }

    pub fn ready(&self) {
        let mut status = self.0.lock().unwrap();
        // An initialization failure must not be hidden by a late successful declaration.
        if status.state == StorageState::Initializing {
            status.state = StorageState::Ready;
        }
    }

    pub fn failed(&self, error: impl ToString) {
        let mut status = self.0.lock().unwrap();
        status.state = StorageState::Failed;
        status.error = Some(error.to_string());
    }

    pub fn stopped(&self) {
        self.0.lock().unwrap().state = StorageState::Stopped;
    }
}

pub(crate) struct WorkerGuard(pub Readiness);
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        if matches!(
            self.0.snapshot().state,
            StorageState::Ready | StorageState::Initializing
        ) {
            self.0.failed("Storage worker exited unexpectedly");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_requires_initialization_and_preserves_failure() {
        let readiness = Readiness::new(true);
        assert_eq!(readiness.snapshot().state, StorageState::Initializing);
        readiness.ready();
        assert_eq!(readiness.snapshot().state, StorageState::Ready);
        readiness.failed("backend unavailable");
        readiness.ready();
        let status = readiness.snapshot();
        assert_eq!(status.state, StorageState::Failed);
        assert_eq!(status.error.as_deref(), Some("backend unavailable"));
        assert!(status.required);
        readiness.stopped();
        assert_eq!(readiness.snapshot().state, StorageState::Stopped);
    }
}
