// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use dynamo_runtime::{component::Namespace, traits::events::EventSubscriber};
use futures::stream::StreamExt;

use crate::kv_router::{scheduler::KVHitRateEvent, KV_HIT_RATE_SUBJECT};

const DEFAULT_MODEL_KV_CACHE_HIT_RATE: f64 = 1.;

// /// Tracks the KV cache utilization for a single request
// #[derive(Debug, Clone)]
// struct RequestKVCacheUtilization {
//     worker_id: i64,
//     isl_blocks: usize,
//     kv_allocation_rate: f64,
//     created_at: Instant,
// }

/// Rate limiter that monitors the aggregated KV cache utilization across all inflight requests for a given model
#[derive(Debug, Clone)]
pub struct KvCacheUtilizationRateLimiter {
    /// Whether the rate limiter is enabled
    enabled: bool,
    /// The current aggregate KV cache utilization for each model
    active_requests: Arc<RwLock<HashMap<String, f64>>>,
    /// The maximum allowed aggregate KV cache utilization rate for each model
    max_kv_cache_utilization_rate_thresholds: Arc<RwLock<HashMap<String, f64>>>,
}

impl Default for KvCacheUtilizationRateLimiter {
    fn default() -> Self {
        Self::new(false)
    }
}

impl KvCacheUtilizationRateLimiter {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            active_requests: Arc::new(RwLock::new(HashMap::new())),
            max_kv_cache_utilization_rate_thresholds: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start monitoring the KV cache utilization across all inflight requests
    pub async fn start_monitoring(
        &self,
        namespace: &Namespace,
        model_name: &str,
        max_kv_cache_utilization_rate: Option<f64>,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut subscriber = namespace.subscribe(KV_HIT_RATE_SUBJECT).await?;
        {
            let mut utilization_thresholds = self
                .max_kv_cache_utilization_rate_thresholds
                .write()
                .unwrap();
            if utilization_thresholds.get(model_name).is_none() {
                utilization_thresholds.insert(
                    model_name.to_string(),
                    max_kv_cache_utilization_rate.unwrap_or(DEFAULT_MODEL_KV_CACHE_HIT_RATE),
                );
            }
        }

        let active_requests = self.active_requests.clone();
        let model_name = model_name.to_string();

        tokio::spawn(async move {
            tracing::info!("Starting KV cache rate limiter monitoring");

            while let Some(event) = subscriber.next().await {
                match serde_json::from_slice::<KVHitRateEvent>(&event.payload) {
                    Ok(kv_event) => {
                        let hit_rate = if kv_event.isl_blocks > 0 {
                            kv_event.overlap_blocks as f64 / kv_event.isl_blocks as f64
                        } else {
                            0.0
                        };

                        let kv_allocation_rate = 1.0 - hit_rate;

                        // let now = Instant::now();
                        // let request_utilization = RequestKVCacheUtilization {
                        //     worker_id: kv_event.worker_id,
                        //     isl_blocks: kv_event.isl_blocks,
                        //     kv_allocation_rate,
                        //     created_at: now,
                        // };

                        let mut requests = active_requests.write().unwrap();
                        *requests.entry(model_name.clone()).or_insert(0.0) += kv_allocation_rate;
                        drop(requests);

                        tracing::debug!(
                            "Updated KV utilization for worker_id: {}, isl_blocks: {}, kv_allocation_rate: {}",
                            kv_event.worker_id,
                            kv_event.isl_blocks,
                            kv_allocation_rate,
                        )
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to deserialize KV cache rate limit metrics: {:?}",
                            e
                        );
                    }
                }
            }

            tracing::warn!("KV cache rate limiter monitoring stopped");
        });

        Ok(())
    }

    /// Checks if a a new request can be scheduled based on the current aggregate utilization
    pub fn can_schedule_request(&self, model_name: &str) -> bool {
        if !self.enabled {
            return true;
        }

        let requests = self.active_requests.read().unwrap();
        let aggregate_kv_cache_utilization = requests.get(model_name).copied().unwrap_or_default();
        drop(requests);

        let max_utilization = self
            .max_kv_cache_utilization_rate_thresholds
            .read()
            .unwrap()
            .get(model_name)
            .copied()
            .unwrap_or(DEFAULT_MODEL_KV_CACHE_HIT_RATE); // NOTE: This should never happen

        aggregate_kv_cache_utilization <= max_utilization
    }

    /// Subtracts the utilization of a finished request from the aggregate utilization
    pub fn subtract_finished_request_utilization(&self, model_name: &str, kv_allocation_rate: f64) {
        let mut requests = self.active_requests.write().unwrap();
        let entry = requests.entry(model_name.to_string()).or_insert(0.0);
        *entry = (*entry - kv_allocation_rate).max(0.0);
    }

    /// Removes the model entry from the rate limiter
    pub fn remove_rate_limiter_model_entry(&self, model_name: &str) {
        let mut requests = self.active_requests.write().unwrap();
        requests.remove(model_name);

        let mut utilization_thresholds = self
            .max_kv_cache_utilization_rate_thresholds
            .write()
            .unwrap();
        utilization_thresholds.remove(model_name);
    }
}
