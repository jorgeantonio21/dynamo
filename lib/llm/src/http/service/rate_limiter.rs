// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use crate::http::service::error::HttpError;
use crate::http::service::metrics::Metrics;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Rate limiter for bounding the number of inflight requests per model
pub struct RateLimiter {
    metrics: Arc<Metrics>,
    max_inflight_requests_per_model: RwLock<HashMap<String, u32>>,
}

impl RateLimiter {
    /// Create a new rate limiter, with optional per-model rate limits
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            max_inflight_requests_per_model: RwLock::new(Default::default()),
        }
    }

    /// Check if a request for the given model should be rate limited, based on the number of inflight requests
    /// Returns an error if the rate limit is exceeded, otherwise returns Ok(())
    pub fn check_inflight_requests_rate_limit(&self, model: &str) -> Result<(), HttpError> {
        let max_inflight = match self.get_max_inflight_count_per_model(model) {
            Some(max_inflight) => max_inflight as i64,
            None => return Ok(()),
        };

        let current_inflight = self.get_inflight_count(model);

        if current_inflight >= max_inflight {
            tracing::warn!(
                model = %model,
                current_inflight = %current_inflight,
                max_inflight = %max_inflight,
                "Ingress number of inflight requests rate limit exceeded"
            );
            return Err(HttpError {
                code: 429,
                message: "Too many requests".to_string(),
            });
        }

        tracing::trace!(
            model = %model,
            current_inflight = %current_inflight,
            max_inflight = %max_inflight,
            "Ingress number of inflight rate limit check passed"
        );
        Ok(())
    }

    fn get_inflight_count(&self, model: &str) -> i64 {
        self.metrics.get_inflight_count(model)
    }

    fn get_max_inflight_count_per_model(&self, model: &str) -> Option<u32> {
        self.max_inflight_requests_per_model
            .read()
            .unwrap()
            .get(model)
            .copied()
    }

    pub fn set_max_inflight_requests_for_model(&self, model: &str, max_inflight_requests: u32) {
        self.max_inflight_requests_per_model
            .write()
            .unwrap()
            .insert(model.to_string(), max_inflight_requests);
    }
}
