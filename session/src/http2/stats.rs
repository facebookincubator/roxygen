/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::time::Duration;

use super::rate_limit::RateLimitSample;

/// Owner-local HTTP/2 session statistics callback.
pub trait Http2SessionStats: 'static {
    fn record_transaction_opened(&self) {}
    fn record_transaction_closed(&self) {}
    fn record_session_reused(&self) {}
    fn record_transactions_served(&self, _transactions: u64) {}
    fn record_connection_idle_time(&self, _idle: Duration) {}
    fn record_transaction_stalled(&self) {}
    fn record_session_stalled(&self) {}
    fn record_buffered_read_bytes(&self, _delta: i64) {}
    fn record_buffered_write_bytes(&self, _delta: i64) {}
    fn record_content_length_mismatch(&self) {}
    fn record_ingress_get_body(&self) {}
    fn record_ping_timeout(&self) {}
    fn record_rate_limit(&self, _sample: RateLimitSample) {}
    fn record_read_loop_limit(&self) {}
}
