/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Shared HTTP/2 protocol transitions independent of stream ownership.

use std::time::Instant;

use roxygen_codec::CodecSetting;
use roxygen_codec::H2ErrorCode;
use roxygen_codec::HttpExceptionDirection;
use roxygen_core::flow_control::Window;
use roxygen_core::http::TransportDirection;
use roxygen_core::ids::StreamId;
use roxygen_http2::settings::SettingsId;

/// Peer settings that affect session admission or stream send credit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerSetting {
    MaxConcurrentStreams(u32),
    EnablePush(bool),
    InitialWindowSize(u32),
    Other,
}

impl From<CodecSetting> for PeerSetting {
    fn from(setting: CodecSetting) -> Self {
        match SettingsId::from_u16(setting.identifier) {
            Some(SettingsId::MaxConcurrentStreams) => {
                Self::MaxConcurrentStreams(setting.value.min(u64::from(u32::MAX)) as u32)
            }
            Some(SettingsId::EnablePush) => Self::EnablePush(setting.value != 0),
            Some(SettingsId::InitialWindowSize) => {
                Self::InitialWindowSize(setting.value.min(u64::from(u32::MAX)) as u32)
            }
            _ => Self::Other,
        }
    }
}

/// Updates each existing window in iteration order, then the peer default.
///
/// A failure leaves earlier windows updated and the peer default unchanged.
pub fn update_initial_send_window<'a>(
    peer_initial_window: &mut u32,
    capacity: u32,
    windows: impl IntoIterator<Item = &'a mut Window>,
) -> bool {
    if capacity > i32::MAX as u32 {
        return false;
    }
    for window in windows {
        if !window.set_capacity(capacity) {
            return false;
        }
    }
    *peer_initial_window = capacity;
    true
}

/// Returns stream credit and clears a stall only when positive credit remains.
///
/// An unrepresentable update returns false without changing either argument.
pub fn free_stream_send(window: &mut Window, stalled: &mut bool, amount: u32) -> bool {
    let freed = window.free(amount);
    if freed && window.non_negative_size() != 0 {
        *stalled = false;
    }
    freed
}

/// Whether peer GOAWAY refuses this locally initiated stream.
pub fn peer_goaway_refuses(
    stream: StreamId,
    last_good_stream: StreamId,
    direction: TransportDirection,
) -> bool {
    stream.get() > last_good_stream.get() && (stream.get() % 2 == 1) == direction.is_upstream()
}

/// The state transition following successful GOAWAY generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoawayPhase {
    Initial,
    Final,
}

/// A GOAWAY frame choice whose state is committed after generation succeeds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GoawayPlan {
    pub last_stream: StreamId,
    pub phase: GoawayPhase,
}

/// Chooses the first graceful GOAWAY or the terminal stream boundary.
pub fn plan_goaway(
    code: H2ErrorCode,
    last_incoming: StreamId,
    double_goaway: bool,
    sent_initial: bool,
) -> GoawayPlan {
    if code == H2ErrorCode::NO_ERROR && double_goaway && !sent_initial {
        GoawayPlan {
            last_stream: StreamId::MAX,
            phase: GoawayPhase::Initial,
        }
    } else {
        GoawayPlan {
            last_stream: last_incoming,
            phase: GoawayPhase::Final,
        }
    }
}

/// A borrowed view of the driver's successful GOAWAY generation state.
pub struct SentGoaway<'a> {
    pub sent_initial: &'a mut bool,
    pub sent_final: &'a mut bool,
    pub deadline: &'a mut Option<Instant>,
}

impl GoawayPhase {
    /// Commits a successfully generated frame; returns whether ingress grace ends.
    ///
    /// The deadline is computed only for an initial GOAWAY, after marking it sent.
    pub fn commit(self, state: SentGoaway<'_>, initial_deadline: impl FnOnce() -> Instant) -> bool {
        match self {
            Self::Initial => {
                *state.sent_initial = true;
                *state.deadline = Some(initial_deadline());
                false
            }
            Self::Final => {
                *state.sent_final = true;
                *state.deadline = None;
                true
            }
        }
    }
}

/// The session facts needed to choose the scope of a codec failure.
///
/// `live_stream` is absent for an unknown stream, or holds `can_send_headers`.
/// `protocol_stream_present` includes records retained after the live stream ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CodecFailureContext {
    pub direction: TransportDirection,
    pub stream: StreamId,
    pub exception_direction: HttpExceptionDirection,
    pub new_stream: bool,
    pub http_status: Option<u16>,
    pub wire_code: Option<H2ErrorCode>,
    pub live_stream: Option<bool>,
    pub discarded: bool,
    pub protocol_stream_present: bool,
}

/// Protocol action to perform without moving or destroying stream-owned state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodecFailureDisposition {
    Response(u16),
    Ignore,
    /// The explicit code overrides a live reset's adapted local error code.
    /// `fallback_wire_code` applies if no live reset update can be produced.
    Reset {
        explicit_wire_code: Option<H2ErrorCode>,
        fallback_wire_code: H2ErrorCode,
    },
    Connection,
}

impl CodecFailureContext {
    /// Chooses response, reset, or connection failure in session callback order.
    pub fn disposition(self) -> CodecFailureDisposition {
        if self.stream.is_unspecified() {
            return CodecFailureDisposition::Connection;
        }
        let ingress_failure = matches!(
            self.exception_direction,
            HttpExceptionDirection::Ingress | HttpExceptionDirection::IngressAndEgress
        );
        if self.direction.is_downstream()
            && ingress_failure
            && self.live_stream == Some(true)
            && let Some(status) = self.http_status
        {
            return CodecFailureDisposition::Response(status);
        }
        if self.discarded
            || (!self.new_stream && self.live_stream.is_none() && !self.protocol_stream_present)
        {
            return CodecFailureDisposition::Ignore;
        }
        CodecFailureDisposition::Reset {
            explicit_wire_code: self.wire_code,
            fallback_wire_code: self.wire_code.unwrap_or(if self.new_stream {
                H2ErrorCode::REFUSED_STREAM
            } else {
                H2ErrorCode::PROTOCOL_ERROR
            }),
        }
    }
}
