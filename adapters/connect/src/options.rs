//! Validated runtime bounds, deadlines, and the session and pool parameters derived from them.

use std::rc::Rc;
use std::time::Duration;

use roxygen_client::connector::ClientSessionStats;
use roxygen_client::connector::SessionParams;
use roxygen_client::pool::AttachedPoolParams;
use roxygen_session::experimental::PingProbeConfig;
use roxygen_session::session::LifecycleObserver;

use crate::error::ConnectError;
use crate::worker::CredentialIndexingStrategy;

/// The largest HTTP/2 flow-control window (RFC 9113 section 6.9.1).
const MAX_WINDOW: u32 = (1 << 31) - 1;
/// The initial HTTP/2 connection window, which a session only grows (RFC 9113 section 6.9.2).
const INITIAL_CONNECTION_WINDOW: u32 = 65_535;
/// `SETTINGS_INITIAL_WINDOW_SIZE`.
const SETTINGS_INITIAL_WINDOW_SIZE: u16 = 4;

/// The native CONNECT `timeout`: zero, so only `response_timeout` bounds the response head and
/// no native read timeout applies to a tunnel's stream.
pub(crate) const NATIVE_CONNECT_TIMEOUT: Duration = Duration::ZERO;

/// Bounds and deadlines shared by every route and tunnel of one runtime.
///
/// Every attached session runs with its stream read, connection read, and write timeouts at
/// zero, and every native CONNECT with a zero timeout: none of them is an option. Liveness comes
/// from HTTP/2 PING probes (`ping_interval`, `ping_timeout`) and the connector's socket options
/// (TCP keepalive and `TCP_USER_TIMEOUT`); setup is bounded by `acquire_timeout` and
/// `response_timeout`.
///
/// Buffered tunnel bytes stay below `max_tunnels × (3 × bridge_capacity + 112 KiB)` for bridges,
/// native egress buffers, the native session's per-stream egress queue (just under 64 KiB, filled
/// from the egress buffer whatever the stream's flow-control window), and copy buffers, plus
/// unread downloads of at most `stream_receive_window` per tunnel and `connection_receive_window`
/// per session, plus 64 KiB transport read and write buffers per session. A relay buffers less
/// than a bridge. With the defaults the first two terms are 304 MiB and 2 GiB.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectOptions {
    /// Live routes, retiring ones included.
    pub max_routes: usize,
    /// Connects from admission until their error is returned, or until their tunnel's work ends.
    /// A `PendingTunnel` keeps its permit, with no deadline, until it is chosen or dropped. A
    /// `Tunnel` keeps it until it is dropped and the worker has finished it (stream aborted, task
    /// ended), so a caller at the bound may briefly see `Overloaded` right after a drop, and a
    /// finished but retained `Tunnel` keeps it. A relay returns it once its work ends, before its
    /// outcome resolves. Admission fails at once with `Overloaded` when every permit is held. At
    /// most `u16::MAX`; the default 1024 tunnels buffer at most 304 MiB.
    pub max_tunnels: usize,
    /// Non-retiring sessions, established or connecting, per route.
    pub max_connections: u32,
    /// Retiring sessions per route beyond `max_connections`; `None` means `max_connections`.
    /// `Some(0)` keeps retiring sessions on their `max_connections` slots.
    pub max_retiring: Option<u32>,
    /// Outgoing streams per session, also bounded by the peer's `SETTINGS_MAX_CONCURRENT_STREAMS`.
    pub max_streams: u32,
    /// Retires a route with no tunnel, queued connect, or pending dial; zero disables eviction.
    pub idle_timeout: Duration,
    /// Native session age limit, jittered by the pool; zero disables expiry.
    pub max_age: Duration,
    /// Bounds one connect's admission queue, pool acquisition, dial, and TLS, from admission.
    pub acquire_timeout: Duration,
    /// Bounds the CONNECT response head after each attempt commits its request.
    pub response_timeout: Duration,
    /// `SETTINGS_INITIAL_WINDOW_SIZE` of every session: the unread download one tunnel's native
    /// stream holds at most. The default 2 MiB allows 2 GiB across 1024 tunnels.
    pub stream_receive_window: u32,
    /// Connection flow-control window of every session: the unread download its tunnels hold
    /// together at most. At least 65,535, the initial HTTP/2 connection window, which sessions
    /// never shrink. The default 32 MiB covers 16 full default stream windows.
    pub connection_receive_window: u32,
    /// Bytes per direction of each bridge, also every tunnel's native egress buffer, which
    /// accepts one write past it. With copy buffers and the native egress queue a bridge buffers
    /// three times this plus 112 KiB, 304 KiB at the default 64 KiB, and a relay once this plus
    /// 112 KiB.
    pub bridge_capacity: usize,
    /// How long a dial that completed TLS without h2 fails its route key at once; zero disables
    /// the negative cache.
    pub h2_unsupported_ttl: Duration,
    /// Retries of a provably unprocessed CONNECT; zero disables retry.
    pub max_unprocessed_retries: u32,
    /// Graceful drain bound of `ConnectRuntime::shutdown` before it aborts tunnels and cancels
    /// sessions, and of each proxy connection's transport shutdown, after which the connection
    /// is closed.
    pub shutdown_timeout: Duration,
    /// Maximum CONNECT authority plus header names and values, in bytes.
    pub max_request_bytes: usize,
    /// Quiet time after a session's last read before it sends a PING probe (`DIV-024`); zero
    /// disables probes. A session with no tunnel stops probing until its next read.
    pub ping_interval: Duration,
    /// How long a probe may go unanswered before its session is dropped. Must be positive when
    /// `ping_interval` is.
    ///
    /// The drop fails the session's established tunnels with `TimedOut`, and their relays resolve
    /// `TimedOut`, unless a tunnel's download already reached END_STREAM: that bridge then fails
    /// with `ConnectionReset` and that relay resolves `PeerReset`. A connect still awaiting its
    /// response head sees a transport failure (pooled CONNECT specification D8): retried once
    /// on a reused session while `acquire_timeout` is open, otherwise `Failed`, committed,
    /// with no HTTP/1.1 fallback.
    pub ping_timeout: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            max_routes: 256,
            max_tunnels: 1024,
            max_connections: 4,
            max_retiring: None,
            max_streams: 100,
            idle_timeout: Duration::from_secs(120),
            max_age: Duration::from_secs(30 * 60),
            acquire_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(30),
            stream_receive_window: 2 << 20,
            connection_receive_window: 32 << 20,
            bridge_capacity: 64 << 10,
            h2_unsupported_ttl: Duration::from_secs(300),
            max_unprocessed_retries: 1,
            shutdown_timeout: Duration::from_secs(5),
            max_request_bytes: 64 << 10,
            ping_interval: Duration::from_secs(20),
            ping_timeout: Duration::from_secs(10),
        }
    }
}

impl ConnectOptions {
    pub(crate) fn validate(&self) -> Result<(), ConnectError> {
        let check = |valid: bool, reason: &'static str| {
            if valid {
                Ok(())
            } else {
                Err(ConnectError::invalid_options(reason))
            }
        };
        check(self.max_routes != 0, "max_routes must be positive")?;
        check(
            (1..=usize::from(u16::MAX)).contains(&self.max_tunnels),
            "max_tunnels must be in 1..=65535",
        )?;
        check(
            self.max_connections != 0,
            "max_connections must be positive",
        )?;
        check(
            self.max_connections
                .checked_add(self.effective_max_retiring())
                .is_some(),
            "max_connections plus max_retiring must fit in u32",
        )?;
        check(self.max_streams != 0, "max_streams must be positive")?;
        check(
            !self.acquire_timeout.is_zero(),
            "acquire_timeout must be positive",
        )?;
        check(
            !self.response_timeout.is_zero(),
            "response_timeout must be positive",
        )?;
        check(
            !self.shutdown_timeout.is_zero(),
            "shutdown_timeout must be positive",
        )?;
        check(
            (1..=MAX_WINDOW).contains(&self.stream_receive_window),
            "stream_receive_window must be in 1..=2^31-1",
        )?;
        check(
            (INITIAL_CONNECTION_WINDOW..=MAX_WINDOW).contains(&self.connection_receive_window),
            "connection_receive_window must be in 65535..=2^31-1",
        )?;
        check(
            (1..=i32::MAX as usize).contains(&self.bridge_capacity),
            "bridge_capacity must be in 1..=i32::MAX",
        )?;
        check(
            self.max_request_bytes != 0,
            "max_request_bytes must be positive",
        )?;
        check(
            self.ping_interval.is_zero() || !self.ping_timeout.is_zero(),
            "ping_timeout must be positive when ping_interval is",
        )
    }

    pub(crate) fn effective_max_retiring(&self) -> u32 {
        self.max_retiring.unwrap_or(self.max_connections)
    }

    /// The parameters of every attached session (pooled CONNECT specification D4, D5).
    pub(crate) fn session_params(
        &self,
        lifecycle_observer: Rc<dyn LifecycleObserver>,
        session_stats: Rc<dyn ClientSessionStats>,
    ) -> SessionParams {
        let defaults = SessionParams::http_client_defaults(Some(Duration::ZERO));
        let mut settings = defaults.settings.clone();
        for setting in &mut settings {
            if setting.identifier == SETTINGS_INITIAL_WINDOW_SIZE {
                setting.value = u64::from(self.stream_receive_window);
            }
        }
        SessionParams {
            settings,
            max_concurrent_outgoing_streams: Some(self.max_streams),
            connection_flow_control: Some(self.connection_receive_window),
            stream_read_timeout: Some(Duration::ZERO),
            connection_read_timeout: Some(Duration::ZERO),
            write_timeout: Some(Duration::ZERO),
            lifecycle_observer: Some(lifecycle_observer),
            session_stats: Some(session_stats),
            header_indexing_strategy: Some(Rc::new(CredentialIndexingStrategy)),
            ping_probes: (!self.ping_interval.is_zero()).then_some(PingProbeConfig {
                interval: self.ping_interval,
                timeout: self.ping_timeout,
                extend_interval_on_ingress: true,
                immediate: false,
            }),
            ..defaults
        }
    }

    /// The parameters of every route's attached pool (`DIV-025`).
    pub(crate) fn pool_params(&self) -> AttachedPoolParams {
        AttachedPoolParams {
            max_connections: self.max_connections,
            max_connection_attempts: 1,
            connect_timeout: self.acquire_timeout,
            max_age: self.max_age,
            max_waiters: u16::try_from(self.max_tunnels).unwrap_or(u16::MAX),
            max_retiring: Some(self.effective_max_retiring()),
            pending_connect_credit: self.max_streams,
            ..AttachedPoolParams::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::time::Duration;

    use roxygen_http2::hpack::HeaderName;
    use roxygen_session::experimental::PingProbeConfig;
    use roxygen_session::http2::stats::Http2SessionStats;
    use roxygen_session::session::LifecycleObserver;

    use super::ConnectOptions;
    use super::NATIVE_CONNECT_TIMEOUT;
    use crate::error::ConnectErrorKind;
    use crate::runtime::ConnectRuntime;

    struct NoObserver;

    impl LifecycleObserver for NoObserver {}

    struct NoStats;

    impl Http2SessionStats for NoStats {}

    #[test]
    fn defaults_match_the_pooled_connect_specification() {
        let options = ConnectOptions::default();
        assert_eq!(options.max_routes, 256);
        assert_eq!(options.max_tunnels, 1024);
        assert_eq!(options.max_connections, 4);
        assert_eq!(options.max_retiring, None);
        assert_eq!(options.max_streams, 100);
        assert_eq!(options.idle_timeout, Duration::from_secs(120));
        assert_eq!(options.max_age, Duration::from_secs(1800));
        assert_eq!(options.acquire_timeout, Duration::from_secs(10));
        assert_eq!(options.response_timeout, Duration::from_secs(30));
        assert_eq!(options.stream_receive_window, 2 * 1024 * 1024);
        assert_eq!(options.connection_receive_window, 32 * 1024 * 1024);
        assert_eq!(options.bridge_capacity, 64 * 1024);
        assert_eq!(options.h2_unsupported_ttl, Duration::from_secs(300));
        assert_eq!(options.max_unprocessed_retries, 1);
        assert_eq!(options.shutdown_timeout, Duration::from_secs(5));
        assert_eq!(options.max_request_bytes, 64 * 1024);
        options.validate().expect("defaults are valid");
    }

    #[test]
    fn validation_rejects_zero_and_unrepresentable_bounds() {
        let invalid: [(&str, ConnectOptions); 19] = [
            (
                "max_routes",
                ConnectOptions {
                    max_routes: 0,
                    ..Default::default()
                },
            ),
            (
                "max_tunnels zero",
                ConnectOptions {
                    max_tunnels: 0,
                    ..Default::default()
                },
            ),
            (
                "max_tunnels above u16",
                ConnectOptions {
                    max_tunnels: 65_536,
                    ..Default::default()
                },
            ),
            (
                "max_connections",
                ConnectOptions {
                    max_connections: 0,
                    ..Default::default()
                },
            ),
            (
                "retiring overflow",
                ConnectOptions {
                    max_connections: u32::MAX,
                    max_retiring: Some(1),
                    ..Default::default()
                },
            ),
            (
                "implicit retiring overflow",
                ConnectOptions {
                    max_connections: u32::MAX / 2 + 1,
                    ..Default::default()
                },
            ),
            (
                "max_streams",
                ConnectOptions {
                    max_streams: 0,
                    ..Default::default()
                },
            ),
            (
                "acquire_timeout",
                ConnectOptions {
                    acquire_timeout: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "response_timeout",
                ConnectOptions {
                    response_timeout: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "shutdown_timeout",
                ConnectOptions {
                    shutdown_timeout: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "stream window zero",
                ConnectOptions {
                    stream_receive_window: 0,
                    ..Default::default()
                },
            ),
            (
                "stream window",
                ConnectOptions {
                    stream_receive_window: 1 << 31,
                    ..Default::default()
                },
            ),
            (
                "connection window zero",
                ConnectOptions {
                    connection_receive_window: 0,
                    ..Default::default()
                },
            ),
            (
                "connection window below the HTTP/2 initial window, which sessions never shrink",
                ConnectOptions {
                    connection_receive_window: 65_534,
                    ..Default::default()
                },
            ),
            (
                "connection window",
                ConnectOptions {
                    connection_receive_window: 1 << 31,
                    ..Default::default()
                },
            ),
            (
                "bridge zero",
                ConnectOptions {
                    bridge_capacity: 0,
                    ..Default::default()
                },
            ),
            (
                "bridge above i32",
                ConnectOptions {
                    bridge_capacity: 1 << 31,
                    ..Default::default()
                },
            ),
            (
                "max_request_bytes",
                ConnectOptions {
                    max_request_bytes: 0,
                    ..Default::default()
                },
            ),
            (
                "u32 windows at the HTTP/2 maximum are valid only up to 2^31-1",
                ConnectOptions {
                    stream_receive_window: u32::MAX,
                    ..Default::default()
                },
            ),
        ];
        for (name, options) in invalid {
            let error = options.validate().expect_err(name);
            assert_eq!(error.kind(), ConnectErrorKind::InvalidOptions, "{name}");
            assert!(!error.committed(), "{name}");
            let construction = ConnectRuntime::new::<()>(options)
                .err()
                .unwrap_or_else(|| panic!("{name}: construction validates its options"));
            assert_eq!(
                construction.kind(),
                ConnectErrorKind::InvalidOptions,
                "{name}"
            );
        }
        let valid: [(&str, ConnectOptions); 11] = [
            (
                "idle eviction disabled",
                ConnectOptions {
                    idle_timeout: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "expiry disabled",
                ConnectOptions {
                    max_age: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "negative cache disabled",
                ConnectOptions {
                    h2_unsupported_ttl: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "retry disabled",
                ConnectOptions {
                    max_unprocessed_retries: 0,
                    ..Default::default()
                },
            ),
            (
                "shared retiring bound",
                ConnectOptions {
                    max_retiring: Some(0),
                    ..Default::default()
                },
            ),
            (
                "u16 tunnels",
                ConnectOptions {
                    max_tunnels: 65_535,
                    ..Default::default()
                },
            ),
            (
                "maximum windows",
                ConnectOptions {
                    stream_receive_window: (1 << 31) - 1,
                    connection_receive_window: (1 << 31) - 1,
                    ..Default::default()
                },
            ),
            (
                "connection window at the HTTP/2 initial window",
                ConnectOptions {
                    connection_receive_window: 65_535,
                    ..Default::default()
                },
            ),
            (
                "one-byte stream window, which sessions advertise as is",
                ConnectOptions {
                    stream_receive_window: 1,
                    ..Default::default()
                },
            ),
            (
                "maximum bridge",
                ConnectOptions {
                    bridge_capacity: i32::MAX as usize,
                    ..Default::default()
                },
            ),
            (
                "u32 ownership bound",
                ConnectOptions {
                    max_connections: u32::MAX - 1,
                    max_retiring: Some(1),
                    ..Default::default()
                },
            ),
        ];
        for (name, options) in valid {
            options
                .validate()
                .unwrap_or_else(|error| panic!("{name}: {error}"));
        }
    }

    #[test]
    fn session_params_disable_the_native_timeouts() {
        let options = ConnectOptions {
            max_streams: 7,
            stream_receive_window: 300_000,
            connection_receive_window: 9_000_000,
            ..Default::default()
        };
        let params = options.session_params(Rc::new(NoObserver), Rc::new(NoStats));
        assert_eq!(params.stream_read_timeout, Some(Duration::ZERO));
        assert_eq!(params.connection_read_timeout, Some(Duration::ZERO));
        assert_eq!(params.write_timeout, Some(Duration::ZERO));
        assert_eq!(NATIVE_CONNECT_TIMEOUT, Duration::ZERO);
        assert_eq!(params.max_concurrent_outgoing_streams, Some(7));
        assert_eq!(params.connection_flow_control, Some(9_000_000));
        let windows: Vec<u64> = params
            .settings
            .iter()
            .filter(|setting| setting.identifier == 4)
            .map(|setting| setting.value)
            .collect();
        assert_eq!(windows, [300_000]);
        assert!(params.lifecycle_observer.is_some());
        assert!(params.session_stats.is_some());
        let strategy = params
            .header_indexing_strategy
            .expect("credential indexing strategy");
        assert!(!strategy.index_header(&HeaderName::new("proxy-authorization"), b"secret", false));
        assert!(strategy.index_header(&HeaderName::new("user-agent"), b"agent", false));
    }

    #[test]
    fn ping_options_default_and_validate() {
        let options = ConnectOptions::default();
        assert_eq!(options.ping_interval, Duration::from_secs(20));
        assert_eq!(options.ping_timeout, Duration::from_secs(10));
        let unbounded = ConnectOptions {
            ping_timeout: Duration::ZERO,
            ..Default::default()
        };
        let error = unbounded
            .validate()
            .expect_err("a nonzero interval needs a nonzero timeout");
        assert_eq!(error.kind(), ConnectErrorKind::InvalidOptions);
        assert!(!error.committed());
        assert_eq!(
            ConnectRuntime::new::<()>(unbounded)
                .err()
                .expect("construction validates the probe options")
                .kind(),
            ConnectErrorKind::InvalidOptions,
        );
        for (name, options) in [
            (
                "disabled probes ignore the timeout",
                ConnectOptions {
                    ping_interval: Duration::ZERO,
                    ping_timeout: Duration::ZERO,
                    ..Default::default()
                },
            ),
            (
                "short probes",
                ConnectOptions {
                    ping_interval: Duration::from_millis(1),
                    ping_timeout: Duration::from_millis(1),
                    ..Default::default()
                },
            ),
        ] {
            options
                .validate()
                .unwrap_or_else(|error| panic!("{name}: {error}"));
        }
    }

    #[test]
    fn session_params_carry_the_ping_probe_options() {
        let options = ConnectOptions {
            ping_interval: Duration::from_millis(300),
            ping_timeout: Duration::from_millis(70),
            ..Default::default()
        };
        let params = options.session_params(Rc::new(NoObserver), Rc::new(NoStats));
        assert_eq!(
            params.ping_probes,
            Some(PingProbeConfig {
                interval: Duration::from_millis(300),
                timeout: Duration::from_millis(70),
                extend_interval_on_ingress: true,
                immediate: false,
            }),
        );
        assert_eq!(params.stream_read_timeout, Some(Duration::ZERO));
        assert_eq!(params.connection_read_timeout, Some(Duration::ZERO));
        assert_eq!(params.write_timeout, Some(Duration::ZERO));
        assert_eq!(
            ConnectOptions::default()
                .session_params(Rc::new(NoObserver), Rc::new(NoStats))
                .ping_probes
                .map(|probes| (probes.interval, probes.timeout)),
            Some((Duration::from_secs(20), Duration::from_secs(10))),
            "every attached session probes by default",
        );
        let disabled = ConnectOptions {
            ping_interval: Duration::ZERO,
            ..options
        };
        assert_eq!(
            disabled
                .session_params(Rc::new(NoObserver), Rc::new(NoStats))
                .ping_probes,
            None,
            "a zero interval sends no probe",
        );
    }

    #[test]
    fn pool_params_opt_into_the_retiring_bound_and_credit() {
        let options = ConnectOptions {
            max_connections: 3,
            max_streams: 50,
            max_tunnels: 700,
            max_age: Duration::from_secs(9),
            acquire_timeout: Duration::from_millis(1500),
            ..Default::default()
        };
        let params = options.pool_params();
        assert_eq!(params.max_connections, 3);
        assert_eq!(params.max_retiring, Some(3));
        assert_eq!(params.pending_connect_credit, 50);
        assert_eq!(params.max_connection_attempts, 1);
        assert_eq!(params.connect_timeout, Duration::from_millis(1500));
        assert_eq!(params.max_age, Duration::from_secs(9));
        assert_eq!(params.max_waiters, 700);
        let explicit = ConnectOptions {
            max_retiring: Some(0),
            ..options
        };
        assert_eq!(explicit.pool_params().max_retiring, Some(0));
    }
}
