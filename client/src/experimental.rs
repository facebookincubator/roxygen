//! Roxygen-only client APIs that prototype proposed Proxygen APIs.
//!
//! **Experimental.** Each API may change or be removed when Proxygen decides
//! on the API it prototypes; `DIVERGENCES.md` records each divergence and its
//! retirement.
//!
//! - `DIV-046`: [`owned`], client sessions on the owned upstream drivers
//!   (`roxygen_session::owned_h1::OwnedH1UpstreamSession` and
//!   `roxygen_session::owned_h2::OwnedH2UpstreamSession`), which run one
//!   future per connection with no task per request, and a session pool for
//!   them with `HttpSessionPool`'s scheduler. They prototype a
//!   protocol-neutral upstream session that frozen Proxygen's
//!   `HTTPCoroConnector` does not have, and refuse the session parameters
//!   they cannot honor instead of ignoring them.

pub mod owned;

#[cfg(test)]
#[path = "../tests/owned_client.rs"]
mod owned_client_tests;

#[cfg(test)]
#[path = "../tests/owned_pool.rs"]
mod owned_pool_tests;
