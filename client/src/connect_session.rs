/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! `roxygen_connect::session::ConnectSession` for `ClientSession`.

use roxygen_connect::session::ConnectSession;
use roxygen_connect::session::LocalConnectFuture;
use roxygen_core::connection::TransportEndpoint;
use roxygen_core::error::HttpError;
use roxygen_core::message::SocketAddress;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_runtime::cancellation::CancellationToken;
use roxygen_runtime::worker::WorkerId;
use roxygen_session::session::RequestReservation;
use roxygen_session::uniplex::UniplexResponse;

use crate::connector::ClientSession;

impl ConnectSession for ClientSession {
    fn worker_id(&self) -> WorkerId {
        ClientSession::worker_id(self)
    }

    fn local_address(&self) -> SocketAddress {
        ClientSession::local_address(self).clone()
    }

    fn peer_address(&self) -> SocketAddress {
        ClientSession::peer_address(self).clone()
    }

    fn local_endpoint(&self) -> TransportEndpoint {
        self.connection_context().local_endpoint().clone()
    }

    fn peer_endpoint(&self) -> TransportEndpoint {
        self.connection_context().peer_endpoint().clone()
    }

    /// The returned future owns a session clone until it resolves, because
    /// dropping the last `ClientSession` drains the session, and a draining
    /// session rejects the uncommitted CONNECT.
    fn send_connect_request(
        &self,
        request: HttpSourceHolder,
        reservation: RequestReservation,
        cancellation: CancellationToken,
    ) -> LocalConnectFuture<Result<UniplexResponse, HttpError>> {
        let session = self.clone();
        let send = self.send_reserved_request_inner(request, reservation, cancellation, None);
        Box::pin(async move {
            let response = send.await;
            drop(session);
            response
        })
    }

    fn initiate_drain(&self) -> Result<bool, HttpError> {
        ClientSession::initiate_drain(self)
    }
}
