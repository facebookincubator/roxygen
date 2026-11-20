/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//! Extended CONNECT (RFC 8441) through `HttpServer` on the owned HTTP/2 driver.

use std::cell::RefCell;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use roxygen_codec::CodecSetting;
use roxygen_core::error::HttpError;
use roxygen_core::error::HttpErrorCode;
use roxygen_core::events::HttpBodyEvent;
use roxygen_core::events::HttpBodyEventKind;
use roxygen_core::events::HttpHeaderEvent;
use roxygen_core::handler::HandlerSessionContext;
use roxygen_core::handler::HttpHandler;
use roxygen_core::handler::HttpHandlerRef;
use roxygen_core::handler::LocalHandlerFuture;
use roxygen_core::http::CodecProtocol;
use roxygen_core::message::HttpMessage;
use roxygen_core::source::HttpSource;
use roxygen_core::source::LocalSourceFuture;
use roxygen_core::source_holder::HttpSourceHolder;
use roxygen_transport::byte_stream::ByteStreamLimits;

use crate::acceptor::ServerSessionConfig;
use crate::server::HandlerFactory;
use crate::server::HttpServer;
use crate::server::ServerConfig;

const TIMEOUT: Duration = Duration::from_secs(10);
/// The bytes the client sends, and the server echoes, through the tunnel.
const TUNNEL_BYTES: usize = 1 << 20;
/// Every flow-control window: both peers' initial stream windows and connection windows.
const WINDOW: u32 = 65_535;
/// The default SETTINGS_MAX_FRAME_SIZE, which neither peer changes.
const MAX_FRAME: usize = 16_384;
/// SETTINGS_ENABLE_CONNECT_PROTOCOL.
const ENABLE_CONNECT_PROTOCOL: u16 = 8;
/// SETTINGS_INITIAL_WINDOW_SIZE.
const INITIAL_WINDOW_SIZE: u16 = 4;

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const H2_DATA: u8 = 0x0;
const H2_HEADERS: u8 = 0x1;
const H2_RST_STREAM: u8 = 0x3;
const H2_SETTINGS: u8 = 0x4;
const H2_PING: u8 = 0x6;
const H2_GOAWAY: u8 = 0x7;
const H2_WINDOW_UPDATE: u8 = 0x8;
const END_STREAM: u8 = 0x1;
const ACK: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
/// HPACK static table index 8, `:status: 200`.
const STATUS_200: u8 = 0x88;
const TUNNEL: u32 = 1;

/// Answers a request that keeps its body open with a 200 head that has no content-length and
/// does not end the response, then echoes the request body as it arrives. A request that ends
/// with its head fails with `MESSAGE_ERROR`.
struct EchoTunnel;

impl HttpHandler for EchoTunnel {
    fn handle_request(
        &self,
        _context: HandlerSessionContext,
        mut request: HttpSourceHolder,
    ) -> LocalHandlerFuture {
        Box::pin(async move {
            let head = request.read_header_event().await?;
            if head.eom() {
                return Err(HttpError::with_message(
                    HttpErrorCode::MESSAGE_ERROR,
                    b"a tunnel request keeps its body open",
                ));
            }
            Ok(HttpSourceHolder::new(Box::new(EchoSource(Rc::new(
                RefCell::new(request),
            )))))
        })
    }
}

/// The echo response: the 200 head, then each request body event other than `Suspend`, in turn.
struct EchoSource(Rc<RefCell<HttpSourceHolder>>);

impl HttpSource for EchoSource {
    fn read_header_event(&mut self) -> LocalSourceFuture<Result<HttpHeaderEvent, HttpError>> {
        let mut response = HttpMessage::new();
        response.set_status_code(200).expect("a response status");
        response.set_http_version(1, 1);
        let head = HttpHeaderEvent::new(response, false).expect("a response head");
        Box::pin(std::future::ready(Ok(head)))
    }

    fn read_body_event(
        &mut self,
        maximum: u32,
    ) -> LocalSourceFuture<Result<HttpBodyEvent, HttpError>> {
        let request = Rc::clone(&self.0);
        Box::pin(async move {
            loop {
                let read = request.borrow_mut().read_body_event(maximum);
                let event = read.await?;
                if !matches!(event.kind(), HttpBodyEventKind::Suspend(_)) {
                    return Ok(event);
                }
            }
        })
    }

    fn stop_reading(&mut self, error: Option<HttpErrorCode>) {
        self.0.borrow_mut().clear(error);
    }
}

struct H2Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    payload: Vec<u8>,
}

fn h2_frame(kind: u8, flags: u8, stream: u32, payload: &[u8]) -> Vec<u8> {
    let length = u32::try_from(payload.len()).unwrap().to_be_bytes();
    let mut frame = length[1..].to_vec();
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn read_h2_frame(stream: &mut TcpStream) -> H2Frame {
    let mut header = [0_u8; 9];
    stream.read_exact(&mut header).expect("a frame header");
    let length =
        usize::from(header[0]) << 16 | usize::from(header[1]) << 8 | usize::from(header[2]);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).expect("a frame payload");
    H2Frame {
        kind: header[3],
        flags: header[4],
        stream: u32::from_be_bytes(header[5..9].try_into().unwrap()) & 0x7fff_ffff,
        payload,
    }
}

fn window_update(stream: u32, amount: usize) -> Vec<u8> {
    let amount = u32::try_from(amount).unwrap();
    h2_frame(H2_WINDOW_UPDATE, 0, stream, &amount.to_be_bytes())
}

/// The `(identifier, value)` pairs of a SETTINGS payload.
fn settings(payload: &[u8]) -> Vec<(u16, u32)> {
    let (settings, _) = payload.as_chunks::<6>();
    settings
        .iter()
        .map(|[a, b, c, d, e, f]| {
            (
                u16::from_be_bytes([*a, *b]),
                u32::from_be_bytes([*c, *d, *e, *f]),
            )
        })
        .collect()
}

/// An HPACK literal without indexing, with a static-table name index or, for index 0, a
/// literal name.
fn hpack_literal(block: &mut Vec<u8>, index: u8, name: &[u8], value: &[u8]) {
    block.push(index);
    if index == 0 {
        block.push(u8::try_from(name.len()).unwrap());
        block.extend_from_slice(name);
    }
    block.push(u8::try_from(value.len()).unwrap());
    block.extend_from_slice(value);
}

/// The header block of an extended CONNECT for `websocket` on `/chat`.
fn extended_connect_block() -> Vec<u8> {
    let mut block = Vec::new();
    hpack_literal(&mut block, 2, b":method", b"CONNECT");
    hpack_literal(&mut block, 0, b":protocol", b"websocket");
    // `:scheme: http`, static table index 6.
    block.push(0x86);
    hpack_literal(&mut block, 4, b":path", b"/chat");
    hpack_literal(&mut block, 1, b":authority", b"localhost");
    block
}

fn tunnel_byte(offset: usize) -> u8 {
    (offset % 251) as u8
}

/// What the client saw of the tunnel.
struct TunnelRun {
    /// The echoed bytes, in order.
    echoed: Vec<u8>,
    /// Bytes the client had sent when the first echoed byte arrived.
    first_echo_at: usize,
    /// The most bytes sent and not yet echoed.
    most_in_flight: usize,
}

/// Opens an extended CONNECT tunnel, then sends `TUNNEL_BYTES` with END_STREAM on the last
/// DATA frame as the server's windows allow, while reading and crediting the echo, until the
/// echo's END_STREAM.
fn run_tunnel(client: &mut TcpStream) -> TunnelRun {
    let mut wire = H2_PREFACE.to_vec();
    wire.extend(h2_frame(H2_SETTINGS, 0, 0, &[]));
    client.write_all(&wire).unwrap();
    let mut stream_credit = loop {
        let frame = read_h2_frame(client);
        if frame.kind == H2_SETTINGS && frame.flags & ACK == 0 {
            client
                .write_all(&h2_frame(H2_SETTINGS, ACK, 0, &[]))
                .unwrap();
            let advertised = settings(&frame.payload);
            assert!(
                advertised.contains(&(ENABLE_CONNECT_PROTOCOL, 1)),
                "the server accepts extended CONNECT: {advertised:?}"
            );
            break advertised
                .iter()
                .rev()
                .find(|(identifier, _)| *identifier == INITIAL_WINDOW_SIZE)
                .map_or(WINDOW, |(_, value)| *value) as usize;
        }
    };
    assert_eq!(stream_credit, WINDOW as usize);
    let mut connection_credit = WINDOW as usize;
    client
        .write_all(&h2_frame(
            H2_HEADERS,
            END_HEADERS,
            TUNNEL,
            &extended_connect_block(),
        ))
        .unwrap();

    let mut sent = 0;
    let mut echoed = Vec::with_capacity(TUNNEL_BYTES);
    let mut head = None;
    let mut first_echo_at = None;
    let mut most_in_flight = 0;
    loop {
        while sent < TUNNEL_BYTES {
            let length = MAX_FRAME
                .min(TUNNEL_BYTES - sent)
                .min(stream_credit)
                .min(connection_credit);
            if length == 0 {
                break;
            }
            let payload: Vec<u8> = (sent..sent + length).map(tunnel_byte).collect();
            let flags = if sent + length == TUNNEL_BYTES {
                END_STREAM
            } else {
                0
            };
            client
                .write_all(&h2_frame(H2_DATA, flags, TUNNEL, &payload))
                .unwrap();
            sent += length;
            stream_credit -= length;
            connection_credit -= length;
            most_in_flight = most_in_flight.max(sent - echoed.len());
        }
        let frame = read_h2_frame(client);
        match frame.kind {
            H2_SETTINGS if frame.flags & ACK == 0 => {
                client
                    .write_all(&h2_frame(H2_SETTINGS, ACK, 0, &[]))
                    .unwrap();
            }
            H2_PING if frame.flags & ACK == 0 => {
                client
                    .write_all(&h2_frame(H2_PING, ACK, 0, &frame.payload))
                    .unwrap();
            }
            H2_WINDOW_UPDATE => {
                let amount = u32::from_be_bytes(frame.payload[..4].try_into().unwrap()) as usize;
                if frame.stream == 0 {
                    connection_credit += amount;
                } else {
                    assert_eq!(frame.stream, TUNNEL);
                    stream_credit += amount;
                }
            }
            H2_HEADERS => {
                assert_eq!(frame.stream, TUNNEL);
                assert!(head.is_none(), "one response head");
                assert_eq!(frame.flags & END_STREAM, 0, "the 200 keeps the tunnel open");
                head = Some(frame.payload);
            }
            H2_DATA => {
                assert_eq!(frame.stream, TUNNEL);
                assert!(head.is_some(), "the head precedes the echo");
                if !frame.payload.is_empty() {
                    first_echo_at.get_or_insert(sent);
                    echoed.extend_from_slice(&frame.payload);
                }
                if frame.flags & END_STREAM != 0 {
                    break;
                }
                if !frame.payload.is_empty() {
                    let mut credit = window_update(0, frame.payload.len());
                    credit.extend(window_update(TUNNEL, frame.payload.len()));
                    client.write_all(&credit).unwrap();
                }
            }
            H2_RST_STREAM | H2_GOAWAY => panic!(
                "the tunnel ended early: frame type {} on stream {} with payload {:?}, after \
                 {sent} bytes sent and {} echoed",
                frame.kind,
                frame.stream,
                frame.payload,
                echoed.len()
            ),
            _ => {}
        }
    }
    assert_eq!(sent, TUNNEL_BYTES, "the echo ends after the upload");
    assert_eq!(
        head.expect("a response head").first(),
        Some(&STATUS_200),
        "the head is a 200"
    );
    TunnelRun {
        echoed,
        first_echo_at: first_echo_at.expect("echoed bytes"),
        most_in_flight,
    }
}

/// A 1 MiB extended CONNECT tunnel in each direction at once, through `HttpServer` with
/// `owned_http2`, an echoing handler and 65535-byte windows: the client's upload and the
/// server's echo interleave under flow control, and every byte returns in order before
/// the echo's END_STREAM ends the stream, with no reset.
#[test]
fn owned_h2_connect_full_duplex_tunnel() {
    let mut session = ServerSessionConfig {
        stream_flow_control: WINDOW,
        connection_flow_control: WINDOW,
        // The connection window is also the session's buffer limit, which bounds each write.
        byte_stream_limits: ByteStreamLimits::new(WINDOW as usize, WINDOW as usize, 1_024)
            .expect("nonzero limits"),
        stream_read_timeout: Duration::ZERO,
        connection_idle_timeout: Duration::ZERO,
        write_timeout: Duration::ZERO,
        owned_http2: true,
        ..ServerSessionConfig::default()
    };
    session.settings.push(CodecSetting {
        identifier: ENABLE_CONNECT_PROTOCOL,
        value: 1,
    });
    let config = ServerConfig {
        plaintext_protocol: CodecProtocol::Http2,
        session,
        shutdown_on_signals: Vec::new(),
        ..ServerConfig::default()
    };
    let factory: HandlerFactory = Arc::new(|_| Rc::new(EchoTunnel) as HttpHandlerRef);
    let server = HttpServer::start(config, factory, None).expect("server starts");
    let address = server.address().expect("one address");
    let mut client = TcpStream::connect_timeout(&address, TIMEOUT).expect("client connects");
    client.set_read_timeout(Some(TIMEOUT)).unwrap();
    client.set_write_timeout(Some(TIMEOUT)).unwrap();

    let run = run_tunnel(&mut client);
    assert_eq!(run.echoed.len(), TUNNEL_BYTES);
    assert!(
        run.echoed
            .iter()
            .enumerate()
            .all(|(offset, byte)| *byte == tunnel_byte(offset)),
        "the echo returns every byte in order"
    );
    assert!(
        run.first_echo_at < TUNNEL_BYTES / 2,
        "the echo flows while the upload continues: first echo after {} bytes",
        run.first_echo_at
    );
    assert!(
        run.most_in_flight <= 4 * WINDOW as usize,
        "the windows bound the bytes in flight: {}",
        run.most_in_flight
    );
    drop(client);
    assert!(server.force_stop().expect("server stops"));
}
