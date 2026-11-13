// Copyright (c) Meta Platforms, Inc. and affiliates.
// SPDX-License-Identifier: MIT

use super::Error;
use super::Event;
use super::Parser;
use super::ParserType;
use super::lenient_flags;

#[derive(Debug, Eq, PartialEq)]
struct Trace {
    url: Vec<u8>,
    body: Vec<u8>,
    fields: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    completions: usize,
    chunks: Vec<u64>,
}

fn parse_fragments(input: &[u8], split: usize) -> Trace {
    let mut parser = Parser::new(ParserType::Request);
    let mut trace = Trace {
        url: Vec::new(),
        body: Vec::new(),
        fields: vec![Vec::new()],
        values: vec![Vec::new()],
        completions: 0,
        chunks: Vec::new(),
    };
    for fragment in [&input[..split], &input[split..]] {
        let result = parser.execute(fragment, |state, event, range| {
            match event {
                Event::Url => trace.url.extend_from_slice(&fragment[range]),
                Event::Body => trace.body.extend_from_slice(&fragment[range]),
                Event::HeaderField => trace
                    .fields
                    .last_mut()
                    .unwrap()
                    .extend_from_slice(&fragment[range]),
                Event::HeaderValue => trace
                    .values
                    .last_mut()
                    .unwrap()
                    .extend_from_slice(&fragment[range]),
                Event::HeaderFieldComplete => trace.fields.push(Vec::new()),
                Event::HeaderValueComplete => trace.values.push(Vec::new()),
                Event::ChunkHeader => trace.chunks.push(state.content_length),
                Event::MessageComplete => trace.completions += 1,
                _ => {}
            }
            0
        });
        assert_eq!(
            result.error,
            Error::OK,
            "split {split}: {}",
            parser.reason()
        );
        assert_eq!(result.consumed, fragment.len());
    }
    assert_eq!(parser.finish(|_, _, _| 0), Error::OK);
    trace
}

#[test]
fn fragmented_chunked_request_keeps_ranges_and_trailers() {
    let request = b"POST /upload?q=1 HTTP/1.1\r\nHost: example.com\r\nTransfer-Encoding: chunked\r\n\r\n3;foo=bar\r\nabc\r\n2\r\nde\r\n0\r\nX-Checksum: yes\r\n\r\n";
    let expected = parse_fragments(request, 0);
    assert_eq!(expected.url, b"/upload?q=1");
    assert_eq!(expected.body, b"abcde");
    assert_eq!(expected.chunks, [3, 2, 0]);
    assert_eq!(expected.completions, 1);
    assert_eq!(expected.fields[2], b"X-Checksum");
    assert_eq!(expected.values[2], b"yes");
    for split in 1..=request.len() {
        assert_eq!(parse_fragments(request, split), expected, "split {split}");
    }
}

#[test]
fn pausing_at_headers_preserves_body_and_empty_input_completion() {
    let input = b"POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc";
    let mut parser = Parser::new(ParserType::Request);
    let head = parser.execute(input, |_, event, _| {
        if event == Event::HeadersComplete {
            Error::PAUSED.0
        } else {
            0
        }
    });
    assert_eq!(head.error, Error::PAUSED);
    assert_eq!(&input[head.consumed..], b"abc");
    parser.resume();
    let body = &input[head.consumed..];
    let mut complete = 0;
    let result = parser.execute(body, |_, event, range| match event {
        Event::Body => {
            assert_eq!(&body[range], b"abc");
            Error::PAUSED.0
        }
        Event::MessageComplete => {
            complete += 1;
            0
        }
        _ => 0,
    });
    assert_eq!(result.error, Error::PAUSED);
    assert_eq!(result.consumed, body.len());
    assert_eq!(complete, 0);
    parser.resume();
    let result = parser.execute(&[], |_, event, _| {
        if event == Event::MessageComplete {
            complete += 1;
        }
        0
    });
    assert_eq!(result.error, Error::OK);
    assert_eq!(result.consumed, 0);
    assert_eq!(complete, 1);
}

#[test]
fn connect_returns_upgrade_boundary_before_tunnel_bytes() {
    let input = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\ntunnel";
    let mut parser = Parser::new(ParserType::Request);
    let result = parser.execute(input, |_, _, _| 0);
    assert_eq!(result.error, Error::PAUSED_UPGRADE);
    assert_eq!(&input[result.consumed..], b"tunnel");
    assert_eq!(parser.upgrade, 1);
    assert_eq!(parser.method_name(), "CONNECT");
}

#[test]
fn response_eof_and_head_body_policy() {
    let input = b"HTTP/1.0 200 OK\r\n\r\nbody";
    let mut parser = Parser::new(ParserType::Response);
    let mut complete = 0;
    assert_eq!(
        parser
            .execute(input, |_, event, _| {
                if event == Event::MessageComplete {
                    complete += 1;
                }
                0
            })
            .error,
        Error::OK
    );
    assert_eq!(complete, 0);
    assert!(parser.message_needs_eof());
    assert!(!parser.should_keep_alive());
    assert_eq!(
        parser.finish(|_, event, _| {
            assert_eq!(event, Event::MessageComplete);
            complete += 1;
            0
        }),
        Error::OK
    );
    assert_eq!(complete, 1);

    let mut parser = Parser::new(ParserType::Response);
    let mut bodies = 0;
    let input = b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n";
    assert_eq!(
        parser
            .execute(input, |state, event, _| {
                if event == Event::HeadersComplete {
                    state.skip_body();
                }
                if event == Event::Body {
                    bodies += 1;
                }
                0
            })
            .error,
        Error::OK
    );
    assert_eq!(bodies, 0);
    assert_eq!(parser.finish(|_, _, _| 0), Error::OK);
}

#[test]
fn strict_errors_are_sticky_and_reset_preserves_leniency() {
    let mut parser = Parser::new(ParserType::Request);
    let input = b"GET / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\n";
    let result = parser.execute(input, |_, _, _| 0);
    assert_eq!(result.error, Error::UNEXPECTED_CONTENT_LENGTH);
    assert!(result.consumed < input.len());
    let result = parser.execute(b"GET / HTTP/1.1\r\n\r\n", |_, _, _| {
        panic!("sticky error invoked callback")
    });
    assert_eq!(result.consumed, 0);
    assert_eq!(result.error, Error::UNEXPECTED_CONTENT_LENGTH);
    parser.lenient_flags = lenient_flags::OPTIONAL_CR_BEFORE_LF;
    parser.reset();
    assert_eq!(parser.parser_type, ParserType::Request as u8);
    assert_eq!(parser.lenient_flags, lenient_flags::OPTIONAL_CR_BEFORE_LF);
    assert_eq!(
        parser.execute(b"GET / HTTP/1.1\n\n", |_, _, _| 0).error,
        Error::OK
    );
}

#[test]
fn content_length_overflow_and_incomplete_eof_fail() {
    for value in ["18446744073709551616", "99999999999999999999999999999999"] {
        let input = format!("POST / HTTP/1.1\r\nContent-Length: {value}\r\n\r\n");
        let mut parser = Parser::new(ParserType::Request);
        assert_eq!(
            parser.execute(input.as_bytes(), |_, _, _| 0).error,
            Error::INVALID_CONTENT_LENGTH
        );
    }
    let mut parser = Parser::new(ParserType::Request);
    assert_eq!(
        parser
            .execute(
                b"POST / HTTP/1.1\r\nContent-Length: 2\r\n\r\na",
                |_, _, _| 0
            )
            .error,
        Error::OK
    );
    assert_eq!(parser.finish(|_, _, _| 0), Error::INVALID_EOF_STATE);
}

#[test]
fn span_callback_error_maps_to_user_error() {
    let mut parser = Parser::new(ParserType::Request);
    let result = parser.execute(b"GET / HTTP/1.1\r\n\r\n", |_, event, _| {
        if event == Event::Url { -1 } else { 0 }
    });
    assert_eq!(result.error, Error::USER);
    assert_eq!(parser.reason(), "Span callback error in on_url");
}
