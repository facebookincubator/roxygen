/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use std::error::Error;
use std::sync::Arc;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::message::HttpMessage;

#[derive(Debug, thiserror::Error)]
#[error("original typed cause")]
struct TypedCause;

fn assert_cause(error: &HttpError, expected: &Arc<TypedCause>) {
    assert!(std::ptr::eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<TypedCause>()
            .unwrap(),
        expected.as_ref(),
    ));
}

#[test]
fn default_message_survives_code_and_context_changes() {
    for original_code in [HttpErrorCode::READ_TIMEOUT, HttpErrorCode::from_u16(0xffff)] {
        let expected = crate::error::get_error_string(original_code).as_bytes();
        let mut error = HttpError::new(original_code);
        error.set_code(HttpErrorCode::NO_ERROR);
        assert_eq!(error.message(), expected);
        assert_eq!(error.clone().message(), expected);
        let cause = Arc::new(TypedCause);
        let mut error = error.with_cause(cause.clone());
        error.set_http_message(Some(HttpMessage::new()));
        assert_eq!(error.message(), expected);
        assert_cause(&error, &cause);
        error.take_http_message().unwrap();
        error.set_http_message(None);
        assert_eq!(error.code(), HttpErrorCode::NO_ERROR);
        assert_eq!(error.message(), expected);
        assert_eq!(error.to_string().as_bytes(), expected);
        assert_eq!(
            error.describe(),
            [b"error=NO_ERROR(0) msg=".as_slice(), expected].concat()
        );
    }
}

#[test]
fn default_and_custom_assignment_preserve_message_and_partial_context() {
    let cause = Arc::new(TypedCause);
    let mut partial = HttpMessage::new();
    partial.set_status_code(418).unwrap();
    let mut target = HttpError::with_message(HttpErrorCode::PROTOCOL_ERROR, b"old")
        .with_http_message(partial)
        .with_cause(cause.clone());
    let mut source = HttpError::new(HttpErrorCode::READ_TIMEOUT);
    source.set_code(HttpErrorCode::DROPPED);
    target.clone_from(&source);
    assert_eq!(target.code(), HttpErrorCode::DROPPED);
    assert_eq!(target.message(), b"READ_TIMEOUT");
    assert!(target.source().is_none());
    assert_eq!(Arc::strong_count(&cause), 1);
    source.set_message(b"");
    target.clone_from(&source);
    assert!(target.message().is_empty());
    assert_eq!(
        target.take_http_message().unwrap().status_code().unwrap(),
        418
    );
    target.clone_from(&HttpError::new(HttpErrorCode::CANCEL));
    assert_eq!(target.message(), b"CANCEL");
    assert!(target.http_message().is_none());
    assert_eq!(
        format!("{target:?}"),
        "HttpError { code: HttpErrorCode(8), message: ErrorMessage([67, 65, 78, 67, 69, 76]), http_message: None, has_cause: false }",
    );
}

#[test]
fn typed_cause_clone_preserves_identity() {
    fn assert_send_sync_error<Value: Error + Send + Sync>() {}
    assert_send_sync_error::<HttpError>();
    let cause = Arc::new(TypedCause);
    let mut partial = HttpMessage::new();
    partial.set_status_code(503).unwrap();
    let original =
        HttpError::with_message(HttpErrorCode::TRANSPORT_READ_ERROR, b"owned\0bytes\xff")
            .with_http_message(partial)
            .with_cause(cause.clone());
    let mut cloned = original.clone();
    assert_cause(&original, &cause);
    assert_cause(&cloned, &cause);
    assert_eq!(original.message(), cloned.message());
    assert_ne!(original.message().as_ptr(), cloned.message().as_ptr());
    cloned
        .http_message_mut()
        .unwrap()
        .set_status_code(200)
        .unwrap();
    assert_eq!(
        original
            .http_message()
            .unwrap()
            .clone()
            .status_code()
            .unwrap(),
        503
    );
    drop(original);
    assert_cause(&cloned, &cause);
    assert_eq!(Arc::strong_count(&cause), 2);
    drop(cloned);
    assert_eq!(Arc::strong_count(&cause), 1);
}

#[test]
fn typed_cause_clone_from_replaces_and_clears() {
    let first = Arc::new(TypedCause);
    let second = Arc::new(TypedCause);
    let mut partial = HttpMessage::new();
    partial.set_status_code(418).unwrap();
    let mut target = HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR)
        .with_http_message(partial)
        .with_cause(first.clone());
    let replacement = HttpError::with_message(HttpErrorCode::TRANSPORT_WRITE_ERROR, b"second")
        .with_cause(second.clone());
    target.clone_from(&replacement);
    assert_cause(&target, &second);
    assert_eq!(Arc::strong_count(&first), 1);
    assert_eq!(target.code(), replacement.code());
    assert_eq!(target.message(), replacement.message());
    assert_eq!(
        target
            .http_message()
            .unwrap()
            .clone()
            .status_code()
            .unwrap(),
        418
    );
    target.clone_from(&HttpError::new(HttpErrorCode::DROPPED));
    assert!(target.source().is_none());
    assert_eq!(target.code(), HttpErrorCode::DROPPED);
    assert_eq!(
        target
            .http_message()
            .unwrap()
            .clone()
            .status_code()
            .unwrap(),
        418
    );
    assert_eq!(Arc::strong_count(&second), 2);
}

#[test]
fn typed_cause_context_setters_preserve_provenance() {
    let cause = Arc::new(TypedCause);
    let mut error = HttpError::new(HttpErrorCode::TRANSPORT_READ_ERROR).with_cause(cause.clone());
    error.set_code(HttpErrorCode::INTERNAL_ERROR);
    error.set_message(b"new diagnostic");
    error.set_http_message(Some(HttpMessage::new()));
    assert_cause(&error, &cause);
    error
        .http_message_mut()
        .unwrap()
        .set_status_code(502)
        .unwrap();
    assert_eq!(
        error.take_http_message().unwrap().status_code().unwrap(),
        502
    );
    error.set_http_message(None);
    assert_cause(&error, &cause);
    let error = error.with_http_message(HttpMessage::new());
    assert_cause(&error, &cause);
    assert_eq!(error.to_string(), "new diagnostic");
    assert_eq!(
        error.describe(),
        b"error=INTERNAL_ERROR(2) msg=new diagnostic"
    );
}

struct HostileCause;

impl std::fmt::Debug for HostileCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("foreign Debug must not run")
    }
}

impl std::fmt::Display for HostileCause {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("foreign Display must not run")
    }
}

impl Error for HostileCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        panic!("foreign source must not run")
    }
}

#[test]
fn typed_cause_diagnostics_remain_opaque() {
    let error = HttpError::with_message(HttpErrorCode::TRANSPORT_EOF, b"owned\0diagnostic\xff")
        .with_cause(Arc::new(HostileCause));
    let cloned = error.clone();
    let mut assigned = HttpError::new(HttpErrorCode::DROPPED);
    assigned.clone_from(&cloned);
    assert!(assigned.source().unwrap().is::<HostileCause>());
    assert_eq!(assigned.to_string(), "owned\0diagnostic\u{fffd}");
    assert_eq!(
        assigned.describe(),
        b"error=TRANSPORT_EOF(4102) msg=owned\0diagnostic\xff"
    );
    assert!(format!("{assigned:?}").contains("has_cause: true"));
    assert!(format!("{:?}", HttpError::new(HttpErrorCode::DROPPED)).contains("has_cause: false"));
}

mod error {
    use crate::error::HttpError;
    use crate::error::HttpErrorCode;
    use crate::error::get_error_string;
    use crate::error::is_cancelled;
    use crate::error::is_connect_error;
    use crate::error::is_excessive_load;
    use crate::error::is_internal_error;
    use crate::error::is_no_error;
    use crate::error::is_protocol_error;
    use crate::error::is_request_rejected;
    use crate::error::is_version_fallback;
    use crate::message::HttpMessage;

    const IN_SCOPE_CODES: &[(HttpErrorCode, u16, &str)] = &[
        (HttpErrorCode::NO_ERROR, 0, "NO_ERROR"),
        (HttpErrorCode::PROTOCOL_ERROR, 1, "PROTOCOL_ERROR"),
        (HttpErrorCode::INTERNAL_ERROR, 2, "INTERNAL_ERROR"),
        (HttpErrorCode::FLOW_CONTROL_ERROR, 3, "FLOW_CONTROL_ERROR"),
        (HttpErrorCode::SETTINGS_TIMEOUT, 4, "SETTINGS_TIMEOUT"),
        (HttpErrorCode::STREAM_CLOSED, 5, "STREAM_CLOSED"),
        (HttpErrorCode::FRAME_SIZE_ERROR, 6, "FRAME_SIZE_ERROR"),
        (HttpErrorCode::REFUSED_STREAM, 7, "REFUSED_STREAM"),
        (HttpErrorCode::CANCEL, 8, "CANCEL"),
        (HttpErrorCode::COMPRESSION_ERROR, 9, "COMPRESSION_ERROR"),
        (HttpErrorCode::CONNECT_ERROR, 10, "CONNECT_ERROR"),
        (HttpErrorCode::ENHANCE_YOUR_CALM, 11, "ENHANCE_YOUR_CALM"),
        (
            HttpErrorCode::INADEQUATE_SECURITY,
            12,
            "INADEQUATE_SECURITY",
        ),
        (HttpErrorCode::HTTP_1_1_REQUIRED, 13, "HTTP_1_1_REQUIRED"),
        (HttpErrorCode::MESSAGE_ERROR, 0x10e, "MESSAGE_ERROR"),
        (
            HttpErrorCode::INVALID_STATE_TRANSITION,
            0x1000,
            "INVALID_STATE_TRANSITION",
        ),
        (
            HttpErrorCode::HEADER_PARSE_ERROR,
            0x1001,
            "HEADER_PARSE_ERROR",
        ),
        (HttpErrorCode::BODY_PARSE_ERROR, 0x1002, "BODY_PARSE_ERROR"),
        (HttpErrorCode::READ_TIMEOUT, 0x1003, "READ_TIMEOUT"),
        (HttpErrorCode::WRITE_TIMEOUT, 0x1004, "WRITE_TIMEOUT"),
        (HttpErrorCode::CORO_CANCELLED, 0x1005, "CORO_CANCELLED"),
        (HttpErrorCode::TRANSPORT_EOF, 0x1006, "TRANSPORT_EOF"),
        (
            HttpErrorCode::TRANSPORT_READ_ERROR,
            0x1007,
            "TRANSPORT_READ_ERROR",
        ),
        (
            HttpErrorCode::TRANSPORT_WRITE_ERROR,
            0x1008,
            "TRANSPORT_WRITE_ERROR",
        ),
        (HttpErrorCode::DROPPED, 0x1009, "DROPPED"),
        (
            HttpErrorCode::CONTENT_LENGTH_MISMATCH,
            0x100a,
            "CONTENT_LENGTH_MISMATCH",
        ),
    ];

    #[test]
    fn stable_in_scope_error_strings() {
        assert_eq!(
            get_error_string(HttpErrorCode::PROTOCOL_ERROR),
            "PROTOCOL_ERROR"
        );
        assert_eq!(
            get_error_string(HttpErrorCode::INVALID_STATE_TRANSITION),
            "INVALID_STATE_TRANSITION",
        );
        assert_eq!(get_error_string(HttpErrorCode::NO_ERROR), "NO_ERROR");
        assert_eq!(
            get_error_string(HttpErrorCode::from_u16(100_000_u32 as u16)),
            "UNKNOWN_ERROR",
        );
    }

    #[test]
    fn in_scope_numeric_values_and_names_are_stable() {
        for &(code, numeric, name) in IN_SCOPE_CODES {
            assert_eq!(code.as_u16(), numeric);
            assert_eq!(HttpErrorCode::from_u16(numeric), code);
            assert_eq!(get_error_string(code), name);
        }
    }

    #[test]
    fn h3_and_qpack_names_remain_excluded() {
        for raw in [
            0x100, 0x101, 0x102, 0x103, 0x104, 0x105, 0x106, 0x107, 0x108, 0x109, 0x10a, 0x10b,
            0x10c, 0x10d, 0x10f, 0x110, 0x200, 0x201, 0x202,
        ] {
            let code = HttpErrorCode::from_u16(raw);
            assert_eq!(get_error_string(code), "UNKNOWN_ERROR", "raw={raw:#x}");
            assert!(!is_internal_error(code));
            assert!(!is_request_rejected(code));
            assert!(!is_cancelled(code));
            assert!(!is_connect_error(code));
            assert!(!is_excessive_load(code));
            assert!(!is_version_fallback(code));
            assert!(!is_protocol_error(code));
            assert!(!is_no_error(code));
        }
        assert_eq!(
            get_error_string(HttpErrorCode::MESSAGE_ERROR),
            "MESSAGE_ERROR"
        );
    }

    #[test]
    fn categories_use_only_the_in_scope_h2_equivalents() {
        assert!(is_internal_error(HttpErrorCode::INTERNAL_ERROR));
        assert!(is_request_rejected(HttpErrorCode::REFUSED_STREAM));
        assert!(is_cancelled(HttpErrorCode::CANCEL));
        assert!(is_connect_error(HttpErrorCode::CONNECT_ERROR));
        assert!(is_excessive_load(HttpErrorCode::ENHANCE_YOUR_CALM));
        assert!(is_version_fallback(HttpErrorCode::HTTP_1_1_REQUIRED));
        assert!(is_protocol_error(HttpErrorCode::PROTOCOL_ERROR));
        assert!(is_no_error(HttpErrorCode::NO_ERROR));

        assert!(!is_cancelled(HttpErrorCode::CORO_CANCELLED));
        assert!(!is_internal_error(HttpErrorCode::MESSAGE_ERROR));
        assert!(!is_no_error(HttpErrorCode::INTERNAL_ERROR));
    }

    #[test]
    fn error_owns_messages_and_describes_exact_bytes() {
        fn assert_std_error<T: std::error::Error>() {}
        assert_std_error::<HttpError>();

        let mut source = b"custom\0message\xff".to_vec();
        let error = HttpError::with_message(HttpErrorCode::READ_TIMEOUT, &source);
        source.fill(b'x');

        assert_eq!(error.code(), HttpErrorCode::READ_TIMEOUT);
        assert_eq!(error.message(), b"custom\0message\xff");
        assert_eq!(
            error.describe().as_slice(),
            b"error=READ_TIMEOUT(4099) msg=custom\0message\xff",
        );
        assert_eq!(HttpError::new(HttpErrorCode::DROPPED).message(), b"DROPPED");
    }

    #[test]
    fn copy_construction_deep_copies_the_partial_message() {
        let mut partial = HttpMessage::new();
        partial.set_status_code(503).expect("response selected");
        let error = HttpError::new(HttpErrorCode::TRANSPORT_EOF).with_http_message(partial);
        let mut copy = error.clone();

        copy.http_message_mut()
            .expect("copy has partial message")
            .set_status_code(200)
            .expect("copy remains a response");
        assert_eq!(
            error
                .http_message()
                .expect("original has partial message")
                .clone()
                .status_code()
                .expect("original remains a response"),
            503,
        );
    }

    #[test]
    fn copy_assignment_retains_target_partial_message_when_source_has_none() {
        let mut partial = HttpMessage::new();
        partial.set_status_code(418).expect("response selected");
        let mut target = HttpError::new(HttpErrorCode::PROTOCOL_ERROR).with_http_message(partial);
        let source = HttpError::with_message(HttpErrorCode::DROPPED, b"replacement");

        target.clone_from(&source);

        assert_eq!(target.code(), HttpErrorCode::DROPPED);
        assert_eq!(target.message(), b"replacement");
        assert_eq!(
            target
                .http_message_mut()
                .expect("source copy assignment retains target partial message")
                .status_code()
                .expect("retained message remains a response"),
            418,
        );
    }

    #[test]
    fn peer_input_never_panics() {
        let result = std::panic::catch_unwind(|| {
            for raw in 0..=u16::MAX {
                let code = HttpErrorCode::from_u16(raw);
                let _ = get_error_string(code);
                let _ = is_internal_error(code);
                let _ = is_request_rejected(code);
                let _ = is_cancelled(code);
                let _ = is_connect_error(code);
                let _ = is_excessive_load(code);
                let _ = is_version_fallback(code);
                let _ = is_protocol_error(code);
                let _ = is_no_error(code);
            }
        });
        assert!(result.is_ok());
    }
}
