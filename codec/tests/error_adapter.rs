mod error_adapter {
    use roxygen_core::error::HttpError;
    use roxygen_core::error::HttpErrorCode;
    use roxygen_core::message::HttpMessage;

    use crate::error_adapter::H2ErrorCode;
    use crate::error_adapter::HttpException;
    use crate::error_adapter::HttpExceptionDirection;
    use crate::error_adapter::ProxygenError;
    use crate::error_adapter::h2_error_code_to_http_error_code;
    use crate::error_adapter::http_error_code_to_h2_error_code;
    use crate::error_adapter::http_error_code_to_proxygen_error;
    use crate::error_adapter::http_error_to_http_exception;
    use crate::error_adapter::http_exception_to_http_error_code;

    #[test]
    fn application_internal_codes_map_to_internal_error() {
        assert_eq!(
            http_error_code_to_h2_error_code(HttpErrorCode::MESSAGE_ERROR, true),
            H2ErrorCode::INTERNAL_ERROR,
        );
        assert_eq!(
            http_error_code_to_h2_error_code(HttpErrorCode::FLOW_CONTROL_ERROR, true),
            H2ErrorCode::INTERNAL_ERROR,
        );
    }

    #[test]
    fn library_h2_codes_preserve_wire_identity() {
        assert_eq!(
            http_error_code_to_h2_error_code(HttpErrorCode::MESSAGE_ERROR, false),
            H2ErrorCode::PROTOCOL_ERROR,
        );
        assert_eq!(
            http_error_code_to_h2_error_code(HttpErrorCode::FLOW_CONTROL_ERROR, false),
            H2ErrorCode::FLOW_CONTROL_ERROR,
        );
    }

    #[test]
    fn h1_h2_mappings_and_h3_rejection() {
        let source_mappings = [
            (HttpErrorCode::NO_ERROR, H2ErrorCode::NO_ERROR),
            (HttpErrorCode::PROTOCOL_ERROR, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::INTERNAL_ERROR, H2ErrorCode::INTERNAL_ERROR),
            (
                HttpErrorCode::FLOW_CONTROL_ERROR,
                H2ErrorCode::INTERNAL_ERROR,
            ),
            (HttpErrorCode::SETTINGS_TIMEOUT, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::STREAM_CLOSED, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::FRAME_SIZE_ERROR, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::READ_TIMEOUT, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::REFUSED_STREAM, H2ErrorCode::REFUSED_STREAM),
            (HttpErrorCode::CANCEL, H2ErrorCode::CANCEL),
            (
                HttpErrorCode::COMPRESSION_ERROR,
                H2ErrorCode::INTERNAL_ERROR,
            ),
            (HttpErrorCode::CONNECT_ERROR, H2ErrorCode::CONNECT_ERROR),
            (
                HttpErrorCode::ENHANCE_YOUR_CALM,
                H2ErrorCode::ENHANCE_YOUR_CALM,
            ),
            (
                HttpErrorCode::INADEQUATE_SECURITY,
                H2ErrorCode::INADEQUATE_SECURITY,
            ),
            (
                HttpErrorCode::HTTP_1_1_REQUIRED,
                H2ErrorCode::HTTP_1_1_REQUIRED,
            ),
            (HttpErrorCode::MESSAGE_ERROR, H2ErrorCode::INTERNAL_ERROR),
            (
                HttpErrorCode::INVALID_STATE_TRANSITION,
                H2ErrorCode::INTERNAL_ERROR,
            ),
            (
                HttpErrorCode::HEADER_PARSE_ERROR,
                H2ErrorCode::INTERNAL_ERROR,
            ),
            (HttpErrorCode::BODY_PARSE_ERROR, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::WRITE_TIMEOUT, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::CORO_CANCELLED, H2ErrorCode::INTERNAL_ERROR),
            (HttpErrorCode::TRANSPORT_EOF, H2ErrorCode::INTERNAL_ERROR),
            (
                HttpErrorCode::TRANSPORT_READ_ERROR,
                H2ErrorCode::INTERNAL_ERROR,
            ),
            (
                HttpErrorCode::TRANSPORT_WRITE_ERROR,
                H2ErrorCode::INTERNAL_ERROR,
            ),
            (HttpErrorCode::DROPPED, H2ErrorCode::INTERNAL_ERROR),
            (
                HttpErrorCode::CONTENT_LENGTH_MISMATCH,
                H2ErrorCode::INTERNAL_ERROR,
            ),
        ];
        for (http_code, h2_code) in source_mappings {
            assert_eq!(http_error_code_to_h2_error_code(http_code, true), h2_code,);
        }

        for (h2_code, canonical_http_code) in [
            (H2ErrorCode::NO_ERROR, HttpErrorCode::NO_ERROR),
            (H2ErrorCode::PROTOCOL_ERROR, HttpErrorCode::PROTOCOL_ERROR),
            (H2ErrorCode::INTERNAL_ERROR, HttpErrorCode::INTERNAL_ERROR),
            (
                H2ErrorCode::FLOW_CONTROL_ERROR,
                HttpErrorCode::FLOW_CONTROL_ERROR,
            ),
            (
                H2ErrorCode::SETTINGS_TIMEOUT,
                HttpErrorCode::SETTINGS_TIMEOUT,
            ),
            (H2ErrorCode::STREAM_CLOSED, HttpErrorCode::STREAM_CLOSED),
            (
                H2ErrorCode::FRAME_SIZE_ERROR,
                HttpErrorCode::FRAME_SIZE_ERROR,
            ),
            (H2ErrorCode::REFUSED_STREAM, HttpErrorCode::REFUSED_STREAM),
            (H2ErrorCode::CANCEL, HttpErrorCode::CANCEL),
            (
                H2ErrorCode::COMPRESSION_ERROR,
                HttpErrorCode::COMPRESSION_ERROR,
            ),
            (H2ErrorCode::CONNECT_ERROR, HttpErrorCode::CONNECT_ERROR),
            (
                H2ErrorCode::ENHANCE_YOUR_CALM,
                HttpErrorCode::ENHANCE_YOUR_CALM,
            ),
            (
                H2ErrorCode::INADEQUATE_SECURITY,
                HttpErrorCode::INADEQUATE_SECURITY,
            ),
            (
                H2ErrorCode::HTTP_1_1_REQUIRED,
                HttpErrorCode::HTTP_1_1_REQUIRED,
            ),
        ] {
            assert_eq!(
                h2_error_code_to_http_error_code(h2_code),
                canonical_http_code,
            );
        }

        assert_eq!(
            h2_error_code_to_http_error_code(H2ErrorCode::from_u32(0xffff_ffff)),
            HttpErrorCode::PROTOCOL_ERROR,
        );
        assert_eq!(
            http_error_code_to_h2_error_code(HttpErrorCode::from_u16(0x7fff), false),
            H2ErrorCode::INTERNAL_ERROR,
        );

        for raw in [
            0x100, 0x101, 0x102, 0x103, 0x104, 0x105, 0x106, 0x107, 0x108, 0x109, 0x10a, 0x10b,
            0x10c, 0x10d, 0x10f, 0x110, 0x200, 0x201, 0x202,
        ] {
            let code = HttpErrorCode::from_u16(raw);
            assert_eq!(
                http_error_code_to_h2_error_code(code, false),
                H2ErrorCode::INTERNAL_ERROR,
                "H3/QPACK value {raw:#x} must not alias an H2 code",
            );
            assert_eq!(
                http_error_code_to_proxygen_error(code),
                ProxygenError::STREAM_ABORT,
                "H3/QPACK value {raw:#x} must not alias an H1 category",
            );
        }

        let h1_mappings = [
            (HttpErrorCode::NO_ERROR, ProxygenError::NONE),
            (HttpErrorCode::MESSAGE_ERROR, ProxygenError::MESSAGE),
            (HttpErrorCode::PROTOCOL_ERROR, ProxygenError::STREAM_ABORT),
            (HttpErrorCode::STREAM_CLOSED, ProxygenError::STREAM_ABORT),
            (HttpErrorCode::CANCEL, ProxygenError::CANCELED),
            (HttpErrorCode::CORO_CANCELLED, ProxygenError::CANCELED),
            (
                HttpErrorCode::REFUSED_STREAM,
                ProxygenError::STREAM_UNACKNOWLEDGED,
            ),
            (
                HttpErrorCode::INVALID_STATE_TRANSITION,
                ProxygenError::INGRESS_STATE_TRANSITION,
            ),
            (
                HttpErrorCode::HEADER_PARSE_ERROR,
                ProxygenError::PARSE_HEADER,
            ),
            (HttpErrorCode::BODY_PARSE_ERROR, ProxygenError::PARSE_BODY),
            (
                HttpErrorCode::CONTENT_LENGTH_MISMATCH,
                ProxygenError::PARSE_BODY,
            ),
            (
                HttpErrorCode::TRANSPORT_READ_ERROR,
                ProxygenError::CONNECTION_RESET,
            ),
            (HttpErrorCode::TRANSPORT_WRITE_ERROR, ProxygenError::WRITE),
            (HttpErrorCode::TRANSPORT_EOF, ProxygenError::EOF),
            (HttpErrorCode::READ_TIMEOUT, ProxygenError::TIMEOUT),
            (HttpErrorCode::DROPPED, ProxygenError::DROPPED),
        ];
        for (http_code, proxygen_error) in h1_mappings {
            assert_eq!(http_error_code_to_proxygen_error(http_code), proxygen_error,);
        }

        let mut h2_exception = HttpException::new(HttpExceptionDirection::Ingress, b"h2");
        h2_exception.set_proxygen_error(ProxygenError::PARSE_HEADER);
        h2_exception.set_codec_status_code(H2ErrorCode::CANCEL);
        assert_eq!(
            http_exception_to_http_error_code(&h2_exception),
            HttpErrorCode::CANCEL,
            "the codec status takes precedence over the legacy H1 category",
        );

        for (proxygen_error, expected) in [
            (
                ProxygenError::PARSE_HEADER,
                HttpErrorCode::HEADER_PARSE_ERROR,
            ),
            (ProxygenError::PARSE_BODY, HttpErrorCode::BODY_PARSE_ERROR),
            (ProxygenError::EOF, HttpErrorCode::TRANSPORT_EOF),
            (ProxygenError::UNKNOWN, HttpErrorCode::PROTOCOL_ERROR),
            (ProxygenError::NONE, HttpErrorCode::PROTOCOL_ERROR),
            (
                ProxygenError::HEADER_CONTENT_VALIDATION,
                HttpErrorCode::PROTOCOL_ERROR,
            ),
        ] {
            let mut exception = HttpException::new(HttpExceptionDirection::IngressAndEgress, b"h1");
            exception.set_proxygen_error(proxygen_error);
            assert_eq!(http_exception_to_http_error_code(&exception), expected);
        }
    }

    #[test]
    fn h1_status_and_header_validation_category_remain_distinct_and_owned() {
        let mut partial = HttpMessage::new();
        partial.set_sequence_number(17);
        let mut exception = HttpException::new(HttpExceptionDirection::Ingress, b"bad header");
        assert!(!exception.has_http_status_code());
        assert_eq!(exception.http_status_code(), None);
        exception.set_http_status_code(400);
        exception.set_proxygen_error(ProxygenError::HEADER_CONTENT_VALIDATION);
        exception.set_partial_message(Some(partial));

        let owned = exception.clone();
        exception.set_http_status_code(0);
        exception.set_partial_message(Some(HttpMessage::new()));

        assert_eq!(owned.http_status_code(), Some(400));
        assert!(owned.has_http_status_code());
        assert_eq!(
            owned.proxygen_error(),
            ProxygenError::HEADER_CONTENT_VALIDATION,
        );
        assert_eq!(owned.proxygen_error().as_u8(), 27);
        assert_ne!(owned.proxygen_error(), ProxygenError::PARSE_HEADER);
        assert_eq!(
            owned.partial_message().map(HttpMessage::sequence_number),
            Some(17),
        );
        assert_eq!(exception.http_status_code(), None);
        assert_eq!(
            http_exception_to_http_error_code(&owned),
            HttpErrorCode::PROTOCOL_ERROR,
            "the frozen adapter's unknown legacy-category fallback remains total",
        );
    }

    #[test]
    fn error_to_exception_preserves_direction_code_message_and_partial_message() {
        let mut partial_message = HttpMessage::new();
        partial_message.set_sequence_number(42);
        let mut error = HttpError::with_message(HttpErrorCode::FLOW_CONTROL_ERROR, b"bad flow")
            .with_http_message(partial_message);

        let exception = http_error_to_http_exception(&error);
        error
            .http_message_mut()
            .expect("the original error retains its partial message")
            .set_sequence_number(99);

        assert_eq!(
            exception.direction(),
            HttpExceptionDirection::IngressAndEgress,
        );
        assert!(exception.is_ingress_exception());
        assert!(exception.is_egress_exception());
        assert_eq!(exception.message(), b"bad flow");
        assert_eq!(exception.proxygen_error(), ProxygenError::STREAM_ABORT);
        assert_eq!(
            exception.codec_status_code(),
            Some(H2ErrorCode::FLOW_CONTROL_ERROR),
        );
        assert_eq!(
            exception
                .partial_message()
                .map(HttpMessage::sequence_number),
            Some(42),
        );
        assert_eq!(
            error.http_message().map(HttpMessage::sequence_number),
            Some(99),
            "the original partial message remains independently mutable",
        );

        let library_exception =
            http_error_to_http_exception(&HttpError::new(HttpErrorCode::CONTENT_LENGTH_MISMATCH));
        assert_eq!(library_exception.codec_status_code(), None);
        assert_eq!(
            library_exception.proxygen_error(),
            ProxygenError::PARSE_BODY,
        );
    }
}
