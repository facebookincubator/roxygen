use crate::headers::HttpHeaderCode;
use crate::headers::HttpHeaders;
use crate::priority::DEFAULT_HTTP_PRIORITY_URGENCY;
use crate::priority::HttpPriority;
use crate::priority::HttpPriorityHeaderAccess;
use crate::priority::MAX_HTTP_PRIORITY_URGENCY;
use crate::priority::http_priority_from_http_message;
use crate::priority::http_priority_from_string;
use crate::priority::http_priority_to_string;

#[derive(Default)]
struct TestMessage {
    headers: HttpHeaders,
}

impl TestMessage {
    fn headers(&self) -> &HttpHeaders {
        &self.headers
    }

    fn headers_mut(&mut self) -> &mut HttpHeaders {
        &mut self.headers
    }
}

impl HttpPriorityHeaderAccess for TestMessage {
    fn priority_header_value(&self) -> &[u8] {
        self.headers.get_single_or_empty(HttpHeaderCode::Priority)
    }

    fn set_priority_header_value(&mut self, value: &[u8]) {
        self.headers.set(HttpHeaderCode::Priority, value);
    }
}

#[test]
fn httpmessage_no_default_httppriority() {
    let message = TestMessage::default();
    assert_eq!(http_priority_from_http_message(&message), None);
}

#[test]
fn httpmessage_httppriority_set_get() {
    let mut message = TestMessage::default();
    message.set_http_priority(HttpPriority::from_urgency_and_incremental(0, true));
    assert_eq!(
        http_priority_from_http_message(&message).unwrap().urgency,
        0
    );
    assert!(
        http_priority_from_http_message(&message)
            .unwrap()
            .incremental
    );
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=0,i",
    );
    let mut priority_via_getter = message.get_http_priority().unwrap();
    assert_eq!(priority_via_getter.urgency, 0);
    assert!(priority_via_getter.incremental);

    message.set_http_priority_parts(1, false);
    assert_eq!(
        http_priority_from_http_message(&message).unwrap().urgency,
        1
    );
    assert!(
        !http_priority_from_http_message(&message)
            .unwrap()
            .incremental
    );
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=1",
    );
    priority_via_getter = message.get_http_priority().unwrap();
    assert_eq!(priority_via_getter.urgency, 1);
    assert!(!priority_via_getter.incremental);
}

#[test]
fn httpmessage_httppriority_set_out_range_urgency() {
    let mut message = TestMessage::default();
    message.set_http_priority(HttpPriority::from_urgency_and_incremental(
        MAX_HTTP_PRIORITY_URGENCY + 10,
        true,
    ));
    assert_eq!(
        http_priority_from_http_message(&message).unwrap().urgency,
        MAX_HTTP_PRIORITY_URGENCY,
    );
    assert!(
        http_priority_from_http_message(&message)
            .unwrap()
            .incremental
    );
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=7,i",
    );
    let priority_via_getter = message.get_http_priority().unwrap();
    assert_eq!(priority_via_getter.urgency, 7);
    assert!(priority_via_getter.incremental);
}

#[test]
fn httpmessage_httppriority_order_id_set_get() {
    let mut message = TestMessage::default();
    message.set_http_priority(HttpPriority::new(7, false, 10, false, 0));
    assert_eq!(
        http_priority_from_http_message(&message).unwrap().order_id,
        10,
    );
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=7,o=10",
    );
    let priority_via_getter = message.get_http_priority().unwrap();
    assert_eq!(priority_via_getter.urgency, 7);
    assert_eq!(priority_via_getter.order_id, 10);
}

#[test]
fn httpmessage_httppriority_paused_set_get() {
    let mut message = TestMessage::default();
    message.set_http_priority(HttpPriority::new(7, false, 0, true, 0));
    assert!(http_priority_from_http_message(&message).unwrap().paused);
    assert_eq!(
        message
            .headers()
            .get_single_or_empty(HttpHeaderCode::Priority),
        b"u=7,p",
    );
}

#[test]
fn httppriority_functions_test_priority_header_urgency_only() {
    let mut request = TestMessage::default();

    request.headers_mut().add(HttpHeaderCode::Priority, "u=5");
    let mut priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 5);
    assert!(!priority.incremental);
    assert_eq!(priority.required_bps, 0);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "u=3, true");
    priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert!(!priority.incremental);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_incremental_only() {
    let mut request = TestMessage::default();

    request.headers_mut().add(HttpHeaderCode::Priority, "i");
    let mut priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 0);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "i, true");
    priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 0);

    request.headers_mut().set(HttpHeaderCode::Priority, "i=?1");
    priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 0);

    request.headers_mut().set(HttpHeaderCode::Priority, "i=?0");
    priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert!(!priority.incremental);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_white_space() {
    let mut request = TestMessage::default();
    request.headers_mut().add(HttpHeaderCode::Priority, "    ");
    assert_eq!(http_priority_from_http_message(&request), None);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "u=4,  i ");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 4);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_urgency_and_incremental() {
    let mut request = TestMessage::default();
    request.headers_mut().add(HttpHeaderCode::Priority, "u=4,i");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 4);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_urgency_and_incremental_uppercase() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "U=4, I");
    assert_eq!(http_priority_from_http_message(&request), None);
}

#[test]
fn httppriority_functions_test_priority_header_bad_urgency() {
    let mut request = TestMessage::default();
    request.headers_mut().add(HttpHeaderCode::Priority, "x=3");
    assert_eq!(http_priority_from_http_message(&request), None);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "x=2, i");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 0);

    request.headers_mut().set(HttpHeaderCode::Priority, "u=8");
    assert_eq!(http_priority_from_http_message(&request), None);

    request.headers_mut().set(HttpHeaderCode::Priority, "u=-2");
    assert_eq!(http_priority_from_http_message(&request), None);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "u=banana,i");
    assert_eq!(http_priority_from_http_message(&request), None);
}

#[test]
fn httppriority_functions_test_priority_header_bad_incremental() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, i=0");
    assert_eq!(http_priority_from_http_message(&request), None);
}

#[test]
fn httppriority_functions_test_priority_header_bad_paused() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, p=0");
    assert_eq!(http_priority_from_http_message(&request), None);
}

#[test]
fn httppriority_functions_test_priority_header_default_order_id() {
    let mut request = TestMessage::default();
    request.headers_mut().add(HttpHeaderCode::Priority, "u=3");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.order_id, 0);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_custom_order_id() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, o=100");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert_eq!(priority.order_id, 100);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_default_unpaused() {
    let mut request = TestMessage::default();
    request.headers_mut().add(HttpHeaderCode::Priority, "u=3");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert!(!priority.paused);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_paused() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, p");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert!(priority.paused);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_required_rate_valid() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=0, i, r=5000000");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 0);
    assert!(priority.incremental);
    assert_eq!(priority.required_bps, 5_000_000);
}

#[test]
fn httppriority_functions_test_priority_header_required_rate_zero() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, r=0");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 3);
    assert_eq!(priority.required_bps, 0);
}

#[test]
fn httppriority_functions_test_priority_header_required_rate_only() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "r=1000000");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, DEFAULT_HTTP_PRIORITY_URGENCY);
    assert!(!priority.incremental);
    assert_eq!(priority.required_bps, 1_000_000);
}

#[test]
fn httppriority_functions_test_priority_header_bad_required_rate() {
    let mut request = TestMessage::default();

    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, r=-100");
    assert_eq!(http_priority_from_http_message(&request), None);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "u=5, i, r=invalid");
    assert_eq!(http_priority_from_http_message(&request), None);

    request
        .headers_mut()
        .set(HttpHeaderCode::Priority, "u=3, R=5000000");
    assert_eq!(http_priority_from_http_message(&request), None);
}

#[test]
fn httppriority_functions_test_priority_header_required_rate_large() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=0, r=100000000000");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 0);
    assert_eq!(priority.required_bps, 100_000_000_000);
}

#[test]
fn httppriority_functions_test_priority_header_required_rate_zero_vs_omitted() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=3, r=0");
    let with_zero = http_priority_from_http_message(&request).unwrap();
    assert_eq!(with_zero.required_bps, 0);

    request.headers_mut().set(HttpHeaderCode::Priority, "u=3");
    let with_omitted = http_priority_from_http_message(&request).unwrap();
    assert_eq!(with_omitted.required_bps, 0);
    assert_eq!(with_zero.required_bps, with_omitted.required_bps);
}

#[test]
fn httppriority_functions_test_priority_header_all_parameters() {
    let mut request = TestMessage::default();
    request
        .headers_mut()
        .add(HttpHeaderCode::Priority, "u=2, i, o=50, p, r=10000000");
    let priority = http_priority_from_http_message(&request).unwrap();
    assert_eq!(priority.urgency, 2);
    assert!(priority.incremental);
    assert_eq!(priority.order_id, 50);
    assert!(priority.paused);
    assert_eq!(priority.required_bps, 10_000_000);
}

#[test]
fn httppriority_functions_test_priority_to_string_with_required_rate() {
    let priority = HttpPriority::new(0, true, 0, false, 5_000_000);
    assert_eq!(http_priority_to_string(priority), "u=0,i,r=5000000");
}

#[test]
fn httppriority_functions_test_priority_to_string_no_required_rate() {
    let priority = HttpPriority::new(3, false, 0, false, 0);
    assert_eq!(http_priority_to_string(priority), "u=3");
}

#[test]
fn httppriority_functions_test_priority_equality_with_required_rate() {
    let a = HttpPriority::new(1, true, 10, false, 5_000_000);
    let b = HttpPriority::new(1, true, 10, false, 5_000_000);
    let c = HttpPriority::new(1, true, 10, false, 6_000_000);
    let d = HttpPriority::new(1, true, 10, false, 0);
    assert_eq!(a, b);
    assert_ne!(a, c);
    assert_ne!(a, d);
}

#[test]
fn parser_preserves_duplicate_and_integer_conversion_behavior() {
    assert_eq!(http_priority_from_string("u=1,u=2"), None);
    assert_eq!(
        http_priority_from_string("o=4294967296").unwrap().order_id,
        0,
    );

    let mut message = TestMessage::default();
    message.headers_mut().add(HttpHeaderCode::Priority, "u=1");
    message.headers_mut().add(HttpHeaderCode::Priority, "u=2");
    assert_eq!(http_priority_from_http_message(&message), None);
}

#[test]
fn direct_parser_rejects_trailing_optional_whitespace_before_end() {
    assert_eq!(http_priority_from_string(b"u=3 "), None);
    assert_eq!(http_priority_from_string(b"u=3\t"), None);

    let mut message = TestMessage::default();
    message
        .headers_mut()
        .add(HttpHeaderCode::Priority, b"u=3 \t");
    assert_eq!(message.get_http_priority().unwrap().urgency, 3);
}

#[test]
fn unknown_binary_members_require_canonical_base64_padding_bits() {
    assert!(http_priority_from_string("u=3,x=*AA==*").is_some());
    assert_eq!(http_priority_from_string("u=3,x=*AB==*"), None);
    assert!(http_priority_from_string("u=3,x=*ABA=*").is_some());
    assert_eq!(http_priority_from_string("u=3,x=*ABC=*"), None);
}

#[test]
fn explicit_boolean_items_only_parse_as_final_members() {
    assert!(!http_priority_from_string("i=?0").unwrap().incremental);
    assert!(http_priority_from_string("i=?1").unwrap().incremental);
    assert_eq!(http_priority_from_string("i=?0,u=3"), None);
    assert_eq!(http_priority_from_string("i=?1,u=3"), None);
}

#[test]
fn serializer_uses_source_member_order_and_omission_rules() {
    let priority = HttpPriority::new(2, true, 50, true, 10_000_000);
    assert_eq!(http_priority_to_string(priority), "u=2,i,o=50,p,r=10000000",);
}
