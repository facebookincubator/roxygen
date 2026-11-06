use crate::flow_control::Window;
use crate::flow_control::WindowContainer;

fn window(capacity: u32) -> Window {
    Window::new(capacity).expect("test capacity must fit the signed window range")
}

fn container(capacity: u32) -> WindowContainer {
    WindowContainer::new(capacity).expect("test capacity must fit the signed window range")
}

#[test]
fn window_test_basic() {
    let mut window = window(100);
    assert!(window.free(10));
    assert_eq!(window.size(), 110);
    assert_eq!(window.capacity(), 100);
    assert!(window.reserve(20, true));
    assert_eq!(window.size(), 90);
    assert!(window.reserve(90, true));
    assert_eq!(window.size(), 0);
    assert!(window.free(5));
    assert_eq!(window.size(), 5);
    assert!(window.free(0));
    assert_eq!(window.size(), 5);
}

#[test]
fn window_test_change_capacity() {
    let mut window = window(100);
    assert!(window.set_capacity(10));
    assert_eq!(window.size(), 10);
    assert_eq!(window.capacity(), 10);
    assert!(window.reserve(7, true));
    assert!(window.set_capacity(5));
    assert_eq!(window.capacity(), 5);
    assert_eq!(window.size(), -2);
    assert!(window.free(1));
    assert_eq!(window.size(), -1);
    assert!(window.set_capacity(6));
    assert_eq!(window.size(), 0);
    assert_eq!(window.capacity(), 6);
    assert!(window.set_capacity(7));
    assert_eq!(window.size(), 1);
    assert_eq!(window.capacity(), 7);
    assert_eq!(window.outstanding(), 6);
}

#[test]
fn window_test_exceed_window() {
    let mut window = window(100);
    assert!(window.reserve(50, true));
    assert!(window.reserve(40, true));
    assert_eq!(window.size(), 10);
    assert!(!window.reserve(20, true));
    assert_eq!(window.size(), 10);
    assert_eq!(window.outstanding(), 90);
}

#[test]
fn window_test_overflow() {
    let mut window = window(0);
    assert!(!window.reserve(i32::MAX as u32, true));
    assert_eq!(window.size(), 0);
    assert_eq!(window.outstanding(), 0);
}

#[test]
fn window_test_underflow() {
    let mut window = window(100);
    assert!(window.free(100));
    assert!(window.free(100));
    assert_eq!(window.size(), 300);
    assert!(!window.free(i32::MAX as u32));
    assert_eq!(window.size(), 300);
}

#[test]
fn window_test_huge_reserve() {
    let mut window = window(100);
    assert!(!window.reserve(u32::MAX, true));
    assert_eq!(window.size(), 100);
    assert_eq!(window.outstanding(), 0);
}

#[test]
fn window_test_huge_free() {
    let mut window1 = window(0);
    assert!(window1.free(i32::MAX as u32));
    let mut window2 = window(1);
    assert!(!window2.free(i32::MAX as u32));
}

#[test]
fn window_test_huge_free2() {
    for capacity in 0_u32..10 {
        let mut window = window(capacity);
        assert!(window.free(i32::MAX as u32 - capacity));
        assert!(!window.free(1));
    }
}

#[test]
fn window_test_bytes_outstanding() {
    let mut window = window(100);
    assert_eq!(window.outstanding(), 0);
    assert!(window.reserve(20, true));
    assert_eq!(window.outstanding(), 20);
    assert!(window.free(30));
    assert_eq!(window.outstanding(), 0);
}

#[test]
fn window_test_bytes_outstanding_after_fail() {
    let mut window = window(100);
    assert_eq!(window.outstanding(), 0);
    assert!(!window.reserve(110, true));
    assert_eq!(window.outstanding(), 0);
}

#[test]
fn window_test_non_strict() {
    let mut window = window(100);
    assert!(window.reserve(110, false));
    assert_eq!(window.outstanding(), 110);
    assert_eq!(window.size(), -10);
    assert_eq!(window.non_negative_size(), 0);
}

#[test]
fn window_test_new_capacity_overflow() {
    let mut window = window(0);
    assert!(window.free(10));
    assert_eq!(window.outstanding(), 0);
    assert_eq!(window.size(), 10);
    assert!(window.set_capacity(i32::MAX as u32 - 10));
    assert_eq!(window.size(), i32::MAX);
    assert!(!window.set_capacity(i32::MAX as u32 - 9));
    assert_eq!(window.size(), i32::MAX);
    assert_eq!(window.capacity(), i32::MAX as u32 - 10);
}

#[test]
fn half_window_triggers_update_below_minimum() {
    const CAPACITY: u32 = 64 * 1024;
    let mut container = container(CAPACITY);
    assert!(container.reserve(CAPACITY as usize, 0, true));
    assert_eq!(container.processed((CAPACITY / 2 - 1) as usize), Some(0));
    assert!(container.processed(1).is_some_and(|update| update > 0));
}

#[test]
fn absolute_minimum_triggers_update() {
    const CAPACITY: u32 = 1024 * 1024;
    const MINIMUM_UPDATE: usize = 128 * 1024;
    let mut container = container(CAPACITY);
    assert!(container.reserve(CAPACITY as usize, 0, true));
    assert_eq!(container.processed(MINIMUM_UPDATE - 1), Some(0));
    assert!(container.processed(1).is_some_and(|update| update > 0));
}

#[test]
fn constructor_and_window_failures_are_atomic_at_integer_boundaries() {
    assert!(Window::new(i32::MAX as u32).is_some());
    assert!(Window::new(i32::MAX as u32 + 1).is_none());
    assert!(WindowContainer::new(i32::MAX as u32 + 1).is_none());

    let mut exhausted_window = window(100);
    assert!(exhausted_window.reserve(100, true));
    let exhausted = exhausted_window;
    assert!(!exhausted_window.reserve(1, true));
    assert_eq!(exhausted_window, exhausted);
    assert!(!exhausted_window.reserve(u32::MAX, false));
    assert_eq!(exhausted_window, exhausted);

    let mut maximum_outstanding = window(i32::MAX as u32);
    assert!(maximum_outstanding.reserve(i32::MAX as u32, true));
    let maximum_outstanding_before_failure = maximum_outstanding;
    assert!(!maximum_outstanding.reserve(1, false));
    assert_eq!(maximum_outstanding, maximum_outstanding_before_failure);

    let mut credited = window(0);
    assert!(credited.free(10));
    assert!(credited.set_capacity(i32::MAX as u32 - 10));
    let maximum_size = credited;
    assert!(!credited.set_capacity(i32::MAX as u32 - 9));
    assert_eq!(credited, maximum_size);
    assert!(!credited.set_capacity(u32::MAX));
    assert_eq!(credited, maximum_size);
}

#[test]
fn padding_is_returned_immediately_and_acknowledged_with_processed_credit() {
    let mut container = container(100);
    assert!(container.reserve(60, 10, true));
    assert_eq!(container.size(), 40);
    assert_eq!(container.processed(39), Some(0));
    assert_eq!(container.size(), 79);
    assert_eq!(container.processed(1), Some(50));
    assert_eq!(container.size(), 80);
}

#[test]
fn container_failures_are_atomic_and_capacity_never_shrinks() {
    let mut state = container(100);
    let original = state;
    assert!(!state.reserve(101, 0, true));
    assert_eq!(state, original);
    assert!(state.reserve(usize::MAX, 1, false));
    assert_eq!(state.size(), 101);
    let after_wrapped_reservation = state;
    assert_eq!(state.processed(u32::MAX as usize + 1), None);
    assert_eq!(state, after_wrapped_reservation);
    assert_eq!(state.set_capacity(99), Some(0));
    assert_eq!(state, after_wrapped_reservation);
    assert_eq!(state.processed(48), Some(0));
    assert_eq!(state.processed(1), Some(50));

    let mut overflow = container(0);
    assert_eq!(overflow.processed(10), Some(10));
    assert_eq!(
        overflow.set_capacity(i32::MAX as u32 - 10),
        Some(i32::MAX as u32 - 10),
    );
    let maximum_size = overflow;
    assert_eq!(overflow.set_capacity(i32::MAX as u32 - 9), None);
    assert_eq!(overflow, maximum_size);
}

#[test]
fn stream_and_connection_windows_are_independent() {
    let mut stream = window(100);
    let mut connection = window(1_000);

    assert!(stream.reserve(80, true));
    assert_eq!(stream.size(), 20);
    assert_eq!(connection.size(), 1_000);
    assert!(connection.reserve(250, true));
    assert_eq!(stream.size(), 20);
    assert_eq!(connection.size(), 750);
}
