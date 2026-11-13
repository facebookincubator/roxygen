//! Bounded cleanup of panic payloads whose destructors can also panic, and
//! the text of a payload.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

const MAX_DISPOSAL_ATTEMPTS: usize = 16;

/// Disposes a suppressed panic without replacing the caller's first failure.
///
/// Each destructor runs inside its own unwind boundary, including during an
/// existing unwind. A payload that repeatedly recreates a panicking payload is
/// retained after a bounded number of destructor attempts.
#[cold]
#[inline(never)]
pub fn discard_panic(mut payload: Box<dyn Any + Send>) {
    for _ in 0..MAX_DISPOSAL_ATTEMPTS {
        match catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            Ok(()) => return,
            Err(next) => payload = next,
        }
    }
    std::mem::forget(payload);
}

/// The text of a payload that `panic!` made from a message: a `&'static str`
/// or a `String`.
#[cfg(any(test, feature = "observability"))]
pub(crate) fn panic_message(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
}

/// The longest prefix of `text` of at most `limit` bytes that ends at a
/// character boundary.
#[cfg(any(test, feature = "observability"))]
pub(crate) fn truncated(text: &str, limit: usize) -> &str {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use std::panic::panic_any;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    struct PayloadChain {
        remaining: usize,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for PayloadChain {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.remaining != 0 {
                panic_any(Self {
                    remaining: self.remaining - 1,
                    drops: Arc::clone(&self.drops),
                });
            }
        }
    }

    #[test]
    fn secondary_payloads_release_their_owned_resources() {
        let drops = Arc::new(AtomicUsize::new(0));
        discard_panic(Box::new(PayloadChain {
            remaining: 3,
            drops: Arc::clone(&drops),
        }));
        assert_eq!(drops.load(Ordering::SeqCst), 4);
        assert_eq!(Arc::strong_count(&drops), 1);
    }

    #[test]
    fn cleanup_during_unwind_preserves_the_original_payload() {
        struct Cleanup(Arc<AtomicUsize>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                discard_panic(Box::new(PayloadChain {
                    remaining: 2,
                    drops: Arc::clone(&self.0),
                }));
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let original = Arc::new(42);
        let caught = catch_unwind(AssertUnwindSafe(|| {
            let _cleanup = Cleanup(Arc::clone(&drops));
            panic_any(Arc::clone(&original));
        }))
        .expect_err("the original failure must escape cleanup");
        assert!(Arc::ptr_eq(
            caught.downcast_ref::<Arc<i32>>().expect("original payload"),
            &original,
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert_eq!(Arc::strong_count(&drops), 1);
    }

    #[test]
    fn self_recreating_payload_cleanup_is_bounded() {
        static DROPS: AtomicUsize = AtomicUsize::new(0);
        struct RepeatingPayload;
        impl Drop for RepeatingPayload {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::SeqCst);
                panic_any(Self);
            }
        }

        discard_panic(Box::new(RepeatingPayload));
        assert_eq!(DROPS.load(Ordering::SeqCst), MAX_DISPOSAL_ATTEMPTS);
    }
}
