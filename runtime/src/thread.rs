//! Stable access to the calling thread's identity.

use std::cell::Cell;
use std::thread::ThreadId;

thread_local! {
    // Const-initialized without a destructor, so it stays readable for the
    // whole life of the thread, including while thread-local destructors run.
    static CURRENT_ID: Cell<Option<ThreadId>> = const { Cell::new(None) };
}

/// Returns the calling thread's [`ThreadId`], equal to
/// `std::thread::current().id()`, without cloning the thread handle after the
/// first call on that thread.
///
/// The cached id remains available after the standard library releases the
/// thread handle during thread exit, where `std::thread::current()` panics.
/// Only a thread whose first call happens after that release reaches
/// `std::thread::current()`; owner threads cache their id when they create a
/// [`Host`](crate::host::Host).
#[inline]
pub fn current_id() -> ThreadId {
    CURRENT_ID
        .try_with(|cached| cached.get().unwrap_or_else(|| cache_current_id(cached)))
        // Key-based TLS targets can release the cache before other destructors.
        .unwrap_or_else(|_| std::thread::current().id())
}

#[cold]
#[inline(never)]
fn cache_current_id(cached: &Cell<Option<ThreadId>>) -> ThreadId {
    let id = std::thread::current().id();
    cached.set(Some(id));
    id
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;

    #[test]
    fn ids_match_std_and_differ_across_threads() {
        let local = current_id();
        assert_eq!(local, std::thread::current().id());
        assert_eq!(local, current_id(), "repeated lookups return the cached id");

        let (spawned, spawned_std) =
            std::thread::spawn(|| (current_id(), std::thread::current().id()))
                .join()
                .expect("spawned thread");
        assert_eq!(spawned, spawned_std);
        assert_ne!(spawned, local, "each thread observes its own id");
    }

    #[test]
    fn first_lookup_during_thread_local_destruction() {
        struct RecordOnDrop(Arc<Mutex<Option<ThreadId>>>);

        impl Drop for RecordOnDrop {
            fn drop(&mut self) {
                *self.0.lock().expect("observed id") = Some(current_id());
            }
        }

        thread_local! {
            static RECORDER: RefCell<Option<RecordOnDrop>> = const { RefCell::new(None) };
        }

        let observed = Arc::new(Mutex::new(None));
        let recorder = RecordOnDrop(Arc::clone(&observed));
        let expected = std::thread::spawn(move || {
            RECORDER.with(|slot| *slot.borrow_mut() = Some(recorder));
            std::thread::current().id()
        })
        .join()
        .expect("spawned thread");
        assert_eq!(
            *observed.lock().expect("observed id"),
            Some(expected),
            "an uncached lookup from a thread-local destructor reports the exiting thread",
        );
    }

    #[cfg(unix)]
    #[test]
    fn foreign_thread_ids_match_std() {
        use std::mem::MaybeUninit;

        static OBSERVED: Mutex<Option<(ThreadId, ThreadId, ThreadId)>> = Mutex::new(None);

        extern "C" fn run(_: *mut libc::c_void) -> *mut libc::c_void {
            let observed = (current_id(), current_id(), std::thread::current().id());
            *OBSERVED.lock().expect("observed ids") = Some(observed);
            std::ptr::null_mut()
        }

        let mut thread = MaybeUninit::<libc::pthread_t>::uninit();
        // SAFETY: `thread` is a valid out-pointer, null attributes select the
        // defaults, and `run` never reads its argument.
        let created = unsafe {
            libc::pthread_create(
                thread.as_mut_ptr(),
                std::ptr::null(),
                run,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(created, 0);
        // SAFETY: `pthread_create` succeeded, so it initialized `thread` with a
        // joinable thread that nothing else joins or detaches.
        let joined = unsafe { libc::pthread_join(thread.assume_init(), std::ptr::null_mut()) };
        assert_eq!(joined, 0);

        let (first, second, std_id) = OBSERVED
            .lock()
            .expect("observed ids")
            .expect("foreign thread ran");
        assert_eq!(first, second, "repeated lookups return the cached id");
        assert_eq!(first, std_id);
        assert_ne!(first, current_id(), "each thread observes its own id");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn cached_id_survives_thread_handle_teardown() {
        use std::panic::catch_unwind;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        struct Probe {
            key: libc::pthread_key_t,
            calls: AtomicUsize,
            observed: Mutex<Option<ThreadId>>,
        }

        unsafe extern "C" fn late_destructor(value: *mut libc::c_void) {
            // SAFETY: `value` is `Arc::as_ptr` of the test's `probe`, which the
            // test holds until `join` returns, and `join` waits for this
            // thread's key destructors.
            let probe = unsafe { &*value.cast::<Probe>() };
            if probe.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                // Re-arming defers the next call until after std's own key
                // destructor has released the thread handle.
                // SAFETY: `key` has not been deleted and `value` stays valid
                // for the same reason as above.
                unsafe { libc::pthread_setspecific(probe.key, value) };
                return;
            }
            *probe.observed.lock().expect("observed lookup") = catch_unwind(current_id).ok();
        }

        let mut key = 0;
        // SAFETY: `key` is a valid out-pointer and `late_destructor` matches
        // the destructor ABI.
        let created = unsafe { libc::pthread_key_create(&mut key, Some(late_destructor)) };
        assert_eq!(created, 0);
        let probe = Arc::new(Probe {
            key,
            calls: AtomicUsize::new(0),
            observed: Mutex::new(None),
        });

        let thread_probe = Arc::clone(&probe);
        let expected = std::thread::spawn(move || {
            let id = current_id();
            // SAFETY: `key` is live, and the stored pointer satisfies
            // `late_destructor`'s lifetime requirement.
            let set = unsafe {
                libc::pthread_setspecific(thread_probe.key, Arc::as_ptr(&thread_probe).cast())
            };
            assert_eq!(set, 0);
            id
        })
        .join()
        .expect("spawned thread");
        // SAFETY: the only thread that stored a value under `key` has exited.
        let deleted = unsafe { libc::pthread_key_delete(key) };
        assert_eq!(deleted, 0);

        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            2,
            "destructor re-armed once"
        );
        assert_eq!(
            probe.observed.lock().expect("observed lookup").take(),
            Some(expected),
            "the cached id outlives std's thread handle teardown",
        );
    }
}
