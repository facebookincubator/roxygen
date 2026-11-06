//! Byte-offset notifications for HTTP headers, body data, and transport writes.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::rc::Rc;
use std::rc::Weak;

use crate::error::HttpError;
use crate::error::HttpErrorCode;
use crate::http::HttpHeaderSize;
use crate::ids::StreamId;

/// The transport milestone at which a registered byte range becomes observable.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum ByteEventType {
    /// The transport accepted the bytes for writing.
    TransportWrite = 0x01,
    /// The kernel accepted the bytes.
    KernelWrite = 0x02,
    /// The network interface transmitted the bytes.
    NicTx = 0x04,
    /// The peer cumulatively acknowledged the bytes.
    CumulativeAck = 0x08,
}

impl ByteEventType {
    pub const ALL: [Self; 4] = [
        Self::TransportWrite,
        Self::KernelWrite,
        Self::NicTx,
        Self::CumulativeAck,
    ];

    pub const fn bit(self) -> u8 {
        self as u8
    }
}

/// The kind of encoded header block associated with a byte event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FieldSectionType {
    /// Request or response headers.
    Headers,
    /// HTTP/2 push-promise headers.
    PushPromise,
    /// Trailing headers.
    Trailers,
}

/// Metadata about the header block associated with a byte event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldSectionInfo {
    /// The kind of header block.
    pub section_type: FieldSectionType,
    /// Whether this block is the final non-informational header block.
    pub final_headers: bool,
    /// Encoded and decoded header sizes.
    pub size: HttpHeaderSize,
}

/// The offsets and HTTP metadata reported when a byte milestone completes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpByteEvent {
    /// The stream associated with the event, when known.
    pub stream_id: Option<StreamId>,
    /// Header-block metadata for header events.
    pub field_section_info: Option<FieldSectionInfo>,
    /// Offset within the HTTP body.
    pub body_offset: u64,
    /// Offset within the transport byte stream.
    pub transport_offset: u64,
    /// Offset within the HTTP stream.
    pub stream_offset: u64,
    /// Whether these bytes complete the HTTP message.
    pub eom: bool,
    /// The transport milestone that completed.
    pub event_type: ByteEventType,
}

impl HttpByteEvent {
    pub const fn new(event_type: ByteEventType) -> Self {
        Self {
            stream_id: None,
            field_section_info: None,
            body_offset: 0,
            transport_offset: 0,
            stream_offset: 0,
            eom: false,
            event_type,
        }
    }
}

/// Receives completion or cancellation for registered byte events.
pub trait HttpByteEventCallback {
    /// Reports that a byte event reached its requested transport milestone.
    fn on_byte_event(&self, event: HttpByteEvent);

    /// Reports that a byte event was cancelled before completion.
    fn on_byte_event_cancelled(&self, event: HttpByteEvent, error: HttpError);
}

type CallbackPanic = Box<dyn Any + Send>;

fn discard_callback_panic(payload: CallbackPanic) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        roxygen_runtime::panic::discard_panic(secondary);
    }
}

fn retain_callback_panic(first: &mut Option<CallbackPanic>, next: Option<CallbackPanic>) {
    if let Some(payload) = next {
        if first.is_none() {
            *first = Some(payload);
        } else {
            discard_callback_panic(payload);
        }
    }
}

fn invoke_and_retire_callback(
    callback: Rc<dyn HttpByteEventCallback>,
    invoke: impl FnOnce(&dyn HttpByteEventCallback),
) -> Option<CallbackPanic> {
    let mut first = catch_unwind(AssertUnwindSafe(|| invoke(callback.as_ref()))).err();
    let retirement = catch_unwind(AssertUnwindSafe(|| drop(callback))).err();
    retain_callback_panic(&mut first, retirement);
    first
}

fn finish_callback_panic(first: Option<CallbackPanic>, already_unwinding: bool) {
    if let Some(payload) = first {
        if already_unwinding {
            discard_callback_panic(payload);
        } else {
            resume_unwind(payload);
        }
    }
}

/// Owner-local registration metadata for one or more byte-event kinds.
///
/// `fire` is a completion seam for a later transport/session owner. It does
/// not schedule transport delivery. Unresolved kinds are cancelled in source
/// order when the registration is explicitly cancelled or dropped.
#[derive(Default)]
pub struct HttpByteEventRegistration {
    stream_id: Option<StreamId>,
    events: u8,
    callback: Option<Weak<dyn HttpByteEventCallback>>,
}

impl HttpByteEventRegistration {
    pub fn new<C>(stream_id: Option<StreamId>, events: u8, callback: &Rc<C>) -> Self
    where
        C: HttpByteEventCallback + 'static,
    {
        let callback: Rc<dyn HttpByteEventCallback> = callback.clone();
        Self {
            stream_id,
            events,
            callback: Some(Rc::downgrade(&callback)),
        }
    }

    pub const fn stream_id(&self) -> Option<StreamId> {
        self.stream_id
    }

    pub(crate) fn set_stream_id_if_missing(&mut self, stream_id: StreamId) {
        self.stream_id.get_or_insert(stream_id);
    }

    pub const fn pending_events(&self) -> u8 {
        self.events
    }

    pub fn callback_is_alive(&self) -> bool {
        self.callback
            .as_ref()
            .is_some_and(|callback| callback.strong_count() != 0)
    }

    /// Splits this registration into independently owned single-kind registrations.
    ///
    /// The returned registrations retain the original event order, stream ID,
    /// and weak callback. Resolving or dropping one kind cannot resolve any of
    /// the other returned kinds.
    pub fn into_event_registrations(mut self) -> Vec<Self> {
        let events = std::mem::take(&mut self.events);
        let callback = self.callback.take();
        ByteEventType::ALL
            .into_iter()
            .filter(|event_type| events & event_type.bit() != 0)
            .map(|event_type| Self {
                stream_id: self.stream_id,
                events: event_type.bit(),
                callback: callback.clone(),
            })
            .collect()
    }

    pub fn fire(&mut self, event: HttpByteEvent) -> bool {
        let event_bit = event.event_type.bit();
        if self.events & event_bit == 0 {
            return false;
        }

        self.events &= !event_bit;
        let callback = if self.events == 0 {
            self.callback.take().and_then(|callback| callback.upgrade())
        } else {
            self.callback.as_ref().and_then(Weak::upgrade)
        };
        if let Some(callback) = callback {
            let already_unwinding = std::thread::panicking();
            let first =
                invoke_and_retire_callback(callback, |callback| callback.on_byte_event(event));
            finish_callback_panic(first, already_unwinding);
        }
        true
    }

    pub fn cancel(&mut self, error: &HttpError, byte_event: Option<&HttpByteEvent>) {
        let cancellation_started_while_panicking = std::thread::panicking();
        let events = std::mem::take(&mut self.events);
        let callback = self.callback.take();
        let Some(callback) = callback else {
            return;
        };
        let mut first_panic = None;

        for event_type in ByteEventType::ALL {
            if events & event_type.bit() == 0 {
                continue;
            }
            let Some(callback) = callback.upgrade() else {
                break;
            };
            let mut event = byte_event
                .cloned()
                .unwrap_or_else(|| cancelled_event(self.stream_id, event_type));
            event.event_type = event_type;
            let attempt = invoke_and_retire_callback(callback, |callback| {
                callback.on_byte_event_cancelled(event, error.clone());
            });
            retain_callback_panic(&mut first_panic, attempt);
        }

        finish_callback_panic(first_panic, cancellation_started_while_panicking);
    }
}

impl Drop for HttpByteEventRegistration {
    fn drop(&mut self) {
        if self.events == 0 {
            return;
        }
        self.cancel(
            &HttpError::with_message(HttpErrorCode::CANCEL, b"ByteEvent registration cancelled"),
            None,
        );
    }
}

const fn cancelled_event(stream_id: Option<StreamId>, event_type: ByteEventType) -> HttpByteEvent {
    HttpByteEvent {
        stream_id,
        field_section_info: None,
        body_offset: 0,
        transport_offset: 0,
        stream_offset: 0,
        eom: false,
        event_type,
    }
}
