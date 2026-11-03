//! Limits on server streams, shared by the main server and the cache node.
//!
//! The layers in `scion_api::limit` and [`crate::deadline::DeadlineLayer`] stop at the response
//! headers, so they bound neither the time a server stream stays open nor how many streams one
//! caller opens. [`StreamLimits`] does both, inside the handler, so a stream ends between two messages:
//!
//! - [`StreamLimits::open_with`] takes one slot of the caller's budget (`--tenant-streams`) or fails.
//!   The slot is free again when the [`Ticket`] drops.
//! - [`StreamLimits::bound`] ends the stream with UNAVAILABLE after `--max-stream-lifetime`. The
//!   client SDK retries that status, so a client reconnects and resumes.
//!
//! A client that stops reading stalls the response body, so the timer in [`Bounded`] does not run.
//! The server wraps each connection in [`crate::age::Aged`] as the backstop. It closes the
//! connection after `--max-stream-lifetime` plus a grace and counts the close as `connection_age`.
//!
//! Each hit counts in `scion_rejected_requests_total{reason}`.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::time::Sleep;
use tokio_stream::Stream;
use tonic::Status;

use crate::reason;

/// Open streams per tenant key.
type Open = Arc<Mutex<HashMap<String, usize>>>;

/// How long a stream stays open, and how many streams one caller keeps open.
#[derive(Clone, Default)]
pub struct StreamLimits {
    lifetime: Option<Duration>,
    budget: Option<(usize, Open)>,
}

/// One open slot of a caller's stream budget. Dropping it frees the slot.
pub struct Ticket {
    slot: Option<(String, Open)>,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let Some((key, open)) = self.slot.take() else { return };
        let mut open = open.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = open.get_mut(&key) {
            *n -= 1;
            if *n == 0 {
                open.remove(&key);
            }
        }
    }
}

impl StreamLimits {
    /// `lifetime` of `None` or zero leaves streams open as long as the client wants.
    /// `per_tenant` of `None` sets no budget.
    pub fn new(lifetime: Option<Duration>, per_tenant: Option<usize>) -> Self {
        Self {
            lifetime: lifetime.filter(|d| !d.is_zero()),
            budget: per_tenant.map(|n| (n.max(1), Arc::default())),
        }
    }

    /// Takes a slot of the budget of `caller`, or returns `refuse(caller)`. Call it after the
    /// handler authorizes the request, so a refused call takes no slot.
    pub fn open_with(&self, caller: &str, refuse: impl FnOnce(&str) -> Status) -> Result<Ticket, Status> {
        let Some((max, open)) = &self.budget else {
            return Ok(Ticket { slot: None });
        };
        let key = caller.to_owned();
        let mut map = open.lock().unwrap_or_else(|e| e.into_inner());
        let n = map.entry(key.clone()).or_insert(0);
        if *n >= *max {
            crate::metrics().reject(reason::TENANT_STREAMS);
            return Err(refuse(&key));
        }
        *n += 1;
        Ok(Ticket {
            slot: Some((key, open.clone())),
        })
    }

    /// Ends `stream` with UNAVAILABLE when the lifetime is over. The ticket lives as long as the
    /// stream does.
    pub fn bound<T, S>(&self, ticket: Ticket, stream: S) -> Bounded<S>
    where
        S: Stream<Item = Result<T, Status>> + Unpin,
    {
        // ponytail: every stream gets the same lifetime. Clients that start together reconnect
        // together. Add jitter if reconnect herds show in scion_rpc_requests_total.
        Bounded {
            stream,
            deadline: self.lifetime.map(|d| (Box::pin(tokio::time::sleep(d)), d)),
            expired: false,
            _ticket: ticket,
        }
    }
}

/// A stream that ends with UNAVAILABLE at its deadline. See [`StreamLimits::bound`].
pub struct Bounded<S> {
    stream: S,
    deadline: Option<(Pin<Box<Sleep>>, Duration)>,
    expired: bool,
    _ticket: Ticket,
}

impl<T, S> Stream for Bounded<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.expired {
            return Poll::Ready(None);
        }
        // The deadline goes first. A stream of ready items (a finite answer) also ends at its
        // lifetime, and the sleep always has our waker. The UNAVAILABLE status makes the client retry.
        if let Some((sleep, lifetime)) = &mut this.deadline {
            if sleep.as_mut().poll(cx).is_ready() {
                let lifetime = *lifetime;
                this.expired = true;
                crate::metrics().reject(reason::STREAM_LIFETIME);
                // Not a clean end: that would silently truncate a stream with a finite answer.
                // No ErrorInfo reason, so the SDK retries on the code alone.
                return Poll::Ready(Some(Err(Status::unavailable(format!(
                    "stream lifetime of {}s reached; reconnect",
                    lifetime.as_secs()
                )))));
            }
        }
        Pin::new(&mut this.stream).poll_next(cx)
    }
}
