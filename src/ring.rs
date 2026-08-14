//! Wrappers around the producer and consumer rings used by libxdp.
//!
//! libxdp does not copy the rings it is passed when creating a UMEM
//! or a socket, it retains the pointers - `xsk->rx`, `xsk->tx`,
//! `ctx->fill`, `ctx->comp` and `umem->fill_save` / `umem->comp_save`
//! - and dereferences them again during teardown to work out which
//! memory to unmap. A ring must therefore neither move nor be freed
//! until libxdp is done with it, which is why the rings here are heap
//! allocated and reference counted.
//!
//! Since the type that owns the socket or UMEM needs to keep the
//! rings alive but has no business reading them, sharing is done via
//! the access-free handles returned by `handle`.

use std::{cell::UnsafeCell, fmt, ptr, sync::Arc};

use libxdp_sys::{xsk_ring_cons, xsk_ring_prod};

/// A consumer ring.
///
/// The ring is reachable only as a raw pointer, through [`as_ptr`]:
/// libxdp's ring functions take pointers and nothing else reads or
/// writes one, so no reference to a ring is ever created and the
/// pointer libxdp holds for the life of the socket or UMEM is never
/// invalidated. The [`UnsafeCell`] is what makes writing through a
/// pointer derived from a shared borrow sound.
///
/// What the wrapper provides is exclusion. It is deliberately not
/// [`Clone`] and the handles returned by [`handle`] grant no access,
/// so it is the only way to reach the ring, and since [`as_ptr`]
/// takes `&self` whatever holds a wrapper has to go on taking `&mut
/// self` for calls that write to the ring.
///
/// [`as_ptr`]: Self::as_ptr
/// [`handle`]: Self::handle
/// [`UnsafeCell`]: std::cell::UnsafeCell
pub(crate) struct XskRingCons(Arc<UnsafeCell<xsk_ring_cons>>);

impl XskRingCons {
    /// A handle that keeps this ring's memory alive but grants no
    /// access to it.
    ///
    /// Should be held by whatever owns the socket or UMEM this ring
    /// was passed to, to guarantee the ring outlives libxdp's use of
    /// it.
    pub(crate) fn handle(&self) -> XskRingConsHandle {
        XskRingConsHandle(Arc::clone(&self.0))
    }

    /// A pointer to the ring, for handing to libxdp.
    ///
    /// Writeable, so see this type's docs on which borrow to take.
    pub(crate) fn as_ptr(&self) -> *mut xsk_ring_cons {
        self.0.get()
    }

    pub(crate) fn is_ring_null(&self) -> bool {
        // SAFETY: the ring is initialised and libxdp is not touching
        // it. Read through the pointer rather than through a
        // reference, so that no borrow of the ring is created.
        unsafe { (*self.0.get()).ring.is_null() }
    }
}

impl Default for XskRingCons {
    // `Arc` rather than `Rc`, as what is shared across threads here
    // is the refcount, not the ring: a queue holding the wrapper can
    // be sent to another thread while the handle keeping the same
    // allocation alive stays behind on this one, so the count has to
    // be atomic.
    #[allow(clippy::arc_with_non_send_sync)]
    fn default() -> Self {
        Self(Arc::new(UnsafeCell::new(xsk_ring_cons {
            cached_prod: 0,
            cached_cons: 0,
            mask: 0,
            size: 0,
            producer: ptr::null_mut(),
            consumer: ptr::null_mut(),
            ring: ptr::null_mut(),
            flags: ptr::null_mut(),
        })))
    }
}

impl fmt::Debug for XskRingCons {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: see `is_ring_null`. Copied out so that what gets
        // borrowed for formatting is the copy and not the ring.
        let ring = unsafe { *self.0.get() };

        f.debug_tuple("XskRingCons").field(&ring).finish()
    }
}

// SAFETY: the ring is only reached through this wrapper, which cannot
// be duplicated, so it is never reached from two threads at once.
// libxdp reaches it on setup and teardown, and a teardown can happen
// on a different thread from the last use, but never concurrently
// with one: a teardown runs on the thread holding the last handle to
// whatever libxdp stored the ring on, and by then the wrapper is
// either gone or reachable only from that same thread.
//
// The ring also outlives every such teardown, since everything libxdp
// stored it on holds a handle, or the wrapper itself, for its life.
//
// Ordering between two such teardowns runs through libxdp's own
// unsynchronised refcounts, which is why a socket is deleted under
// the same UMEM lock its creation takes.
unsafe impl Send for XskRingCons {}

/// Keeps an [`XskRingCons`]'s memory alive without granting any
/// access to it.
pub(crate) struct XskRingConsHandle(Arc<UnsafeCell<xsk_ring_cons>>);

impl fmt::Debug for XskRingConsHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The address, not the contents. A handle is access-free, and
        // reading the ring through one would be reaching around the
        // wrapper that serialises access to it.
        f.debug_tuple("XskRingConsHandle")
            .field(&self.0.get())
            .finish()
    }
}

// SAFETY: this handle grants no access to the ring it keeps alive.
unsafe impl Send for XskRingConsHandle {}

/// A producer ring.
///
/// See [`XskRingCons`].
pub(crate) struct XskRingProd(Arc<UnsafeCell<xsk_ring_prod>>);

impl XskRingProd {
    /// A handle that keeps this ring's memory alive but grants no
    /// access to it.
    ///
    /// See [`XskRingCons::handle`].
    pub(crate) fn handle(&self) -> XskRingProdHandle {
        XskRingProdHandle(Arc::clone(&self.0))
    }

    /// A pointer to the ring, for handing to libxdp.
    ///
    /// See [`XskRingCons::as_ptr`].
    pub(crate) fn as_ptr(&self) -> *mut xsk_ring_prod {
        self.0.get()
    }

    pub(crate) fn is_ring_null(&self) -> bool {
        // SAFETY: see `XskRingCons::is_ring_null`.
        unsafe { (*self.0.get()).ring.is_null() }
    }
}

impl Default for XskRingProd {
    // See the impl for `XskRingCons`.
    #[allow(clippy::arc_with_non_send_sync)]
    fn default() -> Self {
        Self(Arc::new(UnsafeCell::new(xsk_ring_prod {
            cached_prod: 0,
            cached_cons: 0,
            mask: 0,
            size: 0,
            producer: ptr::null_mut(),
            consumer: ptr::null_mut(),
            ring: ptr::null_mut(),
            flags: ptr::null_mut(),
        })))
    }
}

impl fmt::Debug for XskRingProd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // See the impl for `XskRingCons`.
        let ring = unsafe { *self.0.get() };

        f.debug_tuple("XskRingProd").field(&ring).finish()
    }
}

// SAFETY: see the impl for `XskRingCons`.
unsafe impl Send for XskRingProd {}

/// Keeps an [`XskRingProd`]'s memory alive without granting any
/// access to it.
pub(crate) struct XskRingProdHandle(Arc<UnsafeCell<xsk_ring_prod>>);

impl fmt::Debug for XskRingProdHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // See the impl for `XskRingConsHandle`.
        f.debug_tuple("XskRingProdHandle")
            .field(&self.0.get())
            .finish()
    }
}

// SAFETY: this handle grants no access to the ring it keeps alive.
unsafe impl Send for XskRingProdHandle {}
