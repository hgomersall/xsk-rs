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
/// # Invariant
///
/// On the Rust side a ring is only ever accessed through the single
/// `XskRingCons` that wraps it. This type is deliberately not
/// [`Clone`] and the handles returned by [`handle`] grant no access
/// to the ring, so handing out references to it here is sound.
///
/// libxdp holds the pointer too and dereferences it when setting up
/// and tearing down the socket or UMEM it was passed to, but never
/// while the wrapper is in use: the wrapper is not handed out until
/// setup has returned, and the rx and tx queues that hold one keep
/// their socket alive.
///
/// [`handle`]: Self::handle
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
    pub(crate) fn as_ptr(&self) -> *mut xsk_ring_cons {
        self.0.get()
    }

    pub(crate) fn as_mut(&mut self) -> &mut xsk_ring_cons {
        // SAFETY: see the invariant on this type. The `&mut self`
        // borrow rules out any other reference derived from this
        // wrapper, and libxdp is not touching the ring for as long as
        // the wrapper exists.
        unsafe { &mut *self.0.get() }
    }

    pub(crate) fn as_ref(&self) -> &xsk_ring_cons {
        // SAFETY: see the invariant on this type.
        unsafe { &*self.0.get() }
    }

    pub(crate) fn is_ring_null(&self) -> bool {
        self.as_ref().ring.is_null()
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
        f.debug_tuple("XskRingCons").field(self.as_ref()).finish()
    }
}

// SAFETY: the ring is only accessed through this wrapper, which
// cannot be duplicated, so it is never accessed from two threads at
// once, and libxdp only accesses it on setup and teardown.
unsafe impl Send for XskRingCons {}

/// Keeps an [`XskRingCons`]'s memory alive without granting any
/// access to it.
pub(crate) struct XskRingConsHandle(Arc<UnsafeCell<xsk_ring_cons>>);

impl fmt::Debug for XskRingConsHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The address, not the contents: dereferencing here would
        // alias the `&mut` the wrapper hands out.
        f.debug_tuple("XskRingConsHandle")
            .field(&self.0.get())
            .finish()
    }
}

// SAFETY: this handle grants no access to the ring it keeps alive.
unsafe impl Send for XskRingConsHandle {}

/// A producer ring.
///
/// # Invariant
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
    pub(crate) fn as_ptr(&self) -> *mut xsk_ring_prod {
        self.0.get()
    }

    pub(crate) fn as_mut(&mut self) -> &mut xsk_ring_prod {
        // SAFETY: see the invariant on `XskRingCons`. The `&mut self`
        // borrow rules out any other reference derived from this
        // wrapper, and libxdp is not touching the ring for as long as
        // the wrapper exists.
        unsafe { &mut *self.0.get() }
    }

    pub(crate) fn as_ref(&self) -> &xsk_ring_prod {
        // SAFETY: see the invariant on `XskRingCons`.
        unsafe { &*self.0.get() }
    }

    pub(crate) fn is_ring_null(&self) -> bool {
        self.as_ref().ring.is_null()
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
        f.debug_tuple("XskRingProd").field(self.as_ref()).finish()
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
