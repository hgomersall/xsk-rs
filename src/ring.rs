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

/// The number of free slots on a producer ring.
///
/// # Safety
///
/// `ring` must point at an initialised ring, and the caller must
/// have exclusive access to it, as this writes the ring's cached
/// consumer position.
#[inline]
pub(crate) unsafe fn prod_nb_free_exact(ring: *mut xsk_ring_prod) -> u32 {
    // libxdp answers from its cached consumer position whenever that
    // cache already holds as many slots as were asked for, and only
    // reloads the real position otherwise. Asking for the ring size
    // therefore ensures the value returned is always up to date.
    let size = unsafe { (*ring).size };

    unsafe { libxdp_sys::xsk_prod_nb_free(ring, size) }
}

/// The number of entries ready to be consumed on a consumer ring.
///
/// # Safety
///
/// See [`prod_nb_free_exact`]. This writes the ring's cached
/// producer and consumer positions.
#[inline]
pub(crate) unsafe fn cons_nb_avail_exact(ring: *mut xsk_ring_cons) -> u32 {
    let size = unsafe { (*ring).size };

    let mut idx = 0;

    // The first peek bumps cached_cons by the number of frames the
    // cache knew about. The second peek results in a refresh of that
    // cache, and gives us the number of consumable frames the cache
    // did not know about. Together, we have the number of consumable
    // frames at this point in time.
    let n = unsafe {
        libxdp_sys::xsk_ring_cons__peek(ring, size, &mut idx)
            + libxdp_sys::xsk_ring_cons__peek(ring, size, &mut idx)
    };

    // One cancel, after both peeks. Cancelling between them would
    // restore the cache the first peek emptied, so the second would
    // count the same entries again.
    //
    // The cancel does not undo the reload. A following consume
    // sees the current producer position without paying for a
    // reload of its own. Nothing here writes ring memory or the
    // consumer position, and the cancel gives back exactly what
    // the peeks took, so a following consume still hands out
    // every entry from the first.
    unsafe { libxdp_sys::xsk_ring_cons__cancel(ring, n) };

    n
}

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

#[cfg(test)]
mod tests {
    use libxdp_sys::{
        xdp_desc, xsk_ring_cons__peek, xsk_ring_cons__release, xsk_ring_prod__reserve,
        xsk_ring_prod__submit,
    };

    use super::*;

    const SIZE: u32 = 4;

    /// Memory standing in for the pages the kernel maps for a ring.
    ///
    /// Held as raw pointers rather than as boxes. The pointers handed
    /// out below stay in use for as long as the ring holding this
    /// does, and a box asserts exclusive access to what it points at
    /// every time it is moved, which would invalidate every one of
    /// them the moment this is moved into that ring.
    struct RingMem {
        positions: *mut [u32; 2],
        entries: *mut [u64],
    }

    impl RingMem {
        fn new(size: u32) -> Self {
            // An rx or tx ring's entries are `xdp_desc`, wider than
            // the addresses a fill or comp ring holds, so the entry
            // area is sized for the wider of the two.
            let entry_u64s = size_of::<xdp_desc>().div_ceil(size_of::<u64>());

            Self {
                positions: Box::into_raw(Box::new([0; 2])),
                entries: Box::into_raw(vec![0; size as usize * entry_u64s].into_boxed_slice()),
            }
        }

        fn producer(&self) -> *mut u32 {
            self.positions.cast()
        }

        fn consumer(&self) -> *mut u32 {
            // The consumer position sits alongside the producer, so
            // this stays within the allocation the pointer came from.
            unsafe { self.positions.cast::<u32>().add(1) }
        }

        fn entries(&self) -> *mut u64 {
            self.entries.cast()
        }
    }

    impl Drop for RingMem {
        fn drop(&mut self) {
            // SAFETY: both pointers came from `Box::into_raw` in
            // `new`, and nothing else frees them.
            unsafe {
                drop(Box::from_raw(self.positions));
                drop(Box::from_raw(self.entries));
            }
        }
    }

    /// A consumer ring backed by memory the test owns.
    struct FakeCons {
        ring: xsk_ring_cons,
        _mem: RingMem,
    }

    impl FakeCons {
        fn new(size: u32) -> Self {
            let mem = RingMem::new(size);

            // libxdp sets an rx ring's cached positions from the
            // ring's own and leaves a comp ring's zeroed, which on a
            // fresh ring comes to the same thing.
            let ring = xsk_ring_cons {
                cached_prod: 0,
                cached_cons: 0,
                mask: size - 1,
                size,
                producer: mem.producer(),
                consumer: mem.consumer(),
                ring: mem.entries().cast(),
                flags: ptr::null_mut(),
            };

            Self { ring, _mem: mem }
        }

        fn as_ptr(&mut self) -> *mut xsk_ring_cons {
            &mut self.ring
        }

        /// Hand `nb` entries to the ring, as the kernel would.
        fn kernel_produce(&mut self, nb: u32) {
            unsafe { *self.ring.producer += nb };
        }

        fn cached_cons(&self) -> u32 {
            self.ring.cached_cons
        }

        fn consumer(&self) -> u32 {
            unsafe { *self.ring.consumer }
        }
    }

    /// A producer ring backed by memory the test owns.
    ///
    /// See [`FakeCons`].
    struct FakeProd {
        ring: xsk_ring_prod,
        _mem: RingMem,
    }

    impl FakeProd {
        fn new(size: u32) -> Self {
            let mem = RingMem::new(size);

            // libxdp keeps a producer ring's cached consumer
            // position a ring size ahead of the real one, which is
            // the addition `xsk_prod_nb_free` avoids by leaving it
            // there.
            let ring = xsk_ring_prod {
                cached_prod: 0,
                cached_cons: size,
                mask: size - 1,
                size,
                producer: mem.producer(),
                consumer: mem.consumer(),
                ring: mem.entries().cast(),
                flags: ptr::null_mut(),
            };

            Self { ring, _mem: mem }
        }

        fn as_ptr(&mut self) -> *mut xsk_ring_prod {
            &mut self.ring
        }

        /// Take `nb` entries off the ring, as the kernel would.
        fn kernel_consume(&mut self, nb: u32) {
            unsafe { *self.ring.consumer += nb };
        }

        fn cached_prod(&self) -> u32 {
            self.ring.cached_prod
        }
    }

    /// Miri cannot run the tests below, which call into libxdp, so
    /// this stands in for them where `RingMem` is concerned: it holds
    /// the pointers a ring is built from the same way, and writes
    /// through every one of them after the memory has been moved.
    #[test]
    fn ring_mem_pointers_all_stay_usable() {
        struct Ring {
            producer: *mut u32,
            consumer: *mut u32,
            entries: *mut u64,
            _mem: RingMem,
        }

        let mem = RingMem::new(SIZE);

        let ring = Ring {
            producer: mem.producer(),
            consumer: mem.consumer(),
            entries: mem.entries(),
            _mem: mem,
        };

        unsafe {
            for i in 0..SIZE {
                *ring.entries.add(i as usize) = i.into();
            }

            *ring.producer = SIZE;
            *ring.consumer = 1;

            assert_eq!(*ring.producer, SIZE);
            assert_eq!(*ring.consumer, 1);
            assert_eq!(*ring.entries.add(1), 1);
        }
    }

    // These call into libxdp, which Miri cannot execute.

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_is_zero_on_an_empty_ring() {
        let mut cons = FakeCons::new(SIZE);

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 0);
        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_sees_late_arrivals() {
        let mut cons = FakeCons::new(SIZE);

        cons.kernel_produce(1);

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 1);

        cons.kernel_produce(2);

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 3);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_is_idempotent() {
        let mut cons = FakeCons::new(SIZE);

        cons.kernel_produce(2);

        for _ in 0..3 {
            assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 2);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_restores_the_ring_positions() {
        let mut cons = FakeCons::new(SIZE);

        cons.kernel_produce(2);

        let cached_cons = cons.cached_cons();
        let consumer = cons.consumer();

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 2);

        assert_eq!(cons.cached_cons(), cached_cons);
        assert_eq!(cons.consumer(), consumer);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_after_a_partial_consume() {
        let mut cons = FakeCons::new(SIZE);

        cons.kernel_produce(3);

        let mut idx = 0;

        assert_eq!(
            unsafe { xsk_ring_cons__peek(cons.as_ptr(), 1, &mut idx) },
            1
        );

        unsafe { xsk_ring_cons__release(cons.as_ptr(), 1) };

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 2);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_reports_a_full_ring() {
        let mut cons = FakeCons::new(SIZE);

        cons.kernel_produce(SIZE);

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, SIZE);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn cons_nb_avail_exact_leaves_every_entry_for_a_later_peek() {
        let mut cons = FakeCons::new(SIZE);

        cons.kernel_produce(3);

        assert_eq!(unsafe { cons_nb_avail_exact(cons.as_ptr()) }, 3);

        let mut idx = 0;

        assert_eq!(
            unsafe { xsk_ring_cons__peek(cons.as_ptr(), 3, &mut idx) },
            3
        );
        assert_eq!(idx, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn prod_nb_free_exact_is_the_ring_size_on_an_empty_ring() {
        let mut prod = FakeProd::new(SIZE);

        assert_eq!(unsafe { prod_nb_free_exact(prod.as_ptr()) }, SIZE);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn prod_nb_free_exact_is_zero_on_a_full_ring() {
        let mut prod = FakeProd::new(SIZE);

        let mut idx = 0;

        assert_eq!(
            unsafe { xsk_ring_prod__reserve(prod.as_ptr(), SIZE, &mut idx) },
            SIZE
        );

        unsafe { xsk_ring_prod__submit(prod.as_ptr(), SIZE) };

        assert_eq!(unsafe { prod_nb_free_exact(prod.as_ptr()) }, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn prod_nb_free_exact_tracks_reservations() {
        let mut prod = FakeProd::new(SIZE);

        let mut idx = 0;

        assert_eq!(
            unsafe { xsk_ring_prod__reserve(prod.as_ptr(), 2, &mut idx) },
            2
        );

        assert_eq!(unsafe { prod_nb_free_exact(prod.as_ptr()) }, 2);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn prod_nb_free_exact_sees_entries_taken_by_the_kernel() {
        let mut prod = FakeProd::new(SIZE);

        let mut idx = 0;

        assert_eq!(
            unsafe { xsk_ring_prod__reserve(prod.as_ptr(), 2, &mut idx) },
            2
        );

        unsafe { xsk_ring_prod__submit(prod.as_ptr(), 2) };

        prod.kernel_consume(2);

        assert_eq!(unsafe { prod_nb_free_exact(prod.as_ptr()) }, SIZE);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn prod_nb_free_exact_leaves_the_cached_producer_position_alone() {
        let mut prod = FakeProd::new(SIZE);

        let mut idx = 0;

        assert_eq!(
            unsafe { xsk_ring_prod__reserve(prod.as_ptr(), 2, &mut idx) },
            2
        );

        unsafe { xsk_ring_prod__submit(prod.as_ptr(), 2) };

        let cached_prod = prod.cached_prod();

        assert_eq!(unsafe { prod_nb_free_exact(prod.as_ptr()) }, 2);
        assert_eq!(prod.cached_prod(), cached_prod);

        assert_eq!(
            unsafe { xsk_ring_prod__reserve(prod.as_ptr(), 2, &mut idx) },
            2
        );
    }
}
