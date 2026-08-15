//! Types for creating and using an AF_XDP [`Socket`].

mod fd;
pub use fd::{Fd, XdpStatistics};

mod rx_queue;
pub use rx_queue::RxQueue;

mod tx_queue;
pub use tx_queue::TxQueue;

use libxdp_sys::xsk_socket;
use std::{
    error::Error,
    fmt, io,
    ptr::{self, NonNull},
    sync::{Arc, Mutex},
};

use crate::{
    config::{Interface, SocketConfig},
    ring::{XskRingCons, XskRingConsHandle, XskRingProd, XskRingProdHandle},
    umem::{CompQueue, CtxRings, FillQueue, Umem},
};

/// An AF_XDP socket, along with everything libxdp dereferences when
/// deleting it.
///
/// `xsk_socket__delete` reads back the rx and tx rings it was handed
/// at creation to work out which memory to unmap, so they have to
/// outlive it - as do the UMEM's fill and comp rings, which the
/// `umem` handle keeps alive. Keeping all of them here rather than
/// beside this struct is what guarantees they do: [`Drop::drop`] runs
/// before any field is dropped, whatever order the fields are written
/// in.
#[derive(Debug)]
struct SocketInner {
    ptr: NonNull<xsk_socket>,
    _rx_ring: XskRingConsHandle,
    _tx_ring: XskRingProdHandle,
    umem: Umem,
}

impl SocketInner {
    /// # Safety
    ///
    /// Only one instance of this struct may exist for `ptr` since it
    /// deletes the socket as part of its [`Drop`] impl. If there are
    /// copies or clones of `ptr` then care must be taken to ensure
    /// they aren't used once this struct goes out of scope, and that
    /// they don't delete the socket themselves.
    ///
    /// `umem` must be the UMEM `ptr` was created from, since that is
    /// what the deletion is serialised on.
    unsafe fn new(
        ptr: NonNull<xsk_socket>,
        rx_ring: XskRingConsHandle,
        tx_ring: XskRingProdHandle,
        umem: Umem,
    ) -> Self {
        Self {
            ptr,
            _rx_ring: rx_ring,
            _tx_ring: tx_ring,
            umem,
        }
    }

    fn as_ptr(&self) -> *mut xsk_socket {
        self.ptr.as_ptr()
    }
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        // SAFETY: unsafe constructor contract guarantees that the
        // socket has not been deleted already and that it was created
        // from this UMEM, and every ring libxdp is about to
        // dereference is a field of this struct, so none of them have
        // been dropped yet.
        unsafe { self.umem.delete_socket(self.as_ptr()) };
    }
}

unsafe impl Send for SocketInner {}

/// An AF_XDP socket.
///
/// More details can be found in the
/// [docs](https://www.kernel.org/doc/html/latest/networking/af_xdp.html)
#[derive(Debug)]
pub struct Socket {
    fd: Fd,
    _inner: Arc<Mutex<SocketInner>>,
}

impl Socket {
    /// Create and bind a new AF_XDP socket to a given interface and
    /// queue id using the underlying UMEM.
    ///
    /// May require root permissions to create successfully.
    ///
    /// Whether you can expect the returned `Option<(FillQueue,
    /// CompQueue)>` to be [`Some`] or [`None`] depends on a couple of
    /// things:
    ///
    ///  1. If the [`Umem`] is currently shared (i.e. being used for
    ///  >=1 AF_XDP sockets elsewhere):
    ///
    ///    - If the `(if_name, queue_id)` pair is not bound to, expect
    ///    [`Some`].
    ///
    ///    - If the `(if_name, queue_id)` pair is bound to, expect
    ///    [`None`] and use the [`FillQueue`] and [`CompQueue`]
    ///    originally returned for this pair.
    ///
    ///  2. If the [`Umem`] is not currently shared, expect [`Some`].
    ///
    /// For further details on using a shared [`Umem`] please see the
    /// [docs](https://www.kernel.org/doc/html/latest/networking/af_xdp.html#xdp-shared-umem-bind-flag).
    ///
    /// Every queue returned here holds a handle to the socket, which
    /// is deleted once the last of them is dropped. That includes the
    /// [`FillQueue`] and [`CompQueue`], as libxdp unmaps their rings
    /// when the last socket created from this [`Umem`] and bound to
    /// this `(if_name, queue_id)` pair goes away, so dropping just the
    /// [`TxQueue`] and [`RxQueue`] will not release the device.
    ///
    /// Where that pair is bound to more than once from this [`Umem`],
    /// the [`FillQueue`] and [`CompQueue`] belong to the socket that
    /// was handed them, and the device is released only once every
    /// socket on the pair has gone, that one included.
    ///
    /// Deleting the socket takes the same [`Umem`] lock that creating
    /// one does, so dropping the last of its queues can block while
    /// another thread creates or drops a socket on that [`Umem`].
    ///
    /// # Safety
    ///
    /// If sharing the [`Umem`] and the `(if_name, queue_id)` pair is
    /// already bound to, then the
    /// [`XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD`] flag must be
    /// set. Otherwise, a double-free may occur when dropping sockets
    /// if the program has already been detached.
    ///
    /// [`XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD`]: crate::config::LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD
    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::type_complexity)]
    pub unsafe fn new(
        config: SocketConfig,
        umem: &Umem,
        if_name: &Interface,
        queue_id: u32,
    ) -> Result<(TxQueue, RxQueue, Option<(FillQueue, CompQueue)>), SocketCreateError> {
        let mut socket_ptr = ptr::null_mut();
        let tx_q = XskRingProd::default();
        let rx_q = XskRingCons::default();

        let rings = umem
            .with_ptr_and_fq_and_cq(|xsk_umem, fq, cq| unsafe {
                libxdp_sys::xsk_socket__create_shared(
                    &mut socket_ptr,
                    if_name.as_cstr().as_ptr(),
                    queue_id,
                    xsk_umem,
                    rx_q.as_ptr(),
                    tx_q.as_ptr(),
                    fq.as_ptr(),
                    cq.as_ptr(),
                    &config.into(),
                )
            })
            .map_err(|err| SocketCreateError {
                reason: "non-zero error code returned when creating AF_XDP socket",
                err: Some(io::Error::from_raw_os_error(-err)),
            })?;

        let inner = match NonNull::new(socket_ptr) {
            Some(init_xsk) => {
                // SAFETY: this is the only `SocketInner` instance for
                // this pointer, and no other pointers to the socket
                // exist.
                unsafe { SocketInner::new(init_xsk, rx_q.handle(), tx_q.handle(), umem.clone()) }
            }
            None => {
                return Err(SocketCreateError {
                    reason: "returned socket pointer was null",
                    err: None,
                });
            }
        };

        let fd = unsafe { libxdp_sys::xsk_socket__fd(inner.as_ptr()) };

        if fd < 0 {
            return Err(SocketCreateError {
                reason: "failed to retrieve AF_XDP socket file descriptor",
                err: None,
            });
        }

        let socket = Socket {
            fd: Fd::new(fd),
            _inner: Arc::new(Mutex::new(inner)),
        };

        let tx_q = if tx_q.is_ring_null() {
            return Err(SocketCreateError {
                reason: "returned tx queue ring is null",
                err: None,
            });
        } else {
            TxQueue::new(tx_q, socket.clone())
        };

        let rx_q = if rx_q.is_ring_null() {
            return Err(SocketCreateError {
                reason: "returned rx queue ring is null",
                err: None,
            });
        } else {
            RxQueue::new(rx_q, socket.clone())
        };

        let fq_and_cq = match rings {
            CtxRings::New(fq, cq) => {
                let fq = FillQueue::new(fq, socket.clone());
                let cq = CompQueue::new(cq, socket);

                Some((fq, cq))
            }
            CtxRings::Existing => None,
            CtxRings::Mismatched => {
                return Err(SocketCreateError {
                    reason: "fill queue xor comp queue ring is null, either both or neither should be non-null",
                    err: None,
                });
            }
        };

        Ok((tx_q, rx_q, fq_and_cq))
    }
}

impl Clone for Socket {
    fn clone(&self) -> Self {
        Self {
            fd: self.fd.clone(),
            _inner: self._inner.clone(),
        }
    }
}

/// Error detailing why [`Socket`] creation failed.
#[derive(Debug)]
pub struct SocketCreateError {
    reason: &'static str,
    err: Option<io::Error>,
}

impl fmt::Display for SocketCreateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)
    }
}

impl Error for SocketCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.err.as_ref().map(|err| err as _)
    }
}
