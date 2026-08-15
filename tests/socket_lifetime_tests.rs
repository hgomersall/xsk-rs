//! Tests covering the lifetime of the rings handed to libxdp.
//!
//! libxdp does not copy the rings it is passed, it keeps the pointers
//! (`xsk->rx`, `xsk->tx`, `ctx->fill` and `ctx->comp`) and reads them
//! again during teardown to work out which memory to unmap. If a
//! ring has moved or been freed by then, `xsk_socket__delete` unmaps
//! a garbage address. The mapping, and with it the kernel's reference
//! to the socket, is then never released and every subsequent bind to
//! the same device and queue id fails with `EBUSY`.
//!
//! Since it is the *teardown* that is broken, a test only notices if
//! it binds to the same device and queue id a second time, which is
//! what these tests do, in a variety of drop orders.
//!
//! The other half of the story is that a ring must not be unmapped
//! while a queue can still reach it, which is why the fill and comp
//! queues keep their socket alive rather than just the UMEM.

#[allow(dead_code)]
mod setup;

use std::{
    convert::TryInto,
    error::Error,
    fs, io, thread,
    time::{Duration, Instant},
};

use serial_test::serial;
use setup::{VethDevConfig, veth_setup};
use xsk_rs::{
    CompQueue, FillQueue, FrameDesc, RxQueue, Socket, TxQueue, Umem,
    config::{Interface, LibxdpFlags, SocketConfig, UmemConfig},
    socket::SocketCreateError,
};

const FRAME_COUNT: u32 = 64;
const QUEUE_ID: u32 = 0;

/// How long [`assert_can_bind`] and friends wait for a device and
/// queue id to be released.
///
/// Dropping a socket does not free its `(device, queue id)` pair
/// synchronously, so a bind issued immediately after the previous
/// socket went away can still be met with `EBUSY`.
///
/// Waiting does not soften what these tests assert. A ring that
/// libxdp failed to unmap leaves the kernel holding a reference to
/// the socket for good, so the bind being guarded against does not
/// start succeeding however long it is given.
const BIND_TIMEOUT: Duration = Duration::from_secs(2);

/// How long to wait between attempts within [`BIND_TIMEOUT`].
const BIND_RETRY_INTERVAL: Duration = Duration::from_millis(10);

type SocketParts = (TxQueue, RxQueue, Option<(FillQueue, CompQueue)>);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn device_can_be_bound_again_after_socket_dropped() {
    run_with_dev(|if_name| {
        let (umem, _descs) = build_umem();

        let socket = bind_socket(&umem, &if_name).expect("failed to bind first socket");

        drop(socket);
        drop(umem);

        assert_can_bind(&if_name);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn device_can_be_bound_again_when_rx_queue_dropped_before_tx_queue() {
    run_with_dev(|if_name| {
        let (umem, _descs) = build_umem();

        let (tx_q, rx_q, fq_and_cq) = bind_socket(&umem, &if_name).expect("failed to bind socket");

        // The socket is only deleted once the last of its queues is
        // dropped, so dropping the rx queue first releases the rx
        // ring while libxdp still needs it.
        drop(rx_q);
        drop(tx_q);
        drop(fq_and_cq);
        drop(umem);

        assert_can_bind(&if_name);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn device_can_be_bound_again_when_tx_queue_dropped_before_rx_queue() {
    run_with_dev(|if_name| {
        let (umem, _descs) = build_umem();

        let (tx_q, rx_q, fq_and_cq) = bind_socket(&umem, &if_name).expect("failed to bind socket");

        drop(tx_q);
        drop(rx_q);
        drop(fq_and_cq);
        drop(umem);

        assert_can_bind(&if_name);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn device_can_be_bound_again_when_fill_and_comp_queues_dropped_before_socket() {
    run_with_dev(|if_name| {
        let (umem, _descs) = build_umem();

        let (tx_q, rx_q, fq_and_cq) = bind_socket(&umem, &if_name).expect("failed to bind socket");

        // libxdp unmaps the fill and comp rings when the last socket
        // using them is deleted, so they have to survive being
        // dropped here.
        drop(fq_and_cq);
        drop(tx_q);
        drop(rx_q);
        drop(umem);

        assert_can_bind(&if_name);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn fill_and_comp_queues_outliving_their_socket_are_still_usable() {
    run_with_dev(|if_name| {
        let (umem, descs) = build_umem();

        let (tx_q, rx_q, fq_and_cq) = bind_socket(&umem, &if_name).expect("failed to bind socket");

        let (mut fq, mut cq) = fq_and_cq.expect("expected a fill and comp queue");

        // libxdp unmaps a context's fill and comp rings when the last
        // socket using that context is deleted, so the two queues
        // have to keep their socket alive. Were it deleted here, the
        // rings used below would have been unmapped along with it.
        drop(tx_q);
        drop(rx_q);

        assert_cannot_bind(&if_name);

        // SAFETY: the descriptors belong to `umem`, and none of them
        // are in the kernel's hands, this being a fresh socket.
        let produced = unsafe { fq.produce(&descs) };

        assert_eq!(produced, descs.len());

        let mut completed = descs.clone();

        // SAFETY: see above. Nothing has been transmitted, so this
        // only reads the ring, but read or write it faults just the
        // same if the ring is gone.
        let consumed = unsafe { cq.consume(&mut completed) };

        assert_eq!(consumed, 0);

        drop(fq);
        drop(cq);
        drop(umem);

        assert_can_bind(&if_name);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn device_can_be_bound_repeatedly() {
    run_with_dev(|if_name| {
        for i in 0..5 {
            let socket = bind_socket_with_retry(&if_name).unwrap_or_else(|e| {
                panic!("failed to bind socket on iteration {}: {}", i, describe(&e))
            });

            drop(socket);
        }
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn devices_can_be_bound_again_after_sockets_sharing_a_umem_dropped() {
    run_with_dev_pair(|if_name1, if_name2| {
        let (umem, _descs) = build_umem();

        let socket1 = bind_socket(&umem, &if_name1).expect("failed to bind first socket");
        let socket2 = bind_socket(&umem, &if_name2).expect("failed to bind second socket");

        // The two devices get a context each, so each socket has fill
        // and comp rings of its own: the first socket is handed the
        // pair saved when the UMEM was created, the second a freshly
        // allocated pair. Both have to survive until their socket is
        // deleted.
        drop(socket1);
        drop(socket2);
        drop(umem);

        assert_can_bind(&if_name1);
        assert_can_bind(&if_name2);
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn device_can_be_bound_again_after_sockets_sharing_a_context_dropped() {
    run_with_dev(|if_name| {
        let (umem, _descs) = build_umem();

        // Both sockets bind to the same device and queue id, so
        // libxdp puts them on a single context. Only the first is
        // handed the fill and comp rings; the second shares them and
        // gets nothing back.
        let (tx_q1, rx_q1, fq_and_cq) =
            bind_shared_socket(&umem, &if_name).expect("failed to bind first socket");

        assert!(fq_and_cq.is_some());

        let (tx_q2, rx_q2, no_fq_and_cq) =
            bind_shared_socket(&umem, &if_name).expect("failed to bind second socket");

        assert!(no_fq_and_cq.is_none());

        // Drop the socket that owns the rings first. It is the second
        // socket's teardown that drops the context's refcount to zero
        // and unmaps them, so they have to outlive a socket that
        // never saw them.
        drop((tx_q1, rx_q1, fq_and_cq));
        drop((tx_q2, rx_q2));
        drop(umem);

        assert_can_bind(&if_name);
    })
    .await
}

/// Number of create/drop cycles run by
/// [`dropping_a_socket_unmaps_its_rings`].
const CYCLES: usize = 20;

/// A single socket costs ~112 KiB in rx, tx, fill and comp mappings
/// at the default queue sizes, so [`CYCLES`] cycles of creating and
/// dropping sockets would leak a bit over 2 MiB if they weren't being
/// cleaned up. We size the tolerance at roughly four times the growth
/// we'd expect from ordinary allocator churn, to leave some headroom,
/// and a quarter of what we'd expect if sockets were leaking their
/// rings.
const MAPPING_GROWTH_TOLERANCE: u64 = 512 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn dropping_a_socket_unmaps_its_rings() {
    run_with_dev(|if_name| {
        let cycle = |i: usize| {
            let socket = bind_socket_with_retry(&if_name).unwrap_or_else(|e| {
                panic!("failed to bind socket on cycle {}: {}", i, describe(&e))
            });

            drop(socket);
        };

        // Run one cycle before taking the baseline so that any
        // one-off mappings, for example those made when libxdp first
        // loads its XDP program, are already accounted for.
        cycle(0);

        let baseline = mapped_bytes();

        for i in 1..=CYCLES {
            cycle(i);
        }

        let growth = mapped_bytes().saturating_sub(baseline);

        assert!(
            growth < MAPPING_GROWTH_TOLERANCE,
            "mapped memory grew by {} bytes over {} socket create/drop cycles, \
             which suggests the rings are not being unmapped",
            growth,
            CYCLES,
        );
    })
    .await
}

fn build_umem() -> (Umem, Vec<FrameDesc>) {
    Umem::new(
        UmemConfig::default(),
        FRAME_COUNT.try_into().unwrap(),
        false,
    )
    .expect("failed to build umem")
}

fn bind_socket(umem: &Umem, if_name: &Interface) -> Result<SocketParts, SocketCreateError> {
    // SAFETY: no other socket is bound to this device and queue id
    // pair at the point this is called, so there is no need for
    // `XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD`.
    unsafe { Socket::new(SocketConfig::default(), umem, if_name, QUEUE_ID) }
}

/// Binds a socket that may share its device and queue id with one
/// bound earlier.
fn bind_shared_socket(umem: &Umem, if_name: &Interface) -> Result<SocketParts, SocketCreateError> {
    let config = SocketConfig::builder()
        .libxdp_flags(LibxdpFlags::XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD)
        .build();

    // SAFETY: `XSK_LIBXDP_FLAGS_INHIBIT_PROG_LOAD` is set, so no
    // double-free can occur when these sockets are dropped, whatever
    // is already bound to this device and queue id pair.
    unsafe { Socket::new(config, umem, if_name, QUEUE_ID) }
}

/// Binds a fresh socket, on a fresh UMEM, to `if_name`, retrying for
/// up to [`BIND_TIMEOUT`] for as long as the device and queue id are
/// still busy.
fn bind_socket_with_retry(if_name: &Interface) -> Result<SocketParts, SocketCreateError> {
    let start = Instant::now();

    loop {
        let (umem, _descs) = build_umem();

        match bind_socket(&umem, if_name) {
            Ok(socket) => return Ok(socket),
            Err(e) if !is_busy(&e) || start.elapsed() >= BIND_TIMEOUT => return Err(e),
            Err(_) => thread::sleep(BIND_RETRY_INTERVAL),
        }
    }
}

/// Binds a fresh socket, on a fresh UMEM, to `if_name` and drops it
/// again.
///
/// This is the assertion that catches a ring being moved or freed
/// before libxdp is done with it: the previous socket's rings not
/// being unmapped leaves the kernel holding a reference to it, so
/// this bind fails with `EBUSY` and goes on failing.
fn assert_can_bind(if_name: &Interface) {
    if let Err(e) = bind_socket_with_retry(if_name) {
        panic!(
            "failed to bind to device {:?} queue {} within {:?} of dropping the previous socket: {}",
            if_name,
            QUEUE_ID,
            BIND_TIMEOUT,
            describe(&e),
        );
    }
}

/// Asserts that a fresh socket cannot be bound to `if_name`, the
/// kernel rejecting the bind with `EBUSY` for as long as the socket
/// already bound there is alive.
///
/// Unlike [`assert_can_bind`] this gets a single attempt: the socket
/// it is asserting against is deliberately still alive, so there is
/// nothing to wait for.
///
/// The probe inhibits the loading of an XDP program so that a bind it
/// is not expected to win leaves the program attached for `if_name`
/// well alone.
///
/// The error is required to be `EBUSY` so that a bind which failed
/// for an unrelated reason cannot pass the assertion.
fn assert_cannot_bind(if_name: &Interface) {
    let (umem, _descs) = build_umem();

    match bind_shared_socket(&umem, if_name) {
        Ok(_) => panic!(
            "bound a second socket to device {:?} queue {}, so the first was deleted \
             while its fill and comp queues were still alive",
            if_name, QUEUE_ID,
        ),
        Err(e) if !is_busy(&e) => panic!(
            "binding to device {:?} queue {} failed for a reason other than the device \
             being in use: {}",
            if_name,
            QUEUE_ID,
            describe(&e),
        ),
        Err(_) => (),
    }
}

/// Whether the kernel refused a bind because the device and queue id
/// are still in use.
fn is_busy(err: &SocketCreateError) -> bool {
    err.source()
        .and_then(|source| source.downcast_ref::<io::Error>())
        .and_then(io::Error::raw_os_error)
        == Some(libc::EBUSY)
}

fn describe(err: &SocketCreateError) -> String {
    match err.source() {
        Some(source) => format!("{} ({})", err, source),
        None => err.to_string(),
    }
}

/// The total size of the process's memory mappings.
fn mapped_bytes() -> u64 {
    fs::read_to_string("/proc/self/maps")
        .expect("failed to read /proc/self/maps")
        .lines()
        .filter_map(|line| {
            let (start, end) = line.split_whitespace().next()?.split_once('-')?;

            let start = u64::from_str_radix(start, 16).ok()?;
            let end = u64::from_str_radix(end, 16).ok()?;

            Some(end - start)
        })
        .sum()
}

async fn run_with_dev<F>(f: F)
where
    F: FnOnce(Interface) + Send + 'static,
{
    run_with_dev_pair(move |if_name1, _if_name2| f(if_name1)).await
}

async fn run_with_dev_pair<F>(f: F)
where
    F: FnOnce(Interface, Interface) + Send + 'static,
{
    let (dev1_config, dev2_config) = setup::default_veth_dev_configs();

    let inner = move |dev1_config: VethDevConfig, dev2_config: VethDevConfig| {
        f(if_name(&dev1_config), if_name(&dev2_config))
    };

    veth_setup::run_with_veth_pair(inner, dev1_config, dev2_config)
        .await
        .unwrap();
}

fn if_name(config: &VethDevConfig) -> Interface {
    config
        .if_name()
        .parse()
        .expect("failed to parse interface name")
}
