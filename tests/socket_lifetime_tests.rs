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
    config::{Interface, SocketConfig, UmemConfig},
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
