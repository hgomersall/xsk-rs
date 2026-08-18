#[allow(dead_code)]
mod setup;
use std::{convert::TryInto, io::Write};

use setup::{ETHERNET_PACKET, PacketGenerator, WAIT_TIMEOUT, Xsk, XskConfig, wait_until};

use serial_test::serial;
use xsk_rs::config::{QueueSize, SocketConfig, UmemConfig};

const FQ_SIZE: u32 = 4;
const FRAME_COUNT: u32 = 32;

fn build_configs() -> (UmemConfig, SocketConfig) {
    let umem_config = UmemConfig::builder()
        .fill_queue_size(QueueSize::new(FQ_SIZE).unwrap())
        .build()
        .unwrap();

    let socket_config = SocketConfig::default();

    (umem_config, socket_config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn producing_fq_size_frames_is_ok() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[..4]) }, 4);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn producing_more_than_fq_size_frames_fails() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[..5]) }, 0);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn produce_frames_until_full() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[..2]) }, 2);
        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[2..3]) }, 1);
        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[3..8]) }, 0);
        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[3..4]) }, 1);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn produce_one_is_ok() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.fq.produce_one(&xsk1.descs[0]) }, 1);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn nb_free_exact_on_fresh_queue_is_fq_size() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(xsk1.fq.nb_free_exact(), FQ_SIZE);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn nb_free_exact_decreases_as_frames_are_produced() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(xsk1.fq.nb_free_exact(), 4);

        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[..2]) }, 2);

        assert_eq!(xsk1.fq.nb_free_exact(), 2);

        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[2..4]) }, 2);

        assert_eq!(xsk1.fq.nb_free_exact(), 0);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn nb_free_exact_reflects_frames_taken_by_the_kernel() {
    fn test(dev1: (Xsk, PacketGenerator), dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;
        let mut xsk2 = dev2.0;

        // A slot is deliberately left free. libxdp reloads the real
        // consumer position whenever its cached count falls short of
        // what was asked for, so a ring filled completely reports the
        // kernel's progress however the count is taken, and only a
        // ring with room to spare tells a reload from a cached
        // answer.
        let nb = (FQ_SIZE - 1) as usize;

        assert_eq!(unsafe { xsk1.fq.produce(&xsk1.descs[..nb]) }, nb);

        assert_eq!(xsk1.fq.nb_free_exact(), 1);

        unsafe {
            xsk2.umem
                .data_mut(&mut xsk2.descs[0])
                .cursor()
                .write_all(&ETHERNET_PACKET[..])
                .unwrap();

            assert_eq!(xsk2.tx_q.produce_and_wakeup(&xsk2.descs[..1]).unwrap(), 1);
        }

        // How many entries the kernel takes to receive a packet, and
        // when, is up to it, so only that it takes some is asserted.
        wait_until(WAIT_TIMEOUT, || xsk1.fq.nb_free_exact() > 1);
    }

    build_configs_and_run_test(test).await
}

async fn build_configs_and_run_test<F>(test: F)
where
    F: Fn((Xsk, PacketGenerator), (Xsk, PacketGenerator)) + Send + 'static,
{
    let (dev1_umem_config, dev1_socket_config) = build_configs();
    let (dev2_umem_config, dev2_socket_config) = build_configs();

    setup::run_test(
        XskConfig {
            frame_count: FRAME_COUNT.try_into().unwrap(),
            umem_config: dev1_umem_config,
            socket_config: dev1_socket_config,
        },
        XskConfig {
            frame_count: FRAME_COUNT.try_into().unwrap(),
            umem_config: dev2_umem_config,
            socket_config: dev2_socket_config,
        },
        test,
    )
    .await;
}
