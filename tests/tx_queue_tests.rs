#[allow(dead_code)]
mod setup;
use std::{convert::TryInto, io::Write};

use setup::{ETHERNET_PACKET, Xsk};

use serial_test::serial;
use xsk_rs::config::{QueueSize, SocketConfig, UmemConfig};

use crate::setup::{PacketGenerator, WAIT_TIMEOUT, XskConfig, wait_until};

const TX_Q_SIZE: u32 = 4;
const FRAME_COUNT: u32 = 8;

fn build_configs() -> (UmemConfig, SocketConfig) {
    let umem_config = UmemConfig::default();

    let socket_config = SocketConfig::builder()
        .tx_queue_size(QueueSize::new(TX_Q_SIZE).unwrap())
        .build();

    (umem_config, socket_config)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn producing_tx_size_frames_is_ok() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.tx_q.produce(&xsk1.descs[..4]) }, 4);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn produce_greater_than_tx_size_frames_fails() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.tx_q.produce(&xsk1.descs[..5]) }, 0);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn produce_frames_until_full() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        unsafe {
            assert_eq!(xsk1.tx_q.produce(&xsk1.descs[..2]), 2);
            assert_eq!(xsk1.tx_q.produce(&xsk1.descs[2..3]), 1);
            assert_eq!(xsk1.tx_q.produce(&xsk1.descs[3..8]), 0);
            assert_eq!(xsk1.tx_q.produce(&xsk1.descs[3..4]), 1);
        }
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn produce_one_is_ok() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(unsafe { xsk1.tx_q.produce_one(&xsk1.descs[0]) }, 1);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn nb_free_exact_on_fresh_queue_is_tx_q_size() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        assert_eq!(xsk1.tx_q.nb_free_exact(), TX_Q_SIZE);
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn nb_free_exact_decreases_as_frames_are_produced() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        unsafe {
            assert_eq!(xsk1.tx_q.nb_free_exact(), 4);

            assert_eq!(xsk1.tx_q.produce(&xsk1.descs[..2]), 2);

            assert_eq!(xsk1.tx_q.nb_free_exact(), 2);

            assert_eq!(xsk1.tx_q.produce(&xsk1.descs[2..4]), 2);

            assert_eq!(xsk1.tx_q.nb_free_exact(), 0);
        }
    }

    build_configs_and_run_test(test).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn nb_free_exact_reflects_completions() {
    fn test(dev1: (Xsk, PacketGenerator), _dev2: (Xsk, PacketGenerator)) {
        let mut xsk1 = dev1.0;

        for i in 0..2 {
            unsafe {
                xsk1.umem
                    .data_mut(&mut xsk1.descs[i])
                    .cursor()
                    .write_all(&ETHERNET_PACKET[..])
                    .unwrap();
            }
        }

        // Nothing is transmitted until the kernel is woken, so the
        // slots the two frames took are still in use here.
        assert_eq!(unsafe { xsk1.tx_q.produce(&xsk1.descs[..2]) }, 2);

        assert_eq!(xsk1.tx_q.nb_free_exact(), 2);

        xsk1.tx_q.wakeup().unwrap();

        wait_until(WAIT_TIMEOUT, || xsk1.tx_q.nb_free_exact() == TX_Q_SIZE);
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
