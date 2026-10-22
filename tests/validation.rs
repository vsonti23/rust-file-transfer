//! The server's defenses: per-chunk verification, path traversal, limits,
//! timeouts, and protocol violations. These drive the protocol by hand.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;
use rft::hash::sha256;
use rft::progress::NoProgress;
use rft::protocol::{ErrorCode, Message};
use rft::{SendOptions, TransferError, send_file};

const CS: u32 = 4096;

fn data(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn corrupted_chunk_is_nacked_then_accepted_on_resend() {
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let file = data(CS as usize * 2 + 10, 1);
    let whole = sha256(&file);
    let mut c = RawClient::connect(server.addr).await;
    c.send(&hello("chunky.bin", file.len() as u64, CS, whole))
        .await;
    assert_eq!(c.recv().await, Some(Reply::HelloAck { have: 0, of: 3 }));

    // Chunk 1 with one flipped byte but the hash of the original data.
    let good = &file[CS as usize..2 * CS as usize];
    let mut bad = good.to_vec();
    bad[17] ^= 0x01;
    c.send(&Message::Chunk {
        offset: u64::from(CS),
        sha256: sha256(good),
        data: &bad,
    })
    .await;
    assert_eq!(c.recv().await, Some(Reply::Nack(u64::from(CS))));

    for (i, chunk) in file.chunks(CS as usize).enumerate() {
        let offset = i as u64 * u64::from(CS);
        c.send(&Message::Chunk {
            offset,
            sha256: sha256(chunk),
            data: chunk,
        })
        .await;
        assert_eq!(c.recv().await, Some(Reply::Ack(offset)));
    }
    c.send(&Message::Finish).await;
    assert_eq!(c.recv().await, Some(Reply::FinishAck));
    assert_eq!(std::fs::read(dst.path().join("chunky.bin")).unwrap(), file);
}

#[tokio::test]
async fn wrong_whole_file_hash_is_never_committed() {
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let file = data(10_000, 2);
    let mut c = RawClient::connect(server.addr).await;
    c.send(&hello("lie.bin", file.len() as u64, CS, [0xAA; 32]))
        .await;
    assert!(matches!(c.recv().await, Some(Reply::HelloAck { .. })));
    for (i, chunk) in file.chunks(CS as usize).enumerate() {
        c.send(&Message::Chunk {
            offset: i as u64 * u64::from(CS),
            sha256: sha256(chunk),
            data: chunk,
        })
        .await;
        assert!(matches!(c.recv().await, Some(Reply::Ack(_))));
    }
    c.send(&Message::Finish).await;
    match c.recv().await {
        Some(Reply::Error(ErrorCode::ChecksumMismatch, _)) => {}
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }
    assert!(!dst.path().join("lie.bin").exists());
}

#[tokio::test]
async fn finish_before_all_chunks_is_rejected() {
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let file = data(10_000, 3);
    let mut c = RawClient::connect(server.addr).await;
    c.send(&hello("early.bin", file.len() as u64, CS, sha256(&file)))
        .await;
    c.recv().await;
    c.send(&Message::Finish).await;
    assert!(matches!(
        c.recv().await,
        Some(Reply::Error(ErrorCode::Incomplete, _))
    ));
}

#[tokio::test]
async fn path_traversal_names_are_rejected() {
    let outer = tempfile::tempdir().unwrap();
    let root = outer.path().join("inbox");
    let server = start_server(test_config(&root)).await;
    for name in [
        "../escape.txt",
        "../../etc/passwd",
        "/etc/passwd",
        "sub/dir.txt",
        "..\\windows.txt",
        "..",
        ".",
        "",
        "nul\0.txt",
        ".rft-partial",
    ] {
        let mut c = RawClient::connect(server.addr).await;
        c.send(&hello(name, 5, CS, sha256(b"hello"))).await;
        match c.recv().await {
            Some(Reply::Error(ErrorCode::InvalidName, _)) => {}
            other => panic!("{name:?}: expected InvalidName, got {other:?}"),
        }
        // The server closes the connection after rejecting.
        assert_eq!(c.recv().await, None);
    }
    assert!(!outer.path().join("escape.txt").exists());
    let entries: Vec<_> = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(entries, vec![std::ffi::OsString::from(".rft-partial")]);
}

#[tokio::test]
async fn oversized_files_and_bad_chunk_sizes_are_rejected() {
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.max_file_size = 1_000_000;
    let server = start_server(cfg).await;

    let mut c = RawClient::connect(server.addr).await;
    c.send(&hello("big", 1_000_001, CS, [0; 32])).await;
    assert!(matches!(
        c.recv().await,
        Some(Reply::Error(ErrorCode::FileTooLarge, _))
    ));
    for bad_cs in [0, 1, 4095, 16 * 1024 * 1024 + 1] {
        let mut c = RawClient::connect(server.addr).await;
        c.send(&hello("ok", 10, bad_cs, [0; 32])).await;
        assert!(matches!(
            c.recv().await,
            Some(Reply::Error(ErrorCode::InvalidChunkSize, _))
        ));
    }

    // Through the real client: rejected immediately, not retried.
    let src = tempfile::tempdir().unwrap();
    let p = src.path().join("big");
    write_random_file(&p, 2_000_000, 1).await;
    let started = Instant::now();
    let err = send_file(&server.addr_str(), &p, &test_opts(), Arc::new(NoProgress))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        TransferError::Rejected {
            code: ErrorCode::FileTooLarge,
            ..
        }
    ));
    assert!(started.elapsed() < Duration::from_secs(2), "not retried");
}

#[tokio::test]
async fn misaligned_or_out_of_range_chunks_are_protocol_violations() {
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let file = data(10_000, 4);
    let cases: [(u64, usize); 4] = [
        (1, CS as usize),                 // misaligned
        (3 * u64::from(CS), 10),          // past the last chunk
        (0, CS as usize - 1),             // wrong length for a full chunk
        (2 * u64::from(CS), CS as usize), // last chunk is shorter than CS
    ];
    for (offset, len) in cases {
        let mut c = RawClient::connect(server.addr).await;
        c.send(&hello("v.bin", file.len() as u64, CS, sha256(&file)))
            .await;
        c.recv().await;
        let payload = vec![0u8; len];
        c.send(&Message::Chunk {
            offset,
            sha256: sha256(&payload),
            data: &payload,
        })
        .await;
        assert!(
            matches!(
                c.recv().await,
                Some(Reply::Error(ErrorCode::ProtocolViolation, _))
            ),
            "offset {offset} len {len}"
        );
    }
}

#[tokio::test]
async fn first_message_must_be_hello_and_version_must_match() {
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let mut c = RawClient::connect(server.addr).await;
    c.send(&Message::Finish).await;
    assert!(matches!(
        c.recv().await,
        Some(Reply::Error(ErrorCode::ProtocolViolation, _))
    ));

    let mut c = RawClient::connect(server.addr).await;
    c.send(&Message::Hello(rft::protocol::Hello {
        version: 99,
        file_name: "x".into(),
        file_size: 1,
        chunk_size: CS,
        sha256: [0; 32],
    }))
    .await;
    assert!(matches!(
        c.recv().await,
        Some(Reply::Error(ErrorCode::UnsupportedVersion, _))
    ));
}

#[tokio::test]
async fn idle_connections_time_out() {
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.io_timeout = Duration::from_millis(300);
    let server = start_server(cfg).await;

    // Silent before the handshake.
    let mut c = RawClient::connect(server.addr).await;
    let started = Instant::now();
    assert_eq!(c.recv().await, None);
    assert!(started.elapsed() >= Duration::from_millis(250));

    // Silent after the handshake.
    let mut c = RawClient::connect(server.addr).await;
    c.send(&hello("slow", 10_000, CS, [0; 32])).await;
    assert!(matches!(c.recv().await, Some(Reply::HelloAck { .. })));
    let started = Instant::now();
    assert_eq!(c.recv().await, None);
    assert!(started.elapsed() >= Duration::from_millis(250));
}

#[tokio::test]
async fn conflicting_concurrent_transfer_with_same_name_is_rejected() {
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let mut a = RawClient::connect(server.addr).await;
    a.send(&hello("same", 10_000, CS, [1; 32])).await;
    assert!(matches!(a.recv().await, Some(Reply::HelloAck { .. })));
    let mut b = RawClient::connect(server.addr).await;
    b.send(&hello("same", 20_000, CS, [2; 32])).await;
    assert!(matches!(
        b.recv().await,
        Some(Reply::Error(ErrorCode::Conflict, _))
    ));
    // Conflict is transient (the other sender may finish), so it is retryable.
    assert!(
        TransferError::Rejected {
            code: ErrorCode::Conflict,
            message: String::new()
        }
        .is_retryable()
    );
}

#[tokio::test]
async fn client_gives_up_after_retries_when_server_is_down() {
    // Grab a free port, then close the listener so nothing is listening.
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap()
    };
    let src = tempfile::tempdir().unwrap();
    let p = src.path().join("f");
    write_random_file(&p, 1000, 1).await;
    let opts = SendOptions {
        retries: 3,
        backoff_initial: Duration::from_millis(10),
        backoff_max: Duration::from_millis(40),
        ..test_opts()
    };
    let wire = Arc::new(RetryCounter::default());
    let err = send_file(&addr.to_string(), &p, &opts, wire.clone())
        .await
        .unwrap_err();
    assert!(err.is_retryable(), "{err}");
    assert_eq!(wire.0.load(std::sync::atomic::Ordering::SeqCst), 3);
}

#[derive(Default)]
struct RetryCounter(std::sync::atomic::AtomicU32);

impl rft::progress::Progress for RetryCounter {
    fn retry(&self, _: &str, _: u32, _: &TransferError) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
