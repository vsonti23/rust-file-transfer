//! Interrupted transfers must resume from what the server already has.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::*;
use rft::progress::NoProgress;
use rft::protocol::ErrorCode;
use rft::{SendOptions, TransferError, send_file};

const CHUNK: u64 = 64 * 1024;

/// Kill the client half-way (drop the send future, which aborts every stream
/// task and closes the sockets), then send again: the second run must skip
/// the chunks the server already stored and produce an identical file.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_client_resumes_from_completed_chunks() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let size = 32 * 1024 * 1024 + 999;
    let path = src.path().join("resume.bin");
    let digest = write_random_file(&path, size, 7).await;
    let opts = SendOptions {
        retries: 0,
        ..test_opts()
    };

    let wire = TripWire::new(size / 3);
    let addr = server.addr_str();
    let send = send_file(&addr, &path, &opts, wire.clone());
    tokio::select! {
        r = send => panic!("transfer finished before interruption: {r:?}"),
        _ = wire.tripped.notified() => {}
    }
    let acked_before = wire.acked.load(Ordering::SeqCst);
    assert!(acked_before < size, "interrupted mid-transfer");
    assert!(
        !dst.path().join("resume.bin").exists(),
        "nothing committed yet"
    );

    // Wait until the server has noticed the disconnect and checkpointed.
    let state = dst.path().join(".rft-partial/resume.bin.state");
    for _ in 0..200 {
        if state.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(state.exists(), "server persisted resume state");

    let report = send_file(&addr, &path, &opts, Arc::new(NoProgress))
        .await
        .expect("resumed send");
    assert!(
        report.resumed_bytes >= acked_before.saturating_sub(CHUNK * 16),
        "resumed {} bytes but {} were acknowledged before the kill",
        report.resumed_bytes,
        acked_before
    );
    assert!(report.resumed_bytes > 0);
    assert_eq!(
        report.resumed_bytes + report.bytes_sent,
        size,
        "second run sent exactly the missing bytes"
    );
    assert_eq!(file_sha(&dst.path().join("resume.bin")).await, digest);
    assert!(partial_entries(dst.path()).is_empty());
}

/// Crash the *server* (no graceful checkpoint), restart it on the same
/// directory, and resume: progress comes from the last on-disk checkpoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashed_server_resumes_from_on_disk_checkpoint() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.checkpoint_every = 8;
    let server = start_server(cfg.clone()).await;
    let size = 24 * 1024 * 1024 + 5;
    let path = src.path().join("crash.bin");
    let digest = write_random_file(&path, size, 11).await;
    let opts = SendOptions {
        retries: 0,
        ..test_opts()
    };

    let wire = TripWire::new(size / 2);
    let addr = server.addr_str();
    let send = send_file(&addr, &path, &opts, wire.clone());
    tokio::select! {
        r = send => panic!("transfer finished before interruption: {r:?}"),
        _ = wire.tripped.notified() => {}
    }
    server.kill().await;

    let server = start_server(cfg).await;
    let report = send_file(&server.addr_str(), &path, &opts, Arc::new(NoProgress))
        .await
        .expect("resumed send");
    // At least one checkpoint (8 chunks) must have happened before half of
    // the file was acknowledged.
    assert!(
        report.resumed_bytes >= 8 * CHUNK,
        "resumed only {} bytes",
        report.resumed_bytes
    );
    assert!(report.resumed_bytes < size);
    assert_eq!(report.resumed_bytes % CHUNK, 0);
    assert_eq!(file_sha(&dst.path().join("crash.bin")).await, digest);
}

/// The server goes away mid-transfer and comes back on the same port while
/// the client is still retrying: the client's backoff loop reconnects and
/// finishes without the caller doing anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_retries_with_backoff_across_server_restart() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.checkpoint_every = 4;
    let server = start_server(cfg.clone()).await;
    let addr = server.addr_str();
    let size = 16 * 1024 * 1024;
    let path = src.path().join("retry.bin");
    let digest = write_random_file(&path, size, 13).await;
    let opts = SendOptions {
        retries: 20,
        backoff_initial: Duration::from_millis(50),
        backoff_max: Duration::from_millis(200),
        ..test_opts()
    };

    let wire = TripWire::new(size / 4);
    let send = tokio::spawn({
        let (addr, path, opts, wire) = (addr.clone(), path.clone(), opts.clone(), wire.clone());
        async move { send_file(&addr, &path, &opts, wire).await }
    });
    wire.tripped.notified().await;
    server.kill().await;
    tokio::time::sleep(Duration::from_millis(300)).await; // client sees refused connections
    let _server = start_server_on(&addr, cfg).await;

    let report = send.await.unwrap().expect("send eventually succeeds");
    assert!(report.attempts >= 2, "attempts = {}", report.attempts);
    assert_eq!(file_sha(&dst.path().join("retry.bin")).await, digest);
}

/// Corrupt the partially received data on disk between the interruption and
/// the resume. Every chunk still "verified" when it arrived, so only the
/// whole-file SHA-256 can catch it: the server must refuse to commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corruption_of_partial_file_is_caught_by_whole_file_hash() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.checkpoint_every = 4;
    let server = start_server(cfg.clone()).await;
    let size = 8 * 1024 * 1024;
    let path = src.path().join("rot.bin");
    write_random_file(&path, size, 17).await;
    let opts = SendOptions {
        retries: 0,
        ..test_opts()
    };

    let wire = TripWire::new(size / 2);
    let addr = server.addr_str();
    let send = send_file(&addr, &path, &opts, wire.clone());
    tokio::select! {
        r = send => panic!("finished early: {r:?}"),
        _ = wire.tripped.notified() => {}
    }
    server.kill().await;

    // Flip a byte inside the first chunk, which the checkpoint says is done.
    let part = dst.path().join(".rft-partial/rot.bin.part");
    let mut bytes = std::fs::read(&part).unwrap();
    bytes[100] ^= 0xFF;
    std::fs::write(&part, &bytes).unwrap();
    drop(bytes);

    let server = start_server(cfg).await;
    let err = send_file(&server.addr_str(), &path, &opts, Arc::new(NoProgress))
        .await
        .expect_err("must not commit corrupted data");
    assert!(
        matches!(
            err,
            TransferError::Rejected {
                code: ErrorCode::ChecksumMismatch,
                ..
            }
        ),
        "{err}"
    );
    assert!(!err.is_retryable());
    assert!(!dst.path().join("rot.bin").exists());
    assert!(
        partial_entries(dst.path()).is_empty(),
        "bad partial data discarded"
    );

    // The next attempt starts from scratch and succeeds.
    let report = send_file(&server.addr_str(), &path, &opts, Arc::new(NoProgress))
        .await
        .unwrap();
    assert_eq!(report.resumed_bytes, 0);
    assert_eq!(file_sha(&dst.path().join("rot.bin")).await, report.sha256);
}

/// A corrupt sidecar state file is ignored (transfer restarts) rather than
/// trusted or treated as fatal.
#[tokio::test]
async fn corrupt_state_file_restarts_transfer() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let path = src.path().join("s.bin");
    let digest = write_random_file(&path, 1_000_000, 3).await;
    std::fs::create_dir_all(dst.path().join(".rft-partial")).unwrap();
    std::fs::write(dst.path().join(".rft-partial/s.bin.state"), b"garbage").unwrap();
    std::fs::write(
        dst.path().join(".rft-partial/s.bin.part"),
        vec![1u8; 1_000_000],
    )
    .unwrap();

    let report = send_file(
        &server.addr_str(),
        &path,
        &test_opts(),
        Arc::new(NoProgress),
    )
    .await
    .unwrap();
    assert_eq!(report.resumed_bytes, 0);
    assert_eq!(file_sha(&dst.path().join("s.bin")).await, digest);
}

/// If the source file changed since the interrupted run, the stale state
/// (different SHA-256) must not be reused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changed_source_file_does_not_reuse_stale_state() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.checkpoint_every = 2;
    let server = start_server(cfg).await;
    let size = 8 * 1024 * 1024;
    let path = src.path().join("v.bin");
    write_random_file(&path, size, 100).await;
    let opts = SendOptions {
        retries: 0,
        ..test_opts()
    };
    let wire = TripWire::new(size / 2);
    let addr = server.addr_str();
    tokio::select! {
        r = send_file(&addr, &path, &opts, wire.clone()) => panic!("early: {r:?}"),
        _ = wire.tripped.notified() => {}
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let new_digest = write_random_file(&path, size, 200).await;
    let report = send_file(&server.addr_str(), &path, &opts, Arc::new(NoProgress))
        .await
        .unwrap();
    assert_eq!(report.resumed_bytes, 0, "state for old content discarded");
    assert_eq!(file_sha(&dst.path().join("v.bin")).await, new_digest);
}
