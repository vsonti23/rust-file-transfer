//! End-to-end transfers of files of various shapes, alone and concurrently.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use rft::progress::NoProgress;
use rft::protocol::ErrorCode;
use rft::{SendOptions, TransferError, send_file, send_files};

async fn roundtrip(size: u64, opts: SendOptions) {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let path = src.path().join(format!("file-{size}.bin"));
    let digest = write_random_file(&path, size, size).await;

    let report = send_file(&server.addr_str(), &path, &opts, Arc::new(NoProgress))
        .await
        .expect("send");
    assert_eq!(report.file_size, size);
    assert_eq!(report.sha256, digest);
    assert_eq!(report.resumed_bytes, 0);
    assert_eq!(report.bytes_sent, size, "no chunk should be sent twice");
    assert_eq!(report.attempts, 1);

    let received = dst.path().join(path.file_name().unwrap());
    assert_eq!(std::fs::metadata(&received).unwrap().len(), size);
    assert_eq!(file_sha(&received).await, digest);
    assert!(
        partial_entries(dst.path()).is_empty(),
        "no .part/.state left behind"
    );
}

#[tokio::test]
async fn empty_file() {
    roundtrip(0, test_opts()).await;
}

#[tokio::test]
async fn one_byte_file() {
    roundtrip(1, test_opts()).await;
}

#[tokio::test]
async fn file_smaller_than_a_chunk() {
    roundtrip(10_000, test_opts()).await;
}

#[tokio::test]
async fn file_exactly_n_chunks() {
    roundtrip(16 * 64 * 1024, test_opts()).await;
}

#[tokio::test]
async fn file_with_short_last_chunk() {
    roundtrip(16 * 64 * 1024 + 123, test_opts()).await;
}

#[tokio::test]
async fn single_stream_window_of_one() {
    let opts = SendOptions {
        streams: 1,
        window: 1,
        ..test_opts()
    };
    roundtrip(3 * 1024 * 1024 + 7, opts).await;
}

#[tokio::test]
async fn many_streams_wide_window() {
    let opts = SendOptions {
        streams: 8,
        window: 32,
        ..test_opts()
    };
    roundtrip(20 * 1024 * 1024 + 1, opts).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_multi_file_transfer() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;

    let sizes = [
        0u64,
        1,
        4096,
        65_536,
        1_000_000,
        3_333_333,
        8 << 20,
        12 << 20,
    ];
    let mut paths = Vec::new();
    let mut digests = Vec::new();
    for (i, &size) in sizes.iter().enumerate() {
        let p = src.path().join(format!("f{i}.dat"));
        digests.push(write_random_file(&p, size, 1000 + i as u64).await);
        paths.push(p);
    }
    let opts = SendOptions {
        max_files: 4,
        streams: 3,
        ..test_opts()
    };
    let results = send_files(&server.addr_str(), &paths, &opts, Arc::new(NoProgress)).await;
    assert_eq!(results.len(), sizes.len());
    for (i, (path, r)) in results.iter().enumerate() {
        assert_eq!(path, &paths[i], "results are in input order");
        let rep = r.as_ref().expect("transfer succeeded");
        assert_eq!(rep.sha256, digests[i]);
        let got = file_sha(&dst.path().join(format!("f{i}.dat"))).await;
        assert_eq!(got, digests[i], "file {i} content");
    }
    assert!(partial_entries(dst.path()).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn completes_when_server_connection_limit_is_below_demand() {
    // 6 files x 4 streams want 24 connections; the server allows 2. Extra
    // connections wait in the accept backlog, and idle streams give up once
    // all chunks are assigned, so everything still completes.
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let mut cfg = test_config(dst.path());
    cfg.max_connections = 2;
    let server = start_server(cfg).await;

    let mut paths = Vec::new();
    let mut digests = Vec::new();
    for i in 0..6u64 {
        let p = src.path().join(format!("g{i}"));
        digests.push(write_random_file(&p, 2_000_000 + i * 77_777, i).await);
        paths.push(p);
    }
    let opts = SendOptions {
        max_files: 6,
        streams: 4,
        ..test_opts()
    };
    let results = tokio::time::timeout(
        Duration::from_secs(60),
        send_files(&server.addr_str(), &paths, &opts, Arc::new(NoProgress)),
    )
    .await
    .expect("no deadlock under connection limit");
    for (i, (_, r)) in results.iter().enumerate() {
        assert_eq!(r.as_ref().expect("ok").sha256, digests[i]);
    }
}

#[tokio::test]
async fn duplicate_names_in_one_batch_are_rejected_locally() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    std::fs::create_dir(src.path().join("a")).unwrap();
    std::fs::create_dir(src.path().join("b")).unwrap();
    let p1 = src.path().join("a/same.txt");
    let p2 = src.path().join("b/same.txt");
    write_random_file(&p1, 100, 1).await;
    write_random_file(&p2, 200, 2).await;
    let results = send_files(
        &server.addr_str(),
        &[p1, p2],
        &test_opts(),
        Arc::new(NoProgress),
    )
    .await;
    assert!(results[0].1.is_ok());
    assert!(matches!(results[1].1, Err(TransferError::InvalidInput(_))));
}

#[tokio::test]
async fn existing_file_is_not_overwritten() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let p = src.path().join("keep.txt");
    write_random_file(&p, 5000, 9).await;
    send_file(&server.addr_str(), &p, &test_opts(), Arc::new(NoProgress))
        .await
        .unwrap();
    let err = send_file(&server.addr_str(), &p, &test_opts(), Arc::new(NoProgress))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            TransferError::Rejected {
                code: ErrorCode::AlreadyExists,
                ..
            }
        ),
        "{err}"
    );
}

#[tokio::test]
async fn sending_a_directory_or_missing_file_fails_cleanly() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let err = send_file(
        &server.addr_str(),
        src.path(),
        &test_opts(),
        Arc::new(NoProgress),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::InvalidInput(_)));
    let err = send_file(
        &server.addr_str(),
        &src.path().join("missing"),
        &test_opts(),
        Arc::new(NoProgress),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, TransferError::Io(_)));
}
