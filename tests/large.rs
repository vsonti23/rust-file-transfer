//! A 256 MiB transfer that must not hold the file in memory.
//!
//! This lives in its own test binary so the process's peak RSS reflects only
//! this transfer.

mod common;

use std::sync::Arc;

use common::*;
use rft::progress::NoProgress;
use rft::{SendOptions, send_file};

const SIZE: u64 = 256 * 1024 * 1024 + 12_345;

/// Peak resident set size of this process, in bytes (Linux only).
fn peak_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_file_streams_with_bounded_memory() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let server = start_server(test_config(dst.path())).await;
    let path = src.path().join("large.bin");
    let digest = write_random_file(&path, SIZE, 42).await;

    let opts = SendOptions {
        chunk_size: 1024 * 1024,
        streams: 4,
        window: 8,
        ..test_opts()
    };
    let report = send_file(&server.addr_str(), &path, &opts, Arc::new(NoProgress))
        .await
        .expect("send");
    assert_eq!(report.sha256, digest);
    assert_eq!(report.bytes_sent, SIZE);
    assert_eq!(file_sha(&dst.path().join("large.bin")).await, digest);

    if let Some(peak) = peak_rss() {
        eprintln!("peak RSS: {:.1} MiB", peak as f64 / (1024.0 * 1024.0));
        // Client and server run in this one process. Buffers are bounded by
        // streams x chunk size on each side (4 x 1 MiB each), so 64 MiB is a
        // generous ceiling for a 256 MiB file.
        assert!(
            peak < 64 * 1024 * 1024,
            "peak RSS {peak} suggests the file was buffered in memory"
        );
    }
}
