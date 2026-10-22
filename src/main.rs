//! `rft` command-line interface.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use rft::progress::{NoProgress, Progress};
use rft::{SendOptions, Server, ServerConfig, TransferError};
use tracing_subscriber::EnvFilter;

/// Concurrent, resumable, SHA-256-verified file transfer over TCP.
#[derive(Debug, Parser)]
#[command(name = "rft", version, about)]
struct Cli {
    /// Increase log verbosity (-v debug, -vv trace). `RUST_LOG` overrides.
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Receive files into a directory.
    Serve(ServeArgs),
    /// Send one or more files to a server.
    Send(SendArgs),
    /// Print the SHA-256 of a file (streamed, constant memory).
    Hash {
        /// File to hash.
        file: PathBuf,
    },
}

#[derive(Debug, Args)]
struct ServeArgs {
    /// Directory to store received files in (created if missing).
    #[arg(short, long, default_value = ".")]
    dir: PathBuf,
    /// Address to listen on.
    #[arg(short, long, default_value = "0.0.0.0:7878")]
    listen: String,
    /// Maximum simultaneous connections.
    #[arg(long, default_value_t = 64)]
    max_connections: usize,
    /// Largest accepted file (e.g. 500M, 10G, 1T).
    #[arg(long, default_value = "1T", value_parser = parse_size)]
    max_file_size: u64,
    /// Idle timeout per connection, in seconds.
    #[arg(long, default_value_t = 30)]
    timeout: u64,
    /// Persist resume state every N completed chunks.
    #[arg(long, default_value_t = 64)]
    checkpoint_every: u32,
}

#[derive(Debug, Args)]
struct SendArgs {
    /// Files to send.
    #[arg(required = true)]
    files: Vec<PathBuf>,
    /// Server address, `host:port`.
    #[arg(short, long)]
    to: String,
    /// Chunk size (e.g. 256K, 1M, 4M).
    #[arg(short, long, default_value = "1M", value_parser = parse_chunk_size)]
    chunk_size: u32,
    /// Parallel TCP streams per file.
    #[arg(short, long, default_value_t = 4)]
    streams: usize,
    /// Files sent concurrently.
    #[arg(short = 'j', long, default_value_t = 4)]
    max_files: usize,
    /// Unacknowledged chunks in flight per stream.
    #[arg(short, long, default_value_t = 8)]
    window: usize,
    /// Retries after connection errors (each retry resumes).
    #[arg(short, long, default_value_t = 5)]
    retries: u32,
    /// Read/write timeout in seconds.
    #[arg(long, default_value_t = 30)]
    timeout: u64,
    /// Disable progress bars.
    #[arg(long)]
    no_progress: bool,
}

/// Parse sizes like `4096`, `64K`, `1M`, `1MiB`, `10G`, `1T` (binary units).
fn parse_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().map_err(|_| format!("invalid size {s:?}"))?;
    let shift = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 0,
        "k" | "kb" | "kib" => 10,
        "m" | "mb" | "mib" => 20,
        "g" | "gb" | "gib" => 30,
        "t" | "tb" | "tib" => 40,
        other => return Err(format!("unknown size unit {other:?}")),
    };
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| format!("size {s:?} overflows"))
}

fn parse_chunk_size(s: &str) -> Result<u32, String> {
    let n = parse_size(s)?;
    u32::try_from(n).map_err(|_| format!("chunk size {s:?} too large"))
}

fn init_tracing(verbose: u8, default: &str) {
    let level = match verbose {
        0 => default,
        1 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// `indicatif` progress bars, one per file.
struct Bars {
    multi: MultiProgress,
    bars: Mutex<HashMap<String, ProgressBar>>,
    style: ProgressStyle,
}

impl Bars {
    fn new() -> Self {
        let style = ProgressStyle::with_template(
            "{prefix:>24.bold} [{bar:30}] {bytes:>10}/{total_bytes:<10} {binary_bytes_per_sec:>12} eta {eta}",
        )
        .expect("valid template")
        .progress_chars("=> ");
        Self {
            multi: MultiProgress::with_draw_target(ProgressDrawTarget::stderr()),
            bars: Mutex::new(HashMap::new()),
            style,
        }
    }
}

impl Progress for Bars {
    fn start(&self, name: &str, total: u64, already: u64) {
        let mut bars = self.bars.lock().unwrap();
        let bar = bars.entry(name.to_owned()).or_insert_with(|| {
            let b = self.multi.add(ProgressBar::new(total));
            b.set_style(self.style.clone());
            b.set_prefix(truncate(name, 24));
            b
        });
        bar.set_length(total);
        bar.set_position(already);
        bar.reset_eta();
    }
    fn advance(&self, name: &str, bytes: u64) {
        if let Some(b) = self.bars.lock().unwrap().get(name) {
            b.inc(bytes);
        }
    }
    fn retry(&self, name: &str, attempt: u32, error: &TransferError) {
        let _ = self.multi.println(format!(
            "{name}: attempt {attempt} failed ({error}); retrying"
        ));
    }
    fn finish(&self, name: &str) {
        if let Some(b) = self.bars.lock().unwrap().get(name) {
            b.finish();
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_owned()
    } else {
        let tail: String = s
            .chars()
            .rev()
            .take(max - 1)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("…{tail}")
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let config = ServerConfig {
        root: args.dir,
        max_file_size: args.max_file_size,
        max_connections: args.max_connections.max(1),
        io_timeout: Duration::from_secs(args.timeout.max(1)),
        checkpoint_every: args.checkpoint_every.max(1),
    };
    let server = Server::bind(&args.listen, config)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    eprintln!("rft listening on {}", server.local_addr()?);
    server
        .run_until(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn send(args: SendArgs) -> Result<()> {
    let opts = SendOptions {
        chunk_size: args.chunk_size,
        streams: args.streams,
        max_files: args.max_files,
        window: args.window,
        retries: args.retries,
        io_timeout: Duration::from_secs(args.timeout.max(1)),
        ..SendOptions::default()
    };
    let progress: Arc<dyn Progress> = if args.no_progress {
        Arc::new(NoProgress)
    } else {
        Arc::new(Bars::new())
    };
    let results = rft::send_files(&args.to, &args.files, &opts, progress).await;
    let mut failed = 0;
    for (path, r) in &results {
        match r {
            Ok(rep) => {
                let secs = rep.elapsed.as_secs_f64().max(1e-9);
                let resumed = if rep.resumed_bytes > 0 {
                    format!(", resumed from {} bytes", rep.resumed_bytes)
                } else {
                    String::new()
                };
                println!(
                    "sent {} ({} bytes, {:.1} MiB/s, {} attempt(s){resumed}) sha256={}",
                    rep.name,
                    rep.file_size,
                    rep.file_size as f64 / secs / (1024.0 * 1024.0),
                    rep.attempts,
                    rft::hash::to_hex(&rep.sha256),
                );
            }
            Err(e) => {
                failed += 1;
                eprintln!("failed {}: {e}", path.display());
            }
        }
    }
    if failed > 0 {
        bail!("{failed} of {} file(s) failed", results.len());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let default_level = match cli.command {
        Command::Serve(_) => "info",
        _ => "warn",
    };
    init_tracing(cli.verbose, default_level);
    let result = match cli.command {
        Command::Serve(args) => serve(args).await,
        Command::Send(args) => send(args).await,
        Command::Hash { file } => rft::hash::sha256_file(&file, None)
            .await
            .with_context(|| format!("hashing {}", file.display()))
            .map(|d| println!("{}  {}", rft::hash::to_hex(&d), file.display())),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_parse() {
        assert_eq!(parse_size("4096"), Ok(4096));
        assert_eq!(parse_size("64K"), Ok(64 * 1024));
        assert_eq!(parse_size("1MiB"), Ok(1 << 20));
        assert_eq!(parse_size("10g"), Ok(10 << 30));
        assert_eq!(parse_size("1T"), Ok(1 << 40));
        assert!(parse_size("1X").is_err());
        assert!(parse_size("M").is_err());
        assert!(parse_size("99999999999T").is_err());
        assert_eq!(
            parse_chunk_size("1M"),
            Ok(rft::protocol::DEFAULT_CHUNK_SIZE)
        );
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
