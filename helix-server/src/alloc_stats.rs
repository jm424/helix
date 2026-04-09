//! Allocator statistics endpoint.
//!
//! When started with `--alloc-stats-port`, the server accepts TCP connections
//! and responds with a single line of jemalloc stats then closes:
//!
//! ```text
//! allocated=N active=N resident=N retained=N
//! ```
//!
//! - `allocated`: bytes currently held by live allocations. The authoritative
//!   answer to "how much memory is the application actually using?", unaffected
//!   by OS page-reclaim timing or allocator free-list headroom.
//! - `active`: bytes in jemalloc active pages (allocated + alignment rounding).
//! - `resident`: jemalloc's view of resident bytes, analogous to RSS but
//!   accounting for pages jemalloc has `madvise`'d back to the OS.
//! - `retained`: bytes jemalloc has retained from the OS after freeing (near
//!   zero with aggressive `madvise`; represents mapped but unused address space).
//!
//! The gap `rss - allocated` (measured externally via `ps`) reveals how much
//! of RSS is allocator overhead rather than live Rust objects.
//!
//! For callsite attribution ("which data structure holds those bytes?"), enable
//! jemalloc heap profiling at startup:
//! ```text
//! MALLOC_CONF=prof:true,prof_prefix:/tmp/helix.heap ./helix-server ...
//! ```
//! Then analyze the resulting `.heap` files with `jeprof`.

use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpListener;

/// Spawns the allocator stats server on `127.0.0.1:{port}`.
///
/// Fire-and-forget: the task runs for the process lifetime. If the listener
/// fails to bind, an error is logged but the server continues running.
pub fn spawn(port: u16) {
    tokio::spawn(async move {
        if let Err(e) = run(port).await {
            tracing::error!(error = %e, port, "alloc stats server error");
        }
    });
}

async fn run(port: u16) -> std::io::Result<()> {
    // Bind to all interfaces so the endpoint is reachable via kubectl port-forward.
    // The port is not exposed through any K8s Service, so it is only accessible
    // within the cluster or via an explicit port-forward tunnel.
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(port, "alloc stats server listening");
    loop {
        let (mut socket, _peer) = listener.accept().await?;
        tokio::spawn(async move {
            let line = sample();
            let _ = socket.write_all(line.as_bytes()).await;
        });
    }
}

/// Returns the current jemalloc `allocated` counter in bytes.
///
/// Advances the epoch first to flush per-thread caches. Used by WAL
/// recovery to measure memory growth across the replay pass.
///
/// # Panics
///
/// Panics if jemalloc epoch MIB lookup or advance fails.
#[must_use]
pub fn sample_allocated() -> usize {
    use tikv_jemalloc_ctl::{epoch, stats};
    let epoch_mib = epoch::mib().expect("jemalloc epoch mib");
    epoch_mib.advance().expect("jemalloc epoch advance");
    stats::allocated::read().unwrap_or(0)
}

/// Samples jemalloc counters after advancing the epoch.
///
/// Advancing the epoch flushes per-thread cache counters into the global
/// stats, ensuring `allocated` reflects allocations on all threads.
fn sample() -> String {
    use tikv_jemalloc_ctl::{epoch, stats};
    let epoch_mib = epoch::mib().expect("jemalloc epoch mib");
    epoch_mib.advance().expect("jemalloc epoch advance");

    let allocated = stats::allocated::read().unwrap_or(0);
    let active = stats::active::read().unwrap_or(0);
    let resident = stats::resident::read().unwrap_or(0);
    let retained = stats::retained::read().unwrap_or(0);

    format!("allocated={allocated} active={active} resident={resident} retained={retained}\n")
}
