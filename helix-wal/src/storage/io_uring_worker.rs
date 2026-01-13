//! io_uring worker thread implementation.
//!
//! Provides `Send + Sync` storage types that communicate with a dedicated
//! io_uring worker thread via channels. This bridges tokio_uring's single-threaded
//! model with the multi-threaded tokio runtime.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    Tokio Runtime                        │
//! │  ┌─────────┐  ┌─────────┐  ┌─────────┐                 │
//! │  │ Task 1  │  │ Task 2  │  │ Task 3  │                 │
//! │  └────┬────┘  └────┬────┘  └────┬────┘                 │
//! │       └────────────┼────────────┘                       │
//! │                    │ mpsc channel                       │
//! │                    ▼                                    │
//! │  ┌─────────────────────────────────────────────────┐   │
//! │  │           Worker Thread (tokio_uring)           │   │
//! │  │                                                 │   │
//! │  │  files: HashMap<FileHandle, IoUringFile>       │   │
//! │  │                                                 │   │
//! │  └─────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use super::{Storage, StorageFile};
use crate::error::{WalError, WalResult};

/// Unique identifier for an open file on the worker thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileHandle(u64);

/// Commands sent to the io_uring worker thread.
enum IoCommand {
    // Storage operations
    Open {
        handle: FileHandle, // Pre-allocated by client for routing.
        path: PathBuf,
        reply: oneshot::Sender<WalResult<()>>,
    },
    Exists {
        path: PathBuf,
        reply: oneshot::Sender<WalResult<bool>>,
    },
    ListFiles {
        dir: PathBuf,
        extension: String,
        reply: oneshot::Sender<WalResult<Vec<PathBuf>>>,
    },
    Remove {
        path: PathBuf,
        reply: oneshot::Sender<WalResult<()>>,
    },
    CreateDirAll {
        path: PathBuf,
        reply: oneshot::Sender<WalResult<()>>,
    },

    // StorageFile operations
    WriteAt {
        handle: FileHandle,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<WalResult<()>>,
    },
    /// Combined write + sync in single round trip (reduces channel overhead).
    WriteAtAndSync {
        handle: FileHandle,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<WalResult<()>>,
    },
    ReadAt {
        handle: FileHandle,
        offset: u64,
        len: usize,
        reply: oneshot::Sender<WalResult<Bytes>>,
    },
    ReadAll {
        handle: FileHandle,
        reply: oneshot::Sender<WalResult<Bytes>>,
    },
    Sync {
        handle: FileHandle,
        reply: oneshot::Sender<WalResult<()>>,
    },
    Size {
        handle: FileHandle,
        reply: oneshot::Sender<WalResult<u64>>,
    },
    Truncate {
        handle: FileHandle,
        len: u64,
        reply: oneshot::Sender<WalResult<()>>,
    },
    CloseFile {
        handle: FileHandle,
    },

    // Worker control
    Shutdown,
}

/// Error returned when io_uring initialization fails.
#[derive(Debug, Clone)]
pub struct IoUringInitError {
    message: String,
}

impl std::fmt::Display for IoUringInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "io_uring init failed: {}", self.message)
    }
}

impl std::error::Error for IoUringInitError {}

/// Shared state for a single io_uring worker.
struct SingleWorkerState {
    /// Channel to send commands to the worker.
    command_tx: mpsc::UnboundedSender<IoCommand>,
    /// Handle to the worker thread (for shutdown).
    worker_handle: Mutex<Option<JoinHandle<()>>>,
}

/// Shared state for the io_uring worker pool.
struct WorkerPoolState {
    /// Worker threads (one io_uring ring per thread).
    workers: Vec<SingleWorkerState>,
    /// Next file handle (atomic for thread-safe allocation).
    next_handle: std::sync::atomic::AtomicU64,
}

/// io_uring-based storage using a pool of worker threads.
///
/// This type is `Send + Sync` and implements the [`Storage`] trait.
/// I/O operations are dispatched to worker threads, each running its own
/// `tokio_uring` runtime. Files are partitioned across workers by handle
/// for load balancing.
///
/// The storage is cheaply cloneable - all clones share the same worker pool.
/// Workers are shut down when the last clone is dropped.
#[derive(Clone)]
pub struct IoUringWorkerStorage {
    state: Arc<WorkerPoolState>,
}

impl IoUringWorkerStorage {
    /// Default number of worker threads.
    /// Matches typical NVMe queue count and provides good parallelism.
    const DEFAULT_WORKER_COUNT: usize = 4;

    /// Creates a new io_uring worker storage with default worker count.
    ///
    /// Spawns multiple worker threads, each running its own `tokio_uring` runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if any worker thread fails to start or io_uring
    /// initialization fails.
    pub fn try_new() -> Result<Self, IoUringInitError> {
        Self::try_with_workers(Self::DEFAULT_WORKER_COUNT)
    }

    /// Creates a new io_uring worker storage with specified worker count.
    ///
    /// # Errors
    ///
    /// Returns an error if any worker thread fails to start or io_uring
    /// initialization fails.
    pub fn try_with_workers(worker_count: usize) -> Result<Self, IoUringInitError> {
        assert!(worker_count > 0, "worker_count must be > 0");

        let mut workers = Vec::with_capacity(worker_count);

        for i in 0..worker_count {
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let (init_tx, init_rx) = std::sync::mpsc::channel();

            let worker_handle = std::thread::Builder::new()
                .name(format!("io_uring-worker-{i}"))
                .spawn(move || {
                    // Run the tokio_uring runtime.
                    tokio_uring::start(async move {
                        // Signal successful initialization.
                        if init_tx.send(Ok(())).is_err() {
                            return;
                        }

                        // Run the worker loop.
                        worker_loop(command_rx).await;
                    });
                })
                .map_err(|e| IoUringInitError {
                    message: format!("failed to spawn worker thread {i}: {e}"),
                })?;

            // Wait for initialization result.
            init_rx.recv().map_err(|_| IoUringInitError {
                message: format!("worker thread {i} panicked during initialization"),
            })??;

            workers.push(SingleWorkerState {
                command_tx,
                worker_handle: Mutex::new(Some(worker_handle)),
            });
        }

        tracing::info!(worker_count, "io_uring worker pool started");

        Ok(Self {
            state: Arc::new(WorkerPoolState {
                workers,
                next_handle: std::sync::atomic::AtomicU64::new(1),
            }),
        })
    }

    /// Returns the number of worker threads.
    #[must_use]
    pub fn worker_count(&self) -> usize {
        self.state.workers.len()
    }

    /// Selects a worker for a given file handle (consistent hashing).
    fn worker_for_handle(&self, handle: FileHandle) -> &SingleWorkerState {
        let idx = handle.0 as usize % self.state.workers.len();
        &self.state.workers[idx]
    }

    /// Allocates a new file handle.
    fn alloc_handle(&self) -> FileHandle {
        let id = self.state.next_handle.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        FileHandle(id)
    }

    /// Sends a command to a specific worker and waits for the reply.
    async fn send_to_worker<T, F>(worker: &SingleWorkerState, make_command: F) -> WalResult<T>
    where
        F: FnOnce(oneshot::Sender<WalResult<T>>) -> IoCommand,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = make_command(reply_tx);

        worker.command_tx.send(command).map_err(|_| {
            WalError::io(
                "send_command",
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "worker thread stopped"),
            )
        })?;

        reply_rx.await.map_err(|_| {
            WalError::io(
                "recv_reply",
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "worker thread stopped"),
            )
        })?
    }

    /// Sends a command to a random worker (for ops without file affinity).
    async fn send_to_any_worker<T, F>(&self, make_command: F) -> WalResult<T>
    where
        F: FnOnce(oneshot::Sender<WalResult<T>>) -> IoCommand,
    {
        // Use first worker for non-file operations.
        Self::send_to_worker(&self.state.workers[0], make_command).await
    }
}

impl Drop for WorkerPoolState {
    fn drop(&mut self) {
        // Shutdown all workers.
        for (i, worker) in self.workers.iter().enumerate() {
            let _ = worker.command_tx.send(IoCommand::Shutdown);
            if let Ok(mut guard) = worker.worker_handle.lock() {
                if let Some(handle) = guard.take() {
                    if handle.join().is_err() {
                        tracing::warn!(worker = i, "io_uring worker thread panicked");
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Storage for IoUringWorkerStorage {
    async fn open(&self, path: &std::path::Path) -> WalResult<Box<dyn StorageFile>> {
        // Pre-allocate handle to determine which worker handles this file.
        let handle = self.alloc_handle();
        let worker = self.worker_for_handle(handle);
        let path = path.to_path_buf();

        Self::send_to_worker(worker, |reply| IoCommand::Open { handle, path, reply }).await?;

        Ok(Box::new(IoUringWorkerFile {
            handle,
            storage: self.clone(),
        }))
    }

    async fn exists(&self, path: &std::path::Path) -> WalResult<bool> {
        let path = path.to_path_buf();
        self.send_to_any_worker(|reply| IoCommand::Exists { path, reply })
            .await
    }

    async fn list_files(
        &self,
        dir: &std::path::Path,
        extension: &str,
    ) -> WalResult<Vec<PathBuf>> {
        let dir = dir.to_path_buf();
        let extension = extension.to_string();
        self.send_to_any_worker(|reply| IoCommand::ListFiles {
            dir,
            extension,
            reply,
        })
        .await
    }

    async fn remove(&self, path: &std::path::Path) -> WalResult<()> {
        let path = path.to_path_buf();
        self.send_to_any_worker(|reply| IoCommand::Remove { path, reply })
            .await
    }

    async fn create_dir_all(&self, path: &std::path::Path) -> WalResult<()> {
        let path = path.to_path_buf();
        self.send_to_any_worker(|reply| IoCommand::CreateDirAll { path, reply })
            .await
    }
}

/// File handle for io_uring worker storage.
///
/// This type is `Send + Sync` and implements the [`StorageFile`] trait.
/// Operations are dispatched to the appropriate worker thread based on handle.
pub struct IoUringWorkerFile {
    handle: FileHandle,
    storage: IoUringWorkerStorage,
}

impl IoUringWorkerFile {
    /// Sends a command to this file's worker and waits for the reply.
    async fn send_command<T, F>(&self, make_command: F) -> WalResult<T>
    where
        F: FnOnce(oneshot::Sender<WalResult<T>>) -> IoCommand,
    {
        let worker = self.storage.worker_for_handle(self.handle);
        IoUringWorkerStorage::send_to_worker(worker, make_command).await
    }
}

impl Drop for IoUringWorkerFile {
    fn drop(&mut self) {
        // Notify worker to close this file handle.
        let worker = self.storage.worker_for_handle(self.handle);
        let _ = worker.command_tx.send(IoCommand::CloseFile {
            handle: self.handle,
        });
    }
}

#[async_trait]
impl StorageFile for IoUringWorkerFile {
    async fn write_at(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        let data = data.to_vec();
        let handle = self.handle;
        self.send_command(|reply| IoCommand::WriteAt {
            handle,
            offset,
            data,
            reply,
        })
        .await
    }

    /// Optimized write+sync that reduces channel round trips.
    async fn write_at_and_sync(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        let data = data.to_vec();
        let handle = self.handle;
        self.send_command(|reply| IoCommand::WriteAtAndSync {
            handle,
            offset,
            data,
            reply,
        })
        .await
    }

    async fn read_at(&self, offset: u64, len: usize) -> WalResult<Bytes> {
        let handle = self.handle;
        self.send_command(|reply| IoCommand::ReadAt {
            handle,
            offset,
            len,
            reply,
        })
        .await
    }

    async fn read_all(&self) -> WalResult<Bytes> {
        let handle = self.handle;
        self.send_command(|reply| IoCommand::ReadAll { handle, reply })
            .await
    }

    async fn sync(&self) -> WalResult<()> {
        let handle = self.handle;
        self.send_command(|reply| IoCommand::Sync { handle, reply })
            .await
    }

    async fn size(&self) -> WalResult<u64> {
        let handle = self.handle;
        self.send_command(|reply| IoCommand::Size { handle, reply })
            .await
    }

    async fn truncate(&self, len: u64) -> WalResult<()> {
        let handle = self.handle;
        self.send_command(|reply| IoCommand::Truncate { handle, len, reply })
            .await
    }
}

// ============================================================================
// Worker Thread Implementation
// ============================================================================

use futures::future::join_all;

use super::io_uring_storage::{IoUringFile, IoUringStorage};

/// Maximum number of commands to batch before processing.
const MAX_BATCH_SIZE: usize = 64;

/// Worker loop that processes commands from the channel.
///
/// Uses batch collection and concurrent I/O to maximize io_uring throughput.
/// Multiple I/O operations are submitted to the ring simultaneously.
async fn worker_loop(mut command_rx: mpsc::UnboundedReceiver<IoCommand>) {
    let storage = IoUringStorage::new();
    let mut files: HashMap<FileHandle, IoUringFile> = HashMap::new();
    let mut batch: Vec<IoCommand> = Vec::with_capacity(MAX_BATCH_SIZE);

    // Stats for debugging batch efficiency.
    let mut total_batches = 0u64;
    let mut total_io_ops = 0u64;
    let mut max_batch_size = 0usize;

    loop {
        // Wait for at least one command.
        let first = match command_rx.recv().await {
            Some(cmd) => cmd,
            None => break, // Channel closed.
        };

        // Check for shutdown before batching.
        if matches!(first, IoCommand::Shutdown) {
            tracing::debug!("io_uring worker shutting down");
            break;
        }

        batch.push(first);

        // Drain additional pending commands (non-blocking).
        while batch.len() < MAX_BATCH_SIZE {
            match command_rx.try_recv() {
                Ok(IoCommand::Shutdown) => {
                    // Process current batch, then shutdown.
                    tracing::debug!("io_uring worker shutting down after batch");
                    process_batch(&storage, &mut files, &mut batch).await;
                    return;
                }
                Ok(cmd) => batch.push(cmd),
                Err(_) => break, // No more pending commands.
            }
        }

        // Process the collected batch with concurrent I/O.
        let io_count = process_batch(&storage, &mut files, &mut batch).await;
        if io_count > 0 {
            total_batches += 1;
            total_io_ops += io_count as u64;
            max_batch_size = max_batch_size.max(io_count);
        }
    }

    // Report batch efficiency stats (debug only).
    if total_batches > 0 {
        let avg_batch = total_io_ops as f64 / total_batches as f64;
        tracing::debug!(
            total_batches,
            total_io_ops,
            max_batch_size,
            avg_batch_size = %format!("{avg_batch:.1}"),
            "io_uring worker batch stats"
        );
    }

    // Clean up: files are dropped when the HashMap goes out of scope.
    tracing::debug!("io_uring worker stopped, closed {} files", files.len());
}

/// Represents a pending I/O operation with its reply channel.
enum PendingIo {
    Write {
        file: IoUringFile,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<WalResult<()>>,
    },
    WriteAndSync {
        file: IoUringFile,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<WalResult<()>>,
    },
    Read {
        file: IoUringFile,
        offset: u64,
        len: usize,
        reply: oneshot::Sender<WalResult<Bytes>>,
    },
    ReadAll {
        file: IoUringFile,
        reply: oneshot::Sender<WalResult<Bytes>>,
    },
    Sync {
        file: IoUringFile,
        reply: oneshot::Sender<WalResult<()>>,
    },
}

/// Process a batch of commands with concurrent I/O.
///
/// Separates I/O operations from metadata operations. I/O operations are
/// executed concurrently using `join_all`, while metadata operations are
/// processed sequentially.
///
/// Returns the number of I/O operations processed concurrently.
async fn process_batch(
    storage: &IoUringStorage,
    files: &mut HashMap<FileHandle, IoUringFile>,
    batch: &mut Vec<IoCommand>,
) -> usize {
    let mut pending_io: Vec<PendingIo> = Vec::new();

    // First pass: handle metadata operations and collect I/O operations.
    for command in batch.drain(..) {
        match command {
            IoCommand::Shutdown => {
                unreachable!("shutdown handled in worker_loop");
            }

            // Metadata operations - must be sequential.
            IoCommand::Open { handle, path, reply } => {
                let result = storage.open(&path).await;
                let response = match result {
                    Ok(file) => {
                        files.insert(handle, file);
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                let _ = reply.send(response);
            }

            IoCommand::Exists { path, reply } => {
                let _ = reply.send(storage.exists(&path));
            }

            IoCommand::ListFiles { dir, extension, reply } => {
                let _ = reply.send(storage.list_files(&dir, &extension));
            }

            IoCommand::Remove { path, reply } => {
                let _ = reply.send(storage.remove(&path));
            }

            IoCommand::CreateDirAll { path, reply } => {
                let _ = reply.send(storage.create_dir_all(&path));
            }

            IoCommand::CloseFile { handle } => {
                files.remove(&handle);
            }

            // Synchronous file operations.
            IoCommand::Size { handle, reply } => {
                let result = files
                    .get(&handle)
                    .map_or_else(
                        || Err(WalError::io("size", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))),
                        IoUringFile::size,
                    );
                let _ = reply.send(result);
            }

            IoCommand::Truncate { handle, len, reply } => {
                let result = files
                    .get(&handle)
                    .map_or_else(
                        || Err(WalError::io("truncate", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))),
                        |f| f.truncate(len),
                    );
                let _ = reply.send(result);
            }

            // I/O operations - collect for concurrent execution.
            IoCommand::WriteAt { handle, offset, data, reply } => {
                match files.get(&handle) {
                    Some(file) => pending_io.push(PendingIo::Write {
                        file: file.clone(),
                        offset,
                        data,
                        reply,
                    }),
                    None => {
                        let _ = reply.send(Err(WalError::io("write_at", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))));
                    }
                }
            }

            IoCommand::WriteAtAndSync { handle, offset, data, reply } => {
                match files.get(&handle) {
                    Some(file) => pending_io.push(PendingIo::WriteAndSync {
                        file: file.clone(),
                        offset,
                        data,
                        reply,
                    }),
                    None => {
                        let _ = reply.send(Err(WalError::io("write_at_and_sync", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))));
                    }
                }
            }

            IoCommand::ReadAt { handle, offset, len, reply } => {
                match files.get(&handle) {
                    Some(file) => pending_io.push(PendingIo::Read {
                        file: file.clone(),
                        offset,
                        len,
                        reply,
                    }),
                    None => {
                        let _ = reply.send(Err(WalError::io("read_at", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))));
                    }
                }
            }

            IoCommand::ReadAll { handle, reply } => {
                match files.get(&handle) {
                    Some(file) => pending_io.push(PendingIo::ReadAll {
                        file: file.clone(),
                        reply,
                    }),
                    None => {
                        let _ = reply.send(Err(WalError::io("read_all", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))));
                    }
                }
            }

            IoCommand::Sync { handle, reply } => {
                match files.get(&handle) {
                    Some(file) => pending_io.push(PendingIo::Sync {
                        file: file.clone(),
                        reply,
                    }),
                    None => {
                        let _ = reply.send(Err(WalError::io("sync", std::io::Error::new(
                            std::io::ErrorKind::NotFound, "file handle not found"))));
                    }
                }
            }
        }
    }

    // Execute all I/O operations concurrently.
    let io_count = pending_io.len();
    if io_count > 0 {
        let io_futures: Vec<_> = pending_io
            .into_iter()
            .map(|op| async move {
                match op {
                    PendingIo::Write { file, offset, data, reply } => {
                        let result = file.write_at(offset, &data).await;
                        let _ = reply.send(result);
                    }
                    PendingIo::WriteAndSync { file, offset, data, reply } => {
                        let result = match file.write_at(offset, &data).await {
                            Ok(()) => file.sync().await,
                            Err(e) => Err(e),
                        };
                        let _ = reply.send(result);
                    }
                    PendingIo::Read { file, offset, len, reply } => {
                        let result = file.read_at(offset, len).await;
                        let _ = reply.send(result);
                    }
                    PendingIo::ReadAll { file, reply } => {
                        let result = file.read_all().await;
                        let _ = reply.send(result);
                    }
                    PendingIo::Sync { file, reply } => {
                        let result = file.sync().await;
                        let _ = reply.send(result);
                    }
                }
            })
            .collect();

        // This runs all I/O operations concurrently on the io_uring ring.
        join_all(io_futures).await;
    }

    io_count
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn test_io_uring_worker_write_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = IoUringWorkerStorage::try_new().unwrap();
        let file = storage.open(&path).await.unwrap();

        // Write some data.
        let data = b"hello, world!";
        file.write_at(0, data).await.unwrap();
        file.sync().await.unwrap();

        // Read it back.
        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(&read_data[..], data);

        // Check size.
        let size = file.size().await.unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_io_uring_worker_read_all() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = IoUringWorkerStorage::try_new().unwrap();
        let file = storage.open(&path).await.unwrap();

        let data = b"some test data for reading";
        file.write_at(0, data).await.unwrap();
        file.sync().await.unwrap();

        let all_data = file.read_all().await.unwrap();
        assert_eq!(&all_data[..], data);
    }

    #[tokio::test]
    async fn test_io_uring_worker_list_files() {
        let temp_dir = tempfile::tempdir().unwrap();

        let storage = IoUringWorkerStorage::try_new().unwrap();

        // Create some files.
        for i in 0..3 {
            let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
            let _ = storage.open(&path).await.unwrap();
        }

        // Also create a non-.wal file.
        let other_path = temp_dir.path().join("other.txt");
        let _ = storage.open(&other_path).await.unwrap();

        // List .wal files.
        let files = storage.list_files(temp_dir.path(), "wal").await.unwrap();
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_io_uring_worker_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = IoUringWorkerStorage::try_new().unwrap();

        assert!(!storage.exists(&path).await.unwrap());

        let _ = storage.open(&path).await.unwrap();
        assert!(storage.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_io_uring_worker_truncate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = IoUringWorkerStorage::try_new().unwrap();
        let file = storage.open(&path).await.unwrap();

        // Write some data.
        let data = b"hello, world!";
        file.write_at(0, data).await.unwrap();
        file.sync().await.unwrap();

        // Truncate to 5 bytes.
        file.truncate(5).await.unwrap();

        // Check size.
        let size = file.size().await.unwrap();
        assert_eq!(size, 5);

        // Read back.
        let read_data = file.read_at(0, 5).await.unwrap();
        assert_eq!(&read_data[..], b"hello");
    }

    #[tokio::test]
    async fn test_io_uring_worker_multiple_files() {
        let temp_dir = tempfile::tempdir().unwrap();

        let storage = IoUringWorkerStorage::try_new().unwrap();

        // Create and write to multiple files.
        for i in 0..5 {
            let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
            let file = storage.open(&path).await.unwrap();
            let data = format!("data for segment {i}");
            file.write_at(0, data.as_bytes()).await.unwrap();
            file.sync().await.unwrap();
        }

        // Verify each file.
        for i in 0..5 {
            let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
            let file = storage.open(&path).await.unwrap();
            let data = file.read_all().await.unwrap();
            let expected = format!("data for segment {i}");
            assert_eq!(&data[..], expected.as_bytes());
        }
    }

    #[tokio::test]
    async fn test_io_uring_worker_concurrent_access() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = Arc::new(IoUringWorkerStorage::try_new().unwrap());
        let file = storage.open(&path).await.unwrap();

        // Write initial data.
        file.write_at(0, b"initial").await.unwrap();
        file.sync().await.unwrap();

        // Spawn multiple tasks that read concurrently.
        let mut handles = Vec::new();
        for _ in 0..10 {
            let storage = Arc::clone(&storage);
            let path = path.clone();
            handles.push(tokio::spawn(async move {
                let file = storage.open(&path).await.unwrap();
                let data = file.read_all().await.unwrap();
                assert_eq!(&data[..], b"initial");
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
