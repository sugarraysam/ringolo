use std::pin::Pin;
use std::task::Waker;

use crate::context;
use crate::context::buffer_pool::{Bgid, Bid, RingMappedBuffer};
use crate::future::lib::OpcodeError;
use crate::sqe::IoError;
use crate::utils::io_uring::CompletionFlags;
use crate::utils::sys::{CACHE_LINE_SIZE, get_page_size};
use anyhow::anyhow;
use pin_project::pin_project;

const MAX_BUF_RING_ENTRIES: u16 = 32768;

/// Defines the size classes and performance characteristics for buffer rings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BufferFamily {
    /// Optimized for small networking packets (UDP/TCP ACKs).
    /// Size: 2KB (Half Page usually).
    Mtu,

    /// Standard OS Page Size. Good default for general file I/O.
    /// Size: 4KB (typically).
    Page,

    /// Optimized for encrypted traffic (HTTPS/gRPC).
    /// Max TLS record size is 16KB.
    /// Size: 16KB
    TlsLarge,

    /// Optimized for high-throughput streaming (Video/Big Data).
    /// Size: 64KB
    Huge,
}

impl BufferFamily {
    /// Returns the exact size in bytes for a single buffer in this family.
    pub fn size(&self) -> usize {
        let size = match self {
            BufferFamily::Mtu => 2048,
            BufferFamily::Page => get_page_size(),
            // 16KB is the sweet spot for almost all secure networking
            BufferFamily::TlsLarge => 16 * 1024,
            // 64KB fits common pipe buffer limits and L2 cache segments
            BufferFamily::Huge => 64 * 1024,
        };

        debug_assert!(
            size % CACHE_LINE_SIZE == 0,
            "Size must be a multiple of {CACHE_LINE_SIZE} to optimize buffer pool alignment"
        );
        size
    }

    // TODO: granular configuration
    /// Returns the default number of ring entries (power of 2) for this buffer family.
    pub fn initial_entries(&self) -> u16 {
        let res = match self {
            // Target: ~2MB Ring
            // High PPS scenarios (UDP/ACKs). We need deep queues to absorb
            // thousands of tiny packets arriving in a microsecond burst.
            BufferFamily::Mtu => 32, // 1024,

            // Target: ~4MB Ring
            // Standard HTTP/1.1 traffic. 1024 requests in flight per ring
            // is a very healthy concurrency limit for a single IO thread.
            BufferFamily::Page => 32, // 1024,

            // Target: ~8MB Ring
            // Heavy payloads (HTTPS/gRPC). 512 entries * 16KB is enough to
            // buffer a significant amount of throughput while you decrypt/copy.
            BufferFamily::TlsLarge => 32, // 512,

            // Target: ~8MB Ring
            // Streaming/Video. These take longer to copy/process.
            // 128 * 64KB is plenty. If you have more than 128 chunks pending,
            // your bottleneck is likely the consumer, not the ring depth.
            BufferFamily::Huge => 32, // 128,
        };

        debug_assert!(
            u16::is_power_of_two(res) && res < MAX_BUF_RING_ENTRIES,
            "ring_entries has to be power of two and < {MAX_BUF_RING_ENTRIES} per iouring docs.",
        );

        res
    }

    /// Helper to calculate the total memory footprint of one pool in bytes.
    /// Useful for logging or capacity planning.
    pub fn total_bytes(&self) -> usize {
        self.size() * self.initial_entries() as usize
    }
}

// Helper struct to pass configuration from the Mode to the Opcode builder.
pub(crate) struct BufferSubmission {
    pub ptr: *mut u8,
    pub len: u32,
    pub bgid: Bgid,
    pub is_mapped: bool,
}

/// State machine that handles the lifecycle of a buffer (Raw vs Mapped) during an I/O operation.
#[pin_project]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KernelBufferMode {
    // We use an inner type to provide a better UX and force users to use the
    // `new_raw()/new_mapped()` constructors.
    #[pin]
    inner: InnerBufferMode,
}

#[pin_project(project = InnerProj)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum InnerBufferMode {
    Raw(#[pin] Vec<u8>),

    Mapped { bgid: Option<Bgid> },
}

impl KernelBufferMode {
    /// Creates a mode that uses a standard heap-allocated `Vec<u8>`.
    pub fn raw() -> Self {
        Self {
            inner: InnerBufferMode::Raw(Vec::new()),
        }
    }

    /// Creates a mode that uses `io_uring` registered ring buffers.
    pub fn mapped() -> Self {
        Self {
            inner: InnerBufferMode::Mapped { bgid: None },
        }
    }

    /// Prepares the buffer submission data for the opcode builder.
    pub(crate) fn prepare(
        self: Pin<&mut Self>,
        buf_family: BufferFamily,
    ) -> Result<BufferSubmission, OpcodeError> {
        let inner = self.project().inner.project();

        match inner {
            InnerProj::Mapped { bgid } => {
                let assigned_bgid =
                    context::with_ring(|ring| ring.pool_mut().get_or_register_bgid(buf_family))
                        .map_err(OpcodeError::from)?;

                *bgid = Some(assigned_bgid);

                Ok(BufferSubmission {
                    ptr: std::ptr::null_mut(),
                    len: 0,
                    bgid: assigned_bgid,
                    is_mapped: true,
                })
            }
            InnerProj::Raw(raw_buffer) => {
                let raw = raw_buffer.get_mut();
                *raw = vec![0u8; buf_family.size()];

                Ok(BufferSubmission {
                    ptr: raw.as_mut_ptr(),
                    len: raw.len() as u32,
                    bgid: Bgid::new(0),
                    is_mapped: false,
                })
            }
        }
    }

    /// Finalizes the operation, returning the populated `UringBuffer`.
    pub(crate) fn complete(
        self: Pin<&mut Self>,
        len: usize,
        flags: CompletionFlags,
        waker: &Waker,
    ) -> Result<UringBuffer, IoError> {
        let inner = self.project().inner.project();
        match inner {
            InnerProj::Mapped { bgid } => {
                debug_assert!(
                    bgid.is_some(),
                    "unexpected bgid unset for KernelBufferMode::Mapped"
                );
                let bgid = bgid.unwrap();

                let Some(bid) = io_uring::cqueue::buffer_select(flags.bits()) else {
                    return Err(anyhow!("Kernel did not return a buffer!").into());
                };

                let buffer = context::with_ring(|ring| {
                    ring.pool_mut()
                        .resolve_buffer(bgid, Bid::new(bid), len, waker)
                })?;

                Ok(UringBuffer::new_mapped(buffer))
            }
            InnerProj::Raw(raw_buffer) => {
                // Take the vector (leaving an empty one behind)
                let mut buf = std::mem::take(raw_buffer.get_mut());

                // TODO: bug if we check empty before setting len this will fail???
                // See test example where we manually set `len()` on vector.
                if buf.is_empty() {
                    return Err(anyhow!("State Error: Raw buffer missing/empty").into());
                }

                // Safety: We allocated `size` in prepare(), kernel wrote `len`.
                // `len` is trusted from kernel return.
                unsafe { buf.set_len(len) };

                Ok(UringBuffer::new_raw(buf))
            }
        }
    }
}

/// A unified container for data returned by the kernel.
#[derive(Debug)]
pub struct UringBuffer(InnerUringBuffer);

#[derive(Debug)]
pub(crate) enum InnerUringBuffer {
    /// A standard heap-allocated vector.
    Raw(Vec<u8>),

    /// An iouring mapped buffer.
    Mapped(RingMappedBuffer),
}

impl UringBuffer {
    pub(crate) fn new_raw(v: Vec<u8>) -> Self {
        Self(InnerUringBuffer::Raw(v))
    }

    pub(crate) fn new_mapped(m: RingMappedBuffer) -> Self {
        Self(InnerUringBuffer::Mapped(m))
    }

    /// Zero-cost view of the data.
    pub fn as_slice(&self) -> &[u8] {
        match &self.0 {
            InnerUringBuffer::Raw(v) => v.as_slice(),
            InnerUringBuffer::Mapped(m) => m.as_slice(),
        }
    }

    /// Convert into a vector.
    ///
    /// - If Raw: Zero-allocation move.
    /// - If Mapped: Allocates and copies, then recycles the mapped buffer.
    pub fn into_vec(self) -> Vec<u8> {
        match self.0 {
            InnerUringBuffer::Raw(v) => v,
            InnerUringBuffer::Mapped(m) => m.into_vec(),
        }
    }

    /// Convert into Bytes.
    ///
    /// - If Raw: Zero-allocation move.
    /// - If Mapped: Allocates and copies, then recycles the mapped buffer.
    pub fn into_bytes(self) -> bytes::Bytes {
        match self.0 {
            InnerUringBuffer::Raw(v) => bytes::Bytes::from(v),
            InnerUringBuffer::Mapped(m) => m.into_bytes(),
        }
    }

    /// Returns true if the buffer is of the `Raw` variant.
    pub fn is_raw(&self) -> bool {
        matches!(self.0, InnerUringBuffer::Raw(_))
    }

    /// Returns true if the buffer is an iouring mapped buffer.
    pub fn is_mapped(&self) -> bool {
        matches!(self.0, InnerUringBuffer::Mapped(_))
    }

    /// Returns the length of the valid data in the buffer.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// Returns true if the buffer contains no data.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Shortens the buffer, keeping the first `len` bytes.
    ///
    /// If `len` is greater than the buffer's current length, this has no effect.
    pub fn truncate(&mut self, len: usize) {
        match self.0 {
            InnerUringBuffer::Raw(ref mut v) => v.truncate(len),
            InnerUringBuffer::Mapped(ref mut m) => m.truncate(len),
        }
    }
}
