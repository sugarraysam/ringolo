// TODO:
// - test spawn a task to ensure resource accounting
// - error handling better than `anyhow`
// - BufferPoolError ++ thiserror (integration with IoError + OpcodeError)
// - BufferFamily initial_entries configurable on runtime, per thread, per family
// - use madvise?? Advanced Optimization: Huge Pages
//   - (imports) use libc::{MADV_DONTNEED, MADV_HUGEPAGE, c_void, madvise};
//     > If you are allocating large backing rings (e.g., > 2MB), you should advise the kernel to
//     > use Transparent Huge Pages (THP). This reduces TLB (Translation Lookaside Buffer) misses
//     > significantly during heavy IO.
// - if runtime detects ENOBUFS (the kernel ran out of buffers), you simply allocate one
//   more slab of default_entries().
// - the `BufferFamily::default_entries()` should be configurable by user. Default size used each
//   time we need to create a new `BufferRing` associated with this particular size.
// - Do we need interior mutability? IoUring already borrowed as mutable when `on_completion` is
//   invoked, which is when we will need to resolve buffers. But we can simply `borrow_mut` the
//   BufferPool and avoid having all of the buffer_pool's fields require interior mutability
// - Support for `IOU_PBUF_RING_INC`, because kernel might not receive *all* data in one CQE,
//   buffers are partially filled, detect this and wait (cant be complete)

use crate::context;
use crate::future::lib::BufferFamily;
use crate::task::Header;
use crate::utils::sys::{CACHE_LINE_SIZE, get_page_size};
use anyhow::{Context, Result, anyhow};
use io_uring::types::BufRingEntry;
use std::ops::{Deref, DerefMut};

use std::alloc::{Layout, alloc, alloc_zeroed, dealloc};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU16, Ordering};
use std::task::Waker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Bgid(u16);

impl Bgid {
    pub(crate) const fn new(id: u16) -> Self {
        Self(id)
    }

    #[inline(always)]
    pub(crate) const fn val(self) -> u16 {
        self.0
    }

    #[must_use]
    pub(crate) fn next(self) -> Self {
        match self.0.checked_add(1) {
            Some(n) => Self(n),
            None => panic_bgid_space_exhausted(),
        }
    }
}

#[cold]
fn panic_bgid_space_exhausted() -> ! {
    panic!("BGID space exhausted");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Bid(u16);

impl Bid {
    pub(crate) const fn new(id: u16) -> Self {
        Self(id)
    }

    #[inline(always)]
    pub(crate) const fn val(self) -> u16 {
        self.0
    }

    #[inline(always)]
    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }

    #[inline(always)]
    pub(crate) const fn byte_offset(self, buf_size: usize) -> usize {
        self.index() * buf_size
    }
}

#[derive(Debug)]
pub(crate) struct BufferPool {
    buffer_groups: HashMap<Bgid, BufferRing>,
    family_map: HashMap<BufferFamily, VecDeque<Bgid>>,
    next_bgid: Bgid,
    recycled_bgids: Vec<Bgid>,
}

impl BufferPool {
    pub(crate) fn new() -> Self {
        Self {
            buffer_groups: HashMap::new(),
            family_map: HashMap::new(),
            next_bgid: Bgid::new(0),
            recycled_bgids: Vec::new(),
        }
    }

    pub(crate) fn get_or_register_bgid(&mut self, buf_family: BufferFamily) -> Result<Bgid> {
        // 1. Fast path: check if we already have a ring for this size.
        self.family_map
            .get(&buf_family)
            .and_then(|bgids| {
                bgids.iter().find(|&&id| {
                    self.buffer_groups
                        .get(&id)
                        .is_some_and(BufferRing::has_buffers)
                })
            })
            .copied()
            .map(Ok)
            .unwrap_or_else(|| {
                // TODO: impl `BufferFallbackStrategy`, for now we `AllocateBgid` ??
                // Is this even something useful? Feels we *always* want allocate new bgid
                // - AllocateRaw
                // - NewBufRing
                // - Panic

                // 2. Slow path: Register a new ring
                self.register_new_buf_ring(buf_family)
            })
    }

    pub(crate) fn resolve_buffer(
        &mut self,
        bgid: Bgid,
        bid: Bid,
        len: usize,
        waker: &Waker,
    ) -> Result<RingMappedBuffer> {
        self.buffer_groups
            .get_mut(&bgid)
            .context("Invalid BGID")?
            .resolve_buffer(bid, len, waker)
    }

    pub(crate) fn recycle(&mut self, bgid: Bgid, bid: Bid) -> Result<()> {
        let buf_ring = self.buffer_groups.get_mut(&bgid).context("invalid BGID")?;
        buf_ring.recycle(bid)
    }

    pub(super) fn get_buffer_ring(&self, bgid: Bgid) -> Option<&BufferRing> {
        self.buffer_groups.get(&bgid)
    }

    pub(crate) fn len(&self) -> usize {
        self.buffer_groups.len()
    }
}

// Private helpers.
impl BufferPool {
    /// Explicitly creates a new ring for a specific family.
    /// Useful when the runtime detects ENOBUFS and needs to grow the pool.
    fn register_new_buf_ring(&mut self, buf_family: BufferFamily) -> Result<Bgid> {
        let bgid = self.assign_new_bgid()?;

        // Add to the main storage
        self.buffer_groups
            .insert(bgid, BufferRing::try_new(bgid, buf_family)?);

        // Add to the lookup map
        self.family_map
            .entry(buf_family)
            .or_default()
            .push_back(bgid);

        Ok(bgid)
    }

    fn assign_new_bgid(&mut self) -> Result<Bgid> {
        if let Some(recycled) = self.recycled_bgids.pop() {
            Ok(recycled)
        } else {
            let current = self.next_bgid;
            self.next_bgid = current.next();
            Ok(current)
        }
    }

    pub(super) fn release_bgid(&mut self, bgid: Bgid) {
        self.recycled_bgids.push(bgid);
    }
}

#[derive(Debug)]
pub(super) struct BufferRing {
    // Control Ring (Shared with Kernel)
    ring_ptr: NonNull<BufRingEntry>,
    ring_layout: Layout,
    tail_addr: NonNull<AtomicU16>,

    // Data Storage (User Space)
    mem_pool_ptr: NonNull<u8>,
    mem_pool_layout: Layout,
    mem_pool_size: usize,

    // Metadata
    bgid: Bgid,
    buf_size: usize,
    ring_entries: u16,
    mask: u16,

    // Accounting
    available: u16,
}

impl BufferRing {
    /// Creates and registers a new buffer ring.
    pub(super) fn try_new(bgid: Bgid, buf_family: BufferFamily) -> Result<Self> {
        let buf_size = buf_family.size();
        let ring_entries = buf_family.initial_entries();

        // 1. Allocate control ring
        let (ring_ptr, ring_layout) = Self::alloc_control_ring(ring_entries)?;
        let tail_addr = Self::derive_tail_addr(ring_ptr);

        // 2. Allocate buffer pool memory
        let mem_pool_size = buf_size * ring_entries as usize;
        let (mem_pool_ptr, mem_pool_layout) = Self::alloc_mem_pool(mem_pool_size)?;

        // 3. Register
        unsafe {
            context::with_ring(|ring| -> std::io::Result<()> {
                ring.submitter().register_buf_ring_with_flags(
                    ring_ptr.as_ptr() as u64,
                    ring_entries,
                    bgid.val(),
                    0,
                )
            })?;
        }

        let mut ring = Self {
            ring_ptr,
            ring_layout,
            tail_addr,
            mem_pool_ptr,
            mem_pool_layout,
            mem_pool_size,
            bgid,
            buf_size,
            ring_entries,
            mask: ring_entries - 1,
            available: ring_entries,
        };

        ring.initialize()?;

        Ok(ring)
    }

    pub(super) fn resolve_buffer(
        &mut self,
        bid: Bid,
        len: usize,
        waker: &Waker,
    ) -> Result<RingMappedBuffer> {
        if len > self.buf_size {
            anyhow::bail!("Kernel allocated a buffer that is too large.")
        }

        self.available = self
            .available
            .checked_sub(1)
            .context("No buffers available")?;

        let offset = bid.byte_offset(self.buf_size);
        if offset >= self.mem_pool_size {
            anyhow::bail!("Kernel returned invalid BID");
        }

        let ptr = unsafe { NonNull::new_unchecked(self.mem_pool_ptr.as_ptr().add(offset).cast()) };

        Ok(RingMappedBuffer::new(
            ptr,
            BufferMetadata {
                bid,
                bgid: self.bgid,
                len,
                cap: self.buf_size,
            },
            waker,
        ))
    }

    pub(super) fn recycle(&mut self, bid: Bid) -> Result<()> {
        self.write_entry(bid, self.get_tail())?;
        self.advance(1);
        self.available += 1;
        Ok(())
    }

    pub(super) fn has_buffers(&self) -> bool {
        self.available > 0
    }

    pub(super) fn get_tail(&self) -> u16 {
        // Read tail (Relaxed is fine, we own the writer side)
        unsafe { self.tail_addr.as_ref().load(Ordering::Relaxed) }
    }
}

// Private methods.
impl BufferRing {
    fn initialize(&mut self) -> Result<()> {
        for i in 0..self.ring_entries {
            self.write_entry(Bid(i), i)?;
        }

        self.advance(self.ring_entries);
        Ok(())
    }

    fn write_entry(&mut self, bid: Bid, tail_offset: u16) -> Result<()> {
        let idx = (tail_offset & self.mask) as usize;

        let offset = bid.byte_offset(self.buf_size);
        if offset >= self.mem_pool_size {
            anyhow::bail!("invalid BID and memory offset");
        };

        unsafe {
            let buf_ptr = self.mem_pool_ptr.as_ptr().add(offset);
            let entry = self.ring_ptr.as_ptr().add(idx);

            (*entry).set_addr(buf_ptr as u64);
            (*entry).set_len(self.buf_size as u32);
            (*entry).set_bid(bid.val());
        }

        Ok(())
    }

    fn advance(&self, count: u16) {
        unsafe {
            self.tail_addr.as_ref().fetch_add(count, Ordering::Release);
        }
    }

    fn alloc_control_ring(entries: u16) -> Result<(NonNull<BufRingEntry>, Layout)> {
        let entry_size = std::mem::size_of::<BufRingEntry>();
        let layout = Layout::from_size_align(
            entry_size * entries as usize,
            get_page_size(), // Important: has to be page-aligned.
        )?;

        let ptr = NonNull::new(unsafe { alloc_zeroed(layout).cast() })
            .ok_or_else(|| anyhow!("OOM: Failed to alloc control ring"))?;

        Ok((ptr, layout))
    }

    fn alloc_mem_pool(total_mem: usize) -> Result<(NonNull<u8>, Layout)> {
        let layout = Layout::from_size_align(total_mem, CACHE_LINE_SIZE)?;

        let ptr = NonNull::new(unsafe { alloc(layout) })
            .ok_or_else(|| anyhow!("OOM: Failed to alloc memory"))?;

        // TODO: if buf_size larger than 2MB ??
        // OPTIMIZATION: Suggest Huge Pages to the kernel
        // This reduces TLB misses for large buffer pools.
        // unsafe { madvise(ptr.as_ptr().cast(), total_mem, MADV_HUGEPAGE) };

        Ok((ptr, layout))
    }

    fn derive_tail_addr(ring_ptr: NonNull<BufRingEntry>) -> NonNull<AtomicU16> {
        unsafe {
            let tail_ptr = BufRingEntry::tail(ring_ptr.as_ptr());
            NonNull::new_unchecked(tail_ptr as *mut AtomicU16)
        }
    }
}

impl Drop for BufferRing {
    fn drop(&mut self) {
        // Use `try_with` in case TLS was already dropped.
        context::try_with_ring(|ring| {
            ring.pool_mut().release_bgid(self.bgid);
            let res = ring.submitter().unregister_buf_ring(self.bgid.0);
            debug_assert!(res.is_ok(), "Failed to unregister buf ring");
        });

        // Deallocate control ring and memory pool.
        unsafe {
            dealloc(self.ring_ptr.as_ptr().cast(), self.ring_layout);

            // TODO: needed?
            // Optional: Tell kernel we are done with this range immediately
            // madvise(
            //     self.pool_ptr.as_ptr().cast(),
            //     self.pool_layout.size(),
            //     MADV_DONTNEED,
            // );
            dealloc(self.mem_pool_ptr.as_ptr().cast(), self.mem_pool_layout);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BufferMetadata {
    bid: Bid,
    bgid: Bgid,
    len: usize,
    cap: usize,
}

#[derive(Debug)]
pub(crate) struct RingMappedBuffer {
    ptr: NonNull<u8>,
    meta: BufferMetadata,

    // We track the task that owns this resource.
    owning_task: Option<NonNull<Header>>,

    // This marker makes the struct !Send and !Sync
    _marker: PhantomData<*const ()>,
}

impl RingMappedBuffer {
    pub(crate) fn new(ptr: NonNull<u8>, meta: BufferMetadata, waker: &Waker) -> Self {
        // Increment resource count on the task to prevent premature task migration/shutdown
        // while holding a buffer.
        let owning_task = context::with_core(|core| core.increment_task_owned_resources(waker));

        Self {
            ptr,
            meta,
            owning_task,
            _marker: PhantomData,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.meta.len
    }

    pub(crate) fn capacity(&self) -> usize {
        self.meta.cap
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.meta.len == 0
    }

    /// Shortens the buffer, keeping the first `len` bytes. Useful if user read
    /// full page (4096) but only need first 100 bytes. Allows doing
    /// `buf.truncate(100).into_bytes()` to copy less data.
    pub(crate) fn truncate(&mut self, len: usize) {
        if len < self.meta.len {
            self.meta.len = len;
        }
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.meta.len) }
    }

    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.as_slice().to_vec()
    }

    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(self.as_slice())
    }
}

impl Drop for RingMappedBuffer {
    fn drop(&mut self) {
        // 1. Decrement Task Resources
        if let Some(task) = self.owning_task {
            unsafe { Header::decrement_owned_resources(task) };
        }

        // 2. Recycle to BufferRing
        context::try_with_ring(|ring| ring.pool_mut().recycle(self.meta.bgid, self.meta.bid));
    }
}

impl Deref for RingMappedBuffer {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.meta.len) }
    }
}

impl DerefMut for RingMappedBuffer {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.meta.len) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use rstest::rstest;

    #[test]
    #[should_panic]
    fn test_bgid_space_exhausted() {
        let max_bgid = Bgid::new(u16::MAX);
        let _ = max_bgid.next();
    }

    #[rstest]
    #[case::mtu(BufferFamily::Mtu)]
    #[case::page(BufferFamily::Page)]
    #[case::tls_large(BufferFamily::TlsLarge)]
    #[case::huge(BufferFamily::Huge)]
    fn test_register_buffer_pool(#[case] buf_family: BufferFamily) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        context::with_ring(|ring| -> Result<()> {
            let mut pool = ring.pool_mut();
            assert_eq!(pool.len(), 0);

            let bgid1 = pool.get_or_register_bgid(buf_family)?;
            assert_eq!(bgid1.val(), 0);
            assert_eq!(pool.next_bgid.val(), 1);
            assert_eq!(pool.len(), 1);

            // Registering twice does nothing.
            let bgid2 = pool.get_or_register_bgid(buf_family)?;
            assert_eq!(bgid2.val(), 0);
            assert_eq!(pool.next_bgid.val(), 1);
            assert_eq!(pool.len(), 1);

            let buf_ring = pool.get_buffer_ring(bgid1).unwrap();
            assert_eq!(buf_ring.available, buf_family.initial_entries());
            assert_eq!(buf_ring.buf_size, buf_family.size());
            assert_eq!(
                buf_ring.mem_pool_size,
                buf_ring.buf_size * buf_ring.ring_entries as usize
            );

            Ok(())
        })
    }

    #[rstest]
    #[case::mtu(BufferFamily::Mtu)]
    #[case::page(BufferFamily::Page)]
    #[case::tls_large(BufferFamily::TlsLarge)]
    #[case::huge(BufferFamily::Huge)]
    fn test_ring_mapped_buffer_lifecycle(#[case] buf_family: BufferFamily) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, _) = mock_waker();

        context::with_ring(|ring| -> Result<()> {
            let bgid = ring.pool_mut().get_or_register_bgid(buf_family)?;

            // Resolve a buffer (Simulate Kernel providing a buffer)
            let meta = BufferMetadata {
                bid: Bid::new(0),
                bgid,
                len: 100,
                cap: buf_family.size(),
            };

            let mut buffer = ring
                .pool_mut()
                .resolve_buffer(meta.bgid, meta.bid, meta.len, &waker)
                .unwrap();

            ring.pool().get_buffer_ring(bgid).inspect(|buf_ring| {
                assert_eq!(buffer.meta, meta);
                assert_eq!(buf_ring.available, buf_ring.ring_entries - 1);
                assert_eq!(buf_ring.get_tail(), buf_ring.ring_entries);
            });

            // Write to buffer to ensure memory is valid
            buffer[0] = 0xAA;
            buffer[99] = 0xBB;
            assert_eq!(buffer.as_slice()[0], 0xAA);

            // Drop buffer - triggers RAII and returns buffer to BufferRing
            drop(buffer);

            ring.pool().get_buffer_ring(bgid).inspect(|buf_ring| {
                assert_eq!(buf_ring.available, buf_ring.ring_entries);
                assert_eq!(buf_ring.get_tail(), buf_ring.ring_entries + 1);
            });

            Ok(())
        })
    }

    #[rstest]
    #[case::mtu(BufferFamily::Mtu)]
    #[case::page(BufferFamily::Page)]
    #[case::tls_large(BufferFamily::TlsLarge)]
    #[case::huge(BufferFamily::Huge)]
    fn test_pool_exhaustion_and_growth(#[case] buf_family: BufferFamily) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, _) = mock_waker();

        context::with_ring(|ring| -> Result<()> {
            // Register `bgid1` and exhaust all buffers available.
            let bgid1 = ring.pool_mut().get_or_register_bgid(buf_family)?;
            let keep_alive = (0..buf_family.initial_entries())
                .map(|i| {
                    ring.pool_mut()
                        .resolve_buffer(bgid1, Bid::new(i), 10, &waker)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert!(!ring.pool().get_buffer_ring(bgid1).unwrap().has_buffers());

            // Requesting a bgid for the same buffer buf_family will allocate a new
            // `buf_ring` because there are no buffers available in `bgid1`.
            let bgid2 = ring.pool_mut().get_or_register_bgid(buf_family)?;

            assert_ne!(bgid1, bgid2, "Should have allocated a new BGID");
            assert_eq!(ring.pool().len(), 2);
            assert_eq!(ring.pool().family_map.get(&buf_family).unwrap().len(), 2);

            // Verify we can resolve from the new ring
            let buffer = ring
                .pool_mut()
                .resolve_buffer(bgid2, Bid::new(0), 10, &waker)
                .unwrap();

            assert_eq!(
                buffer.meta,
                BufferMetadata {
                    bid: Bid::new(0),
                    bgid: bgid2,
                    len: 10,
                    cap: buf_family.size()
                }
            );

            // Dropping `keep_alive` returns all buffers to `bgid1` so next time we
            // resolve a `bgid` we get `bgid1` because we resolve in ascending order
            // of bgid's.
            drop(keep_alive);
            let bgid3 = ring.pool_mut().get_or_register_bgid(buf_family)?;
            assert_eq!(bgid1, bgid3);

            Ok(())
        })
    }

    #[rstest]
    #[case::mtu(BufferFamily::Mtu)]
    #[case::page(BufferFamily::Page)]
    #[case::tls_large(BufferFamily::TlsLarge)]
    #[case::huge(BufferFamily::Huge)]
    fn test_invalid_bgid_and_bid(#[case] buf_family: BufferFamily) -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;
        let (waker, _) = mock_waker();

        context::with_ring(|ring| -> Result<()> {
            let mut pool = ring.pool_mut();
            let bgid = pool.get_or_register_bgid(buf_family)?;

            // Case 1: Invalid BGID
            let invalid_bgid = Bgid::new(999);
            assert!(
                pool.resolve_buffer(invalid_bgid, Bid::new(0), 10, &waker)
                    .is_err(),
            );
            assert!(pool.recycle(invalid_bgid, Bid::new(0)).is_err());

            // Case 2: Out of bounds BID (Index too high)
            let invalid_bid = Bid::new(buf_family.initial_entries() + 100);
            assert!(pool.resolve_buffer(bgid, invalid_bid, 10, &waker).is_err(),);
            assert!(pool.recycle(bgid, invalid_bid).is_err());

            Ok(())
        })
    }

    #[rstest]
    fn test_bgid_recycling_logic() -> Result<()> {
        let (_runtime, _scheduler) = init_local_runtime_and_context(None)?;

        context::with_ring(|ring| -> Result<()> {
            let mut pool = ring.pool_mut();

            // 1. Allocate a few rings (internally creates Bgid 1, 2, 3)
            let bgid1 = pool.assign_new_bgid()?;
            let bgid2 = pool.assign_new_bgid()?;
            let bgid3 = pool.assign_new_bgid()?;

            assert_eq!(bgid1.val(), 0);
            assert_eq!(bgid2.val(), 1);
            assert_eq!(bgid3.val(), 2);

            // 2. Release bgid1
            pool.release_bgid(bgid1);
            assert!(pool.recycled_bgids.contains(&bgid1));

            // 3. Request new one - should get b1 back (LIFO usually, or just from pool)
            let recycled = pool.assign_new_bgid()?;
            assert_eq!(recycled, bgid1, "Should prefer recycled IDs over new ones");

            // 4. Request new one - should continue from b3
            let bgid4 = pool.assign_new_bgid()?;
            assert_eq!(bgid4.val(), bgid3.val() + 1);

            Ok(())
        })
    }
}
