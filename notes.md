# Notes

- All iouring supported operations:
  <https://github.com/axboe/liburing/blob/master/src/include/liburing.h>
  - look for `io_uring_prep_*`
- CRTP == no virtual dispatch
- object pool == no memory alloc

## IO thread algorithm and prioritization

1.  Receive IOPS on MPSC channel (deque N) :: default state
2.  Prep and submit SQEs (submit N)
3.  Busy loop M times over
    - any completions ? goto 5 complete
    - any new IOPS on MPSC channel ? goto 1 prep+submit
4.  If busy loop fails M times, then if
    - `inflight > 0` ? check completions : wait on MPSC channel
5.  Complete IOs with for loop helper, if complete C
    - MPSC channel empty? yes continue : no goto 1 prep+submit

**Notes**

- keep track of "inflight" IOs, submitted but not yet completed
- to be able to exit cleanly, we need to be able to timeout on both:
  - waiting on MPSC channel
  - waiting for completions
- we prioritize submitting, so will interrupt completion
  - controlled by `COMPLETION_BATCH_SIZE`
- keep track of last action, influences next action
  - if we submitted :: check for completion first
  - if we completed :: check for submissions first
- always do a busy loop first, before selecting an action, action
  selection is always:
  - `inflight > 0` ? check completions : wait on MPSC channel

## Unsolved problems

- how to use the `reinterpret_cast` in rust with `unsafe {}`
- opsFreeList more granular than a single lock on queue (multiple
  threads can deque and enqueue?)
  - rust fast sync MPMC queue?
- each IO thread need a MPSC channel, that is buffered
  - how to size the buffer?
  - how to avoid mem allocations?
  - need to call `io_uring_prep_sqe` on the IO thread that will
    submit+complete
- Need rust/tokio awaitable? Equivalent to `IoUringAwaitable` ? Faster
  than returning a value on a oneshot channel?
  - oneshot channel sounds great, just regen a new one everytime, and
    set "receiver" on IoWrapper
- when N iothreads, how to load-balance IOPS across them?

## Cool long term features

- autoscaling of IO thread pool
  - based on load, number of pending IOs
  - safe to downscale? can delete iouring at runtime?
- buffer pool implementation for disk read/writes
