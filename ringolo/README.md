# Ringolo - An `io_uring` Async Runtime

[![Rust](https://github.com/sugarraysam/ringolo/actions/workflows/rust.yml/badge.svg)](https://github.com/sugarraysam/ringolo/actions/workflows/rust.yml)
[![Crates.io](https://img.shields.io/crates/v/ringolo.svg)](https://crates.io/crates/ringolo)
[![Docs.rs](https://docs.rs/ringolo/badge.svg)](https://docs.rs/ringolo)
[![License](https://img.shields.io/crates/l/ringolo.svg)](https://github.com/sugarraysam/ringolo/blob/main/LICENSE)

Ringolo is a completion-driven asynchronous runtime built on top of `io_uring`. Its task system is derived from Tokio's
battle-tested [task module], which implements a state machine for managing a future's lifecycle.

[task module]: https://docs.rs/tokio/latest/tokio/#working-with-tasks

## Quick Start

```rust
#[ringolo::main]
async fn main() {
    println!("Hello from the runtime!");

    let join_handle = ringolo::spawn(async {
        // ... do some async work ...
        "Hello from a spawned task!"
    });

    let result = join_handle.await.unwrap();
    println!("{}", result);
}
```

## Key Features

### I/O Model: Readiness vs. Completion

A key difference from Tokio is the underlying I/O model. Tokio is **readiness-based**, typically using `epoll`. This
`epoll` context is global and accessible by all threads. Consequently, if a task is stolen by another thread, readiness events remain valid.

Ringolo is **completion-based**, using `io_uring`. This model is fundamentally **thread-local**, as each worker thread
manages its own ring. Many resources, such as registered direct descriptors or provided buffers, are bound to that
specific ring and are invalid on any other thread.

### Work-Stealing and Thread-Local Resources

This thread-local design presents a core challenge for work-stealing. When an I/O operation is submitted on a thread's
ring, its corresponding completion event *must* be processed by the *same thread*.

If a task were migrated to another thread *after* submission but *before* completion, the resulting I/O event would be
delivered to the original thread's ring, but the task would no longer be there to process it. This would lead to lost I/O
and undefined behavior.

Ringolo's work-stealing scheduler is designed around this constraint. It performs resource and pending I/O accounting to
determine when a task is "stealable". View the detailed implementation within the [task header module].

[task header module]: https://docs.rs/ringolo/latest/ringolo/task/struct.Header.html#method.is_stealable

### Structured Concurrency

Another key difference from Tokio is Ringolo's adoption of [Structured Concurrency]. While this can be an overloaded term,
in Ringolo it provides a simple guarantee: **tasks are not allowed to outlive their parent.**

To enforce this, the runtime maintains a [global task tree] to track the task hierarchy. When a parent task exits, all
of its child tasks are automatically cancelled.

This behavior is controlled by the orphan policy. The default policy is [`OrphanPolicy::Enforced`], which is the
recommended setting for most programs. This behavior can be relaxed in two ways:

1.  **Per-Task:** To create a single "detached" task, you can explicitly use [`TaskOpts::BACKGROUND_TASK`]. This option
    bypasses the current task's hierarchy by attaching the new task directly to the `ROOT_NODE` of the task tree.

2.  **Globally:** You can configure the entire runtime with [`OrphanPolicy::Permissive`]. This setting effectively
    disables structured concurrency guarantees for all tasks, but it is not the intended model for typical use.

[Structured Concurrency]: https://en.wikipedia.org/wiki/Structured_concurrency
[global task tree]: https://docs.rs/ringolo/latest/ringolo/task/node/struct.TaskNode.html
[`OrphanPolicy::Enforced`]: https://docs.rs/ringolo/latest/ringolo/runtime/enum.OrphanPolicy.html
[`TaskOpts::BACKGROUND_TASK`]: https://docs.rs/ringolo/latest/ringolo/runtime/spawn/struct.TaskOpts.html
[`OrphanPolicy::Permissive`]: https://docs.rs/ringolo/latest/ringolo/runtime/enum.OrphanPolicy.html

#### Motivation: Safer Cancellation APIs

The primary motivation for this design is to provide powerful and safe cancellation APIs.

Experience with other asynchronous frameworks, including Tokio and [folly/coro], shows that relying on [cancellation tokens]
has significant drawbacks. Manually passing tokens is error-prone, introduces code bloat, and becomes exceptionally
difficult to manage correctly in a large codebase.

Ringolo's design provides a user-friendly way to perform cancellation *without* requiring tokens to be passed throughout
the call stack. The global task tree enables this robust, built-in cancellation model.

For a detailed guide, please see the [cancellation APIs].

[folly/coro]: https://github.com/facebook/folly/tree/main/folly/experimental/coro
[cancellation tokens]: https://github.com/facebook/folly/blob/main/folly/CancellationToken.h#L50-L68
[cancellation APIs]: https://docs.rs/ringolo/latest/ringolo/runtime/cancel/index.html

### Async Cleanup

The thread-local design of `io_uring` also dictates the model for resource cleanup. Certain "leaky" operations, like
[multishot timers], and thread-local resources, like [direct descriptors], must be explicitly unregistered or closed.

This cleanup **must** happen on the same thread's ring that created them. To solve this without blocking on `drop`,
Ringolo uses a [maintenance task] on each worker thread. When a future is dropped, it enqueues an async cleanup operation
with its local maintenance task. This task then batches and submits these operations, ensuring all resources are freed
on the correct thread.

The runtime's behavior on a failed cleanup operation is controlled by the [OnCleanupError] policy.

[multishot timers]: https://docs.rs/ringolo/latest/ringolo/future/lib/struct.TimeoutMultishot.html
[direct descriptors]: https://docs.rs/ringolo/latest/ringolo/future/lib/enum.UringFdKind.html#variant.Fixed
[maintenance task]: https://docs.rs/ringolo/latest/ringolo/context/maintenance/task/struct.MaintenanceTask.html
[OnCleanupError]: https://docs.rs/ringolo/latest/ringolo/context/maintenance/enum.OnCleanupError.html

## Exiting Alpha Status

The current releases will remain in alpha status until the following milestones are met:

- [ ] Support for all [`io_uring` opcodes](https://docs.rs/io-uring/latest/io_uring/opcode/index.html)
- [ ] Support for [Provided Buffers](https://github.com/axboe/liburing/wiki/io_uring-and-networking-in-2023#provided-buffers)
- [ ] Support for Registered Buffers

## License

This project is licensed under the (e.g., MIT OR Apache-2.0) license. See the [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE) files for details.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
