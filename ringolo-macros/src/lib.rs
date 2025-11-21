#![allow(clippy::needless_doctest_main)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]
#![doc(test(
    no_crate_inject,
    attr(deny(warnings, rust_2018_idioms), allow(dead_code, unused_variables))
))]

//! Macros for user with Ringolo (copied from tokio-macros)

mod entry;
mod parse;

/// Marks async function to be executed by the selected runtime. This macro
/// helps set up a `Runtime` without requiring the user to use Runtime or Builder.
///
/// Note: This macro is designed to be simplistic and targets applications that
/// do not require a complex setup. If the provided functionality is not
/// sufficient, you may be interested in using `runtime::Builder` which provides
/// a more powerful interface.
///
/// # Non-worker async function
///
/// Note that the async function marked with this macro does not run as a
/// worker. The expectation is that other tasks are spawned by the function here.
/// Awaiting on other futures from the function provided here will not
/// perform as fast as those spawned as workers.
///
/// # Runtime flavors
///
/// The macro can be configured with a `flavor` parameter to select
/// different runtime configurations.
///
/// ## Task Stealing Runtime (multi-thread)
///
/// To use the task stealing runtime, the macro can be configured using
///
/// ```no_run
/// #[ringolo::main(flavor = "stealing", worker_threads = 10)]
/// # async fn main() {}
/// ```
///
/// The `worker_threads` option configures the number of worker threads, and
/// defaults to the number of cpus on the system. This is the default flavor.
///
/// ## Local (current-thread)
///
/// To use the local runtime the macro can be configured using
///
/// ```rust
/// #[ringolo::main(flavor = "local")]
/// # async fn main() {}
/// ```
///
/// # Function arguments
///
/// Function arguments are NOT allowed.
///
/// # Usage
///
/// ## Using the stealing runtime (default runtime)
///
/// ```no_run
/// #[ringolo::main]
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
///
/// Equivalent code not using `#[ringolo::main]`
///
/// ```no_run
/// fn main() {
///     ringolo::runtime::Builder::new_stealing()
///         .try_build()
///         .unwrap()
///         .block_on(async {
///             println!("Hello world");
///         })
/// }
/// ```
///
/// ## Using the local runtime
///
/// ```rust
/// #[ringolo::main(flavor = "local")]
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
///
/// Equivalent code not using `#[ringolo::main]`
///
/// ```rust
/// fn main() {
///     ringolo::runtime::Builder::new_local()
///         .try_build()
///         .unwrap()
///         .block_on(async {
///             println!("Hello world");
///         })
/// }
/// ```
///
/// ## Set number of worker threads
///
/// ```no_run
/// #[ringolo::main(worker_threads = 2)]
/// async fn main() {
///     println!("Hello world");
/// }
/// ```
///
/// Equivalent code not using `#[ringolo::main]`
///
/// ```no_run
/// fn main() {
///     ringolo::runtime::Builder::new_stealing()
///         .worker_threads(2)
///         .try_build()
///         .unwrap()
///         .block_on(async {
///             println!("Hello world");
///         })
/// }
/// ```
#[proc_macro_attribute]
pub fn main(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    entry::main(args.into(), item.into()).into()
}

/// Marks async function to be executed by runtime, suitable to test environment.
/// This macro helps set up a `Runtime` without requiring the user to use `Runtime`
/// or `Builder` directly.
///
/// Note: This macro is designed to be simplistic and targets applications that
/// do not require a complex setup. If the provided functionality is not
/// sufficient, you may be interested in using `Builder` which provides a more
/// powerful interface.
///
/// # Stealing runtime
///
/// To use the multi-threaded runtime, the macro can be configured using
///
/// ```no_run
/// #[ringolo::test(flavor = "stealing", worker_threads = 1)]
/// async fn my_test() {
///     assert!(true);
/// }
/// ```
///
/// The `worker_threads` option configures the number of worker threads, and
/// defaults to the number of cpus on the system.
///
/// # Local runtime
///
/// The default test runtime is single-threaded. Each test gets a
/// separate local runtime.
///
/// ```no_run
/// #[ringolo::test]
/// async fn my_test() {
///     assert!(true);
/// }
/// ```
///
/// ## Usage
///
/// ### Using the stealing runtime
///
/// ```no_run
/// #[ringolo::test(flavor = "stealing")]
/// async fn my_test() {
///     assert!(true);
/// }
/// ```
///
/// Equivalent code not using `#[ringolo::test]`
///
/// ```no_run
/// #[test]
/// fn my_test() {
///     ringolo::runtime::Builder::new_stealing()
///         .try_build()
///         .unwrap()
///         .block_on(async {
///             assert!(true);
///         })
/// }
/// ```
///
/// ### Using local runtime
///
/// ```no_run
/// #[ringolo::test]
/// async fn my_test() {
///     assert!(true);
/// }
/// ```
///
/// Equivalent code not using `#[ringolo::test]`
///
/// ```no_run
/// #[test]
/// fn my_test() {
///     ringolo::runtime::Builder::new_local()
///         .try_build()
///         .unwrap()
///         .block_on(async {
///             assert!(true);
///         })
/// }
/// ```
///
/// ### Set number of worker threads
///
/// ```no_run
/// #[ringolo::test(flavor = "stealing", worker_threads = 2)]
/// async fn my_test() {
///     assert!(true);
/// }
/// ```
///
/// Equivalent code not using `#[ringolo::test]`
///
/// ```no_run
/// #[test]
/// fn my_test() {
///     ringolo::runtime::Builder::new_stealing()
///         .worker_threads(2)
///         .try_build()
///         .unwrap()
///         .block_on(async {
///             assert!(true);
///         })
/// }
/// ```
#[proc_macro_attribute]
pub fn test(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    entry::test(args.into(), item.into()).into()
}
