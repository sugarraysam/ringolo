use crate::future::lib::{AsRawOrDirect, OpcodeError, OwnedUringFd};
use crate::future::lib::{OpPayload, single::*};
use crate::sqe::list::{SqeBatchBuilder, SqeChainBuilder, SqeListBuilder};
use crate::sqe::{IoError, Sqe, SqeList, SqeListKind};
use paste::paste;
use pin_project::{pin_project, pinned_drop};
use std::mem::MaybeUninit;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

// Defines a top-level operation enum and its corresponding output enum.
//
// This macro generates:
// 1. The `OutputEnum` with variants matching the `-> OutputType`.
// 2. The `OpEnum<T>` with `#[pin_project]`, using `paste` to create the projection.
// 3. The `impl OpPayload for OpEnum<T>` block, delegating calls.
// 4. All required `impl From<Variant> for OpEnum<T>` implementations.
macro_rules! define_any_op {
    (
        // The name for the main operation enum, e.g., AnyOp
        op_enum: $OpEnum:ident,
        // The name for the output enum, e.g., AnyOpOutput
        output_enum: $OutputEnum:ident,
        // List of variants: VariantName(VariantType) -> OutputType
        variants: [
            $(
                $Variant:ident($VariantType:ty) -> $OutputType:ty
            ),*
            $(,)? // Allow trailing comma
        ]
    ) => {
        // Use `paste` to wrap the entire expansion, allowing us to
        // create identifiers like `AnyOpProj` from `AnyOp`.
        paste! {
            #[allow(dead_code)]
            #[derive(Debug)]
            pub enum $OutputEnum {
                $(
                    $Variant($OutputType),
                )*
            }

            /// AnyOp is an enum wrapper around all valid operations that can be used to
            /// build a batch or a chain of ops. The contract for batch and chain is that
            /// they will be submitted in the same `io_uring_enter` syscall, and only completes
            /// once all operations have finished. You can achieve the same thing by writing
            /// N ops but using the batch or chain API is *way more* efficient because:
            /// - All ops are submitted in a single `io_uring_enter` syscall
            /// - A single task is created on the executor instead of N tasks
            /// - We use a shared countdown counter and only wake the task when all ops are
            ///   done, instead of waking up N tasks one time.
            /// - The chain leverages `io_uring`'s chain API.

            #[allow(dead_code)]
            #[derive(Debug)]
            #[pin_project(project = [<$OpEnum Proj>])] // e.g., creates `AnyOpProj`
            pub enum $OpEnum<T: AsRawOrDirect> {
                $(
                    $Variant(#[pin] $VariantType),
                )*
            }

            // OpPayload generated impl.
            impl<T: AsRawOrDirect> OpPayload for $OpEnum<T> {
                type Output = $OutputEnum;

                fn create_entry(self: Pin<&mut Self>) -> Result<io_uring::squeue::Entry, OpcodeError> {
                    match self.project() {
                        $(
                            // Use the generated projection name
                            [<$OpEnum Proj>]::$Variant(op) => op.create_entry(),
                        )*
                    }
                }

                fn into_output(
                    self: Pin<&mut Self>,
                    result: Result<i32, IoError>,
                ) -> Result<Self::Output, IoError> {
                    match self.project() {
                        $(
                            // Use generated projection and map to the OutputEnum variant
                            [<$OpEnum Proj>]::$Variant(op) => {
                                op.into_output(result).map($OutputEnum::$Variant)
                            }
                        )*
                    }
                }
            }

            // Generated From impls.
            $(
                impl<T: AsRawOrDirect + Unpin> From<$VariantType> for $OpEnum<T> {
                    fn from(op: $VariantType) -> Self {
                        $OpEnum::$Variant(op)
                    }
                }
            )*

            // Not all AnyOp<T> are safe to send between threads at all times.
            // But this bound is required by `ringolo::spawn` so we have to impl.
            // The runtime prevents most cases where sending tasks between workers
            // is  dangerous.
            unsafe impl<T: AsRawOrDirect> Send for $OpEnum<T> {}
        } // end paste!
    };
}

// Generates a static dispatch enum to allow heterogeneous arbitrary length
// chains and batches of SQEs.
//
// Why static dispatch enum?
// - We could use `Vec<Box<dyn T>>` but we then need to heap-alloc every Op and
//   pay the price of dynamic dispatch.
// - The downside of static enum dispatch is that the size of AnyOp is the size
//   of it's largest variant, which is a bit wasteful. Still think it's better
//   than heap-alloc + dynamic dispatch.
//
// To add a variant, simply insert `$VariantName($OpType) -> Output` in the
// `variants [ ... ]` list below.
define_any_op! {
    op_enum: AnyOp,
    output_enum: AnyOpOutput,
    variants: [
        AsyncCancel(AsyncCancel) -> i32,
        Accept(Accept<T>) -> (OwnedUringFd, Option<SocketAddr>),
        Bind(Bind<T>) -> (),
        Close(Close) -> i32,
        Connect(Connect<T>) -> (),
        Listen(Listen<T>) -> (),
        Nop(Nop) -> (),
        Socket(Socket) -> OwnedUringFd,
        SetSockOpt(SetSockOpt<T>) -> (),
        Timeout(Timeout) -> (),
        TimeoutRemove(TimeoutRemove) -> i32,
    ]
}

#[pin_project(PinnedDrop)]
pub(crate) struct OpList<T: AsRawOrDirect + Unpin> {
    #[pin]
    ops: Vec<AnyOp<T>>,
    kind: SqeListKind,

    #[pin]
    backend: MaybeUninit<Sqe<SqeList>>,

    initialized: bool,
    dropped: bool,
}

impl<T: AsRawOrDirect + Unpin> OpList<T> {
    pub fn new_batch(ops: Vec<AnyOp<T>>) -> Self {
        Self::new(ops, SqeListKind::Batch)
    }

    pub fn new_chain(ops: Vec<AnyOp<T>>) -> Self {
        Self::new(ops, SqeListKind::Chain)
    }

    fn new(ops: Vec<AnyOp<T>>, kind: SqeListKind) -> Self {
        Self {
            ops,
            kind,
            backend: MaybeUninit::uninit(),
            initialized: false,
            dropped: false,
        }
    }

    fn create_params(self: Pin<&mut Self>) -> Result<SqeList, IoError> {
        match self.kind {
            SqeListKind::Batch => self.create_params_with_builder(SqeBatchBuilder::new()),
            SqeListKind::Chain => self.create_params_with_builder(SqeChainBuilder::new()),
        }
    }

    fn create_params_with_builder<B: SqeListBuilder>(
        self: Pin<&mut Self>,
        builder: B,
    ) -> Result<SqeList, IoError> {
        let mut this = self.project();

        let builder = this.ops.iter_mut().try_fold(builder, |b, op| {
            // SAFETY: The `OpList` future is pinned, which means its `ops` Vec
            // lives at a stable memory location. It is safe to unpack every
            // self-referential `op` struct as long as we don't reallocate
            // `this.ops`'s Vec.
            let entry = Pin::new(op).create_entry()?;
            Ok(b.add_entry(entry, None))
        });

        builder.map(|b| b.build())
    }
}

impl<T: AsRawOrDirect + Unpin> Future for OpList<T> {
    type Output = Result<Vec<Result<AnyOpOutput, IoError>>, IoError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.initialized {
            let backend = Sqe::new(self.as_mut().create_params()?);

            let mut this = self.as_mut().project();
            this.backend.write(backend);
            *this.initialized = true;
        }

        // Safety: The logic above guarantees that `this.backend` has been initialized.
        let mut this = self.project();
        let backend = Pin::new(unsafe { this.backend.assume_init_mut() });

        let results = ready!(backend.poll(cx));

        if let Err(e) = results {
            return Poll::Ready(Err(e));
        }

        let outputs = this
            .ops
            .iter_mut()
            .zip(results.unwrap())
            .map(|(op, res)| -> Result<AnyOpOutput, IoError> {
                // Safety: same as above.
                let pinned_op = Pin::new(op);
                pinned_op.into_output(res.map_err(IoError::from))
            })
            .collect::<Vec<_>>();

        Poll::Ready(Ok(outputs))
    }
}

#[pinned_drop]
impl<T: AsRawOrDirect + Unpin> PinnedDrop for OpList<T> {
    fn drop(mut self: Pin<&mut Self>) {
        let mut this = self.project();
        let dropped = std::mem::replace(this.dropped, true);

        // We must manually drop the Sqe<SqeList> if it was initialized. Make
        // sure we invoke the backend destructor only once with the dropped flag.
        if *this.initialized && !dropped {
            // SAFETY: We know the backend was initialized and has not been dropped.
            unsafe { this.backend.assume_init_drop() };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as ringolo;
    use crate::future::lib::{
        Accept, AnySockOpt, Bind, KernelFdMode, Listen, Op, ReceiveTimeout, ReuseAddr, ReusePort,
        TcpNoDelay,
    };
    use crate::task::JoinHandle;
    use crate::test_utils::*;
    use crate::{any_extract, any_extract_all, any_vec};
    use anyhow::{Context, Result};
    use nix::sys::socket::AddressFamily;
    use rstest::rstest;
    use std::net::IpAddr;

    #[rstest]
    #[case::legacy_ipv4(KernelFdMode::Legacy, AddressFamily::Inet, LOCALHOST4, 9100)]
    #[case::legacy_ipv6(KernelFdMode::Legacy, AddressFamily::Inet6, LOCALHOST6, 9101)]
    #[case::auto_ipv4(KernelFdMode::DirectAuto, AddressFamily::Inet, LOCALHOST4, 9102)]
    #[case::auto_ipv6(KernelFdMode::DirectAuto, AddressFamily::Inet6, LOCALHOST6, 9103)]
    #[ringolo::test]
    async fn test_chain_socket_bind_listen_accept(
        #[case] mode: KernelFdMode,
        #[case] addr_family: AddressFamily,
        #[case] ip_addr: IpAddr,
        #[case] port: u16,
    ) -> Result<()> {
        let sock_addr = SocketAddr::new(ip_addr, port);

        // Spawn connect
        let handle: JoinHandle<Result<()>> = ringolo::spawn(async move {
            let sockfd = tcp_socket(mode, addr_family)
                .await
                .context("failed to create socket")?;
            let sockfd_ref = sockfd.borrow();

            let mut max_retries = 3;

            // We have to loop as we will get ECONNREFUSED until Listen
            loop {
                match Op::new(Connect::new(sockfd_ref, &sock_addr)).await {
                    Ok(()) => break,
                    Err(e) => {
                        if e.raw_os_error() == Some(libc::ECONNREFUSED) {
                            max_retries -= 1;
                            if max_retries == 0 {
                                break;
                            }
                            continue;
                        } else {
                            panic!("unexpected connect error: {:?}", e);
                        }
                    }
                }
            }

            Ok(())
        });

        let listener = tcp_socket(mode, addr_family)
            .await
            .context("failed to create socket")?;
        let listener_ref = listener.borrow();

        let results = OpList::new_chain(any_vec![
            SetSockOpt::new(listener_ref, ReuseAddr::new(true)),
            Bind::new(listener_ref, &sock_addr),
            Listen::new(listener_ref, 128),
            Accept::new(listener_ref, mode, true, None),
        ])
        .await
        .context("failed to await chain")?;

        let (sockopt_res, bind_res, listen_res, accept_res) =
            any_extract_all!(results, SetSockOpt, Bind, Listen, Accept);

        let _ = sockopt_res.context("setsockopt res")?;
        let _ = bind_res.context("bind res")?;
        let _ = listen_res.context("listen res")?;
        let (recvfd, got_addr) = accept_res.context("accept res")?;

        assert_eq!(recvfd.strong_count(), 1);
        assert!(got_addr.is_some());
        assert_eq!(got_addr.unwrap().ip(), ip_addr);

        let _ = handle.await.context("connect failed")?;

        Ok(())
    }

    #[rstest]
    #[case::legacy_ipv4(
        KernelFdMode::Legacy,
        AddressFamily::Inet,
        vec![
            ReuseAddr::new(true).into(),
            ReusePort::new(true).into(),
        ]
    )]
    #[case::legacy_ipv6(
        KernelFdMode::Legacy,
        AddressFamily::Inet6,
        vec![
            ReuseAddr::new(true).into(),
            ReusePort::new(true).into(),
            TcpNoDelay::new(true).into(),
            ReceiveTimeout::new(libc::timeval{tv_sec: 1, tv_usec: 0}).into(),
        ]
    )]
    #[case::auto_ipv4(
        KernelFdMode::DirectAuto,
        AddressFamily::Inet,
        vec![
            ReuseAddr::new(true).into(),
            ReusePort::new(true).into(),
        ]
    )]
    #[case::auto_ipv6(
        KernelFdMode::DirectAuto,
        AddressFamily::Inet6,
        vec![
            ReuseAddr::new(true).into(),
            ReusePort::new(true).into(),
            TcpNoDelay::new(true).into(),
            ReceiveTimeout::new(libc::timeval{tv_sec: 1, tv_usec: 0}).into(),
        ]
    )]
    #[ringolo::test]
    async fn test_batch_setsockopt(
        #[case] mode: KernelFdMode,
        #[case] addr_family: AddressFamily,
        #[case] sockopts: Vec<AnySockOpt>,
    ) -> Result<()> {
        let expected_len = sockopts.len();

        let sockfd = tcp_socket(mode, addr_family)
            .await
            .context("failed to create socket")?;
        let sockfd_ref = sockfd.borrow();

        let results = OpList::new_batch(
            sockopts
                .into_iter()
                .map(|o| SetSockOpt::new(sockfd_ref, o).into())
                .collect::<Vec<_>>(),
        )
        .await
        .context("failed to await batch")?;

        assert_eq!(results.len(), expected_len);

        for res in results {
            assert!(matches!(any_extract!(res, SetSockOpt), Ok(_)));
        }

        Ok(())
    }
}
