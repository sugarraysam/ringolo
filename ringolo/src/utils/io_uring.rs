use bitflags::bitflags;

// This is a temporary solution, waiting for release 0.8 and this PR to land
// in `io_uring` crate upstream: https://github.com/tokio-rs/io-uring/pull/371

// Values copied from:
// https://github.com/tokio-rs/io-uring/blob/master/src/sys/sys_x86_64.rs#L138-L142
const IORING_CQE_F_BUFFER: u32 = 1;
const IORING_CQE_F_MORE: u32 = 2;
const IORING_CQE_F_SOCK_NONEMPTY: u32 = 4;
const IORING_CQE_F_NOTIF: u32 = 8;
const IORING_CQE_F_BUF_MORE: u32 = 16;

bitflags!(
    /// Request specific information carried in CQE flags field.
    /// See man page for complete description:
    /// https://man7.org/linux/man-pages/man7/io_uring.7.html
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct CompletionFlags: u32 {
        /// If set, the upper 16 bits of the flags field carries the
        /// buffer ID that was chosen for this request. The request
        /// must have been issued with IOSQE_BUFFER_SELECT set, and
        /// used with a request type that supports buffer selection.
        /// Additionally, buffers must have been provided upfront
        /// either via the IORING_OP_PROVIDE_BUFFERS or the
        /// IORING_REGISTER_PBUF_RING methods.
        const BUFFER = IORING_CQE_F_BUFFER;

        /// If set, the application should expect more completions from
        /// the request. This is used for requests that can generate
        /// multiple completions, such as multi-shot requests, receive,
        /// or accept.
        const MORE = IORING_CQE_F_MORE;

        /// If set, upon receiving the data from the socket in the
        /// current request, the socket still had data left on
        /// completion of this request.
        const SOCK_NONEMPTY = IORING_CQE_F_SOCK_NONEMPTY;

        /// Set for notification CQEs, as seen with the zero-copy
        /// networking send and receive support.
        const NOTIF = IORING_CQE_F_NOTIF;

        /// If set, the buffer ID set in the completion will get more
        /// completions. This means that the provided buffer has been
        /// partially consumed and there's more buffer space left, and
        /// hence the application should expect more completions with
        /// this buffer ID. Each completion will continue where the
        /// previous one left off. This can only happen if the provided
        /// buffer ring has been setup with IOU_PBUF_RING_INC to allow
        /// for incremental / partial consumption of buffers.
        const BUF_MORE = IORING_CQE_F_BUF_MORE;
    }
);

impl From<u32> for CompletionFlags {
    fn from(value: u32) -> Self {
        Self::from_bits_retain(value)
    }
}
