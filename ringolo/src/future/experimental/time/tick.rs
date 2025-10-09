use crate::sqe::{SqeStream, SqeStreamError};
use anyhow::{Result, anyhow};
use futures::Stream;
use io_uring::opcode::Timeout;
use io_uring::types::{TimeoutFlags, Timespec};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

// TODO: pin_project impl drop already?
pin_project! {
    pub struct Tick {
        #[pin]
        inner: SqeStream,

        // Will be used in drop impl to determinie if we need to cancel the
        // MULTISHOT inner Timeout.
        finished: bool,
    }
}

impl Tick {
    pub fn try_new(freq: Duration, count: Option<u32>) -> Result<Self> {
        if count.map_or(false, |c| c <= 1) {
            return Err(anyhow!("Specify a count that is > 1 you silly goose"));
        }

        let timespec = Timespec::new()
            .sec(freq.as_secs())
            .nsec(freq.subsec_nanos());

        let timeout = {
            let mut t = Timeout::new(&timespec);

            if let Some(c) = count {
                t = t.count(c);
            }

            t.flags(TimeoutFlags::MULTISHOT).build()
        };

        Ok(Self {
            inner: SqeStream::try_new(timeout, count)?,
            finished: false,
        })
    }
}

impl Drop for Tick {
    fn drop(&mut self) {
        if !self.finished {
            // TODO:
            // - use ctx.scheduler, enqueue new task with `is_new==false`, so it lands
            //   on this worker (this thread) OR access worker directly? `add_local_task`?
            //     -> TASK CANNOT BE STEALABLE
            // - more involved than it looks like
            // - log errors async (eprintln) or even panic
            let _user_data = self.inner.get_idx();
            unimplemented!("TODO");
        }
    }
}

impl Stream for Tick {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = ready!(this.inner.poll_next(cx));

        // Stream successfully completed.
        if res.is_none() {
            *this.finished = true;
            return Poll::Ready(None);
        }

        // Safety: we just checked for none
        match res.unwrap() {
            Ok(_) => Poll::Ready(Some(())),

            // -62 ETIME is a success case as the kernel is telling us the
            // timeout occurred and triggered the completion event.
            Err(SqeStreamError::Io(e)) if e.raw_os_error() == Some(libc::ETIME) => {
                Poll::Ready(Some(()))
            }

            Err(e) => panic!("Unexpected tick error: {:?}", e),
        }
    }
}
