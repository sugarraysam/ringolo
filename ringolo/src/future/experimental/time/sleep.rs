use crate::future::opcodes::{Timeout, TimeoutBuilder};
use anyhow::Result;
use io_uring::types::Timespec;
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

pin_project! {
    pub struct Sleep {
        #[pin]
        inner: Timeout,
    }
}

impl Sleep {
    pub fn try_new(when: Duration) -> Result<Self> {
        let timespec = Timespec::new()
            .sec(when.as_secs())
            .nsec(when.subsec_nanos());

        Ok(Self {
            inner: TimeoutBuilder::new(&timespec).build(),
        })
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        let res = ready!(this.inner.poll(cx));

        if let Ok((_, res)) = &res {
            match res {
                Ok(_) => Poll::Ready(()),
                // -62 ETIME is a success case as the kernel is telling us the
                // timeout occurred and triggered the completion event.
                Err(res) if res.raw_os_error() == Some(libc::ETIME) => Poll::Ready(()),
                Err(e) => panic!("Unexpected sleep error: {:?}", e),
            }
        } else {
            panic!("Unexpected sleep error: {:?}", res.unwrap_err());
        }
    }
}
