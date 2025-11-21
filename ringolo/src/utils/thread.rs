#![allow(unused)]

use crate::runtime::runtime::ThreadNameFn;
use anyhow::{Result, anyhow};
use std::ffi::CStr;

const MAX_PTHREAD_NAME_LEN: usize = 16;

// Linux uses pthread_setname_np(pthread_t, *const c_char)
// It's limited to 16 bytes, including the null terminator.
#[cfg(target_os = "linux")]
pub(crate) fn set_current_thread_name(thread_name_fn: &ThreadNameFn) {
    let name = thread_name_fn.0();
    let bytes = name.as_bytes();
    let len_to_copy = std::cmp::min(bytes.len(), MAX_PTHREAD_NAME_LEN - 1);

    // Copy the name bytes into our C buffer. The buffer is guaranteed to be
    // null-terminated because it was zero-initialized, and we only wrote to
    // (at most) the first `MAX_PTHREAD_NAME_LEN - 1` bytes.
    let mut c_name_buf: [libc::c_char; MAX_PTHREAD_NAME_LEN] = [0; MAX_PTHREAD_NAME_LEN];
    for i in 0..len_to_copy {
        c_name_buf[i] = bytes[i] as libc::c_char;
    }

    unsafe {
        let thread = libc::pthread_self();
        libc::pthread_setname_np(thread, c_name_buf.as_ptr());
    }
}

/// Gets the name of the current thread.
///
/// This uses `pthread_getname_np` on Linux, which is the counterpart
/// to `pthread_setname_np`.
#[cfg(target_os = "linux")]
pub(crate) fn get_current_thread_name() -> Result<String> {
    // Create a buffer to store the name.
    // It's initialized to zeros, so it's guaranteed to be null-terminated.
    let mut c_name_buf: [libc::c_char; MAX_PTHREAD_NAME_LEN] = [0; MAX_PTHREAD_NAME_LEN];

    let ret = unsafe {
        let thread = libc::pthread_self();
        libc::pthread_getname_np(thread, c_name_buf.as_mut_ptr(), MAX_PTHREAD_NAME_LEN)
    };

    if ret != 0 {
        return Err(anyhow!(
            "pthread_getname_np failed with error code: {}",
            ret
        ));
    }

    // Convert the null-terminated C string back to a Rust String
    let c_str = unsafe { CStr::from_ptr(c_name_buf.as_ptr()) };
    c_str
        .to_str()
        .map(|s| s.to_string())
        .map_err(|e| anyhow!("Failed to convert thread name from CStr: {}", e))
}
