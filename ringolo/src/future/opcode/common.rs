use std::os::fd::RawFd;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Fd {
    Unregistered(RawFd),
    Registered(u32),
}
