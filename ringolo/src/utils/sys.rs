use std::sync::OnceLock;

pub(crate) const CACHE_LINE_SIZE: usize = 64;

pub(crate) fn get_page_size() -> usize {
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    *PAGE_SIZE.get_or_init(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize })
}
