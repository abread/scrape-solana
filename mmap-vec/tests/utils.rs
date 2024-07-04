use std::sync::OnceLock;
static PAGE_SIZE: OnceLock<usize> = OnceLock::new();

pub fn page_size() -> usize {
    *PAGE_SIZE.get_or_init(|| unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize })
}
