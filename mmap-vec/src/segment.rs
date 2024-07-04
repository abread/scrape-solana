use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    mem::{self, MaybeUninit},
    ops::{Deref, DerefMut},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    ptr, slice,
    sync::atomic::Ordering,
};

use crate::{
    stats::{COUNT_ACTIVE_SEGMENT, COUNT_FTRUNCATE_FAILED, COUNT_MMAP_FAILED, COUNT_MUNMAP_FAILED},
    utils::{check_zst, page_size},
};

/// Segment is a constant slice of type T that is memory mapped to disk.
///
/// It is the basic building block of memory mapped data structure.
///
/// It cannot growth / shrink.
pub struct Segment<T> {
    pub(crate) addr: ptr::NonNull<T>,
    meta: SegmentMetadata,
    meta_path: Option<PathBuf>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for Segment<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let addr = if self.addr == ptr::NonNull::dangling() {
            ptr::null_mut()
        } else {
            self.addr.as_ptr()
        };

        f.debug_struct("Segment")
            .field("addr", &addr)
            .field("len", &self.meta.len)
            .field("capacity", &self.meta.capacity)
            //.field("meta_path", &self.meta_path)
            .finish()
    }
}

#[derive(Debug, Default, Clone)]
struct SegmentMetadata {
    len: usize,
    capacity: usize,
}

#[repr(C, packed)]
struct SegmentMetadataRepr {
    len: u64,
    capacity: u64,
}

impl<T> Segment<T> {
    /// Create a zero size segment.
    #[inline(always)]
    pub const fn null() -> Self {
        check_zst::<T>();
        Self {
            addr: ptr::NonNull::dangling(),
            meta: SegmentMetadata {
                len: 0,
                capacity: 0,
            },
            meta_path: None,
        }
    }

    /// Memory map a segment to disk.
    ///
    /// File will be created and init with computed capacity.
    pub fn open_rw<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        check_zst::<T>();
        if capacity == 0 {
            return Ok(Self::null());
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // Fill the file with 0
        unsafe { ftruncate::<T>(&file, capacity) }?;

        // Map the block
        let addr = unsafe { mmap(&file, capacity) }?;
        Ok(Self {
            addr,
            meta: SegmentMetadata { len: 0, capacity },
            meta_path: None,
        })
    }

    /// Currently used segment size.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.meta.capacity as usize
    }

    /// Shortens the segment, keeping the first `new_len` elements and dropping
    /// the rest.
    pub fn truncate(&mut self, new_len: usize) {
        if new_len > self.meta.len as usize {
            return;
        }

        unsafe {
            let remaining_len = self.meta.len - new_len;
            let items =
                ptr::slice_from_raw_parts_mut(self.addr.as_ptr().add(new_len), remaining_len);
            self.set_len(new_len);
            self.sync_meta().unwrap();
            ptr::drop_in_place(items);
        }
    }

    /// Remove `delete_count` element at beginning of the segment.
    ///
    /// Element will be drop in place.
    ///
    /// If delete count is greater than the segment len, then this call will be
    /// equivalent to calling `clear` function.
    pub fn truncate_first(&mut self, delete_count: usize) {
        let new_len = self
            .meta
            .len
            .saturating_add_signed(-(delete_count as isize));
        if new_len == 0 {
            self.clear()
        } else {
            unsafe {
                let items = slice::from_raw_parts_mut(self.addr.as_ptr(), delete_count);
                ptr::drop_in_place(items);
                ptr::copy(
                    self.addr.as_ptr().add(delete_count),
                    self.addr.as_ptr(),
                    new_len,
                );
                self.set_len(new_len);
                self.sync_meta().unwrap();
            }
        }
    }

    /// Clears the segment, removing all values.
    #[inline]
    pub fn clear(&mut self) {
        unsafe {
            let items = slice::from_raw_parts_mut(self.addr.as_ptr(), self.meta.len);
            self.set_len(0);
            self.sync_meta().unwrap();
            ptr::drop_in_place(items);
        }
    }

    /// Forces the length of the segment to `new_len`.
    #[allow(clippy::missing_safety_doc)]
    #[inline(always)]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity());
        self.meta.len = new_len;
    }

    /// Bytes use on disk for this segment.
    #[inline(always)]
    pub fn disk_size(&self) -> usize {
        self.meta.capacity * mem::size_of::<T>()
    }

    /// Try to add new element to the segment.
    ///
    /// If the segment is already full, value will be return in `Err`.
    #[inline]
    pub fn push_within_capacity(&mut self, value: T) -> Result<(), T> {
        if self.meta.len == self.meta.capacity {
            return Err(value);
        }

        unsafe {
            let dst = self.addr.as_ptr().add(self.meta.len);
            ptr::write(dst, value);
        }

        self.meta.len += 1;
        Ok(())
    }

    /// Remove last element of the segment and reduce its capacity.
    ///
    /// Value will be return if segment is not empty.
    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.meta.len == 0 {
            return None;
        }

        self.meta.len -= 1;
        self.sync_meta().unwrap();
        unsafe {
            let src = self.addr.as_ptr().add(self.meta.len);
            Some(ptr::read(src))
        }
    }

    /// Move data contained in `other` segment to the end of current segment.
    ///
    /// ```rust
    /// # use mmap_vec::Segment;
    /// let mut s1 = Segment::<i32>::open_rw("test_extend_from_segment_1.seg", 2).unwrap();
    /// let mut s2 = Segment::<i32>::open_rw("test_extend_from_segment_2.seg", 5).unwrap();
    ///
    /// s1.push_within_capacity(7);
    /// s1.push_within_capacity(-3);
    /// s2.push_within_capacity(-4);
    /// s2.push_within_capacity(37);
    ///
    /// assert_eq!(&s1[..], [7, -3]);
    /// assert_eq!(&s2[..], [-4, 37]);
    ///
    /// s2.extend_from_segment(s1);
    /// assert_eq!(&s2[..], [-4, 37, 7, -3]);
    ///
    /// # let _ = std::fs::remove_file("test_extend_from_segment_1.seg");
    /// # let _ = std::fs::remove_file("test_extend_from_segment_2.seg");
    /// ```
    pub fn extend_from_segment(&mut self, mut other: Segment<T>) -> io::Result<()> {
        if other.len() == 0 {
            return Ok(()); // nothing to copy
        }

        if self.capacity() < self.len() + other.len() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "segment too small for new data",
            ));
        }

        let new_len = self.len() + other.len();
        unsafe {
            ptr::copy_nonoverlapping(
                other.addr.as_ptr(),
                self.addr.as_ptr().add(self.len()),
                other.len(),
            );
            self.set_len(new_len);
            other.set_len(0);
        };

        Ok(())
    }

    /// Inform the kernel that the complete segment will be access in a near future.
    ///
    /// All underlying pages should be load in RAM.
    ///
    /// This function is only a wrapper above `libc::madvise`.
    ///
    /// Will panic if `libc::madvise` return an error.
    pub fn advice_prefetch_all_pages(&self) {
        if self.addr == ptr::NonNull::dangling() || self.meta.len == 0 {
            return;
        }

        let madvise_code = unsafe {
            libc::madvise(
                self.addr.as_ptr().cast(),
                self.meta.len * mem::size_of::<T>(),
                libc::MADV_WILLNEED,
            )
        };
        assert_eq!(
            madvise_code,
            0,
            "madvise error: {}",
            io::Error::last_os_error()
        );
    }

    /// Inform the kernel that underlying page for `index` will be access in a near future.
    ///
    /// This function is only a wrapper above `libc::madvise`.
    pub fn advice_prefetch_page_at(&self, index: usize) {
        if self.addr == ptr::NonNull::dangling() || index >= self.meta.len {
            return;
        }

        let page_size = page_size();
        let page_mask = !(page_size.wrapping_add_signed(-1));

        let madvise_code = unsafe {
            libc::madvise(
                (self.addr.as_ptr().add(index) as usize & page_mask) as *mut libc::c_void,
                page_size,
                libc::MADV_WILLNEED,
            )
        };
        assert_eq!(
            madvise_code,
            0,
            "madvise error: {}",
            io::Error::last_os_error()
        );
    }

    pub(crate) fn is_persistent(&self) -> bool {
        self.meta_path.is_some()
    }

    /// Sync mmap vec to disk.
    pub(crate) fn sync(&self) -> io::Result<()> {
        unsafe {
            libc::msync(self.addr.as_ptr().cast(), self.meta.capacity, libc::MS_SYNC);
        }
        self.sync_meta()?;
        Ok(())
    }

    /// Sync mmap vec metadata (len, capacity) to disk.
    /// Should be used for implementing crash-consistent collections on top of MmapVec.
    pub(crate) fn sync_meta(&self) -> io::Result<()> {
        if let Some(p) = &self.meta_path {
            let mut f = fs::OpenOptions::new().create(true).write(true).open(p)?;

            let m: SegmentMetadataRepr = self.meta.clone().into();
            f.write_all(m.bytes())?;
            f.flush()?;
        }

        Ok(())
    }
}

impl<T: Unpin> Segment<T> {
    /// Memory map a segment to disk.
    ///
    /// File will be created and init with computed capacity.
    pub unsafe fn open_rw_existing<P: AsRef<Path>>(path: P, capacity: usize) -> io::Result<Self> {
        check_zst::<T>();
        if capacity == 0 {
            return Ok(Self::null());
        }

        let meta_path = path.as_ref().with_extension("meta");
        let mut meta_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&meta_path)?;

        let existing_metadata: SegmentMetadata = {
            let mut metadata: MaybeUninit<SegmentMetadataRepr> = MaybeUninit::uninit();

            let metadata_as_slice =
                metadata.as_mut_ptr() as *mut [u8; std::mem::size_of::<SegmentMetadataRepr>()];
            // Safety: metadata has compatible size and alignment with [u8; size_of<SegmentMetadata>]
            let metadata_as_slice = unsafe { &mut *metadata_as_slice };

            match meta_file.read_exact(metadata_as_slice) {
                // Safety: metadata was correctly loaded from file
                Ok(()) => {
                    let mut m: SegmentMetadata = unsafe { metadata.assume_init() }.into();
                    m.capacity = m.capacity.max(capacity);
                    Ok(m)
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(SegmentMetadata {
                    len: 0,
                    capacity: capacity,
                }),
                Err(e) => Err(e),
            }
        }?;

        let mut res = Self::open_rw(path, existing_metadata.capacity as usize)?;
        dbg!(&existing_metadata);
        res.meta = existing_metadata;
        res.meta_path = Some(meta_path);
        Ok(res)
    }
}

impl<T> Deref for Segment<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.addr.as_ptr(), self.meta.len) }
    }
}

impl<T> DerefMut for Segment<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { slice::from_raw_parts_mut(self.addr.as_ptr(), self.meta.len) }
    }
}

impl<T> Drop for Segment<T> {
    fn drop(&mut self) {
        if self.meta.len > 0 {
            unsafe {
                ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                    self.addr.as_ptr(),
                    self.meta.len,
                ))
            }
        }

        if self.addr != ptr::NonNull::dangling() {
            unsafe { munmap(self.addr, self.meta.capacity) }.expect("unmap error");
        }
    }
}

unsafe impl<T> Send for Segment<T> {}
unsafe impl<T> Sync for Segment<T> {}

unsafe fn ftruncate<T>(file: &File, capacity: usize) -> io::Result<()> {
    check_zst::<T>();
    let segment_size = capacity * mem::size_of::<T>();
    let fd = file.as_raw_fd();

    if libc::ftruncate(fd, segment_size as libc::off_t) != 0 {
        COUNT_FTRUNCATE_FAILED.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

unsafe fn mmap<T>(file: &File, capacity: usize) -> io::Result<ptr::NonNull<T>> {
    check_zst::<T>();
    let segment_size = capacity * mem::size_of::<T>();

    // It is safe to not keep a reference to the initial file descriptor.
    // See: https://stackoverflow.com/questions/17490033/do-i-need-to-keep-a-file-open-after-calling-mmap-on-it
    let fd = file.as_raw_fd();

    let addr = libc::mmap(
        std::ptr::null_mut(),
        segment_size as libc::size_t,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_SHARED,
        fd,
        0,
    );

    if addr == libc::MAP_FAILED {
        COUNT_MMAP_FAILED.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::last_os_error())
    } else {
        COUNT_ACTIVE_SEGMENT.fetch_add(1, Ordering::Relaxed);
        ptr::NonNull::new(addr.cast()).ok_or(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "mmap returned null pointer",
        ))
    }
}

unsafe fn munmap<T>(addr: ptr::NonNull<T>, capacity: usize) -> io::Result<()> {
    check_zst::<T>();
    debug_assert!(capacity > 0);

    let unmap_code = libc::munmap(addr.as_ptr().cast(), capacity * mem::size_of::<T>());

    if unmap_code != 0 {
        COUNT_MUNMAP_FAILED.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::last_os_error())
    } else {
        COUNT_ACTIVE_SEGMENT.fetch_sub(1, Ordering::Relaxed);
        Ok(())
    }
}

impl From<SegmentMetadataRepr> for SegmentMetadata {
    fn from(value: SegmentMetadataRepr) -> Self {
        SegmentMetadata {
            len: value.len as usize,
            capacity: value.capacity as usize,
        }
    }
}

impl From<SegmentMetadata> for SegmentMetadataRepr {
    fn from(value: SegmentMetadata) -> Self {
        SegmentMetadataRepr {
            len: value.len as u64,
            capacity: value.capacity as u64,
        }
    }
}

impl SegmentMetadataRepr {
    fn bytes(&self) -> &[u8] {
        let sptr = self as *const Self;
        unsafe { core::slice::from_raw_parts(sptr as *const _, std::mem::size_of::<Self>()) }
    }
}
