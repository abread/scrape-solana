use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};

/// A fixed capacity array of elements.
pub(crate) struct Chunk<T> {
    capacity: usize,
    len: AtomicUsize,

    // TODO: consider removing indirection and using [T] here. HAS SAFETY IMPLICATIONS IN CHUNK CACHE
    data: NonNull<T>,
}

/// A (de)serializable Chunk, with additional metadata for sanity checking.
///
/// Currently metadata only includes the index of the first chunk element in the overall HugeVec.
#[derive(Serialize, Deserialize)]
pub(crate) struct StoredChunk<T> {
    start_idx: u64,
    capacity: usize,
    // note: we do not have len here because data is a Vec<T> and its length is used instead
    data: Vec<T>,
}

impl<T> StoredChunk<T> {
    pub(crate) fn new(start_idx: u64, capacity: usize) -> Self {
        Self {
            start_idx,
            capacity,
            data: Vec::with_capacity(capacity),
        }
    }

    /// Copy chunk into a new StoredChunk.
    ///
    /// # Safety
    /// Caller must ensure that existing chunk elements are not concurrently modified.
    pub(crate) unsafe fn from_chunk_ref(chunk: &Chunk<T>, start_idx: u64) -> Self
    where
        T: Clone,
    {
        Self {
            start_idx,
            capacity: chunk.capacity,
            data: chunk.as_slice().to_vec(),
        }
    }

    pub(crate) fn from_chunk(chunk: Chunk<T>, start_idx: u64) -> Self {
        let capacity = chunk.capacity;
        let data = chunk.into_vec();
        Self {
            start_idx,
            capacity,
            data,
        }
    }

    pub(crate) fn into_chunk(self) -> Chunk<T> {
        Chunk::from_iter(self.capacity, self.data.into_iter())
    }
}

impl<T> Chunk<T> {
    /// Create a new empty chunk with given capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity * std::mem::size_of::<T>() < isize::MAX as usize); // should always hold
        assert_ne!(
            std::mem::size_of::<T>(),
            0,
            "chunks of zero-sized types are not supported"
        );

        let alloc_layout = Layout::array::<T>(capacity).unwrap();
        let storage = unsafe { std::alloc::alloc(alloc_layout) as *mut T };
        let storage = NonNull::new(storage).expect("alloc fail");

        Self {
            capacity,
            len: AtomicUsize::new(0),
            data: storage,
        }
    }

    /// Create a chunk with given capacity from an existing vector.
    /// Panics if the vector has more than `capacity` elements.
    pub(crate) fn from_vec(capacity: usize, vec: Vec<T>) -> Self {
        // TODO: reuse allocation? almost certainly requires calling realloc when Vec::capacity != capacity.
        Self::from_iter(capacity, vec.into_iter())
    }

    /// Create a new chunk with given capacity and fill it with values from an iterator.
    /// Panics if the iterator has more than `capacity` elements.
    pub(crate) fn from_iter<It: Iterator<Item = T> + ExactSizeIterator>(
        capacity: usize,
        values: It,
    ) -> Self {
        assert!(values.len() <= capacity);
        let chunk = Self::new(capacity);

        // move values to chunk
        for v in values {
            // Safety:
            // - push will not be called concurrently (we have the only reference to chunk)
            // - chunk is not full (values.len() <= capacity)
            unsafe {
                chunk.push(v);
            }
        }

        chunk
    }

    /// Insert a value at the end of the chunk.
    ///
    /// # Safety
    /// Caller must ensure chunk is not full and that this method is not called concurrently with itself.
    pub(crate) unsafe fn push(&self, value: T) {
        let len = self.len.load(Ordering::SeqCst);

        // TODO: make it a debug_assert?
        assert!(len < self.capacity, "attempted to push beyond chunk limits");

        // Safety: len < capacity (< isize::MAX), see Chunk::new
        let ptr = unsafe { self.data.offset(len as isize) };

        // Safety:
        // - pointer is aligned (guaranteed in alloc)
        // - pointer is non-null (guaranteed by type)
        // - pointer has appropriate provenance (derived from alloc)
        // - points to a valid T (guaranteed by push and caller guarantees, idx is within pushed bounds)
        // - respects aliasing:
        //     * no other reference to the same element exists (guaranteed by caller)
        //     * no other accesses exist (guaranteed by caller - not mid-push, and by Chunk API)
        // Bonus: self.storage.offset(self.len) is uninitialized, so there's no T to drop.
        unsafe {
            ptr.write(value);
        }

        // TODO: make it a debug_assert and just increment len?
        if self
            .len
            .compare_exchange(len, len + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            // UB if we make it here, but we might as well try to crash
            panic!("concurrent push detected");
        }
    }

    /// Get reference into chunk element at index.
    ///
    /// # Safety
    /// Caller must ensure that:
    /// - index is within bounds.
    /// - no *mutable* reference to the same element exists (e.g. called get_mut and held on to result).
    /// - element is not mid-push (i.e. if push is concurrently called, it started with len > index).
    pub(crate) unsafe fn get(&self, idx: usize) -> &T {
        // TODO: make it a debug_assert?
        assert!(idx < self.len.load(Ordering::SeqCst));

        // Safety: idx < len and idx < isize::MAX (see Chunk::new)
        let ptr = unsafe { self.data.offset(idx as isize) };

        // Safety:
        // - pointer is aligned (guaranteed in alloc)
        // - pointer is non-null (guaranteed by type)
        // - pointer has appropriate provenance (derived from alloc)
        // - points to a valid T (guaranteed by push and caller guarantees, idx is within pushed bounds)
        // - respects aliasing:
        //     * no mutable reference to the same element exists (guaranteed by caller)
        //     * no other accesses exist (guaranteed by caller - not mid-push, and by Chunk API)
        unsafe { ptr.as_ref() }
    }

    /// Get mutable reference into chunk element at index.
    ///
    /// # Safety
    /// Caller must ensure that:
    /// - index is within bounds.
    /// - no references to the same element exists (e.g. called get(idx)/as_slice and held on to result).
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn get_mut(&self, idx: usize) -> &mut T {
        // TODO: make it a debug_assert?
        assert!(idx < self.len.load(Ordering::SeqCst));

        // Safety: idx < len and idx < isize::MAX (see Chunk::new)
        let mut ptr = unsafe { self.data.offset(idx as isize) };

        // Safety:
        // - pointer is aligned (guaranteed in alloc)
        // - pointer is non-null (guaranteed by type)
        // - pointer has appropriate provenance (derived from alloc)
        // - points to a valid T (guaranteed by push and caller guarantees, idx is within pushed bounds)
        // - respects aliasing:
        //     * no other reference to the same element exists (guaranteed by caller)
        //     * no other accesses exist (guaranteed by caller and Chunk API)
        ptr.as_mut()
    }

    /// Maximum number of elements that can be stored in the chunk.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current number of elements stored in the chunk.
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::SeqCst)
    }

    /// True if chunk is full.
    pub(crate) fn is_full(&self) -> bool {
        self.len.load(Ordering::SeqCst) == self.capacity
    }

    /// Slice of current chunk elements.
    ///
    /// # Safety
    /// Caller must ensure that no mutable references to chunk elements exist (i.e. don't call Chunk::get_mut).
    /// However, it is safe to push more elements into the chunk, and even to call get_mut on those new elements.
    /// The slice will only contain elements that were pushed before the slice was created.
    pub(crate) unsafe fn as_slice(&self) -> &[T] {
        // Safety:
        // - storage is non-null
        // - storage is valid for reads (guaranteed by Chunk API)
        // - storage is at least self.len*size<T>() long and in a single allocation (guaranteed by Chunk API)
        // - storage has len consecutive T's (guaranteed by Chunk::push API)
        // - memory referenced by slice is not modified while slice ref is live (guaranteed by Chunk API)
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len.load(Ordering::SeqCst)) }
    }

    /// Convert chunk into a Vec, consuming it.
    pub(crate) fn into_vec(self) -> Vec<T> {
        // Safety:
        // - ptr was allocated with global allocator (see Chunk::new)
        // - T has the same alignment as ptr allocation (see Chunk::new)
        // - size of ptr allocation is size of T * capacity (see Chunk::new)
        // - len is <= capacity (guaranteed by Chunk API, see push/from_iter)
        // - ptr was allocated with size of T * capacity (see Chunk::new)
        // - size of T * capacity < isisze::MAX (see Chunk::new)
        unsafe { Vec::from_raw_parts(self.data.as_ptr(), self.len(), self.capacity) }
    }
}

impl<T> Drop for Chunk<T> {
    fn drop(&mut self) {
        for idx in 0..self.len.load(Ordering::SeqCst) {
            // Safety: idx < len and idx < isize::MAX (see Chunk::new)
            let ptr = unsafe { self.data.offset(idx as isize) };

            // Safety: ptr is a valid pointer to a chunk element, not referenced by anything at the moment
            // this leaves memory uninitialized, but it's okay because we do not reference it again.
            unsafe {
                ptr.drop_in_place();
            }
        }

        let layout = Layout::array::<T>(self.capacity).unwrap();
        // Safety: self.storage was allocated in Chunk::new with this exact layout and std::alloc::alloc
        unsafe {
            std::alloc::dealloc(self.data.as_ptr() as *mut u8, layout);
        }
    }
}
