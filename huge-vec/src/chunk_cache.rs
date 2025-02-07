use std::{
    cell::UnsafeCell,
    collections::{BTreeSet, HashMap, VecDeque},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock, Mutex, MutexGuard,
    },
};

use crate::chunk::Chunk;

static ASYNC_WRITE_THREADPOOL: LazyLock<rayon::ThreadPool> = LazyLock::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(0)
        .thread_name(|i| format!("asyncwr-{}", i))
        .build()
        .expect("failed to create async write threadpool")
});

struct CachedChunk<T> {
    chunk: NonNull<Chunk<T>>,
    dirty: bool,
    writeback_in_progress: bool,
    last_update: ChunkAge,
    refcount: u64,
}

/// ChunkRef objects (non-mutably) reference a Chunk in a ChunkCache.
///
/// Despite the existence of ChunkRefMut, the ChunkCache has no notion of chunk locking.
/// Thus, while ChunkRefs ensure the corresponding chunk remains live in the cache, they do not
/// ensure there are no exclusive references to said chunk and using them is (a little bit) unsafe.
pub struct ChunkRef<T> {
    cache: Arc<ChunkCache<T>>,
    no: u64,
    chunk: NonNull<Chunk<T>>,
}

/// ChunkRefMut is a variant of ChunkRef that sets the dirty bit on the first mutable reference
/// creation.
pub struct ChunkRefMut<T> {
    inner: ChunkRef<T>,
    dirtied: bool,
}

type ChunkNo = u64;
type ChunkAge = u64;

pub(crate) struct ChunkCache<T> {
    inner: Mutex<ChunkCacheInner<T>>,
}

struct ChunkCacheInner<T> {
    clock: u64,
    target_size: usize,
    hot_chunks: HashMap<ChunkNo, CachedChunk<T>>,
    cold_chunks: HashMap<ChunkNo, CachedChunk<T>>,
}

impl<T> ChunkCache<T> {
    pub(crate) fn get(self: &Arc<Self>, chunk_no: u64) -> ChunkRef<T> {
        let mut inner = self.inner.lock().expect("lock poisoned");

        let cached_chunk = inner.entry_incr_refcount(chunk_no).chunk;
        inner.gc_step();
        // Note: we increment the cached chunk's refcount *before* running GC, so it's never freed
        // during GC.

        ChunkRef {
            cache: Arc::clone(self),
            no: chunk_no,
            chunk: cached_chunk,
        }
    }

    pub(crate) fn get_mut(self: &Arc<Self>, chunk_no: u64) -> ChunkRefMut<T> {
        let chunk_ref = self.get(chunk_no);

        // Chunk references themselves are not special for mut vs non-mut apart from setting the dirty bit.
        // Synchronization to the underlying chunk is delegated to the caller.
        ChunkRefMut {
            inner: chunk_ref,
            dirtied: false,
        }
    }
}

impl<T> ChunkRef<T> {
    /// Obtains shared reference to underlying chunk.
    ///
    /// # Safety
    /// Caller must guarantee there are no exclusive references to the same chunk.
    pub(crate) unsafe fn chunk_ref(&self) -> &Chunk<T> {
        // Safety: guaranteed by caller
        unsafe { self.chunk.as_ref() }
    }
}
impl<T> Deref for ChunkRefMut<T> {
    type Target = ChunkRef<T>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl<T> ChunkRefMut<T> {
    /// Obtains exclusive reference to underlying chunk.
    ///
    /// # Safety
    /// Caller must guarantee there are no other references to the same chunk.
    pub(crate) unsafe fn chunk_mut(&mut self) -> &mut Chunk<T> {
        if !self.dirtied {
            // mark chunk as dirty in cache, we're about to write to it
            self.inner
                .cache
                .inner
                .lock()
                .expect("lock poisoned")
                .hot_entry(self.inner.no)
                .dirty = true;

            self.dirtied = true; // skip this block next time, already marked as dirty
        }

        // Safety: guaranteed by caller
        unsafe { self.inner.chunk.as_mut() }
    }
}

impl<T> ChunkCacheInner<T> {
    fn hot_entry(&mut self, chunk_no: u64) -> &mut CachedChunk<T> {
        let cached_chunk = self
            .hot_chunks
            .get_mut(&chunk_no)
            .expect("missing chunk from cache");
        cached_chunk.last_update = self.clock;
        self.clock += 1;
        cached_chunk
    }

    fn decr_refcount(&mut self, chunk_no: u64) {
        let entry = self
            .hot_chunks
            .get_mut(&chunk_no)
            .expect("missing chunk from cache");
        entry.refcount -= 1;
        if entry.refcount == 0 {
            let entry = self.hot_chunks.remove(&chunk_no).unwrap();
            self.cold_chunks.insert(chunk_no, entry);
        }
    }

    fn entry_incr_refcount(&mut self, chunk_no: u64) -> &mut CachedChunk<T> {
        let entry = match self.cold_chunks.remove(&chunk_no) {
            Some(entry) => {
                // move to hot chunks map => it will be moved back to cold when refcount drops to 0
                self.hot_chunks.insert(chunk_no, entry);
                self.hot_chunks.get_mut(&chunk_no).unwrap()
            }
            None => self.hot_chunks.entry(chunk_no).or_insert_with(|| {
                let chunk = todo!();

                CachedChunk {
                    chunk,
                    dirty: false,
                    writeback_in_progress: false,
                    last_update: self.clock,
                    refcount: 0,
                }
            }),
        };

        entry.last_update = self.clock;
        self.clock += 1;
        entry.refcount += 1;
        entry
    }

    fn gc_step(&mut self) {
        // schedule async writebacks
        if let Some((chunk_no, cached)) = self.cold_chunks.iter_mut().find(|(_chunk_no, cached)| {
            !cached.writeback_in_progress
                && cached.dirty
                && unsafe { cached.chunk.as_ref() }.is_full()
        }) {
            cached.writeback_in_progress = true;
            cached.dirty = false;

            ASYNC_WRITE_THREADPOOL.spawn(move || {
                todo!();
            });
        }

        let total_size = self.hot_chunks.len() + self.cold_chunks.len();
        let target_clean_size = total_size
            .saturating_sub(self.target_size)
            .min(self.cold_chunks.len());
        if target_clean_size == 0 {
            // nothing can be done
            return;
        }

        todo!();
    }
}

impl<T> Drop for ChunkRef<T> {
    fn drop(&mut self) {
        self.cache
            .inner
            .lock()
            .expect("lock poisoned")
            .decr_refcount(self.no);
    }
}

impl<T> Drop for CachedChunk<T> {
    fn drop(&mut self) {
        // reconstruct box to drop it
        let chunk = unsafe { Box::from_raw(self.chunk.as_ptr()) };
        std::mem::drop(chunk);
    }
}
