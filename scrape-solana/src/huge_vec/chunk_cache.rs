use std::{cell::RefCell, collections::BTreeMap, fmt::Debug, rc::Rc};

use super::{
    Chunk, IndexedStorage,
    prefetch_storage::{FetchReq, PrefetchableStore},
};
use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
};

type CacheEntry<T, const CHUNK_SZ: usize> = (Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>, u64);
pub(crate) struct ChunkCache<T, Store, const CHUNK_SZ: usize>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    chunk_store: PrefetchableStore<Chunk<T, CHUNK_SZ>, Store>,
    cached_chunks: BTreeMap<usize, CacheEntry<T, CHUNK_SZ>>,
    chunk_count: usize,
    clock: u64,
}

pub(super) const READAHEAD_COUNT: usize = 4;

impl<T, Store, const CHUNK_SZ: usize> ChunkCache<T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    pub(crate) fn new(chunk_store: Store) -> Result<Self, Store::Error> {
        let chunk_count = chunk_store.len()?;
        let chunk_store =
            PrefetchableStore::new(chunk_store).expect("failed to create prefetchable store");

        Ok(Self {
            chunk_store,
            cached_chunks: BTreeMap::new(),
            chunk_count,
            clock: 0,
        })
    }

    pub(crate) fn get(
        &mut self,
        chunk_idx: usize,
    ) -> Result<Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>, Store::Error> {
        self.writeback_oldest_dirty()?;
        if self.cached_chunks.len() % READAHEAD_COUNT == 0 && !self.cached_chunks.is_empty() {
            self.gc();
        }
        let use_time = self.clock_tick();

        if chunk_idx == self.chunk_count {
            // create new chunk
            self.chunk_count += 1;
            let cached_chunk = Rc::new(RefCell::new(CachedChunk::new(Chunk::new())));
            self.cached_chunks
                .insert(chunk_idx, (Rc::clone(&cached_chunk), use_time));
            return Ok(cached_chunk);
        }

        self.cached_chunks
            .get_mut(&chunk_idx)
            .map(|(chunk, last_access)| {
                // cached chunk found, update use time for GC and return it
                *last_access = use_time;
                Ok(Rc::clone(chunk))
            })
            .unwrap_or_else(|| {
                // not cached: load into cache and return it
                let (prefetched_chunks, chunk) = self.chunk_store.load(chunk_idx);
                self.store_prefetched_chunks(prefetched_chunks);
                let chunk = chunk?;

                let cached_chunk = Rc::new(RefCell::new(CachedChunk::new(chunk)));
                self.cached_chunks
                    .insert(chunk_idx, (Rc::clone(&cached_chunk), use_time));

                // request prefetch of next chunks
                for chunk_idx in (chunk_idx + 1..)
                    .take(READAHEAD_COUNT)
                    .filter(|&idx| idx < self.chunk_count)
                    .filter(|idx| !self.cached_chunks.contains_key(idx))
                {
                    self.chunk_store.prefetch(chunk_idx);
                }

                Ok(cached_chunk)
            })
    }

    pub(crate) fn clear(&mut self) -> Result<(), Store::Error> {
        self.cached_chunks.clear();
        self.chunk_count = 0;
        self.clock = 0; // can reset, as we're clearing everything
        self.chunk_store.clear()
    }

    pub(crate) fn chunk_count(&self) -> usize {
        self.chunk_count
    }

    #[cfg(test)]
    pub(crate) fn dirty_chunk_count(&self) -> usize {
        self.cached_chunks
            .values()
            .filter(|(c, _)| (**c).borrow().is_dirty())
            .count()
    }

    pub(crate) fn truncate(&mut self, n_chunks: usize) -> Result<(), Store::Error> {
        // remove deleted chunks from cache
        self.cached_chunks.retain(|&idx, _| idx < n_chunks);

        // truncate storage
        self.chunk_store.truncate(n_chunks)?;

        // update chunk count
        self.chunk_count = n_chunks;

        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<(), Store::Error> {
        // fast path that avoids locking
        if self
            .cached_chunks
            .iter()
            .filter(|(_, (cached_chunk, _))| RefCell::borrow(cached_chunk).is_dirty())
            .count()
            == 0
        {
            return Ok(());
        }

        self.chunk_store.with_inner_mut(|chunk_store| {
            for (id, (cached_chunk, _)) in self.cached_chunks.iter() {
                let mut c = cached_chunk.borrow_mut();
                if c.is_dirty() {
                    c.clean_dirty_with(|chunk| chunk_store.store(*id, chunk))?;
                }
            }

            chunk_store.sync()?;
            Ok(())
        })
    }

    fn writeback_oldest_dirty(&mut self) -> Result<(), Store::Error> {
        let wb_threshold = if self.cached_chunks.len() < 2 * READAHEAD_COUNT {
            CHUNK_SZ as u64
        } else {
            2
        };

        if let Some((chunk_idx, (oldest_dirty, _))) = self
            .cached_chunks
            .iter()
            .filter(|(_, (cached_chunk, last_used))| {
                self.clock - last_used > wb_threshold && Rc::strong_count(cached_chunk) == 1
            })
            .filter(|(_, (cached_chunk, _))| {
                let c = RefCell::borrow(cached_chunk);
                c.is_dirty() && c.len() > 0
            })
            .min_by_key(|(_, (_, last_used))| *last_used)
        {
            oldest_dirty.borrow_mut().clean_dirty_with(|chunk| {
                // we write the chunk but it remains cached, so prefetching cannot overwrite it with stale data
                self.chunk_store.store_no_invalidate(*chunk_idx, chunk)
            })?;
        }

        Ok(())
    }

    fn gc(&mut self) {
        let mut removable_chunks = self
            .cached_chunks
            .iter()
            // only select unused chunks
            .filter(|(_, (chunk, _))| Rc::strong_count(chunk) == 1)
            // only select clean chunks (that can be immediately released)
            .filter(|(_, (chunk, _))| !RefCell::borrow(chunk).is_dirty())
            .map(|(idx, (_, chunk_last_use))| (*idx, *chunk_last_use))
            .collect::<Vec<_>>();
        removable_chunks
            .sort_unstable_by(|(_, a_last_used), (_, b_last_used)| b_last_used.cmp(a_last_used));

        let mut removed_chunks = false;
        for chunk_idx in removable_chunks
            .into_iter()
            .map(|(idx, _)| idx)
            .take(self.cached_chunks.len().saturating_sub(READAHEAD_COUNT + 1))
        {
            let removed = self.cached_chunks.remove(&chunk_idx);
            removed_chunks = true;
            debug_assert!(
                !Rc::try_unwrap(removed.unwrap().0)
                    .unwrap()
                    .into_inner()
                    .is_dirty()
            );
        }

        if removed_chunks {
            // writeback_oldest_dirty does not invalidate fetches because chunks are kept in cache
            // we removed chunks so this is no longer true: we must invalidate potentially stale prefetches
            self.chunk_store.invalidate_prefetches();
        }
    }

    fn store_prefetched_chunks(&mut self, chunks: Vec<(FetchReq, Chunk<T, CHUNK_SZ>)>) {
        const PREFETCH_CLOCK_PENALTY: u64 = 1;
        let prefetch_chunk_time = self.clock.saturating_sub(PREFETCH_CLOCK_PENALTY);
        for (req, chunk) in chunks {
            self.cached_chunks.entry(req.object_idx).or_insert_with(|| {
                (
                    Rc::new(RefCell::new(CachedChunk::new(chunk))),
                    prefetch_chunk_time,
                )
            });
        }
    }

    fn clock_tick(&mut self) -> u64 {
        let c = self.clock;
        self.clock += 1;
        c
    }
}

impl<T, Store, const CHUNK_SZ: usize> Drop for ChunkCache<T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.sync().expect("failed to sync on drop");
    }
}

#[derive(Debug)]
pub(crate) struct CachedChunk<T, const SZ: usize> {
    chunk: Chunk<T, SZ>,
    is_dirty: bool,
}

impl<T, const SZ: usize> CachedChunk<T, SZ> {
    pub fn new(chunk: Chunk<T, SZ>) -> Self {
        Self {
            chunk,
            is_dirty: false,
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn clean_dirty_with<E>(
        &mut self,
        cleaner: impl FnOnce(&Chunk<T, SZ>) -> Result<(), E>,
    ) -> Result<(), E> {
        cleaner(&self.chunk)?;
        self.is_dirty = false;
        Ok(())
    }
}

impl<T, const SZ: usize> Deref for CachedChunk<T, SZ> {
    type Target = Chunk<T, SZ>;

    fn deref(&self) -> &Self::Target {
        &self.chunk
    }
}

impl<T, const SZ: usize> DerefMut for CachedChunk<T, SZ> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.is_dirty = true;
        &mut self.chunk
    }
}

impl<T, const SZ: usize> Borrow<Chunk<T, SZ>> for CachedChunk<T, SZ> {
    fn borrow(&self) -> &Chunk<T, SZ> {
        &self.chunk
    }
}
