use std::{
    cell::{Ref, RefCell, RefMut},
    fmt::Debug,
    iter::{ExactSizeIterator, FusedIterator},
    ops::{Deref, DerefMut},
    rc::Rc,
};

mod chunk;
mod io_transformer;
mod storage;

pub use chunk::Chunk;
pub use io_transformer::{IOTransformer, ZstdTransformer};
pub use storage::{FsStore, FsStoreError, IndexedStorage};

pub(crate) const CHUNK_CACHE_RECLAMATION_INTERVAL: usize = 1000;

pub struct HugeVec<T, Store, const CHUNK_SZ: usize = 4096>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    chunk_cache: RefCell<ChunkCache<T, Store, CHUNK_SZ>>, // TODO: left-right(?)
    len: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum HugeVecError<StoreErr> {
    #[error("Error in chunk storage")]
    StorageError(#[from] StoreErr),

    #[error("Access out of bounds")]
    OutOfBoundsError { len: u64, idx: u64 },
}

impl<T, Store, const CHUNK_SZ: usize> HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    pub fn new(chunk_storage: Store) -> Result<Self, HugeVecError<Store::Error>> {
        let mut chunk_cache = ChunkCache::new(chunk_storage)?;

        let chunk_count = chunk_cache.chunk_count();

        let len = if chunk_count == 0 {
            0
        } else {
            let last_chunk_idx = chunk_count - 1;
            let last_chunk = chunk_cache.get(last_chunk_idx)?;
            let last_chunk_len = last_chunk.borrow_mut().len();

            (chunk_count - 1) as u64 * CHUNK_SZ as u64 + last_chunk_len as u64
        };

        Ok(Self {
            chunk_cache: RefCell::new(chunk_cache),
            len,
        })
    }

    pub fn push(&mut self, val: T) -> Result<(), HugeVecError<Store::Error>> {
        let idx = self.len;
        let chunk_idx = (idx / CHUNK_SZ as u64) as usize;

        let cached_chunk = self.chunk_cache.get_mut().get(chunk_idx)?;
        cached_chunk
            .borrow_mut()
            .push_within_capacity(val)
            .expect("chunk size mismatch");

        self.len += 1;

        Ok(())
    }

    pub fn clear(&mut self) -> Result<(), HugeVecError<Store::Error>> {
        self.chunk_cache.borrow_mut().clear()?;
        self.len = 0;
        Ok(())
    }

    pub fn get(&self, idx: u64) -> Result<ItemRef<'_, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        self.slice().map_get(idx)
    }

    pub fn get_mut(
        &mut self,
        idx: u64,
    ) -> Result<ItemRefMut<'_, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        self.slice_mut().map_get_mut(idx)
    }

    pub fn slice(&self) -> HugeVecSlice<'_, T, Store, CHUNK_SZ> {
        HugeVecSlice(HugeVecSliceMut {
            chunk_cache: &self.chunk_cache,
            offset: 0,
            len: self.len,
        })
    }

    pub fn iter(&self) -> HugeVecIter<'_, T, Store, CHUNK_SZ> {
        HugeVecIter::new(self)
    }

    pub fn slice_mut(&mut self) -> HugeVecSliceMut<'_, T, Store, CHUNK_SZ> {
        HugeVecSliceMut {
            chunk_cache: &self.chunk_cache,
            offset: 0,
            len: self.len,
        }
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len > 0
    }

    pub fn truncate(&mut self, new_len: u64) -> Result<(), HugeVecError<Store::Error>> {
        if new_len >= self.len {
            return Ok(());
        }

        let mut chunk_cache = self.chunk_cache.borrow_mut();

        // cut out trailing chunks
        let new_n_chunks =
            new_len / CHUNK_SZ as u64 + if new_len % CHUNK_SZ as u64 > 0 { 1 } else { 0 };
        chunk_cache.truncate(new_n_chunks as usize)?;

        // truncate last chunk
        let last_chunk = new_n_chunks.saturating_sub(1) as usize;
        let last_chunk_len = (new_len % CHUNK_SZ as u64) as usize;
        chunk_cache
            .get(last_chunk)?
            .borrow_mut()
            .truncate(last_chunk_len);

        self.len = new_len;

        Ok(())
    }

    pub fn last(&self) -> Result<Option<ItemRef<'_, T, T, CHUNK_SZ>>, HugeVecError<Store::Error>> {
        if self.len == 0 {
            Ok(None)
        } else {
            self.get(self.len - 1).map(Some)
        }
    }

    pub fn last_mut(
        &mut self,
    ) -> Result<Option<ItemRefMut<'_, T, T, CHUNK_SZ>>, HugeVecError<Store::Error>> {
        if self.len == 0 {
            Ok(None)
        } else {
            self.get_mut(self.len - 1).map(Some)
        }
    }

    pub fn sync(&mut self) -> Result<(), HugeVecError<Store::Error>> {
        self.chunk_cache.get_mut().sync()?;
        Ok(())
    }
}

impl<'r, T, Store, const CHUNK_SZ: usize> IntoIterator for &'r HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type Item = ItemRef<'r, T, T, CHUNK_SZ>;
    type IntoIter = HugeVecIter<'r, T, Store, CHUNK_SZ>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct ItemRef<'r, T, TR, const CHUNK_SZ: usize> {
    chunk_rc: Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>,
    item_ref: Ref<'r, TR>,
}

impl<'r, T, TR, const CHUNK_SZ: usize> Deref for ItemRef<'r, T, TR, CHUNK_SZ> {
    type Target = TR;

    fn deref(&self) -> &Self::Target {
        self.item_ref.deref()
    }
}
impl<'r, T, TR: 'r, const CHUNK_SZ: usize> vector_trees::Ref<'r, TR>
    for ItemRef<'r, T, TR, CHUNK_SZ>
{
    fn map<U: 'r>(self, mapper: impl FnOnce(&TR) -> &U) -> impl vector_trees::Ref<'r, U> {
        ItemRef {
            chunk_rc: Rc::clone(&self.chunk_rc),
            item_ref: Ref::map(self.item_ref, mapper),
        }
    }
}

pub struct ItemRefMut<'r, T, TR, const CHUNK_SZ: usize> {
    chunk_rc: Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>,
    item_ref: RefMut<'r, TR>,
}

impl<'r, T, TR, const CHUNK_SZ: usize> Deref for ItemRefMut<'r, T, TR, CHUNK_SZ> {
    type Target = TR;

    fn deref(&self) -> &Self::Target {
        self.item_ref.deref()
    }
}
impl<'r, T, TR, const CHUNK_SZ: usize> DerefMut for ItemRefMut<'r, T, TR, CHUNK_SZ> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.item_ref.deref_mut()
    }
}
impl<'r, T, TR: 'r, const CHUNK_SZ: usize> vector_trees::RefMut<'r, TR>
    for ItemRefMut<'r, T, TR, CHUNK_SZ>
{
    fn map_mut<U: 'r>(
        self,
        mapper: impl FnOnce(&mut TR) -> &mut U,
    ) -> impl vector_trees::RefMut<'r, U> {
        ItemRefMut {
            chunk_rc: Rc::clone(&self.chunk_rc),
            item_ref: RefMut::map(self.item_ref, mapper),
        }
    }
}

pub struct HugeVecIter<'v, T, Store, const CHUNK_SZ: usize>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    vec: &'v HugeVec<T, Store, CHUNK_SZ>,
    len: u64,
    idx: u64,
}

impl<'v, T, Store, const CHUNK_SZ: usize> HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    fn new(vec: &'v HugeVec<T, Store, CHUNK_SZ>) -> Self {
        Self {
            vec,
            len: vec.len,
            idx: 0,
        }
    }
}

impl<'v, T, Store, const CHUNK_SZ: usize> Iterator for HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type Item = ItemRef<'v, T, T, CHUNK_SZ>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.len {
            return None;
        }

        let res = self.vec.get(self.idx).unwrap();
        self.idx += 1;
        Some(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.idx;
        (remaining as usize, Some(remaining as usize))
    }
}

impl<'v, T, Store, const CHUNK_SZ: usize> FusedIterator for HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
}

impl<'v, T, Store, const CHUNK_SZ: usize> ExactSizeIterator for HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
}

impl<'v, T, Store, const CHUNK_SZ: usize> DoubleEndedIterator
    for HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx == self.len || self.len == 0 {
            return None;
        }

        let res = self.vec.get(self.len - 1).unwrap();
        self.len -= 1;
        Some(res)
    }
}

pub struct HugeVecSliceMut<'s, T, Store, const CHUNK_SZ: usize>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    chunk_cache: &'s RefCell<ChunkCache<T, Store, CHUNK_SZ>>,
    offset: u64,
    len: u64,
}

impl<'s, T, Store, const CHUNK_SZ: usize> HugeVecSliceMut<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    pub fn get<'r>(
        &'r self,
        idx: u64,
    ) -> Result<ItemRef<'r, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        if idx >= self.len {
            return Err(HugeVecError::OutOfBoundsError { len: self.len, idx });
        }

        let chunk_idx = (idx / CHUNK_SZ as u64) as usize;
        let chunk_offset = (idx % CHUNK_SZ as u64) as usize;

        let cached_chunk = self.chunk_cache.borrow_mut().get(chunk_idx)?;
        let chunk_rc = Rc::clone(&cached_chunk);

        // create a reference to the chunk data that lives as long as needed
        // Safety: we will not drop chunk_rc while the reference (and any others derivable from it) are still in use
        let chunk_indef_ref: &'_ RefCell<CachedChunk<T, CHUNK_SZ>> = &chunk_rc;
        let chunk_indef_ref: &'s RefCell<CachedChunk<T, CHUNK_SZ>> =
            unsafe { std::mem::transmute(chunk_indef_ref) };

        let item_ref = Ref::map(chunk_indef_ref.borrow(), |c| &c[chunk_offset]);

        Ok(ItemRef { chunk_rc, item_ref })
    }

    pub fn get_mut<'r>(
        &'r mut self,
        idx: u64,
    ) -> Result<ItemRefMut<'r, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        if idx >= self.len {
            return Err(HugeVecError::OutOfBoundsError { len: self.len, idx });
        }

        let chunk_idx = (idx / CHUNK_SZ as u64) as usize;
        let chunk_offset = (idx % CHUNK_SZ as u64) as usize;

        let cached_chunk = self.chunk_cache.borrow_mut().get(chunk_idx)?;
        let chunk_rc = Rc::clone(&cached_chunk);

        // create a reference to the chunk data that lives as long as needed
        // Safety: we will not drop chunk_rc while the reference (and any others derivable from it) are still in use
        let chunk_indef_ref: &'_ RefCell<CachedChunk<T, CHUNK_SZ>> = &chunk_rc;
        let chunk_indef_ref: &'s RefCell<CachedChunk<T, CHUNK_SZ>> =
            unsafe { std::mem::transmute(chunk_indef_ref) };

        let item_ref = RefMut::map(chunk_indef_ref.borrow_mut(), |c| &mut c[chunk_offset]);

        Ok(ItemRefMut { chunk_rc, item_ref })
    }

    pub fn map_get(
        self,
        idx: u64,
    ) -> Result<ItemRef<'s, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        // Safety: the underlying data is guaranteed to be live for 's, and we do not return any references to self
        let res: Result<ItemRef<'_, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> = self.get(idx);
        let res: Result<ItemRef<'s, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> =
            unsafe { std::mem::transmute(res) };
        res
    }

    pub fn map_get_mut(
        mut self,
        idx: u64,
    ) -> Result<ItemRefMut<'s, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        // Safety: the underlying data is guaranteed to be live for 's, and we do not return any references to self
        let res: Result<ItemRefMut<'_, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> =
            self.get_mut(idx);
        let res: Result<ItemRefMut<'s, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> =
            unsafe { std::mem::transmute(res) };
        res
    }
}

impl<'s, T, Store, const CHUNK_SZ: usize> vector_trees::VectorSlice<'s, T>
    for HugeVecSliceMut<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type Ref<'r> = ItemRef<'r, T, T, CHUNK_SZ> where 's: 'r;

    fn len(&self) -> usize {
        self.len as usize
    }

    fn get(&self, idx: usize) -> Option<Self::Ref<'_>> {
        self.get(idx as u64).ok()
    }

    fn map_get(self, idx: usize) -> Option<Self::Ref<'s>> {
        self.map_get(idx as u64).ok()
    }
}

impl<'s, T, Store, const CHUNK_SZ: usize> vector_trees::VectorSliceMut<'s, T>
    for HugeVecSliceMut<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type RefMut<'r> = ItemRefMut<'r, T, T, CHUNK_SZ> where 's: 'r;

    fn get_mut(&mut self, idx: usize) -> Option<Self::RefMut<'_>> {
        match self.get_mut(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsError { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }

    fn map_get_mut(self, idx: usize) -> Option<Self::RefMut<'s>> {
        match self.map_get_mut(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsError { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }

    fn split_at_mut(self, idx: usize) -> (Self, Self) {
        if idx as u64 > self.len {
            panic!("out of bounds");
        }

        let left = HugeVecSliceMut {
            chunk_cache: self.chunk_cache,
            offset: self.offset,
            len: idx as u64,
        };
        let right = HugeVecSliceMut {
            chunk_cache: self.chunk_cache,
            offset: self.offset + idx as u64,
            len: self.len - idx as u64,
        };

        (left, right)
    }

    fn split_first_mut(self) -> Option<(Self::RefMut<'s>, Self)> {
        if self.len == 0 {
            panic!("out of bounds");
        }

        let (left, right) = self.split_at_mut(1);
        Some((left.map_get_mut(0).unwrap(), right))
    }
}

pub struct HugeVecSlice<'s, T, Store, const CHUNK_SZ: usize>(
    HugeVecSliceMut<'s, T, Store, CHUNK_SZ>,
)
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>;

impl<'s, T, Store, const CHUNK_SZ: usize> Deref for HugeVecSlice<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type Target = HugeVecSliceMut<'s, T, Store, CHUNK_SZ>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'s, T, Store, const CHUNK_SZ: usize> HugeVecSlice<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    pub fn map_get(
        self,
        idx: u64,
    ) -> Result<ItemRef<'s, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        // Safety: the underlying data is guaranteed to be live for 's, and we do not return any references to self
        let res: Result<ItemRef<'_, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> = self.get(idx);
        let res: Result<ItemRef<'s, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> =
            unsafe { std::mem::transmute(res) };
        res
    }
}

impl<'s, T, Store, const CHUNK_SZ: usize> vector_trees::VectorSlice<'s, T>
    for HugeVecSlice<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type Ref<'r> = ItemRef<'r, T, T, CHUNK_SZ> where 's: 'r;

    fn len(&self) -> usize {
        self.deref().len()
    }

    fn get(&self, idx: usize) -> Option<Self::Ref<'_>> {
        match self.deref().get(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsError { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }

    fn map_get(self, idx: usize) -> Option<Self::Ref<'s>> {
        match self.map_get(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsError { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }
}

impl<T, Store, const CHUNK_SZ: usize> vector_trees::Vector<T> for HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
{
    type Slice<'s> = HugeVecSlice<'s, T, Store, CHUNK_SZ> where T: 's, Self: 's;
    type SliceMut<'s> = HugeVecSliceMut<'s, T, Store, CHUNK_SZ> where T: 's, Self: 's;

    fn clear(&mut self) {
        self.clear().expect("clear error");
    }

    fn push(&mut self, value: T) {
        self.push(value).expect("push error");
    }

    fn slice(&self) -> Self::Slice<'_> {
        self.slice()
    }

    fn slice_mut(&mut self) -> Self::SliceMut<'_> {
        self.slice_mut()
    }
}

use chunk_cache::ChunkCache;

use self::chunk_cache::CachedChunk;
mod chunk_cache {
    use std::{
        cell::RefCell,
        collections::{btree_map::Entry, BTreeMap},
        fmt::Debug,
        rc::Rc,
    };

    use super::{Chunk, IndexedStorage, CHUNK_CACHE_RECLAMATION_INTERVAL};
    use std::{
        borrow::Borrow,
        ops::{Deref, DerefMut},
    };

    pub(crate) struct ChunkCache<T, Store, const CHUNK_SZ: usize>
    where
        T: Debug,
        Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
    {
        chunk_store: Store,
        cached_chunks: BTreeMap<usize, (Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>, u64)>,
        chunk_count: usize,
        clock: u64,
    }

    impl<T, Store, const CHUNK_SZ: usize> ChunkCache<T, Store, CHUNK_SZ>
    where
        T: Debug,
        Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
    {
        pub(crate) fn new(chunk_store: Store) -> Result<Self, Store::Error> {
            let chunk_count = chunk_store.len()?;
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
            if self.cached_chunks.len() % CHUNK_CACHE_RECLAMATION_INTERVAL == 0
                && !self.cached_chunks.is_empty()
            {
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

            // get existing chunk
            let cached_chunk = match self.cached_chunks.entry(chunk_idx) {
                Entry::Occupied(mut o) => {
                    // cached chunk found, update use time for GC and return it
                    o.get_mut().1 = use_time;
                    Rc::clone(&o.into_mut().0)
                }
                Entry::Vacant(v) => {
                    // not cached: load into cache and return it
                    let chunk = self.chunk_store.load(chunk_idx)?;
                    let cached_chunk = Rc::new(RefCell::new(CachedChunk::new(chunk)));
                    Rc::clone(&v.insert((cached_chunk, use_time)).0)
                }
            };

            Ok(cached_chunk)
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

        pub(crate) fn dirty_chunk_count(&self) -> usize {
            self.cached_chunks
                .values()
                .filter(|(c, _)| (**c).borrow().is_dirty())
                .count()
        }

        pub(crate) fn truncate(&mut self, n_chunks: usize) -> Result<(), Store::Error> {
            // remove deleted chunks from cache
            self.cached_chunks.retain(|&idx, _| idx > n_chunks);

            // truncate storage
            self.chunk_store.truncate(n_chunks)?;

            Ok(())
        }

        pub(crate) fn sync(&mut self) -> Result<(), Store::Error> {
            for (id, (cached_chunk, _)) in self.cached_chunks.iter() {
                let mut c = cached_chunk.borrow_mut();
                if c.is_dirty() {
                    c.clean_dirty_with(|chunk| self.chunk_store.store(*id, chunk))?;
                }
            }

            Ok(())
        }

        fn writeback_oldest_dirty(&mut self) -> Result<(), Store::Error> {
            if let Some((chunk_idx, (oldest_dirty, _))) = self
                .cached_chunks
                .iter()
                .filter(|(_, (cached_chunk, last_used))| {
                    self.clock - last_used >= CHUNK_SZ as u64 && Rc::strong_count(cached_chunk) == 1
                })
                .filter(|(_, (cached_chunk, _))| {
                    let c = RefCell::borrow(cached_chunk);
                    c.is_dirty() && c.len() > 0
                })
                .min_by_key(|(_, (_, last_used))| *last_used)
            {
                oldest_dirty
                    .borrow_mut()
                    .clean_dirty_with(|chunk| self.chunk_store.store(*chunk_idx, chunk))?;
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
            removable_chunks.sort_unstable_by(|(_, a_last_used), (_, b_last_used)| {
                b_last_used.cmp(a_last_used)
            });

            let max_removed = removable_chunks.len() / 2;
            for chunk_idx in removable_chunks
                .into_iter()
                .map(|(idx, _)| idx)
                .take(max_removed)
            {
                let removed = self.cached_chunks.remove(&chunk_idx);
                debug_assert!(!Rc::try_unwrap(removed.unwrap().0)
                    .unwrap()
                    .into_inner()
                    .is_dirty());
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
        T: Debug,
        Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
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

        pub fn into_inner(self) -> Chunk<T, SZ> {
            self.chunk
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
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use crate::huge_vec::FsStore;
    use crate::huge_vec::IOTransformer;
    use crate::huge_vec::CHUNK_CACHE_RECLAMATION_INTERVAL;

    use super::HugeVec;
    use super::ZstdTransformer;

    macro_rules! test_push_readback {
        ($name:ident, $chunk_sz:literal * ( $chunk_count:expr ) + $extra_items:literal) => {
            test_push_readback!($name, $chunk_sz, $chunk_count, $extra_items);
        };
        ($name:ident, $chunk_sz:literal * $chunk_count:literal + $extra_items:literal) => {
            test_push_readback!($name, $chunk_sz, $chunk_count, $extra_items);
        };
        ($name:ident, $chunk_sz:expr, $chunk_count:expr, $extra_items:expr) => {
            #[test]
            fn $name() {
                let dir = tempdir::TempDir::new(stringify!($name)).unwrap();
                let store_builder = || FsStore::open(&dir, ZstdTransformer::default()).unwrap();
                let vec_opener = || HugeVec::<_, _, $chunk_sz>::new(store_builder()).unwrap();

                let num_items: u64 = $chunk_sz * $chunk_count + $extra_items;
                let range = 0..num_items;

                {
                    let mut vec = vec_opener();

                    for i in range.clone() {
                        vec.push(i).unwrap();
                    }

                    assert_eq!(vec.len(), num_items);
                    for i in range.clone() {
                        assert_eq!(*vec.get(i).unwrap(), i);
                    }
                }

                {
                    let vec = vec_opener();

                    for i in range.clone() {
                        assert_eq!(*vec.get(i).unwrap(), i);
                    }
                    assert_eq!(vec.len(), num_items);
                }
            }
        };
    }
    test_push_readback!(single_chunk_push, 8 * 1 + 0);
    test_push_readback!(partial_chunk_push, 8 * 0 + 1);
    test_push_readback!(multi_chunk_push, 8 * 3 + 4);
    test_push_readback!(
        multi_chunk_push_gc,
        2 * (CHUNK_CACHE_RECLAMATION_INTERVAL as u64 * 2) + 4
    );

    #[test]
    fn truncate() {
        let dir = tempdir::TempDir::new("truncate").unwrap();
        let vec_opener = || {
            let store = FsStore::open(&dir, ()).unwrap();
            HugeVec::<u8, _, 4>::new(store).unwrap()
        };

        {
            let mut vec = vec_opener();
            for i in 0..=255u8 {
                vec.push(i).unwrap();
            }
        }

        for offset in (0..4).rev() {
            let prev_len = {
                let prev_offset = offset + 1;
                if prev_offset == 4 {
                    256
                } else {
                    (prev_offset + 1) * 10 + prev_offset
                }
            };

            let mut vec = vec_opener();
            assert_eq!(
                vec.iter().map(|i| *i).collect::<Vec<_>>(),
                (0..prev_len as u8).collect::<Vec<_>>()
            );
            vec.truncate(((offset + 1) * 10 + offset) as u64).unwrap();
        }

        let vec = vec_opener();
        assert_eq!(
            vec.iter().map(|i| *i).collect::<Vec<_>>(),
            (0..10u8).collect::<Vec<_>>()
        );
    }

    #[test]
    fn constant_seq_write_memuse() {
        let dir = tempdir::TempDir::new("constant_seq_write_memuse").unwrap();
        let store = FsStore::open(&dir, ()).unwrap();
        let mut vec = HugeVec::<u8, _, 1>::new(store).unwrap();

        for _ in 0..(CHUNK_CACHE_RECLAMATION_INTERVAL * 10) {
            vec.push(42).unwrap();

            let num_dirty_chunks = vec.chunk_cache.borrow().dirty_chunk_count();
            assert!(num_dirty_chunks < 2);
        }
    }

    #[test]
    fn seq_write_no_early_writeback() {
        let dir = tempdir::TempDir::new("constant_seq_write_memuse").unwrap();

        struct Recorder {
            writebacks: AtomicU64,
        }
        impl IOTransformer for &Recorder {
            type Error = std::convert::Infallible;

            fn wrap_reader(
                &self,
                reader: impl std::io::BufRead,
            ) -> Result<impl std::io::Read, Self::Error> {
                Ok(reader)
            }

            fn wrap_writer(
                &self,
                writer: impl std::io::Write,
            ) -> Result<impl std::io::Write, Self::Error> {
                self.writebacks.fetch_add(1, Ordering::Release);
                Ok(writer)
            }
        }

        let recorder = Recorder {
            writebacks: AtomicU64::new(0),
        };
        let fs_store = FsStore::open(&dir, &recorder).unwrap();
        let mut vec = HugeVec::<u8, _, 4>::new(fs_store).unwrap();

        for _ in 0..(CHUNK_CACHE_RECLAMATION_INTERVAL * 4 * 3) {
            vec.push(42).unwrap();
        }

        // both metadata and data use wrap_writer, so we must divide by 2
        let no_writebacks = recorder.writebacks.load(Ordering::Acquire) / 2;
        assert!(no_writebacks < CHUNK_CACHE_RECLAMATION_INTERVAL as u64 * 3);
    }
}
