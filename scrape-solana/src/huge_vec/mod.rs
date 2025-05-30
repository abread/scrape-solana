use std::{
    cell::{Ref, RefCell, RefMut},
    cmp::Ordering,
    fmt::Debug,
    iter::{ExactSizeIterator, FusedIterator},
    ops::{Deref, DerefMut},
    rc::Rc,
};

mod chunk;
mod chunk_cache;
mod io_transformer;
mod prefetch_storage;
mod storage;

pub use chunk::Chunk;
use chunk_cache::{CachedChunk, ChunkCache};
pub use io_transformer::{IOTransformer, ZstdTransformer};
pub use prefetch_storage::PREFETCH_THREADPOOL;
pub use storage::{FsStore, FsStoreError, IndexedStorage};

pub struct HugeVec<T, Store, const CHUNK_SZ: usize = 4096>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    chunk_cache: RefCell<ChunkCache<T, Store, CHUNK_SZ>>, // TODO: left-right(?)
    len: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum HugeVecError<StoreErr> {
    #[error("Error in chunk storage")]
    Storage(#[from] StoreErr),

    #[error("Access out of bounds")]
    OutOfBoundsAccess { len: u64, idx: u64 },

    #[error("Out of bounds access: chunk corrupted")]
    ChunkCorrupted {
        chunk_len: usize,
        access_offset: usize,
    },
}

impl<T, Store, const CHUNK_SZ: usize> HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
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

    pub fn assume_max_size_for_heal(&mut self) -> Result<(), HugeVecError<Store::Error>> {
        let mut chunk_cache = self.chunk_cache.borrow_mut();
        chunk_cache.assume_max_size_for_heal()?;
        self.len = chunk_cache.chunk_count() as u64 * CHUNK_SZ as u64;
        Ok(())
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
        self.len == 0
    }

    pub fn truncate(&mut self, new_len: u64) -> Result<(), HugeVecError<Store::Error>> {
        if new_len >= self.len {
            return Ok(());
        }

        let mut chunk_cache = self.chunk_cache.borrow_mut();

        // cut out trailing chunks
        let new_n_chunks =
            new_len / CHUNK_SZ as u64 + if new_len % CHUNK_SZ as u64 > 0 { 1 } else { 0 };
        assert!(new_n_chunks * CHUNK_SZ as u64 >= new_len);

        chunk_cache.truncate(new_n_chunks as usize)?;

        // truncate last chunk
        if new_len % CHUNK_SZ as u64 > 0 {
            let last_chunk = new_n_chunks.saturating_sub(1) as usize;
            let last_chunk_len = (new_len % CHUNK_SZ as u64) as usize;

            assert_eq!(
                new_n_chunks.saturating_sub(1) * CHUNK_SZ as u64 + last_chunk_len as u64,
                new_len
            );

            chunk_cache
                .get(last_chunk)?
                .borrow_mut()
                .truncate(last_chunk_len);
        }

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

    pub fn second_last(
        &self,
    ) -> Result<Option<ItemRef<'_, T, T, CHUNK_SZ>>, HugeVecError<Store::Error>> {
        if self.len < 2 {
            Ok(None)
        } else {
            self.get(self.len - 2).map(Some)
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

    pub fn binary_search_by<F>(&self, f: F) -> std::result::Result<u64, u64>
    where
        F: FnMut(ItemRef<'_, T, T, CHUNK_SZ>) -> std::cmp::Ordering,
    {
        self.slice().binary_search_by(f)
    }

    pub fn partition_point<P>(&self, pred: P) -> u64
    where
        P: FnMut(ItemRef<'_, T, T, CHUNK_SZ>) -> bool,
    {
        self.slice().partition_point(pred)
    }
}

impl<'r, T, Store, const CHUNK_SZ: usize> IntoIterator for &'r HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type Item = Result<ItemRef<'r, T, T, CHUNK_SZ>, HugeVecError<Store::Error>>;
    type IntoIter = HugeVecIter<'r, T, Store, CHUNK_SZ>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct ItemRef<'r, T, TR, const CHUNK_SZ: usize> {
    item_ref: Ref<'r, TR>,
    // chunk_rc must be declared after item_ref so that it is dropped only after it
    // preventing item_ref from being invalidated
    chunk_rc: Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>,
}

impl<T, TR, const CHUNK_SZ: usize> Deref for ItemRef<'_, T, TR, CHUNK_SZ> {
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
impl<T, TR, const CHUNK_SZ: usize, U: PartialEq<TR>> PartialEq<U> for ItemRef<'_, T, TR, CHUNK_SZ>
where
    TR: PartialEq,
{
    fn eq(&self, other: &U) -> bool {
        other.eq(self.item_ref.deref())
    }
}
impl<T, TR, const CHUNK_SZ: usize> Debug for ItemRef<'_, T, TR, CHUNK_SZ>
where
    TR: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ItemRef")
            .field(self.item_ref.deref())
            .finish()
    }
}

pub struct ItemRefMut<'r, T, TR, const CHUNK_SZ: usize> {
    item_ref: RefMut<'r, TR>,
    // chunk_rc must be declared after item_ref so that it is dropped only after it
    // preventing item_ref from being invalidated
    chunk_rc: Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>,
}

impl<T, TR, const CHUNK_SZ: usize> Deref for ItemRefMut<'_, T, TR, CHUNK_SZ> {
    type Target = TR;

    fn deref(&self) -> &Self::Target {
        self.item_ref.deref()
    }
}
impl<T, TR, const CHUNK_SZ: usize> DerefMut for ItemRefMut<'_, T, TR, CHUNK_SZ> {
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
impl<T, TR, const CHUNK_SZ: usize, U: PartialEq<TR>> PartialEq<U>
    for ItemRefMut<'_, T, TR, CHUNK_SZ>
where
    TR: PartialEq,
{
    fn eq(&self, other: &U) -> bool {
        other.eq(self.item_ref.deref())
    }
}
impl<T, TR, const CHUNK_SZ: usize> Debug for ItemRefMut<'_, T, TR, CHUNK_SZ>
where
    TR: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ItemRefMut")
            .field(self.item_ref.deref())
            .finish()
    }
}

pub struct HugeVecIter<'v, T, Store, const CHUNK_SZ: usize>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    vec: &'v HugeVec<T, Store, CHUNK_SZ>,
    len: u64,
    idx: u64,

    /// The chunk in the chunk cache that contains the current index. We keep a reference to it
    /// to avoid 1) repeatedly looking it up in the chunk cache and 2) having the chunk cache
    /// garbage collect it mid-iteration.
    chunk: Option<(usize, Rc<RefCell<CachedChunk<T, CHUNK_SZ>>>)>,
}

impl<'v, T, Store, const CHUNK_SZ: usize> HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    fn new(vec: &'v HugeVec<T, Store, CHUNK_SZ>) -> Self {
        Self {
            vec,
            len: vec.len,
            idx: 0,
            chunk: None,
        }
    }

    pub fn nth(&mut self, idx: u64) -> <Self as Iterator>::Item {
        let chunk_idx = (idx / CHUNK_SZ as u64) as usize;
        let chunk_offset = (idx % CHUNK_SZ as u64) as usize;

        // grab the chunk for the current index
        // we cache it in the iterator to keep a live reference to it when garbage collection runs
        let chunk_rc = match self.chunk.as_ref() {
            Some((cached_chunk_idx, cached_chunk)) if *cached_chunk_idx == chunk_idx => {
                cached_chunk.clone()
            }
            _ => {
                let chunk_rc = match self.vec.chunk_cache.borrow_mut().get(chunk_idx) {
                    Ok(c) => c,

                    // errors are not cloneable, so we just retry every time :/
                    Err(e) => return Err(HugeVecError::Storage(e)),
                };

                // cache the chunk for the next iteration
                self.chunk = Some((chunk_idx, chunk_rc.clone()));
                chunk_rc
            }
        };

        // create a reference to the chunk data that lives as long as needed
        // Safety: we will not drop chunk_rc while the reference (and any others derivable from it) are still in use
        let chunk_indef_ref: &'_ RefCell<CachedChunk<T, CHUNK_SZ>> = chunk_rc.deref();
        let chunk_indef_ref: &'_ RefCell<CachedChunk<T, CHUNK_SZ>> =
            unsafe { std::mem::transmute(chunk_indef_ref) };

        // chunk may be corrupted
        {
            let chunk_len = chunk_indef_ref.borrow().len();
            if chunk_offset >= chunk_len {
                return Err(HugeVecError::ChunkCorrupted {
                    chunk_len,
                    access_offset: chunk_offset,
                });
            }
        }

        let item_ref = Ref::map(chunk_indef_ref.borrow(), |c| &c[chunk_offset]);
        Ok(ItemRef {
            chunk_rc: chunk_rc.clone(),
            item_ref,
        })
    }
}

impl<'v, T, Store, const CHUNK_SZ: usize> Iterator for HugeVecIter<'v, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type Item = Result<ItemRef<'v, T, T, CHUNK_SZ>, HugeVecError<Store::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.len {
            return None;
        }

        let item = self.nth(self.idx);

        self.idx += 1;

        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.idx;
        (remaining as usize, Some(remaining as usize))
    }
}

impl<T, Store, const CHUNK_SZ: usize> FusedIterator for HugeVecIter<'_, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
}

impl<T, Store, const CHUNK_SZ: usize> ExactSizeIterator for HugeVecIter<'_, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
}

impl<T, Store, const CHUNK_SZ: usize> DoubleEndedIterator for HugeVecIter<'_, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx == self.len || self.len == 0 {
            return None;
        }

        let item = self.nth(self.len - 1);
        self.len -= 1;
        Some(item)
    }
}

pub struct HugeVecSliceMut<'s, T, Store, const CHUNK_SZ: usize>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    chunk_cache: &'s RefCell<ChunkCache<T, Store, CHUNK_SZ>>,
    offset: u64,
    len: u64,
}

impl<'s, T, Store, const CHUNK_SZ: usize> HugeVecSliceMut<'s, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    pub fn get<'r>(
        &'r self,
        idx: u64,
    ) -> Result<ItemRef<'r, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        if idx >= self.len {
            return Err(HugeVecError::OutOfBoundsAccess { len: self.len, idx });
        }

        let chunk_idx = (idx / CHUNK_SZ as u64) as usize;
        let chunk_offset = (idx % CHUNK_SZ as u64) as usize;

        let chunk_rc = self.chunk_cache.borrow_mut().get(chunk_idx)?;

        // create a reference to the chunk data that lives as long as needed
        // Safety: we will not drop chunk_rc while the reference (and any others derivable from it) are still in use
        let chunk_indef_ref: &'_ RefCell<CachedChunk<T, CHUNK_SZ>> = &chunk_rc;
        let chunk_indef_ref: &'s RefCell<CachedChunk<T, CHUNK_SZ>> =
            unsafe { std::mem::transmute(chunk_indef_ref) };

        // chunk may be corrupted
        let chunk_len = chunk_indef_ref.borrow().len();
        if chunk_offset >= chunk_len {
            return Err(HugeVecError::ChunkCorrupted {
                chunk_len,
                access_offset: chunk_offset,
            });
        }

        let item_ref = Ref::map(chunk_indef_ref.borrow(), |c| &c[chunk_offset]);
        Ok(ItemRef { chunk_rc, item_ref })
    }

    pub fn get_mut<'r>(
        &'r mut self,
        idx: u64,
    ) -> Result<ItemRefMut<'r, T, T, CHUNK_SZ>, HugeVecError<Store::Error>> {
        if idx >= self.len {
            return Err(HugeVecError::OutOfBoundsAccess { len: self.len, idx });
        }

        let chunk_idx = (idx / CHUNK_SZ as u64) as usize;
        let chunk_offset = (idx % CHUNK_SZ as u64) as usize;

        let chunk_rc = self.chunk_cache.borrow_mut().get(chunk_idx)?;

        // create a reference to the chunk data that lives as long as needed
        // Safety: we will not drop chunk_rc while the reference (and any others derivable from it) are still in use
        let chunk_indef_ref: &'_ RefCell<CachedChunk<T, CHUNK_SZ>> = &chunk_rc;
        let chunk_indef_ref: &'s RefCell<CachedChunk<T, CHUNK_SZ>> =
            unsafe { std::mem::transmute(chunk_indef_ref) };

        // chunk may be corrupted
        {
            let chunk_len = chunk_indef_ref.borrow().len();
            if chunk_offset >= chunk_len {
                return Err(HugeVecError::ChunkCorrupted {
                    chunk_len,
                    access_offset: chunk_offset,
                });
            }
        }

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
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type Ref<'r>
        = ItemRef<'r, T, T, CHUNK_SZ>
    where
        's: 'r;

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
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type RefMut<'r>
        = ItemRefMut<'r, T, T, CHUNK_SZ>
    where
        's: 'r;

    fn get_mut(&mut self, idx: usize) -> Option<Self::RefMut<'_>> {
        match self.get_mut(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsAccess { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }

    fn map_get_mut(self, idx: usize) -> Option<Self::RefMut<'s>> {
        match self.map_get_mut(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsAccess { .. }) => None,
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
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static;

impl<'s, T, Store, const CHUNK_SZ: usize> Deref for HugeVecSlice<'s, T, Store, CHUNK_SZ>
where
    T: Debug + 's,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>>,
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type Target = HugeVecSliceMut<'s, T, Store, CHUNK_SZ>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'s, T, Store, const CHUNK_SZ: usize> HugeVecSlice<'s, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
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

    pub fn binary_search_by<'a, F>(&'a self, mut f: F) -> std::result::Result<u64, u64>
    where
        's: 'a,
        F: FnMut(ItemRef<'a, T, T, CHUNK_SZ>) -> std::cmp::Ordering,
    {
        // Implementation copied from Rust source code ([T]::binary_search_by)
        let mut size = self.len;
        if size == 0 {
            return Err(0);
        }
        let mut base = 0u64;

        // This loop intentionally doesn't have an early exit if the comparison
        // returns Equal. We want the number of loop iterations to depend *only*
        // on the size of the input slice so that the CPU can reliably predict
        // the loop count.
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            // SAFETY: the call is made safe by the following inconstants:
            // - `mid >= 0`: by definition
            // - `mid < size`: `mid = size / 2 + size / 4 + size / 8 ...`
            let cmp = f(self.get(mid).expect("corrupted hugevec chunk"));

            // TODO: force compiler to use conditional moves when supported like [T]::binary_search_by
            // this currently relies on core::intrinsics::select_unpredictable
            base = if cmp == Ordering::Greater { base } else { mid };

            // This is imprecise in the case where `size` is odd and the
            // comparison returns Greater: the mid element still gets included
            // by `size` even though it's known to be larger than the element
            // being searched for.
            //
            // This is fine though: we gain more performance by keeping the
            // loop iteration count invariant (and thus predictable) than we
            // lose from considering one additional element.
            size -= half;
        }

        // SAFETY: base is always in [0, size) because base <= mid.
        let cmp = f(self.get(base).expect("corrupted hugevec chunk"));
        if cmp == Ordering::Equal {
            // SAFETY: same as the `get_unchecked` above.
            unsafe { std::hint::assert_unchecked(base < self.len) };
            Ok(base)
        } else {
            let result = base + (cmp == Ordering::Less) as u64;
            // SAFETY: same as the `get_unchecked` above.
            // Note that this is `<=`, unlike the assume in the `Ok` path.
            unsafe { std::hint::assert_unchecked(result <= self.len) };
            Err(result)
        }
    }

    pub fn partition_point<'a, P>(&'s self, mut pred: P) -> u64
    where
        's: 'a,
        P: FnMut(ItemRef<'a, T, T, CHUNK_SZ>) -> bool,
    {
        self.binary_search_by(|x| {
            if pred(x) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
        .unwrap_or_else(|i| i)
    }
}

impl<'s, T, Store, const CHUNK_SZ: usize> vector_trees::VectorSlice<'s, T>
    for HugeVecSlice<'s, T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type Ref<'r>
        = ItemRef<'r, T, T, CHUNK_SZ>
    where
        's: 'r;

    fn len(&self) -> usize {
        self.deref().len()
    }

    fn get(&self, idx: usize) -> Option<Self::Ref<'_>> {
        match self.deref().get(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsAccess { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }

    fn map_get(self, idx: usize) -> Option<Self::Ref<'s>> {
        match self.map_get(idx as u64) {
            Ok(x) => Some(x),
            Err(HugeVecError::OutOfBoundsAccess { .. }) => None,
            Err(e) => panic!("{e:#?}"),
        }
    }
}

impl<T, Store, const CHUNK_SZ: usize> vector_trees::Vector<T> for HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    type Slice<'s>
        = HugeVecSlice<'s, T, Store, CHUNK_SZ>
    where
        T: 's,
        Self: 's;
    type SliceMut<'s>
        = HugeVecSliceMut<'s, T, Store, CHUNK_SZ>
    where
        T: 's,
        Self: 's;

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

// Safety:
// HugeVec uses non-Send types internally but all references to them bounded by the lifetime of the HugeVec
// thus they may be sent to other threads safely.
unsafe impl<T, Store, const CHUNK_SZ: usize> Send for HugeVec<T, Store, CHUNK_SZ>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
}

#[cfg(test)]
mod test {
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use crate::huge_vec::FsStore;
    use crate::huge_vec::IOTransformer;

    use super::HugeVec;
    use super::ZstdTransformer;
    use super::chunk_cache::READAHEAD_COUNT;

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
    test_push_readback!(multi_chunk_push_gc, 2 * (READAHEAD_COUNT as u64 * 2) + 4);

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
                vec.iter()
                    .map(|ri| ri.map(|i| *i))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                (0..=(prev_len - 1) as u8).collect::<Vec<_>>()
            );
            vec.truncate(((offset + 1) * 10 + offset) as u64).unwrap();
        }

        {
            let mut vec = vec_opener();
            assert_eq!(
                vec.iter()
                    .map(|ri| ri.map(|i| *i))
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
                (0..10u8).collect::<Vec<_>>()
            );

            vec.truncate(0).unwrap();
        }

        let vec = vec_opener();
        assert!(vec.is_empty());
    }

    #[test]
    fn constant_seq_write_memuse() {
        let dir = tempdir::TempDir::new("constant_seq_write_memuse").unwrap();
        let store = FsStore::open(&dir, ()).unwrap();
        let mut vec = HugeVec::<u8, _, 2>::new(store).unwrap();

        for _ in 0..(READAHEAD_COUNT * 2 * 100) {
            vec.push(42).unwrap();

            let num_dirty_chunks = vec.chunk_cache.borrow().dirty_chunk_count();
            assert!(num_dirty_chunks < 4);
        }
    }

    #[test]
    fn seq_write_no_early_writeback() {
        let dir = tempdir::TempDir::new("constant_seq_write_memuse").unwrap();

        struct Recorder {
            writebacks: AtomicU64,
        }
        impl IOTransformer for Arc<Recorder> {
            type Error = io::Error;
            type Reader<R>
                = R
            where
                R: io::BufRead;
            type Writer<W>
                = W
            where
                W: io::Write;

            fn wrap_read<R: io::BufRead, T, E>(
                &self,
                mut orig_reader: R,
                read_fn: impl FnOnce(&mut Self::Reader<R>) -> Result<T, E>,
            ) -> Result<T, E>
            where
                E: From<Self::Error>,
            {
                read_fn(&mut orig_reader)
            }

            fn wrap_write<W: io::Write, T, E>(
                &self,
                mut orig_writer: W,
                write_fn: impl FnOnce(&mut Self::Writer<W>) -> Result<T, E>,
            ) -> Result<T, E>
            where
                E: From<Self::Error>,
            {
                self.writebacks.fetch_add(1, Ordering::Release);
                write_fn(&mut orig_writer)
            }
        }

        let recorder = Arc::new(Recorder {
            writebacks: AtomicU64::new(0),
        });
        let fs_store = FsStore::open(&dir, Arc::clone(&recorder)).unwrap();
        const CHUNK_SZ: usize = 4;
        let mut vec = HugeVec::<u8, _, CHUNK_SZ>::new(fs_store).unwrap();

        const ITER_MUL: usize = 128;
        for _ in 0..(READAHEAD_COUNT * ITER_MUL * CHUNK_SZ) {
            vec.push(42).unwrap();
        }

        // both metadata and data use wrap_writer, so we must divide by 2
        let num_writebacks = recorder.writebacks.load(Ordering::Acquire) / 2;
        assert!((num_writebacks as usize) < READAHEAD_COUNT * ITER_MUL);
    }
}
