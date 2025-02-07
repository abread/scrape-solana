use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut, RangeBounds},
};

use crate::Slicish;
use generic_ref::RefMut;

pub trait SlicishMut<'s, T>: Slicish<'s, T>
where
    Self: 's,
    T: 's,
{
    type ItemRefMut<'i>: RefMut<'i, T>
    where
        's: 'i;
    unsafe fn get_mut_unchecked(&mut self, index: u64) -> Self::ItemRefMut<'_>;
    fn get_mut(&mut self, index: u64) -> Option<Self::ItemRefMut<'_>> {
        if index < self.len() {
            Some(unsafe { self.get_mut_unchecked(index) })
        } else {
            None
        }
    }

    type SubsliceMut<'s2>: SlicishMut<'s2, T>
    where
        's: 's2;
    fn get_range_mut<R: RangeBounds<u64>>(&mut self, range: R) -> Self::SubsliceMut<'_>;

    type MappedItemRefMut: RefMut<'s, T>;
    fn map_get_mut(self, index: u64) -> Option<Self::MappedItemRefMut>;

    type IterMut: Iterator<Item = Self::ItemRefMut<'s>> + DoubleEndedIterator + ExactSizeIterator;
    fn iter_mut(self) -> Self::IterMut;

    fn split_at_mut(&mut self, mid: u64) -> (Self::SubsliceMut<'_>, Self::SubsliceMut<'_>);

    type MappedSubsliceMut: SlicishMut<'s, T>;
    fn map_split_at_mut(self, mid: u64) -> (Self::MappedSubsliceMut, Self::MappedSubsliceMut);

    fn first_mut(&mut self) -> Option<Self::ItemRefMut<'_>> {
        self.get_mut(0)
    }
    fn map_first_mut(self) -> Option<Self::MappedItemRefMut>
    where
        Self: Sized,
    {
        self.map_get_mut(0)
    }
    fn last_mut(&mut self) -> Option<Self::ItemRefMut<'_>> {
        self.get_mut(self.len().saturating_sub(1))
    }
    fn map_last_mut(self) -> Option<Self::MappedItemRefMut>
    where
        Self: Sized,
    {
        let last_idx = self.len().saturating_sub(1);
        self.map_get_mut(last_idx)
    }

    fn split_first_mut(
        &mut self,
    ) -> Option<(
        <Self::SubsliceMut<'_> as SlicishMut<'_, T>>::MappedItemRefMut,
        Self::SubsliceMut<'_>,
    )>
    where
        Self: Sized,
    {
        let (first, rest) = self.split_at_mut(1);
        first.map_get_mut(0).map(|item| (item, rest))
    }
    fn map_split_first_mut(
        self,
    ) -> Option<(
        <Self::MappedSubsliceMut as SlicishMut<'s, T>>::MappedItemRefMut,
        Self::MappedSubsliceMut,
    )>
    where
        Self: Sized,
    {
        let (first, rest) = self.map_split_at_mut(1);
        first.map_get_mut(0).map(|item| (item, rest))
    }
    fn split_last_mut(
        &mut self,
    ) -> Option<(
        <Self::SubsliceMut<'_> as SlicishMut<'_, T>>::MappedItemRefMut,
        Self::SubsliceMut<'_>,
    )>
    where
        Self: Sized,
    {
        let last_idx = self.len().saturating_sub(1);
        let (start, last) = self.split_at_mut(last_idx);
        last.map_get_mut(0).map(|item| (item, start))
    }
    fn map_split_last_mut(
        self,
    ) -> Option<(
        <Self::MappedSubsliceMut as SlicishMut<'s, T>>::MappedItemRefMut,
        Self::MappedSubsliceMut,
    )>
    where
        Self: Sized,
    {
        let last_idx = self.len().saturating_sub(1);
        let (start, last) = self.map_split_at_mut(last_idx);
        last.map_get_mut(0).map(|item| (item, start))
    }

    fn swap(&mut self, a: u64, b: u64) {
        assert!(a < self.len(), "index out of bounds");
        assert!(b < self.len(), "index out of bounds");
        if a == b {
            return;
        }

        let (a, b) = if a < b { (a, b) } else { (b, a) };

        let (mut a_slice, mut b_slice) = self.split_at_mut(b);
        let mut a = unsafe { a_slice.get_mut_unchecked(a) };
        let mut b = unsafe { b_slice.get_mut_unchecked(0) };
        std::mem::swap(a.deref_mut(), b.deref_mut());
    }

    fn reverse(&mut self) {
        let len = self.len();
        let mut i = 0;
        let mut j = len.saturating_sub(1);
        while i < j {
            self.swap(i, j);
            i += 1;
            j = j.saturating_sub(1);
        }
    }

    // chunks_mut, chunks_exact_mut, rchunks_mut, rchunks_exact_mut, chunk_by_mut, split_mut, split_inclusive_mut, rsplit_mut, splitn_mut, rsplitn_mut

    // sort
}
