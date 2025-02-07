use std::{
    cmp::Ordering,
    ops::{Deref, RangeBounds},
};

use generic_ref::Ref;

pub trait Slicish<'s, T>
where
    Self: 's,
    T: 's,
{
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    type ItemRef<'i>: Ref<'i, T>
    where
        's: 'i;
    unsafe fn get_unchecked(&self, index: u64) -> Self::ItemRef<'_>;
    fn get(&self, index: u64) -> Option<Self::ItemRef<'_>> {
        if index < self.len() {
            Some(unsafe { self.get_unchecked(index) })
        } else {
            None
        }
    }

    type Subslice<'s2>: Slicish<'s2, T>
    where
        's: 's2;
    fn get_range<R: RangeBounds<u64>>(&self, range: R) -> Self::Subslice<'_>;

    type MappedItemRef: Ref<'s, T>;
    fn map_get(self, index: u64) -> Option<Self::MappedItemRef>;

    type Iter: Iterator<Item = Self::ItemRef<'s>> + DoubleEndedIterator + ExactSizeIterator;
    fn iter(self) -> Self::Iter;

    type MappedSubslice: Slicish<'s, T>;
    fn split_at(self, mid: u64) -> (Self::MappedSubslice, Self::MappedSubslice);

    fn first(&self) -> Option<Self::ItemRef<'_>> {
        self.get(0)
    }
    fn map_first(self) -> Option<Self::MappedItemRef>
    where
        Self: Sized,
    {
        self.map_get(0)
    }
    fn last(&self) -> Option<Self::ItemRef<'_>> {
        self.get(self.len().saturating_sub(1))
    }
    fn map_last(self) -> Option<Self::MappedItemRef>
    where
        Self: Sized,
    {
        let last_idx = self.len().saturating_sub(1);
        self.map_get(last_idx)
    }

    fn split_first(
        self,
    ) -> Option<(
        <Self::MappedSubslice as Slicish<'s, T>>::MappedItemRef,
        Self::MappedSubslice,
    )>
    where
        Self: Sized,
    {
        let (first, rest) = self.split_at(1);
        first.map_get(0).map(|item| (item, rest))
    }
    fn split_last(
        self,
    ) -> Option<(
        <Self::MappedSubslice as Slicish<'s, T>>::MappedItemRef,
        Self::MappedSubslice,
    )>
    where
        Self: Sized,
    {
        let last_idx = self.len().saturating_sub(1);
        let (start, last) = self.split_at(last_idx);
        last.map_get(0).map(|item| (item, start))
    }

    // windows, chunks, chunks_exact, rchunks, rchunks_exact, chunk_by, split, split_inclusive, rsplit, splitn, rsplitn

    fn contains(&self, x: &T) -> bool
    where
        T: PartialEq,
    {
        for i in 0..self.len() {
            if self.get(i).map(|item| x == &*item).unwrap_or(false) {
                return true;
            }
        }
        false
    }
    fn starts_with<TRef: AsRef<T>>(&self, prefix: impl IntoIterator<Item = TRef>) -> bool
    where
        T: PartialEq,
        Self: Clone,
    {
        self.clone()
            .iter()
            .zip(prefix.into_iter())
            .all(|(a, b)| a.deref() == b.as_ref())
    }
    fn ends_with<TRef: AsRef<T>, S>(&self, suffix: S) -> bool
    where
        T: PartialEq,
        S: IntoIterator<Item = TRef>,
        <S as IntoIterator>::IntoIter: DoubleEndedIterator,
        Self: Clone,
    {
        self.clone()
            .iter()
            .rev()
            .zip(suffix.into_iter().rev())
            .all(|(a, b)| a.deref() == b.as_ref())
    }

    fn binary_search_by<F>(&self, mut f: F) -> Result<u64, u64>
    where
        F: FnMut(&T) -> std::cmp::Ordering,
    {
        // Rust core::slice::[T]::binary_search_by implementation from stable
        // without compiler hint intrinsics (which we don't have access to).

        let mut size = Slicish::len(self); // TODO: why no self.len()?
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
            let cmp = f(unsafe { self.get_unchecked(mid) }.deref());

            // Binary search interacts poorly with branch prediction, so force
            // the compiler to use conditional moves if supported by the target
            // architecture.
            //base = select_unpredictable(cmp == Greater, base, mid);
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
        let cmp = f(unsafe { self.get_unchecked(base) }.deref());
        if cmp == Ordering::Equal {
            // SAFETY: same as the `get_unchecked` above.
            unsafe { std::hint::assert_unchecked(base < self.len()) };
            Ok(base)
        } else {
            let result = base + (cmp == Ordering::Less) as u64;
            // SAFETY: same as the `get_unchecked` above.
            // Note that this is `<=`, unlike the assume in the `Ok` path.
            unsafe { std::hint::assert_unchecked(result <= self.len()) };
            Err(result)
        }
    }

    fn binary_search(&self, x: &T) -> Result<u64, u64>
    where
        T: Ord,
    {
        self.binary_search_by(|k| k.cmp(x))
    }

    fn binary_search_by_key<B, F>(&self, key: &B, mut f: F) -> Result<u64, u64>
    where
        B: Ord,
        F: FnMut(&T) -> B,
    {
        self.binary_search_by(|k| f(k).cmp(key))
    }
    fn partition_point(&self, mut pred: impl FnMut(&T) -> bool) -> u64 {
        self.binary_search_by(|x| {
            if pred(x) {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        })
        .unwrap_or_else(|i| i)
    }

    fn is_sorted_by(&self, mut compare: impl FnMut(&T, &T) -> bool) -> bool {
        for i in 0..self.len() {
            if !compare(
                unsafe { self.get_unchecked(i - 1) }.deref(),
                unsafe { self.get_unchecked(i) }.deref(),
            ) {
                return false;
            }
        }
        true
    }
    fn is_sorted_by_key<K: PartialOrd>(&self, mut key: impl FnMut(&T) -> K) -> bool {
        self.is_sorted_by(|a, b| key(a) <= key(b))
    }
    fn is_sorted(&self) -> bool
    where
        T: PartialOrd,
    {
        self.is_sorted_by(|a, b| a <= b)
    }
}

impl<'s, T> Slicish<'s, T> for &'s [T] {
    fn len(&self) -> u64 {
        (*self).len() as u64
    }

    type ItemRef<'i>
        = &'i T
    where
        's: 'i;
    unsafe fn get_unchecked(&self, index: u64) -> Self::ItemRef<'_> {
        (*self).get_unchecked(index as usize)
    }

    type Subslice<'s2>
        = &'s2 [T]
    where
        's: 's2;
    fn get_range<R: RangeBounds<u64>>(&self, range: R) -> Self::Subslice<'_> {
        use std::ops::Bound::*;
        match (range.start_bound(), range.end_bound()) {
            (Included(&start), Included(&end)) => &self[start as usize..=end as usize],
            (Included(&start), Excluded(&end)) => &self[start as usize..end as usize],
            (Included(&start), Unbounded) => &self[start as usize..],
            (Unbounded, Included(&end)) => &self[..=end as usize],
            (Unbounded, Excluded(&end)) => &self[..end as usize],
            (Unbounded, Unbounded) => &self[..],
            (Excluded(_), _) => unreachable!("slice ranges can't start with Excluded(?)"),
        }
    }

    type MappedItemRef = &'s T;
    fn map_get(self, index: u64) -> Option<Self::MappedItemRef> {
        self.get(index as usize)
    }

    type Iter = std::slice::Iter<'s, T>;
    fn iter(self) -> Self::Iter {
        self.iter()
    }

    type MappedSubslice = &'s [T];
    fn split_at(self, mid: u64) -> (Self::MappedSubslice, Self::MappedSubslice) {
        self.split_at(mid as usize)
    }
}

impl<'s, T> Slicish<'s, T> for &'s mut [T] {
    fn len(&self) -> u64 {
        (**self).len() as u64
    }

    type ItemRef<'i>
        = &'i T
    where
        's: 'i;
    unsafe fn get_unchecked(&self, index: u64) -> Self::ItemRef<'_> {
        (**self).get_unchecked(index as usize)
    }

    type Subslice<'s2>
        = &'s2 [T]
    where
        's: 's2;
    fn get_range<R: RangeBounds<u64>>(&self, range: R) -> Self::Subslice<'_> {
        use std::ops::Bound::*;
        match (range.start_bound(), range.end_bound()) {
            (Included(&start), Included(&end)) => &self[start as usize..=end as usize],
            (Included(&start), Excluded(&end)) => &self[start as usize..end as usize],
            (Included(&start), Unbounded) => &self[start as usize..],
            (Unbounded, Included(&end)) => &self[..=end as usize],
            (Unbounded, Excluded(&end)) => &self[..end as usize],
            (Unbounded, Unbounded) => &self[..],
            (Excluded(_), _) => unreachable!("slice ranges can't start with Excluded(?)"),
        }
    }

    type MappedItemRef = &'s T;
    fn map_get(self, index: u64) -> Option<Self::MappedItemRef> {
        (*self).get(index as usize)
    }

    type Iter = std::slice::Iter<'s, T>;
    fn iter(self) -> Self::Iter {
        (*self).iter()
    }

    type MappedSubslice = &'s [T];
    fn split_at(self, mid: u64) -> (Self::MappedSubslice, Self::MappedSubslice) {
        (*self).split_at(mid as usize)
    }
}
