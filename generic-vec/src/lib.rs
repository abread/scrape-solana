use std::fmt::Debug;

use generic_ref::{Ref, RefMut};

mod slicish;
pub use slicish::Slicish;

mod slicish_mut;
pub use slicish_mut::SlicishMut;

pub trait Vecish<T> {
    fn capacity(&self) -> u64;
    fn reserve(&mut self, additional: u64) {
        self.try_reserve(additional).expect("try_reserve failed");
    }
    fn reserve_exact(&mut self, additional: u64) {
        self.try_reserve_exact(additional)
            .expect("try_reserve_exact failed");
    }

    type TryReserveError: Debug;
    fn try_reserve(&mut self, additional: u64) -> Result<(), Self::TryReserveError> {
        self.try_reserve_exact(additional)
    }
    fn try_reserve_exact(&mut self, additional: u64) -> Result<(), Self::TryReserveError>;

    fn shrink_to_fit(&mut self) {
        self.shrink_to(self.len());
    }
    fn shrink_to(&mut self, min_capacity: u64);

    fn truncate(&mut self, len: u64);

    type ItemRef<'r>: Ref<'r, T>
    where
        T: 'r;
    type ItemRefMut<'r>: RefMut<'r, T>
    where
        T: 'r;

    type Slicish<'s>: Slicish<'s, T>
    where
        Self: 's,
        T: 's;
    fn as_slicish(&self) -> Self::Slicish<'_>;
    type SlicishMut<'s>
    where
        Self: 's;
    fn as_slicish_mut(&mut self) -> Self::SlicishMut<'_>;

    fn swap_remove(&mut self, index: u64) -> T {
        todo!();
    }

    fn insert(&mut self, index: u64, element: T);
    fn remove(&mut self, index: u64) -> T;

    fn retain(&mut self, mut f: impl FnMut(&T) -> bool) {
        self.retain_mut(|item| f(&*item));
    }
    fn retain_mut(&mut self, f: impl FnMut(&mut T) -> bool);

    fn dedup_by_key<F, K>(&mut self, mut key: F)
    where
        F: FnMut(&mut T) -> K,
        K: PartialEq<K>,
    {
        self.dedup_by(|a, b| key(a) == key(b));
    }
    fn dedup_by<F>(&mut self, same_bucket: F)
    where
        F: FnMut(&mut T, &mut T) -> bool;

    fn push(&mut self, element: T) {
        if self.len() == self.capacity() {
            self.reserve(1);
        }
        if self.push_within_capacity(element).is_err() && cfg!(debug_assertions) {
            panic!("broken reserve");
        }
    }
    fn push_within_capacity(&mut self, element: T) -> Result<(), T>;

    fn pop(&mut self) -> Option<T>;
    fn pop_if(&mut self, f: impl FnOnce(&T) -> bool) -> Option<T> {
        todo!()
    }

    fn append<Other: Vecish<T>>(&mut self, other: &mut Other);

    type DrainIter<'vref>: Iterator<Item = T>
    where
        T: 'vref,
        Self: 'vref;
    fn drain<R>(&mut self, range: R) -> Self::DrainIter<'_>
    where
        R: std::ops::RangeBounds<u64>;

    fn clear(&mut self);
    fn len(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn split_off(&mut self, at: u64) -> Self;

    fn resize_with(&mut self, new_len: u64, mut f: impl FnMut() -> T) {
        if new_len > self.len() {
            self.reserve(new_len - self.len());
            for _ in self.len()..new_len {
                if self.push_within_capacity(f()).is_err() && cfg!(debug_assertions) {
                    panic!("broken reserve");
                }
            }
        } else {
            self.truncate(new_len);
        }
    }

    unsafe fn set_len(&mut self, new_len: u64);
    type UninitSlicishMut<'s>
    where
        Self: 's;
    fn spare_capacity_mut(&mut self) -> Self::UninitSlicishMut<'_>;
    fn split_at_spare_mut(&mut self) -> (Self::SlicishMut<'_>, Self::UninitSlicishMut<'_>);

    fn resize(&mut self, new_len: u64, value: T)
    where
        T: Clone,
    {
        self.resize_with(new_len, || value.clone());
    }

    // TODO: Other: Slicish<T>
    fn extend_from_slice<Other>(&mut self, other: Other)
    where
        T: Clone,
    {
        todo!()
    }
    fn extend_from_within<R>(&mut self, range: R)
    where
        R: std::ops::RangeBounds<u64>,
        T: Clone,
    {
        todo!()
    }

    // TODO: into_flattened?

    fn dedup(&mut self)
    where
        T: PartialEq;

    type SpliceIter<'vref>: Iterator<Item = T>
    where
        T: 'vref,
        Self: 'vref;
    fn splice<R, I>(&mut self, range: R, replace_with: I) -> Self::SpliceIter<'_>
    where
        R: std::ops::RangeBounds<u64>,
        I: IntoIterator<Item = T>;

    type ExtractIfIter<'vref>: Iterator<Item = T>
    where
        T: 'vref,
        Self: 'vref;
    fn extract_if(&mut self, f: impl FnMut(&T) -> bool) -> Self::ExtractIfIter<'_>;
}
