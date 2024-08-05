use core::ops::{Deref, DerefMut};

use alloc::vec::Vec;

/// This trait abstracts away a vector implementation.
///
/// It is useful for supporting other vectors as tree's backing storage, such as SmallVec and Bumpalo's Vec.
pub trait Vector<T> {
    type Slice<'s>: VectorSlice<'s, T>
    where
        T: 's,
        Self: 's;
    type SliceMut<'s>: VectorSliceMut<'s, T>
    where
        T: 's,
        Self: 's;

    fn clear(&mut self);
    fn push(&mut self, value: T);
    fn slice(&self) -> Self::Slice<'_>;
    fn slice_mut(&mut self) -> Self::SliceMut<'_>;

    fn len(&self) -> usize {
        self.slice().len()
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get<'r>(&'r self, idx: usize) -> Option<impl Ref<'r, T>>
    where
        T: 'r,
    {
        self.slice().map_get(idx)
    }
    fn get_mut<'r>(&'r mut self, idx: usize) -> Option<impl RefMut<'r, T>>
    where
        T: 'r,
    {
        self.slice_mut().map_get_mut(idx)
    }
}

pub trait VectorSlice<'s, T: 's> {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn get<'r>(&'r self, idx: usize) -> Option<impl Ref<'r, T>>
    where
        's: 'r;
    fn map_get(self, idx: usize) -> Option<impl Ref<'s, T>>;
}

pub trait VectorSliceMut<'s, T: 's>: VectorSlice<'s, T>
where
    Self: Sized,
{
    fn get_mut<'r>(&'r mut self, idx: usize) -> Option<impl RefMut<'r, T>>
    where
        's: 'r;
    fn split_at_mut(self, idx: usize) -> (Self, Self);
    fn split_first_mut(self) -> Option<(impl RefMut<'s, T>, Self)>;

    fn map_get_mut(self, idx: usize) -> Option<impl RefMut<'s, T>>
    where
        Self: Sized,
    {
        let (_left, right) = self.split_at_mut(idx);
        right.split_first_mut().map(|(first, _rest)| first)
    }
}

pub trait Ref<'s, T: 's>: Deref<Target = T> {
    fn map<U: 's>(self, mapper: impl FnOnce(&T) -> &U) -> impl Ref<'s, U>;
}
impl<'s, T: 's> Ref<'s, T> for &'s T {
    fn map<U: 's>(self, mapper: impl FnOnce(&T) -> &U) -> impl Ref<'s, U> {
        mapper(self)
    }
}
impl<'s, T: 's> Ref<'s, T> for &'s mut T {
    fn map<U: 's>(self, mapper: impl FnOnce(&T) -> &U) -> impl Ref<'s, U> {
        mapper(self)
    }
}
impl<'s, T: 's> Ref<'s, T> for core::cell::Ref<'s, T> {
    fn map<U: 's>(self, mapper: impl FnOnce(&T) -> &U) -> impl Ref<'s, U> {
        core::cell::Ref::map(self, mapper)
    }
}

pub trait RefMut<'s, T: 's>: Deref<Target = T> + DerefMut {
    fn map_mut<U: 's>(self, mapper: impl FnOnce(&mut T) -> &mut U) -> impl RefMut<'s, U>;
}
impl<'s, T: 's> RefMut<'s, T> for &'s mut T {
    fn map_mut<U: 's>(self, mapper: impl FnOnce(&mut T) -> &mut U) -> impl RefMut<'s, U> {
        mapper(self)
    }
}
impl<'s, T: 's> RefMut<'s, T> for core::cell::RefMut<'s, T> {
    fn map_mut<U: 's>(self, mapper: impl FnOnce(&mut T) -> &mut U) -> impl RefMut<'s, U> {
        core::cell::RefMut::map(self, mapper)
    }
}
/*
Depends on mapped_lock_guards nightly feature

#[cfg(feature = "std")]
impl<'s, T:'s> RefMut<'s, T> for std::sync::MutexGuard<'s, T> {
    fn map_mut<U: 's>(self, mapper: impl FnOnce(&mut T) -> &mut U) -> impl RefMut<'s, U> {
        std::sync::MutexGuard::map(self, mapper)
    }
}
#[cfg(feature = "std")]
impl<'s, T:'s> RefMut<'s, T> for std::sync::MappedMutexGuard<'s, T> {
    fn map_mut<U: 's>(self, mapper: impl FnOnce(&mut T) -> &mut U) -> impl RefMut<'s, U> {
        std::sync::MappedMutexGuard::map(self, mapper)
    }
}
*/

impl<T> Vector<T> for Vec<T> {
    type Slice<'s> = &'s [T] where T: 's;
    type SliceMut<'s> = &'s mut [T] where T: 's;

    fn clear(&mut self) {
        Vec::clear(self);
    }

    fn push(&mut self, value: T) {
        Vec::push(self, value);
    }

    fn slice(&self) -> Self::Slice<'_> {
        Vec::as_slice(self)
    }

    fn slice_mut(&mut self) -> Self::SliceMut<'_> {
        Vec::as_mut_slice(self)
    }

    fn len(&self) -> usize {
        Vec::len(self)
    }
}

impl<'s, T> VectorSlice<'s, T> for &'s [T] {
    fn len(&self) -> usize {
        (*self).len()
    }

    fn get<'r>(&'r self, idx: usize) -> Option<impl Ref<'r, T>>
    where
        's: 'r,
    {
        (*self).get(idx)
    }

    fn map_get(self, idx: usize) -> Option<impl Ref<'s, T>> {
        self.get(idx)
    }
}

impl<'s, T> VectorSlice<'s, T> for &'s mut [T] {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn get<'r>(&'r self, idx: usize) -> Option<impl Ref<'r, T>>
    where
        's: 'r,
    {
        (**self).get(idx)
    }

    fn map_get(self, idx: usize) -> Option<impl Ref<'s, T>> {
        (*self).get(idx)
    }
}

impl<'s, T> VectorSliceMut<'s, T> for &'s mut [T] {
    fn get_mut<'r>(&'r mut self, idx: usize) -> Option<impl RefMut<'r, T>>
    where
        's: 'r,
    {
        (*self).get_mut(idx)
    }

    fn split_at_mut(self, idx: usize) -> (Self, Self) {
        (*self).split_at_mut(idx)
    }

    fn split_first_mut(self) -> Option<(impl RefMut<'s, T>, Self)> {
        (*self).split_first_mut()
    }
}

#[cfg(feature = "bumpalo")]
impl<T> Vector<T> for bumpalo::collections::Vec<'_, T> {
    fn clear(&mut self) {
        bumpalo::collections::Vec::clear(self);
    }

    fn len(&self) -> usize {
        bumpalo::collections::Vec::len(self)
    }

    fn push(&mut self, value: T) {
        bumpalo::collections::Vec::push(self, value);
    }

    fn slice(&self) -> impl VectorSlice<'_, T> {
        bumpalo::collections::Vec::as_slice(self)
    }

    fn slice_mut(&mut self) -> impl VectorSliceMut<'_, T> {
        bumpalo::collections::Vec::as_mut_slice(self)
    }
}
