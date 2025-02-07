use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
    rc::Rc,
    sync::Arc,
};

pub trait Ref<'r, T>: Deref<Target = T> {
    type MapTarget<U>: Ref<'r, U>
    where
        U: 'r;
    fn map<U>(self, f: impl FnOnce(&T) -> &U) -> Self::MapTarget<U>;
    fn filter_map<U>(self, f: impl FnOnce(&T) -> Option<&U>) -> Result<Self::MapTarget<U>, Self>
    where
        Self: Sized;
    fn map_split<U1, U2>(
        self,
        f: impl FnOnce(&T) -> (&U1, &U2),
    ) -> (Self::MapTarget<U1>, Self::MapTarget<U2>);
}

impl<'r, T> Ref<'r, T> for &'r T {
    type MapTarget<U>
        = &'r U
    where
        U: 'r;

    fn map<U>(self, f: impl FnOnce(&T) -> &U) -> Self::MapTarget<U> {
        f(self)
    }
    fn filter_map<U>(self, f: impl FnOnce(&T) -> Option<&U>) -> Result<Self::MapTarget<U>, Self> {
        f(self).ok_or(self)
    }
    fn map_split<U1, U2>(
        self,
        f: impl FnOnce(&T) -> (&U1, &U2),
    ) -> (Self::MapTarget<U1>, Self::MapTarget<U2>) {
        f(self)
    }
}

impl<'r, T> Ref<'r, T> for &'r mut T {
    type MapTarget<U>
        = &'r U
    where
        U: 'r;

    fn map<U>(self, f: impl FnOnce(&T) -> &U) -> Self::MapTarget<U> {
        f(&*self)
    }
    fn filter_map<U>(self, f: impl FnOnce(&T) -> Option<&U>) -> Result<Self::MapTarget<U>, Self>
    where
        Self: Sized,
    {
        let self_ptr = self as *mut _;

        // Safety: equivalent to &*self, held only for the duration of the call
        match f(unsafe { &*self_ptr }) {
            Some(mapped) => Ok(mapped),

            // Safety: self is only de-referenced inside of the function, which already completed execution
            None => Err(unsafe { &mut *self_ptr }),
        }
    }
    fn map_split<U1, U2>(
        self,
        f: impl FnOnce(&T) -> (&U1, &U2),
    ) -> (Self::MapTarget<U1>, Self::MapTarget<U2>) {
        f(&*self)
    }
}

impl<'r, T> Ref<'r, T> for std::cell::Ref<'r, T> {
    type MapTarget<U>
        = std::cell::Ref<'r, U>
    where
        U: 'r;

    fn map<U>(self, f: impl FnOnce(&T) -> &U) -> Self::MapTarget<U> {
        std::cell::Ref::map(self, f)
    }
    fn filter_map<U>(self, f: impl FnOnce(&T) -> Option<&U>) -> Result<Self::MapTarget<U>, Self> {
        std::cell::Ref::filter_map(self, f)
    }
    fn map_split<U1, U2>(
        self,
        f: impl FnOnce(&T) -> (&U1, &U2),
    ) -> (Self::MapTarget<U1>, Self::MapTarget<U2>) {
        std::cell::Ref::map_split(self, f)
    }
}

pub struct RefCellImutRefMut<'r, T, G> {
    ptr: NonNull<T>,
    marker: std::marker::PhantomData<&'r T>,
    // guard is the last field to ensure it is dropped last
    guard: Rc<G>,
}
impl<'r, T> RefCellImutRefMut<'r, T, std::cell::RefMut<'r, T>> {
    fn new(cell_ref_mut: std::cell::RefMut<'r, T>) -> Self {
        // Safety: cell::RefMut has exclusive access to the underlying data while it is live, so we can always construct shared references to its data if we keep it alive and never construct mutable references from it.
        // TODO: extract guard from RefCell and use it directly? (needs changes in Rust)
        RefCellImutRefMut {
            ptr: NonNull::from(&*cell_ref_mut),
            marker: std::marker::PhantomData,
            guard: Rc::new(cell_ref_mut),
        }
    }
}
impl<'r, T, G> Deref for RefCellImutRefMut<'r, T, G> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        // Safety: CellImutRefMut is constructed such that we can always construct a valid reference from self.ptr as long as self.guard is alive.
        unsafe { self.ptr.as_ref() }
    }
}
impl<'r, T, G> Ref<'r, T> for RefCellImutRefMut<'r, T, G> {
    type MapTarget<U>
        = RefCellImutRefMut<'r, U, G>
    where
        U: 'r;

    fn map<U>(self, f: impl FnOnce(&T) -> &U) -> Self::MapTarget<U> {
        let mapped = self.deref().map(f);
        // Safety: mapped is a valid reference as long as self.guard is not dropped.
        RefCellImutRefMut {
            ptr: NonNull::from(mapped),
            marker: std::marker::PhantomData,
            guard: self.guard,
        }
    }
    fn filter_map<U>(self, f: impl FnOnce(&T) -> Option<&U>) -> Result<Self::MapTarget<U>, Self>
    where
        Self: Sized,
    {
        match self.deref().filter_map(f) {
            // Safety: mapped is a valid reference as long as self.guard is not dropped.
            Ok(mapped) => Ok(RefCellImutRefMut {
                ptr: NonNull::from(mapped),
                marker: std::marker::PhantomData,
                guard: self.guard,
            }),
            Err(_) => Err(self),
        }
    }
    fn map_split<U1, U2>(
        self,
        f: impl FnOnce(&T) -> (&U1, &U2),
    ) -> (Self::MapTarget<U1>, Self::MapTarget<U2>) {
        let (mapped1, mapped2) = self.deref().map_split(f);
        // Safety: mapped1 and mapped2 are valid references as long as self.guard is not dropped. The guard is behind an Rc which guarantees it will only be dropped when all its related CellImutRefMut are dropped.
        (
            RefCellImutRefMut {
                ptr: NonNull::from(mapped1),
                marker: std::marker::PhantomData,
                guard: Rc::clone(&self.guard),
            },
            RefCellImutRefMut {
                ptr: NonNull::from(mapped2),
                marker: std::marker::PhantomData,
                guard: self.guard,
            },
        )
    }
}

impl<'r, T> Ref<'r, T> for std::cell::RefMut<'r, T> {
    type MapTarget<U>
        = RefCellImutRefMut<'r, U, std::cell::RefMut<'r, T>>
    where
        U: 'r;

    fn map<U>(self, f: impl FnOnce(&T) -> &U) -> Self::MapTarget<U> {
        RefCellImutRefMut::new(self).map(f)
    }
    fn filter_map<U>(self, f: impl FnOnce(&T) -> Option<&U>) -> Result<Self::MapTarget<U>, Self> {
        match RefCellImutRefMut::new(self).filter_map(f) {
            Ok(mapped) => Ok(mapped),
            Err(wrapped_orig) => Err(Rc::into_inner(wrapped_orig.guard).unwrap()),
        }
    }
    fn map_split<U1, U2>(
        self,
        f: impl FnOnce(&T) -> (&U1, &U2),
    ) -> (Self::MapTarget<U1>, Self::MapTarget<U2>) {
        RefCellImutRefMut::new(self).map_split(f)
    }
}

pub trait RefMut<'r, T>: Ref<'r, T> + DerefMut + Sized {
    type MapMutTarget<U>: RefMut<'r, U>
    where
        U: 'r;
    fn map_mut<U>(self, f: impl FnOnce(&mut T) -> &mut U) -> Self::MapMutTarget<U>;
    fn filter_map_mut<U>(
        self,
        f: impl FnOnce(&mut T) -> Option<&mut U>,
    ) -> Result<Self::MapMutTarget<U>, Self>;
    fn map_split_mut<U1, U2>(
        self,
        f: impl FnOnce(&mut T) -> (&mut U1, &mut U2),
    ) -> (Self::MapMutTarget<U1>, Self::MapMutTarget<U2>);
}

impl<'r, T> RefMut<'r, T> for &'r mut T {
    type MapMutTarget<U>
        = &'r mut U
    where
        U: 'r;

    fn map_mut<U>(self, f: impl FnOnce(&mut T) -> &mut U) -> Self::MapMutTarget<U> {
        f(self)
    }
    fn filter_map_mut<U>(
        self,
        f: impl FnOnce(&mut T) -> Option<&mut U>,
    ) -> Result<Self::MapMutTarget<U>, Self> {
        let self_ptr = self as *mut _;

        // SAFETY: function holds onto an exclusive reference for the duration
        // of its call through `orig`, and the pointer is only de-referenced
        // inside of the function call never allowing the exclusive reference to
        // escape.
        if let Some(mapped) = f(unsafe { &mut *self_ptr }) {
            Ok(mapped)
        } else {
            Err(unsafe { &mut *self_ptr })
        }
    }
    fn map_split_mut<U1, U2>(
        self,
        f: impl FnOnce(&mut T) -> (&mut U1, &mut U2),
    ) -> (Self::MapMutTarget<U1>, Self::MapMutTarget<U2>) {
        f(self)
    }
}

impl<'r, T> RefMut<'r, T> for std::cell::RefMut<'r, T> {
    type MapMutTarget<U>
        = std::cell::RefMut<'r, U>
    where
        U: 'r;

    fn map_mut<U>(self, f: impl FnOnce(&mut T) -> &mut U) -> Self::MapMutTarget<U> {
        std::cell::RefMut::map(self, f)
    }
    fn filter_map_mut<U>(
        self,
        f: impl FnOnce(&mut T) -> Option<&mut U>,
    ) -> Result<Self::MapMutTarget<U>, Self> {
        std::cell::RefMut::filter_map(self, f)
    }
    fn map_split_mut<U1, U2>(
        self,
        f: impl FnOnce(&mut T) -> (&mut U1, &mut U2),
    ) -> (Self::MapMutTarget<U1>, Self::MapMutTarget<U2>) {
        std::cell::RefMut::map_split(self, f)
    }
}

// TODO: mapped lock guards (currently unstable)
