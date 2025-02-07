use std::{
    alloc::Layout,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use serde::{ser::SerializeStruct, Deserialize, Serialize};

pub struct Chunk<T> {
    capacity: usize,
    len: usize,
    storage: NonNull<T>,
}

impl<T> Chunk<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity * std::mem::size_of::<T>() < isize::MAX as usize); // should always hold

        let alloc_layout = Layout::array::<T>(capacity).unwrap();
        let storage = unsafe { std::alloc::alloc(alloc_layout) as *mut T };
        let storage = NonNull::new(storage).expect("alloc fail");

        Self {
            capacity,
            len: 0,
            storage,
        }
    }

    fn new_from_parts(capacity: usize, mut values: Vec<T>) -> Self {
        assert!(values.len() <= capacity);
        let mut chunk = Self::new(capacity);

        // move values to chunk
        for v in values.drain(..) {
            chunk.push(v);
        }

        chunk
    }

    pub fn push(&mut self, value: T) {
        if self.len < self.capacity {
            // Safety: len < capacity (< isize::MAX)
            let ptr = unsafe { self.storage.offset(self.len as isize) };

            // Safety: self.storage is aligned for Ts, .offset(len) is within the allocation
            // Bonus: self.storage.offset(self.len) is uninitialized, so there's no T to drop.
            unsafe {
                ptr.write(value);
            }

            self.len += 1;
        } else {
            unreachable!("attempted to push beyond chunk limits");
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.storage.as_ptr() as *const _, self.len) }
    }

    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.storage.as_ptr(), self.len) }
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }
}

impl<T> Deref for Chunk<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T> DerefMut for Chunk<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}

impl<T> Drop for Chunk<T> {
    fn drop(&mut self) {
        for idx in 0..self.len {
            // Safety: idx < len and idx < isize::MAX (see Chunk::new)
            let ptr = unsafe { self.storage.offset(idx as isize) };

            // Safety: ptr is a valid pointer to a chunk element, not referenced by anything at the moment
            // this leaves memory uninitialized, but it's okay because we do not reference it again.
            unsafe {
                ptr.drop_in_place();
            }
        }

        let layout = Layout::array::<T>(self.capacity).unwrap();
        // Safety: self.storage was allocated in Chunk::new with this exact layout and std::alloc::alloc
        unsafe {
            std::alloc::dealloc(self.storage.as_ptr() as *mut u8, layout);
        }
    }
}

impl<T: Serialize> Serialize for Chunk<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("Chunk", 2)?;
        s.serialize_field("capacity", &self.capacity)?;
        s.serialize_field("values", self.as_slice())?;
        s.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Chunk<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Capacity,
            Values,
        }

        struct ChunkVisitor<T> {
            _a: PhantomData<T>,
        }
        impl<T> Default for ChunkVisitor<T> {
            fn default() -> Self {
                Self { _a: PhantomData }
            }
        }
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for ChunkVisitor<T> {
            type Value = Chunk<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "struct Chunk<T>")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let capacity = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let values = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;

                Ok(Chunk::new_from_parts(capacity, values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut capacity = None;
                let mut values = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Capacity => {
                            if capacity.is_some() {
                                return Err(serde::de::Error::duplicate_field("capacity"));
                            }
                            capacity = Some(map.next_value()?);
                        }
                        Field::Values => {
                            if values.is_some() {
                                return Err(serde::de::Error::duplicate_field("values"));
                            }
                            values = Some(map.next_value()?);
                        }
                    }
                }

                let capacity =
                    capacity.ok_or_else(|| serde::de::Error::missing_field("capacity"))?;
                let values = values.ok_or_else(|| serde::de::Error::missing_field("values"))?;
                Ok(Chunk::new_from_parts(capacity, values))
            }
        }

        deserializer.deserialize_struct("Chunk", &["capacity", "values"], ChunkVisitor::default())
    }
}
