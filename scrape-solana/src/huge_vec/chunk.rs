use serde::{Deserialize, Serialize};
use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk<T, const SZ: usize> {
    storage: Vec<T>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("Chunk is full.")]
pub struct ChunkFull;

impl<T, const SZ: usize> Deref for Chunk<T, SZ> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<T, const SZ: usize> DerefMut for Chunk<T, SZ> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

impl<T, const SZ: usize> Chunk<T, SZ> {
    pub fn new() -> Self {
        Self {
            storage: Vec::with_capacity(SZ),
        }
    }

    pub fn push_within_capacity(&mut self, val: T) -> Result<(), ChunkFull> {
        if self.storage.len() == SZ {
            Err(ChunkFull)
        } else {
            self.storage.push(val);
            Ok(())
        }
    }

    pub fn truncate(&mut self, new_len: usize) {
        self.storage.truncate(new_len);
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub const fn capacity(&self) -> usize {
        SZ
    }
}

impl<T, const SZ: usize> Default for Chunk<T, SZ> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod test {
    use super::Chunk;

    #[test]
    fn vec() {
        let mut c: Chunk<i32, 8> = Chunk::new();
        for i in 0..8 {
            c.push_within_capacity(i).unwrap();
        }
        assert_eq!(c.push_within_capacity(8), Err(super::ChunkFull));

        assert_eq!(c.len(), 8);
        for i in 0..8 {
            c[i] *= 2;
        }
        for i in 0..8 {
            assert_eq!(c[i], 2 * i as i32);
        }
    }

    #[test]
    fn serialization_full() {
        let mut c: Chunk<i32, 8> = Chunk::new();
        for i in 0..8 {
            c.push_within_capacity(i).unwrap();
        }

        let serialized = bincode::serialize(&c).unwrap();
        let deserialized: Chunk<i32, 8> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(c, deserialized);
    }

    #[test]
    fn serialization_partial() {
        let mut c: Chunk<i32, 8> = Chunk::new();
        for i in 0..4 {
            c.push_within_capacity(i).unwrap();
        }

        let serialized = bincode::serialize(&c).unwrap();
        let deserialized: Chunk<i32, 8> = bincode::deserialize(&serialized).unwrap();

        assert_eq!(c, deserialized);
    }
}
