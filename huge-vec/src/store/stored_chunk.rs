use std::ops::{Deref, DerefMut};

use crate::chunk::Chunk;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct StoredChunk<T> {
    start_idx: u64,
    data: Chunk<T>,
}

impl<T> StoredChunk<T> {
    pub fn new(start_idx: u64, capacity: usize) -> Self {
        Self {
            start_idx,
            data: Chunk::new(capacity),
        }
    }

    pub fn start_idx(&self) -> u64 {
        self.start_idx
    }

    pub fn into_data(self) -> Chunk<T> {
        self.data
    }
}
impl<T> Deref for StoredChunk<T> {
    type Target = Chunk<T>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl<T> DerefMut for StoredChunk<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
