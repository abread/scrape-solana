pub mod actors;
mod crc_checksum_serde;
mod db;
pub mod huge_vec;
mod model;
pub mod solana_api;

use rand::Rng;
use vector_trees::Vector;

pub(crate) fn select_random_elements<'r, T: 'r, C: Collection<T>>(
    collection: &'r C,
    mut n_samples: u64,
) -> impl Iterator<Item = (usize, <C::Iter<'r> as Iterator>::Item)> {
    let mut rng = rand::thread_rng();
    const N_ELEMENTS_ENDS: usize = 5;
    if n_samples > 2 * N_ELEMENTS_ENDS as u64 {
        n_samples -= 2 * N_ELEMENTS_ENDS as u64;
    }

    collection
        .iter()
        .enumerate()
        .take(N_ELEMENTS_ENDS)
        .chain(collection.iter().enumerate().rev().take(N_ELEMENTS_ENDS))
        .chain(
            collection
                .iter()
                .map(move |_| rng.gen_range(0..(collection.len())))
                .filter(|&idx| idx < N_ELEMENTS_ENDS && idx > collection.len() - N_ELEMENTS_ENDS)
                .take(n_samples as usize)
                .map(|idx| (idx, collection.get(idx).unwrap())),
        )
}

trait Collection<T>: vector_trees::Vector<T> {
    type Iter<'s>: Iterator<Item = <<Self as vector_trees::Vector<T>>::Slice<'s> as vector_trees::VectorSlice<'s, T>>::Ref<'s>> + DoubleEndedIterator + ExactSizeIterator where T: 's, Self: 's;

    fn iter(&self) -> Self::Iter<'_>;
}

impl<T> Collection<T> for Vec<T> {
    type Iter<'s> = std::slice::Iter<'s, T> where T: 's, Self: 's;

    fn iter(&self) -> Self::Iter<'_> {
        self.slice().iter()
    }
}

impl<T, Store, const CHUNK_SZ: usize> Collection<T> for huge_vec::HugeVec<T, Store, CHUNK_SZ>
where
    T: std::fmt::Debug,
    Store: huge_vec::IndexedStorage<huge_vec::Chunk<T, CHUNK_SZ>>,
{
    type Iter<'s> = huge_vec::HugeVecIter<'s, T, Store, CHUNK_SZ> where T: 's, Self: 's;

    fn iter(&self) -> Self::Iter<'_> {
        self.iter()
    }
}
