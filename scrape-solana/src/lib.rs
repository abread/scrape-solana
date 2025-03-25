pub mod actors;
mod crc_checksum_serde;
pub mod db;
pub mod huge_vec;
pub mod model;
pub mod solana_api;

use std::fmt::Debug;

use huge_vec::{Chunk, HugeVec, IndexedStorage, ItemRef};
use rand::Rng;

pub(crate) fn select_random_elements<T, Store, const CHUNK_SZ: usize>(
    vec: &HugeVec<T, Store, CHUNK_SZ>,
    mut n_samples: u64,
) -> impl Iterator<Item = (u64, ItemRef<'_, T, T, CHUNK_SZ>)>
where
    T: Debug + Send + 'static,
    Store: IndexedStorage<Chunk<T, CHUNK_SZ>> + Send + Sync + 'static,
{
    let mut rng = rand::rng();
    const N_ELEMENTS_ENDS: usize = 5;
    if n_samples > 2 * N_ELEMENTS_ENDS as u64 {
        n_samples -= 2 * N_ELEMENTS_ENDS as u64;
    }

    vec.iter()
        .enumerate()
        .take(N_ELEMENTS_ENDS)
        .filter_map(|(idx, maybe_el)| maybe_el.ok().map(|el| (idx as u64, el)))
        .chain(
            vec.iter()
                .enumerate()
                .rev()
                .take(N_ELEMENTS_ENDS)
                .filter_map(|(idx, maybe_el)| maybe_el.ok().map(|el| (idx as u64, el))),
        )
        .chain(
            vec.iter()
                .map(move |_| rng.random_range(0..(vec.len())))
                .filter(|&idx| {
                    idx < N_ELEMENTS_ENDS as u64
                        && idx > vec.len().saturating_sub(N_ELEMENTS_ENDS as u64)
                })
                .take(n_samples as usize)
                .filter_map(|idx| vec.get(idx).ok().map(|el| (idx, el))),
        )
}
