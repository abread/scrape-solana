#![cfg_attr(all(not(test), not(feature = "std")), no_std)]
extern crate alloc;

#[cfg(feature = "avltree")]
pub mod avltree;
pub mod btree;
pub mod vector;

#[cfg(feature = "avltree")]
pub use avltree::VecAVLTree;
pub use btree::BVecTreeMap;
pub use vector::{Ref, RefMut, Vector, VectorSlice, VectorSliceMut};
