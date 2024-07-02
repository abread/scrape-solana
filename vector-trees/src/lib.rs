#![cfg_attr(not(test), no_std)]
extern crate alloc;

#[cfg(feature = "avltree")]
pub mod avltree;
pub mod btree;
pub mod vector;

#[cfg(feature = "avltree")]
pub use avltree::VecAVLTree;
pub use btree::BVecTreeMap;
pub use vector::Vector;
