use crate::vector::{Ref, RefMut};
use crate::{Vector, VectorSlice, VectorSliceMut};
use alloc::collections::BTreeMap;
use alloc::vec::Vec;
use core::cmp::{Ord, Ordering};
use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::Deref;
use nonmax::NonMaxU64;

pub const B: usize = 8;
pub const MAX_CHILDREN: usize = B * 2;
pub const MAX_KEYS: usize = MAX_CHILDREN - 1;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug)]
pub struct BVecTreeNode<K, V> {
    keys: [Option<(K, V)>; MAX_KEYS],
    #[cfg_attr(feature = "serde", serde(with = "children_serde"))]
    children: [Option<NonMaxU64>; MAX_CHILDREN],
    cur_keys: usize,
    leaf: bool,
}

mod children_serde {
    use super::*;
    use core::fmt;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(
        children: &[Option<NonMaxU64>; MAX_CHILDREN],
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_seq(children.iter().map(|x| (*x).map(Into::<u64>::into)))
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<[Option<NonMaxU64>; MAX_CHILDREN], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor<'de>(PhantomData<&'de ()>);
        impl<'de> serde::de::Visitor<'de> for Visitor<'de> {
            type Value = [Option<NonMaxU64>; MAX_CHILDREN];

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_fmt(format_args!(
                    "a sequence of {} Option<NonMaxU64>s",
                    MAX_CHILDREN
                ))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut children = [None; MAX_CHILDREN];

                let mut i = 0;
                while let Some(el) = seq.next_element()? {
                    if i > MAX_CHILDREN {
                        return Err(serde::de::Error::custom("too many children"));
                    }

                    children[i] =
                        match el {
                            None => None,
                            Some(n) => Some(NonMaxU64::new(n).ok_or_else(|| {
                                serde::de::Error::custom("invalid child: u64::MAX")
                            })?),
                        };

                    i += 1;
                }

                if i != MAX_CHILDREN {
                    return Err(serde::de::Error::custom("not enough children"));
                }

                Ok(children)
            }
        }

        deserializer.deserialize_seq(Visitor(PhantomData))
    }
}

impl<K, V> Default for BVecTreeNode<K, V> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            children: Default::default(),
            cur_keys: 0,
            leaf: false,
        }
    }
}

impl<K: Ord, V> BVecTreeNode<K, V> {
    pub fn find_key_id(&self, value: &K) -> (usize, bool) {
        //Binary search is simply slower when optimized
        /*match self
            .keys[..self.cur_keys]
            .binary_search_by(|key| value.cmp(key.as_ref().unwrap())) {
            Ok(idx) => (idx, true),
            Err(idx) => (idx, false)
        }*/

        let keys = &self.keys[..self.cur_keys];

        for (i, item) in keys.iter().enumerate().take(self.cur_keys) {
            match value.cmp(&item.as_ref().unwrap().0) {
                Ordering::Greater => {}
                Ordering::Equal => {
                    return (i, true);
                }
                Ordering::Less => {
                    return (i, false);
                }
            }
        }
        (self.cur_keys, false)
    }

    /// Shifts keys and children right so that there is a space for one at `idx`
    pub fn shift_right(&mut self, idx: usize) {
        debug_assert!(self.cur_keys != MAX_KEYS);

        self.keys[idx..(self.cur_keys + 1)].rotate_right(1);
        if !self.leaf {
            self.children[idx..(self.cur_keys + 2)].rotate_right(1);
        }
        self.cur_keys += 1;
    }

    /// Shifts keys and children right so that there is a space for key at `idx`, and child at `idx + 1`
    pub fn shift_right_rchild(&mut self, idx: usize) {
        debug_assert!(self.cur_keys != MAX_KEYS);

        self.keys[idx..(self.cur_keys + 1)].rotate_right(1);
        if !self.leaf {
            self.children[(idx + 1)..(self.cur_keys + 2)].rotate_right(1);
        }
        self.cur_keys += 1;
    }

    /// Shifts keys and children left to fill in a gap at `idx`
    pub fn shift_left(&mut self, idx: usize) {
        debug_assert!(self.keys[idx].is_none());
        debug_assert!(self.children[idx].is_none());

        self.keys[idx..self.cur_keys].rotate_left(1);
        self.children[idx..(self.cur_keys + 1)].rotate_left(1);
        self.cur_keys -= 1;
    }

    /// Shifts keys and children left to fill in a gap at `idx`, used when the right child is none
    pub fn shift_left_rchild(&mut self, idx: usize) {
        debug_assert!(self.keys[idx].is_none());
        debug_assert!(self.children[idx + 1].is_none());

        self.keys[idx..self.cur_keys].rotate_left(1);
        self.children[(idx + 1)..(self.cur_keys + 1)].rotate_left(1);
        self.cur_keys -= 1;
    }

    fn remove_key(&mut self, key_id: usize) -> (Option<(K, V)>, Option<NonMaxU64>) {
        let key = self.keys[key_id].take();
        let child = self.children[key_id].take();

        self.shift_left(key_id);

        (key, child)
    }

    fn remove_key_rchild(&mut self, key_id: usize) -> (Option<(K, V)>, Option<NonMaxU64>) {
        let key = self.keys[key_id].take();
        let child = self.children[key_id + 1].take();

        self.shift_left_rchild(key_id);

        (key, child)
    }

    pub fn insert_leaf_key(&mut self, idx: usize, key: (K, V)) {
        debug_assert!(self.leaf);
        debug_assert!(idx <= self.cur_keys);
        self.shift_right(idx);
        self.keys[idx] = Some(key);
    }

    pub fn insert_node_at(&mut self, value: (K, V), idx: usize) -> Option<(K, V)> {
        let exact = if self.cur_keys > idx {
            value.0 == self.keys[idx].as_ref().unwrap().0
        } else {
            false
        };

        if exact {
            mem::replace(&mut self.keys[idx], Some(value))
        } else {
            self.shift_right(idx);
            self.keys[idx] = Some(value);
            None
        }
    }

    pub fn insert_node(&mut self, value: (K, V)) -> Option<(K, V)> {
        let idx = self.find_key_id(&value.0).0;
        self.insert_node_at(value, idx)
    }

    pub fn insert_node_rchild_at(&mut self, value: (K, V), idx: usize) -> Option<(K, V)> {
        let exact = if self.cur_keys > idx {
            debug_assert!(value.0 <= self.keys[idx].as_ref().unwrap().0);
            value.0 == self.keys[idx].as_ref().unwrap().0
        } else {
            false
        };

        if exact {
            mem::replace(&mut self.keys[idx], Some(value))
        } else {
            self.shift_right_rchild(idx);
            self.keys[idx] = Some(value);
            None
        }
    }

    pub fn insert_node_rchild(&mut self, value: (K, V)) -> Option<(K, V)> {
        let idx = self.find_key_id(&value.0).0;
        self.insert_node_rchild_at(value, idx)
    }

    /// Appends all keys and children of other to the end of `self`, adding `mid` as key in the middle
    pub fn merge<'s>(&mut self, mid: (K, V), mut other: impl RefMut<'s, Self>)
    where
        V: 's,
        K: 's,
    {
        debug_assert!(self.cur_keys + 1 + other.cur_keys <= MAX_KEYS);

        if self.cur_keys > 0 {
            debug_assert!(mid.0 > self.keys[self.cur_keys - 1].as_ref().unwrap().0);
        }

        if other.cur_keys > 0 {
            debug_assert!(mid.0 < other.keys[0].as_ref().unwrap().0);
        }

        self.keys[self.cur_keys] = Some(mid);

        for i in 0..other.cur_keys {
            self.keys[self.cur_keys + 1 + i] = other.keys[i].take();
        }

        for i in 0..=other.cur_keys {
            self.children[self.cur_keys + 1 + i] = other.children[i].take();
        }

        self.cur_keys += 1 + other.cur_keys;
        other.cur_keys = 0;
    }
}

pub struct BVecTreeMap<S, K, V>(BVecTreeMapData<S, K, V>);
pub struct BVecTreeMapData<S, K, V> {
    pub root: Option<NonMaxU64>,
    pub free_head: Option<NonMaxU64>,
    pub tree_buf: S,
    pub len: u64,
    pub _phantom: PhantomData<(K, V)>,
}

impl<S: Default + Vector<BVecTreeNode<K, V>>, K, V> Default for BVecTreeMapData<S, K, V> {
    fn default() -> Self {
        Self {
            root: None,
            free_head: None,
            tree_buf: S::default(),
            len: 0,
            _phantom: PhantomData,
        }
    }
}
impl<S: Default + Vector<BVecTreeNode<K, V>>, K, V> Default for BVecTreeMap<S, K, V> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K: Ord + Debug, V: Debug> BVecTreeMap<Vec<BVecTreeNode<K, V>>, K, V> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S: Vector<BVecTreeNode<K, V>>, K: Ord + Debug, V: Debug> BVecTreeMap<S, K, V> {
    //TODO: (for full feature parity)
    //append
    //entry
    //get
    //get_mut
    //iter
    //iter_mut
    //keys
    //range
    //range_mut
    //split_off
    //values
    //values_mut

    pub fn new_in(buf: S) -> Self {
        Self(BVecTreeMapData {
            tree_buf: buf,
            root: None,
            free_head: None,
            len: 0,
            _phantom: PhantomData,
        })
    }

    pub fn len(&self) -> u64 {
        self.0.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        self.0.root = None;
        self.0.free_head = None;
        self.0.len = 0;
        self.0.tree_buf.clear();
    }

    pub fn height(&self) -> usize {
        if let Some(idx) = self.0.root {
            let mut ret = 1;

            let mut cur_node = idx;

            while !self.get_node(cur_node).leaf {
                cur_node = self.get_node(cur_node).children[0].unwrap();
                ret += 1
            }

            ret
        } else {
            0
        }
    }

    pub fn get(&self, key: &K) -> Option<impl Ref<'_, V>> {
        if let Some(idx) = self.0.root {
            let mut cur_node = idx;

            loop {
                let node = self.get_node(cur_node);

                let (idx, exact) = node.find_key_id(key);

                if exact {
                    return Some(
                        self.get_node(cur_node)
                            .map(move |r| &r.keys[idx].as_ref().unwrap().1),
                    );
                }

                if node.leaf {
                    break;
                }

                cur_node = node.children[idx].unwrap();
            }
        }

        None
    }

    pub fn get_mut(&mut self, key: &K) -> Option<impl RefMut<'_, V>> {
        if let Some(idx) = self.0.root {
            let mut cur_node = idx;

            loop {
                let node = self.get_node_mut(cur_node);

                let (idx, exact) = node.find_key_id(key);

                if exact {
                    core::mem::drop(node);
                    return Some(
                        self.get_node_mut(cur_node)
                            .map_mut(move |r| &mut r.keys[idx].as_mut().unwrap().1),
                    );
                }

                if node.leaf {
                    break;
                }

                cur_node = node.children[idx].unwrap();
            }
        }

        None
    }

    pub fn contains_key(&self, key: &K) -> bool {
        if let Some(idx) = self.0.root {
            let mut cur_node = idx;

            loop {
                let node = self.get_node(cur_node);

                let (idx, exact) = node.find_key_id(key);

                if exact {
                    return true;
                }

                if node.leaf {
                    break;
                }

                cur_node = node.children[idx].unwrap();
            }
        }

        false
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let prev = self.insert_internal((key, value));
        if let Some((_, prev_val)) = prev {
            Some(prev_val)
        } else {
            self.0.len += 1;
            None
        }
    }

    fn insert_internal(&mut self, value: (K, V)) -> Option<(K, V)> {
        if let Some(idx) = self.0.root {
            let root_node_cur_keys = self.get_node_mut(idx).cur_keys;

            if root_node_cur_keys == MAX_KEYS {
                let new_root = self.allocate_node();

                {
                    let mut new_root_node_ref = self.get_node_mut(new_root);
                    new_root_node_ref.children[0] = Some(idx);
                }

                self.0.root = Some(new_root);
                self.split_child(new_root, 0);
            }
        } else {
            self.0.root = Some(self.allocate_node());
            self.get_node_mut(self.0.root.unwrap()).leaf = true;
        }

        let mut cur_node = self.0.root.unwrap();

        loop {
            let mut node = self.get_node_mut(cur_node);

            if node.leaf {
                break;
            }

            let (mut idx, exact) = node.find_key_id(&value.0);

            if exact {
                return node.insert_node_at(value, idx);
            } else {
                let child = node.children[idx].unwrap();
                core::mem::drop(node); // satisfy borrow checker

                if self.get_node(child).cur_keys == MAX_KEYS {
                    self.split_child(cur_node, idx);

                    let cmp = value
                        .0
                        .cmp(&self.get_node(cur_node).keys[idx].as_ref().unwrap().0);

                    match cmp {
                        Ordering::Greater => {
                            idx += 1;
                        }
                        Ordering::Equal => {
                            return self.get_node_mut(cur_node).insert_node_at(value, idx);
                        }
                        Ordering::Less => {}
                    }
                }

                cur_node = self.get_node(cur_node).children[idx].unwrap();
            }
        }

        self.insert_node(cur_node, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let ret = self.remove_entry(key);
        if ret.is_some() {
            self.0.len -= 1;
            Some(ret.unwrap().1)
        } else {
            None
        }
    }

    pub fn remove_entry(&mut self, key: &K) -> Option<(K, V)> {
        let mut cur_node = self.0.root;

        while let Some(node_idx) = cur_node {
            let node = self.get_node(node_idx);

            let (idx, exact) = node.find_key_id(key);

            if exact {
                if node.leaf {
                    core::mem::drop(node); // satisfy borrow checker
                    let ret = self.remove_key(node_idx, idx).0;
                    return ret;
                } else {
                    let left_child = node.children[idx].unwrap();
                    let right_child = node.children[idx + 1].unwrap();
                    core::mem::drop(node); // satisfy borrow checker

                    if self.get_node(left_child).cur_keys > B - 1 {
                        let mut lr_child = left_child;
                        while !self.get_node(lr_child).leaf {
                            let lr_child_cur_keys = self.get_node(lr_child).cur_keys;
                            self.ensure_node_degree(lr_child, lr_child_cur_keys);
                            let lr_node = self.get_node(lr_child);
                            lr_child = lr_node.children[lr_node.cur_keys].unwrap();
                        }

                        let key_to_remove = self.get_node(lr_child).deref().cur_keys - 1;
                        let (pred, _) = self.remove_key(lr_child, key_to_remove);
                        return mem::replace(&mut self.get_node_mut(node_idx).keys[idx], pred);
                    } else if self.get_node(right_child).cur_keys > B - 1 {
                        let mut rl_child = right_child;
                        while !self.get_node(rl_child).leaf {
                            self.ensure_node_degree(rl_child, 0);
                            let rl_node = self.get_node(rl_child);
                            rl_child = rl_node.children[0].unwrap();
                        }
                        let (succ, _) = self.remove_key(rl_child, 0);
                        return mem::replace(&mut self.get_node_mut(node_idx).keys[idx], succ);
                    } else {
                        let ret = self.merge_children(node_idx, idx);
                        if cur_node == self.0.root {
                            self.0.root = Some(ret);
                        }
                        cur_node = Some(left_child);
                        continue;
                    }
                }
            }

            if !node.leaf {
                core::mem::drop(node); // satisfy borrow checker
                let ret = self.ensure_node_degree(node_idx, idx);
                if ret != node_idx {
                    if cur_node == self.0.root {
                        self.0.root = Some(ret);
                    }
                    cur_node = Some(ret);
                } else {
                    let node = self.get_node(node_idx);
                    cur_node = node.children[node.find_key_id(key).0];
                }
            } else {
                cur_node = node.children[idx];
            }
        }

        None
    }

    fn remove_key(
        &mut self,
        node_id: NonMaxU64,
        key_id: usize,
    ) -> (Option<(K, V)>, Option<NonMaxU64>) {
        let mut node = self.get_node_mut(node_id);
        node.remove_key(key_id)
    }

    /// Merge `key_id` child of `parent` with `key_id + 1` child
    ///
    /// Returns new parent
    fn merge_children(&mut self, parent: NonMaxU64, key_id: usize) -> NonMaxU64 {
        let mut parent_node = self.get_node_mut(parent);
        let left_child = parent_node.children[key_id].unwrap();
        let right_child = parent_node.children[key_id + 1].unwrap();

        let (mid, _) = parent_node.remove_key_rchild(key_id);
        core::mem::drop(parent_node); // satisfy borrow checker
        let (mut left_node, right_node) = self.get_two_nodes_mut(left_child, right_child);

        left_node.merge(mid.unwrap(), right_node);

        core::mem::drop(left_node);
        self.free_node(right_child);

        if self.get_node(parent).cur_keys == 0 {
            self.get_node_mut(parent).children[0] = None;
            self.free_node(parent);
            left_child
        } else {
            parent
        }
    }

    fn ensure_node_degree(&mut self, parent: NonMaxU64, child_id: usize) -> NonMaxU64 {
        let parent_node = self.get_node(parent);
        let child_node_id = parent_node.children[child_id].unwrap();
        let child_node = self.get_node(child_node_id);

        if child_node.cur_keys < B {
            core::mem::drop(child_node); // satisfy borrow checker

            if child_id != 0
                && self
                    .get_node(parent_node.children[child_id - 1].unwrap())
                    .cur_keys
                    > B - 1
            {
                core::mem::drop(parent_node); // satisfy borrow checker

                let (mut key, (mut left, mut right)) = self.get_key_nodes_mut(parent, child_id - 1);
                let left_key = Option::take(&mut key).unwrap();
                right.insert_node(left_key);
                let left_cur_keys = left.cur_keys;
                let (nkey, rchild) = left.remove_key_rchild(left_cur_keys - 1);
                right.children[0] = rchild;
                *key = nkey;
            } else if child_id != parent_node.cur_keys
                && self
                    .get_node(parent_node.children[child_id + 1].unwrap())
                    .cur_keys
                    > B - 1
            {
                core::mem::drop(parent_node); // satisfy borrow checker

                let (mut key, (mut left, mut right)) = self.get_key_nodes_mut(parent, child_id);
                let right_key = Option::take(&mut key).unwrap();
                left.insert_node_rchild(right_key);
                let (nkey, lchild) = right.remove_key(0);

                let left_cur_keys = left.cur_keys;
                left.children[left_cur_keys] = lchild;
                *key = nkey;
            } else if child_id > 0 {
                core::mem::drop(parent_node); // satisfy borrow checker

                return self.merge_children(parent, child_id - 1);
            } else {
                core::mem::drop(parent_node); // satisfy borrow checker

                return self.merge_children(parent, child_id);
            }
        }

        parent
    }

    fn split_child(&mut self, parent: NonMaxU64, child_id: usize) {
        let node_to_split = self.get_node(parent).children[child_id].unwrap();
        let new_node = self.allocate_node();

        let (mut left, mut right) = self.get_two_nodes_mut(node_to_split, new_node);

        //Copy the second half of node_to_split over to new_node
        for i in 0..(B - 1) {
            right.keys[i] = left.keys[i + B].take();
        }

        let mid = left.keys[B - 1].take().unwrap();

        left.cur_keys = B - 1;
        right.cur_keys = B - 1;

        if left.leaf {
            right.leaf = true;
        } else {
            for i in 0..B {
                right.children[i] = left.children[i + B].take();
            }
        }

        core::mem::drop(left);
        core::mem::drop(right);
        self.insert_node(parent, mid);

        debug_assert!(self.get_node(parent).children[child_id].is_none());
        debug_assert!(self.get_node(parent).children[child_id + 1].is_some());
        let right_child = self.get_node_mut(parent).children[child_id + 1].take();
        self.get_node_mut(parent).children[child_id] = right_child;
        self.get_node_mut(parent).children[child_id + 1] = Some(new_node);
    }

    fn insert_node(&mut self, node_id: NonMaxU64, value: (K, V)) -> Option<(K, V)> {
        self.get_node_mut(node_id).insert_node(value)
    }

    fn get_node_mut(&mut self, id: NonMaxU64) -> impl RefMut<'_, BVecTreeNode<K, V>> {
        self.0
            .tree_buf
            .slice_mut()
            .map_get_mut(Into::<u64>::into(id) as usize)
            .unwrap()
    }

    /// Returns 2 individual mutable nodes
    fn get_two_nodes_mut(
        &mut self,
        left: NonMaxU64,
        right: NonMaxU64,
    ) -> (
        impl RefMut<'_, BVecTreeNode<K, V>>,
        impl RefMut<'_, BVecTreeNode<K, V>>,
    ) {
        debug_assert!(left != right);

        if left < right {
            let (_, br) = self
                .0
                .tree_buf
                .slice_mut()
                .split_at_mut(Into::<u64>::into(left) as usize);
            let (left_ret, right_side) = br.split_first_mut().unwrap();
            let (_, br) = right_side
                .split_at_mut((Into::<u64>::into(right) - Into::<u64>::into(left) - 1) as usize);
            let (right_ret, _) = br.split_first_mut().unwrap();
            (left_ret, right_ret)
        } else {
            let (_, br) = self
                .0
                .tree_buf
                .slice_mut()
                .split_at_mut(Into::<u64>::into(right) as usize);
            let (right_ret, right_side) = br.split_first_mut().unwrap();
            let (_, br) = right_side
                .split_at_mut((Into::<u64>::into(left) - Into::<u64>::into(right) - 1) as usize);
            let (left_ret, _) = br.split_first_mut().unwrap();
            (left_ret, right_ret)
        }
    }

    fn get_key_nodes_mut(
        &mut self,
        parent: NonMaxU64,
        key: usize,
    ) -> (
        impl RefMut<'_, Option<(K, V)>>,
        (
            impl RefMut<'_, BVecTreeNode<K, V>>,
            impl RefMut<'_, BVecTreeNode<K, V>>,
        ),
    ) {
        let parent: u64 = parent.into();

        let mut slices = BTreeMap::new();
        let (l, r) = self.0.tree_buf.slice_mut().split_at_mut(parent as usize);
        let (parent_node, r) = r.split_first_mut().unwrap();
        if l.len() != 0 {
            slices.insert(0, l);
        }
        if r.len() != 0 {
            slices.insert(parent + 1, r);
        }

        let left: u64 = parent_node.children[key].unwrap().into();
        let right: u64 = parent_node.children[key + 1].unwrap().into();
        debug_assert!(left != parent);
        debug_assert!(right != parent);

        let key_mut = parent_node.map_mut(move |r| &mut r.keys[key]);

        if !slices.contains_key(&left) {
            slices = slices
                .into_iter()
                .flat_map(|(start_id, slice)| {
                    if start_id < left && start_id + slice.len() as u64 > left {
                        let (l, r) = slice.split_at_mut((left - start_id) as usize);
                        [Some((start_id, l)), Some((left, r))]
                    } else {
                        [Some((start_id, slice)), None]
                    }
                })
                .flatten()
                .collect();
        }
        let (left_node, rest) = slices.remove(&left).unwrap().split_first_mut().unwrap();
        if rest.len() != 0 {
            slices.insert(left + 1, rest);
        }

        if !slices.contains_key(&right) {
            slices = slices
                .into_iter()
                .flat_map(|(start_id, slice)| {
                    if start_id < right && start_id + slice.len() as u64 > right {
                        let (l, r) = slice.split_at_mut((right - start_id) as usize);
                        [Some((start_id, l)), Some((right, r))]
                    } else {
                        [Some((start_id, slice)), None]
                    }
                })
                .flatten()
                .collect();
        }
        let (right_node, _rest) = slices.remove(&right).unwrap().split_first_mut().unwrap();

        (key_mut, (left_node, right_node))
    }

    fn get_node(&self, id: NonMaxU64) -> impl Ref<'_, BVecTreeNode<K, V>> {
        self.0
            .tree_buf
            .slice()
            .map_get(Into::<u64>::into(id) as usize)
            .unwrap()
    }

    fn allocate_node(&mut self) -> NonMaxU64 {
        if let Some(idx) = self.0.free_head {
            let mut free_node = self.get_node_mut(idx);
            let child_zero = free_node.children[0];
            *free_node = BVecTreeNode::default();
            core::mem::drop(free_node); // satisfy borrow checker
            self.0.free_head = child_zero;
            idx
        } else {
            let ret = NonMaxU64::new(self.0.tree_buf.len() as u64).expect("alloc fail");
            self.0.tree_buf.push(BVecTreeNode::default());
            ret
        }
    }

    fn free_node(&mut self, node_id: NonMaxU64) {
        let head = self.0.free_head;
        let mut node = self.get_node_mut(node_id);

        //Make sure all the keys and children are taken out before freeing
        debug_assert!(node.keys.iter().filter_map(|x| x.as_ref()).count() == 0);
        debug_assert!(node.children.iter().filter_map(|x| x.as_ref()).count() == 0);

        node.children[0] = head;
        core::mem::drop(node); // satisfy borrow checker
        self.0.free_head = Some(node_id);
    }
}

impl<S, K, V> BVecTreeMap<S, K, V> {
    pub fn inner(&self) -> &BVecTreeMapData<S, K, V> {
        &self.0
    }

    /// # Safety
    /// Caller must maintain map valid (this is used for persistence only)
    /// TODO: write better safety section
    pub unsafe fn inner_mut(&mut self) -> &mut BVecTreeMapData<S, K, V> {
        &mut self.0
    }

    pub fn into_inner(self) -> BVecTreeMapData<S, K, V> {
        self.0
    }

    /// # Safety
    /// Must be called with a valid map obtained from into_inner() or equivalent
    /// TODO: write better safety section
    pub unsafe fn from_inner(data: BVecTreeMapData<S, K, V>) -> Self {
        Self(data)
    }
}

#[cfg(test)]
mod tests {
    use crate::BVecTreeMap;
    use rand::{seq::SliceRandom, Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use std::collections::BTreeSet;

    #[test]
    fn test_random_add() {
        for _ in 0..200 {
            let seed = rand::thread_rng().gen_range(0, !0u64);
            println!("Seed: {:x}", seed);

            let mut rng: XorShiftRng = SeedableRng::seed_from_u64(seed);

            let entries: Vec<_> = (0..1000).map(|_| rng.gen_range(0, 50000usize)).collect();
            let entries_s: Vec<_> = (0..1000).map(|_| rng.gen_range(0, 50000usize)).collect();

            let mut tree = BVecTreeMap::new();
            let mut set = BTreeSet::new();

            for i in entries.iter() {
                set.insert(*i);
                tree.insert(*i, ());
            }

            for i in entries_s.iter() {
                assert_eq!(set.contains(i), tree.contains_key(i));
            }

            assert_eq!(tree.len(), set.len() as u64);
        }
    }

    #[test]
    fn test_random_remove() {
        for _ in 0..500 {
            let seed = rand::thread_rng().gen_range(0, !0u64);
            println!("Seed: {:x}", seed);

            let mut rng: XorShiftRng = SeedableRng::seed_from_u64(seed);

            let entries: Vec<_> = (0..1000).map(|_| rng.gen_range(0, 50000usize)).collect();

            let mut tree = BVecTreeMap::new();
            let mut set = BTreeSet::new();

            for i in entries.iter() {
                set.insert(*i);
                tree.insert(*i, ());
            }

            let mut entries_r: Vec<_> = set.iter().copied().collect();
            entries_r.shuffle(&mut rng);

            for i in entries_r.iter().take(200) {
                let ret_set = set.remove(i);
                let ret_tree = tree.remove(i);

                assert!(
                    ret_tree.is_some() || !ret_set,
                    "{:?} {:?} {:?}",
                    ret_tree,
                    i,
                    tree.contains_key(i)
                );
            }

            assert_eq!(tree.len(), set.len() as u64);
        }
    }
}
