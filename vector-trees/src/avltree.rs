use core::cmp::{Ord, Ordering};
use core::fmt::Debug;
use core::mem::replace;

#[derive(Debug)]
struct AVLNode<V> {
    entry: Option<V>,
    left: Option<u32>,
    right: Option<u32>,
    height: u8,
}

impl<V> Default for AVLNode<V> {
    fn default() -> Self {
        Self {
            entry: None,
            left: None,
            right: None,
            height: 1,
        }
    }
}

pub struct VecAVLTree<V> {
    root: Option<u32>,
    free_head: Option<u32>,
    tree_buf: Vec<AVLNode<V>>,
}

impl<V> Default for VecAVLTree<V> {
    fn default() -> Self {
        Self {
            root: None,
            free_head: None,
            tree_buf: vec![],
        }
    }
}

impl<V: Ord + Debug> VecAVLTree<V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.tree_buf.clear();
        self.root = None;
        self.free_head = None
    }

    pub fn iter(&self) -> impl Iterator<Item = &V> {
        self.tree_buf.iter().filter_map(|x| x.entry.as_ref())
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.tree_buf.iter_mut().filter_map(|x| x.entry.as_mut())
    }

    pub fn len(&self) -> usize {
        self.iter().count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, value: V) -> Option<V> {
        match self.root {
            None => {
                let root = self.alloc_entry();
                self.tree_buf[root as usize].entry = Some(value);
                self.root = Some(root);
                None
            }
            Some(idx) => {
                let (entry, new_root) = self.insert_internal(value, idx);
                self.root = Some(new_root);
                entry
            }
        }
    }

    pub fn contains(&self, value: &V) -> bool {
        match self.root {
            None => false,
            Some(idx) => self.contains_internal(value, idx),
        }
    }

    pub fn remove(&mut self, value: &V) -> Option<V> {
        if let Some(idx) = self.root {
            let (entry, new_root) = self.remove_internal(value, idx);
            self.root = new_root;
            entry
        } else {
            None
        }
    }

    pub fn height(&self) -> usize {
        if self.root.is_some() {
            self.get_node(self.root.unwrap()).height as usize
        } else {
            0
        }
    }

    fn get_node_mut(&mut self, idx: u32) -> &mut AVLNode<V> {
        self.tree_buf.get_mut(idx as usize).unwrap()
    }

    fn get_node(&self, idx: u32) -> &AVLNode<V> {
        self.tree_buf.get(idx as usize).unwrap()
    }

    fn node_height(&self, idx: u32) -> u8 {
        self.get_node(idx).height
    }

    fn subnode_height(&self, idx: Option<u32>) -> u8 {
        if let Some(idx) = idx {
            self.node_height(idx)
        } else {
            0
        }
    }

    fn update_node_height(&mut self, idx: u32) {
        let node = self.get_node(idx);

        let new_height = 1 + core::cmp::max(
            self.subnode_height(node.left),
            self.subnode_height(node.right),
        );

        self.get_node_mut(idx).height = new_height;
    }

    fn get_node_rank(&self, idx: u32) -> i32 {
        let node = self.get_node(idx);

        self.subnode_height(node.right) as i32 - self.subnode_height(node.left) as i32
    }

    fn rotate_node_right(&mut self, cur_node: u32) -> u32 {
        let node = self.get_node(cur_node);
        let left = node.left.unwrap();
        self.get_node_mut(cur_node).left = self.get_node(left).right;
        self.get_node_mut(left).right = Some(cur_node);
        self.update_node_height(cur_node);
        self.update_node_height(left);
        left
    }

    fn rotate_node_left(&mut self, cur_node: u32) -> u32 {
        let node = self.get_node(cur_node);
        let right = node.right.unwrap();
        self.get_node_mut(cur_node).right = self.get_node(right).left;
        self.get_node_mut(right).left = Some(cur_node);
        self.update_node_height(cur_node);
        self.update_node_height(right);
        right
    }

    fn balance(&mut self, cur_node: u32) -> u32 {
        self.update_node_height(cur_node);
        let node = self.get_node(cur_node);

        match self.get_node_rank(cur_node) {
            -1..=1 => cur_node,
            -2 => {
                let left = node.left.unwrap();
                let left_node = self.get_node(left);
                if self.subnode_height(left_node.left) < self.subnode_height(left_node.right) {
                    self.get_node_mut(cur_node).left = Some(self.rotate_node_left(left));
                }
                self.rotate_node_right(cur_node)
            }
            2 => {
                let right = node.right.unwrap();
                let right_node = self.get_node(right);
                if self.subnode_height(right_node.right) < self.subnode_height(right_node.left) {
                    self.get_node_mut(cur_node).right = Some(self.rotate_node_right(right));
                }
                self.rotate_node_left(cur_node)
            }
            x => panic!(
                "Bug in VecAVLTree {} ({} {})",
                x,
                self.subnode_height(node.left),
                self.subnode_height(node.right)
            ),
        }
    }

    fn insert_internal(&mut self, value: V, cur_node: u32) -> (Option<V>, u32) {
        let node = self.get_node_mut(cur_node);
        let entry = node.entry.as_mut().unwrap();

        match value.cmp(entry) {
            Ordering::Less => match node.left {
                Some(idx) => {
                    let (ret_entry, new_left) = self.insert_internal(value, idx);
                    self.get_node_mut(cur_node).left = Some(new_left);
                    let ret_node = self.balance(cur_node);
                    (ret_entry, ret_node)
                }
                None => {
                    let new_left = self.alloc_entry();
                    self.get_node_mut(cur_node).left = Some(new_left);
                    self.get_node_mut(new_left).entry = Some(value);
                    self.update_node_height(cur_node);
                    (None, cur_node)
                }
            },
            Ordering::Greater => match node.right {
                Some(idx) => {
                    let (ret_entry, new_right) = self.insert_internal(value, idx);
                    self.get_node_mut(cur_node).right = Some(new_right);
                    let ret_node = self.balance(cur_node);
                    (ret_entry, ret_node)
                }
                None => {
                    let new_right = self.alloc_entry();
                    self.get_node_mut(cur_node).right = Some(new_right);
                    self.get_node_mut(new_right).entry = Some(value);
                    self.update_node_height(cur_node);
                    (None, cur_node)
                }
            },
            Ordering::Equal => (replace(&mut node.entry, Some(value)), cur_node),
        }
    }

    pub fn print_tree(&self) {
        if let Some(root) = self.root {
            self.print_heights(root, 0, 0);
            println!();
        }
    }

    fn print_heights(&self, cur_node: u32, cur_depth: u32, min_height: u32) {
        let node = self.get_node(cur_node);

        let v = cur_depth;
        let rank = self.get_node_rank(cur_node);
        if rank.abs() > 1 {
            print!(" {:02} [{:02}]", v, rank);
        } else {
            print!(" {:02} ({:02})", v, rank);
        }

        if let Some(right) = node.right {
            self.print_heights(right, cur_depth + 1, min_height);
        }

        if let Some(left) = node.left {
            println!();
            for _ in 0..cur_depth {
                print!(" |      ");
            }
            print!(" |======");
            self.print_heights(left, cur_depth + 1, cur_depth);
        }
    }

    /// Swaps the smallest subtree element with the target node, freeing the target node,
    /// and returning its data
    fn swap_min(&mut self, swap_node: u32, parent: u32, cur_node: u32) -> V {
        if let Some(left_node) = self.get_node(cur_node).left {
            let val = self.swap_min(swap_node, cur_node, left_node);

            if let Some(left) = self.get_node(cur_node).left {
                let new_left = self.balance(left);
                let new_left = self.balance(new_left);
                self.get_node_mut(cur_node).left = Some(new_left);
            }
            val
        } else {
            debug_assert!(
                self.get_node(cur_node).entry.as_ref().unwrap()
                    >= self.get_node(swap_node).entry.as_ref().unwrap()
            );

            //Swap the elements
            let swappee_data = self.get_node_mut(swap_node).entry.take();
            let cur_data = self.get_node_mut(cur_node).entry.take();
            self.get_node_mut(swap_node).entry = cur_data;
            self.get_node_mut(cur_node).entry = swappee_data;

            let cur_right = self.get_node_mut(cur_node).right.take();

            if parent == swap_node {
                self.get_node_mut(parent).right = cur_right;
            } else {
                self.get_node_mut(parent).left = cur_right;
            }

            self.update_node_height(parent);

            self.free_entry(cur_node)
        }
    }

    fn remove_internal(&mut self, value: &V, cur_node: u32) -> (Option<V>, Option<u32>) {
        let node = self.get_node(cur_node);
        let entry = node.entry.as_ref().unwrap();

        match value.cmp(entry) {
            Ordering::Less => match node.left {
                Some(idx) => {
                    let (ret_entry, new_left) = self.remove_internal(value, idx);
                    self.get_node_mut(cur_node).left = new_left;
                    let ret_node = self.balance(cur_node);
                    (ret_entry, Some(ret_node))
                }
                None => (None, Some(cur_node)),
            },
            Ordering::Greater => match node.right {
                Some(idx) => {
                    let (ret_entry, new_right) = self.remove_internal(value, idx);
                    self.get_node_mut(cur_node).right = new_right;
                    let ret_node = self.balance(cur_node);
                    (ret_entry, Some(ret_node))
                }
                None => (None, Some(cur_node)),
            },
            Ordering::Equal => {
                let node = self.get_node_mut(cur_node);

                if node.left.is_some() && node.right.is_some() {
                    let ret =
                        self.swap_min(cur_node, cur_node, self.get_node(cur_node).right.unwrap());
                    if let Some(right) = self.get_node(cur_node).right {
                        self.get_node_mut(cur_node).right = Some(self.balance(right));
                    }
                    (Some(ret), Some(self.balance(cur_node)))
                } else if node.left.is_some() {
                    let left = node.left.take();
                    let ret = self.free_entry(cur_node);
                    (Some(ret), left)
                } else if node.right.is_some() {
                    let right = node.right.take();
                    let ret = self.free_entry(cur_node);
                    (Some(ret), right)
                } else {
                    let ret = self.free_entry(cur_node);
                    (Some(ret), None)
                }
            }
        }
    }

    fn contains_internal(&self, value: &V, mut cur_node: u32) -> bool {
        loop {
            let node = self.get_node(cur_node);
            let entry = node.entry.as_ref().unwrap();

            match value.cmp(entry) {
                Ordering::Less => match node.left {
                    Some(idx) => {
                        cur_node = idx;
                        continue;
                    }
                    None => {
                        return false;
                    }
                },
                Ordering::Greater => match node.right {
                    Some(idx) => {
                        cur_node = idx;
                        continue;
                    }
                    None => {
                        return false;
                    }
                },
                Ordering::Equal => {
                    return true;
                }
            }
        }
    }

    fn alloc_entry(&mut self) -> u32 {
        match self.free_head.take() {
            Some(idx) => {
                self.free_head = self.tree_buf[idx as usize].left;
                self.tree_buf[idx as usize] = AVLNode::default();
                idx
            }
            None => {
                let idx = self.tree_buf.len() as u32;
                self.tree_buf.push(AVLNode::default());
                idx
            }
        }
    }

    fn free_entry(&mut self, entry: u32) -> V {
        let free_head = self.free_head.take();
        let node = self.get_node_mut(entry);
        debug_assert!(node.left.is_none());
        debug_assert!(node.right.is_none());
        let ret = replace(&mut node.entry, None);
        node.left = free_head;
        self.free_head = Some(entry);
        ret.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::VecAVLTree;
    use rand::{seq::SliceRandom, Rng, SeedableRng};
    use rand_xorshift::XorShiftRng;
    use std::collections::BTreeSet;

    #[test]
    fn test_random_add() {
        let mut tree = VecAVLTree::new();
        let mut set = BTreeSet::new();
        for i in (0..1000).map(|_| rand::thread_rng().gen_range(0, 5000000usize)) {
            tree.insert(i);
            set.insert(i);
        }

        assert_eq!(tree.iter().count(), set.iter().count());
        assert_eq!(tree.len(), set.len());
    }

    #[test]
    fn test_rand_remove() {
        let mut tree = VecAVLTree::new();

        for _ in 0..10 {
            tree.clear();
            let seed = rand::thread_rng().gen_range(0, !0u64);

            println!("Seed: {:x}", seed);

            let mut rng: XorShiftRng = SeedableRng::seed_from_u64(seed);

            let remove_cnt = 10;

            let entries: Vec<_> = (0..1000)
                .map(|_| rng.gen_range(0, 500000000usize))
                .collect();

            let mut set = BTreeSet::new();

            for i in entries.iter() {
                set.insert(*i);
            }

            for i in entries.iter() {
                tree.insert(*i);
            }

            let mut remove_elems: Vec<_> = tree.iter().copied().take(remove_cnt).collect();
            remove_elems.shuffle(&mut rng);

            for i in remove_elems.into_iter() {
                assert!(tree.remove(&i).is_some());
                set.remove(&i);
            }

            assert_eq!(tree.iter().count(), set.iter().count());
            assert_eq!(tree.len(), set.len());
        }
    }
}
