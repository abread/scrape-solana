use std::{
    collections::{HashMap, HashSet},
    iter::FusedIterator,
    sync::mpsc::{Receiver, SyncSender, sync_channel},
};

use crate::{huge_vec::PREFETCH_THREADPOOL, model::Block};

use super::{MonotonousBlockDb, check_rebuild_block};

pub struct BlockIter<'db, const BCS: usize, const TXCS: usize> {
    db: &'db MonotonousBlockDb<BCS, TXCS>,
    pending_blocks: HashSet<u64>,
    cache: HashMap<u64, eyre::Result<Block>>,
    block_tx: SyncSender<(u64, eyre::Result<Block>)>,
    block_rx: Receiver<(u64, eyre::Result<Block>)>,
    idx: u64,
    idx_back: u64,
    min_idx: u64,
    max_idx: u64,
}

impl<'db, const BCS: usize, const TXCS: usize> BlockIter<'db, BCS, TXCS> {
    pub(super) fn new(db: &'db MonotonousBlockDb<BCS, TXCS>) -> Self {
        let (block_tx, block_rx) = sync_channel(2 * rayon::current_num_threads());
        Self {
            db,
            pending_blocks: HashSet::new(),
            cache: HashMap::new(),
            block_tx,
            block_rx,
            idx: 0,
            idx_back: db.block_records.len().saturating_sub(1),
            min_idx: 0,
            max_idx: db.block_records.len().saturating_sub(1),
        }
    }

    pub(super) fn new_range(db: &'db MonotonousBlockDb<BCS, TXCS>, start: u64, end: u64) -> Self {
        let (block_tx, block_rx) = sync_channel(2 * rayon::current_num_threads());

        let end = end.min(db.block_records.len().saturating_sub(1));
        let start = start.min(end);

        Self {
            db,
            pending_blocks: HashSet::new(),
            cache: HashMap::new(),
            block_tx,
            block_rx,
            idx: start,
            idx_back: end,
            min_idx: start,
            max_idx: end,
        }
    }

    pub fn skip(mut self, n: u64) -> Self {
        let n = n.min(self.max_idx - self.idx);
        self.idx += n;
        self.min_idx += n;
        self
    }

    pub fn take(mut self, n: u64) -> Self {
        let n = n.min(self.max_idx - self.idx);
        self.max_idx = (self.min_idx + n).min(self.max_idx);
        self
    }

    fn get_block(&mut self, idx: u64, direction: i8) -> eyre::Result<Block> {
        self.fill_cache();
        let block = if let Some(block) = self.cache.remove(&idx) {
            block
        } else if self.pending_blocks.contains(&idx) {
            loop {
                match self.block_rx.recv() {
                    Ok((recvd_idx, recvd_block)) => {
                        self.pending_blocks.remove(&recvd_idx);
                        if recvd_idx == idx {
                            break recvd_block;
                        } else {
                            self.cache.insert(recvd_idx, recvd_block);
                        }
                    }
                    Err(_) => return Err(eyre::eyre!("block fetcher stopped")),
                }
            }
        } else {
            // compute the checksum NOW
            self.db
                .get_block_unchecked(idx)
                .and_then(|(block_rec, txs)| check_rebuild_block(block_rec, txs))
        };

        for i in 1..=PREFETCH_THREADPOOL.current_num_threads() / 2 {
            let idx = if direction > 0 {
                idx + i as u64
            } else {
                if idx < i as u64 {
                    continue;
                }
                idx - i as u64
            };

            if idx < self.max_idx && idx > self.min_idx {
                self.prefetch(idx);
            }
        }

        block
    }

    fn fill_cache(&mut self) {
        while let Ok((idx, block)) = self.block_rx.try_recv() {
            self.pending_blocks.remove(&idx);
            self.cache.insert(idx, block);
        }
    }

    fn prefetch(&mut self, idx: u64) {
        if self.pending_blocks.contains(&idx) || self.cache.contains_key(&idx) {
            return;
        }

        match self.db.get_block_unchecked(idx) {
            Ok((block_rec, txs)) => {
                let block_tx = self.block_tx.clone();
                PREFETCH_THREADPOOL.spawn(move || {
                    let block = check_rebuild_block(block_rec, txs);
                    let _ = block_tx.send((idx, block));
                });

                self.pending_blocks.insert(idx);
            }
            Err(e) => {
                self.cache.insert(idx, Err(e));
            }
        }
    }
}

impl<const BCS: usize, const TXCS: usize> Iterator for BlockIter<'_, BCS, TXCS> {
    type Item = eyre::Result<Block>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.max_idx {
            None
        } else {
            debug_assert!(
                !self
                    .db
                    .block_records
                    .get(self.idx)
                    .map(|r| r.is_endcap())
                    .expect("bad block record"),
                "corrupted block records"
            );
            while self
                .db
                .block_records
                .get(self.idx)
                .map(|r| r.is_endcap())
                .unwrap_or(true)
                && self.idx < self.max_idx
            {
                self.idx += 1;
            }
            if self.idx >= self.max_idx {
                return None;
            }

            let block = self.get_block(self.idx, 1);
            self.idx += 1;
            Some(block)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = (self.max_idx - self.min_idx) as usize;
        (len, Some(len))
    }

    fn last(mut self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        self.next_back()
    }
}

impl<const BCS: usize, const TXCS: usize> DoubleEndedIterator for BlockIter<'_, BCS, TXCS> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx_back <= self.min_idx {
            None
        } else {
            self.idx_back -= 1;

            debug_assert!(
                !self
                    .db
                    .block_records
                    .get(self.idx_back)
                    .map(|r| r.is_endcap())
                    .expect("bad block record"),
                "corrupted block records"
            );
            while self
                .db
                .block_records
                .get(self.idx_back)
                .map(|r| r.is_endcap())
                .unwrap_or(true)
                && self.idx_back > self.min_idx
            {
                self.idx_back = self.idx_back.saturating_sub(1);
            }
            if self.idx_back <= self.min_idx {
                return None;
            }

            Some(self.get_block(self.idx_back, -1))
        }
    }
}

impl<const BCS: usize, const TXCS: usize> ExactSizeIterator for BlockIter<'_, BCS, TXCS> {
    fn len(&self) -> usize {
        self.db.block_records.len().saturating_sub(1) as usize
    }
}

impl<const BCS: usize, const TXCS: usize> FusedIterator for BlockIter<'_, BCS, TXCS> {}
