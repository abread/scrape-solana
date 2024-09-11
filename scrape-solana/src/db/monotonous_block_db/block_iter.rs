use std::{
    collections::{HashMap, HashSet},
    iter::FusedIterator,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
};

use crate::model::Block;

use super::{check_rebuild_block, MonotonousBlockDb};

pub struct BlockIter<'db, const BCS: usize, const TXCS: usize> {
    db: &'db MonotonousBlockDb<BCS, TXCS>,
    pending_blocks: HashSet<u64>,
    cache: HashMap<u64, eyre::Result<Block>>,
    block_tx: SyncSender<(u64, eyre::Result<Block>)>,
    block_rx: Receiver<(u64, eyre::Result<Block>)>,
    idx: u64,
    idx_back: u64,
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
        }
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

        for i in 0..rayon::current_num_threads() {
            let idx = if direction > 0 {
                idx + i as u64
            } else {
                idx - i as u64
            };

            self.prefetch(idx);
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
                rayon::spawn(move || {
                    let block = check_rebuild_block(block_rec, txs);
                    block_tx.send((idx, block)).unwrap();
                });

                self.pending_blocks.insert(idx);
            }
            Err(e) => {
                self.cache.insert(idx, Err(e));
            }
        }
    }
}

impl<'db, const BCS: usize, const TXCS: usize> Iterator for BlockIter<'db, BCS, TXCS> {
    type Item = eyre::Result<Block>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.db.block_records.len().saturating_sub(1) {
            None
        } else {
            while self.idx < self.db.block_records.len().saturating_sub(1)
                && self
                    .db
                    .block_records
                    .get(self.idx)
                    .map(|r| r.is_endcap())
                    .unwrap_or(false)
            {
                self.idx += 1;
            }

            let block = self.get_block(self.idx, 1);
            self.idx += 1;
            Some(block)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.db.block_records.len().saturating_sub(2) as usize;
        (len, Some(len))
    }
}

impl<'db, const BCS: usize, const TXCS: usize> DoubleEndedIterator for BlockIter<'db, BCS, TXCS> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx_back == 0 {
            None
        } else {
            self.idx_back -= 1;

            while self.idx > 0
                && self
                    .db
                    .block_records
                    .get(self.idx)
                    .map(|r| r.is_endcap())
                    .unwrap_or(false)
            {
                self.idx -= 1;
            }
            if self
                .db
                .block_records
                .get(self.idx)
                .map(|r| r.is_endcap())
                .unwrap_or(false)
            {
                None
            } else {
                Some(self.get_block(self.idx_back, -1))
            }
        }
    }
}

impl<'db, const BCS: usize, const TXCS: usize> ExactSizeIterator for BlockIter<'db, BCS, TXCS> {
    fn len(&self) -> usize {
        self.db.block_records.len().saturating_sub(1) as usize
    }
}

impl<'db, const BCS: usize, const TXCS: usize> FusedIterator for BlockIter<'db, BCS, TXCS> {}
