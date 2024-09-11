use std::iter::FusedIterator;

use crate::model::Block;

use super::MonotonousBlockDb;

pub struct BlockIter<'db, const BCS: usize, const TXCS: usize> {
    db: &'db MonotonousBlockDb<BCS, TXCS>,
    idx: u64,
    idx_back: u64,
}
impl<'db, const BCS: usize, const TXCS: usize> BlockIter<'db, BCS, TXCS> {
    pub(super) fn new(db: &'db MonotonousBlockDb<BCS, TXCS>) -> Self {
        Self {
            db,
            idx: 0,
            idx_back: db.block_records.len().saturating_sub(1),
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

            let block = self.db.get_block(self.idx);
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
                Some(self.db.get_block(self.idx_back))
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
