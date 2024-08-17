use std::iter::FusedIterator;

use eyre::{eyre, WrapErr};

use super::HugeVec;
use crate::crc_checksum_serde::checksum;
use crate::model::{Block, Tx};
use crate::select_random_elements;

mod model;
use model::BlockRecord;

const MAX_AUTO_TX_LOSS: u64 = 16_384;

pub struct MonotonousBlockDb<const BCS: usize, const TXCS: usize> {
    pub(super) block_records: HugeVec<BlockRecord, BCS>,
    pub(super) txs: HugeVec<Tx, TXCS>,
}

impl<const BCS: usize, const TXCS: usize> MonotonousBlockDb<BCS, TXCS> {
    pub fn initialize(&mut self) -> eyre::Result<()> {
        if self.block_records.is_empty() {
            eyre::ensure!(self.txs.is_empty());

            // insert endcap
            self.block_records.push(BlockRecord::endcap(0))?;
        }

        Ok(())
    }

    pub fn push(&mut self, block: &Block) -> eyre::Result<()> {
        let block_records_old_len = self.block_records.len();
        let block_records_old_endcap = self.block_records.last()?.expect("missing endcap").clone();

        if self.block_records.len() >= 3 {
            let last = self.block_records.get(self.block_records.len() - 2)?.slot;
            let second_to_last = self.block_records.get(self.block_records.len() - 3)?.slot;

            eyre::ensure!(
                (last as i128 - second_to_last as i128).signum()
                    == (block.slot as i128 - last as i128).signum(),
                "block slot is not monotonous"
            );
        }

        let tx_records_old_len = self.txs.len();

        match self.push_impl(block) {
            Ok(()) => Ok(()),
            Err(e) => {
                // restore endcaps
                {
                    let mut block_rec_endcap_slot = self
                        .block_records
                        .get_mut(block_records_old_len - 1)
                        .expect("push rollback fail: replace block_records endcap");
                    let _ =
                        std::mem::replace(&mut *block_rec_endcap_slot, block_records_old_endcap);
                }

                // discard appended data
                self.block_records
                    .truncate(block_records_old_len)
                    .expect("push rollback fail: prune block_records");
                self.txs
                    .truncate(tx_records_old_len)
                    .expect("push rollback fail: prune tx_records");

                self.sync().expect("push rollback fail: sync");

                Err(e)
            }
        }
    }

    fn push_impl(&mut self, block: &Block) -> eyre::Result<()> {
        let txs_start_idx = self.txs.len();

        for tx in &block.txs {
            self.txs.push(tx.clone())?;
        }

        let block_rec = BlockRecord::new(block, txs_start_idx);

        // push a new endcap, replace the old one with the new record
        self.block_records
            .push(BlockRecord::endcap(self.txs.len()))?;
        let mut old_endcap = self.block_records.get_mut(self.block_records.len() - 2)?;
        let _ = std::mem::replace(&mut *old_endcap, block_rec);

        Ok(())
    }

    pub fn sync(&mut self) -> eyre::Result<()> {
        self.txs.sync()?;
        self.block_records.sync()?;

        Ok(())
    }

    pub fn heal(&mut self, n_samples: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
        if self.block_records.is_empty() {
            issues.push("block records empty, missing endcap: reinserting".to_owned());
            self.block_records.push(BlockRecord::endcap(0))?;
            return Ok(());
        }

        while self.block_records.len() >= 2
            && self
                .block_records
                .get(self.block_records.len() - 2)?
                .is_endcap()
        {
            issues.push("block records have >1 trailing endcap: removing".to_owned());
            self.block_records.truncate(self.block_records.len() - 1)?;
        }

        if self
            .block_records
            .last()?
            .map(|b| !b.is_endcap() || b.txs_start_idx > self.txs.len())
            .unwrap()
        {
            let (n_bad_blocks, n_bad_txs) = {
                let new_endcap_idx = self
                    .block_records
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, block_rec)| block_rec.txs_start_idx <= self.txs.len());

                if let Some((idx, block_rec)) = new_endcap_idx {
                    let n_bad_txs = self.txs.len() - block_rec.txs_start_idx;
                    let n_bad_blocks = self.block_records.len() - (idx as u64 + 1);
                    (n_bad_blocks, n_bad_txs)
                } else {
                    let n_bad_txs = self.txs.len();
                    let n_bad_blocks = self.block_records.len();
                    (n_bad_blocks, n_bad_txs)
                }
            };

            if n_bad_blocks >= MAX_AUTO_TX_LOSS || n_bad_txs >= MAX_AUTO_TX_LOSS {
                issues.push(format!("block records bad endcap: would drop {} blocks and {} txs to autofix (out of {} blocks and {} txs). aborting", n_bad_blocks, n_bad_txs, self.block_records.len(), self.txs.len()));
                return Err(eyre!(
                    "block records missing endcap. too many dropped txs/blocks to autofix"
                ));
            } else {
                issues.push(format!("block records bad endcap: dropped {} blocks and {} txs to autofix (out of {} blocks and {} txs)", n_bad_blocks, n_bad_txs, self.block_records.len(), self.txs.len()));
                self.block_records
                    .truncate(self.block_records.len() - n_bad_blocks)?;
                self.txs.truncate(self.txs.len() - n_bad_txs)?;
            }
        }

        assert!(!self.block_records.is_empty());
        let endcap_idx = self.block_records.len() - 1;
        let elements_to_check = select_random_elements(&self.block_records, n_samples)
            .map(|(idx, _)| idx)
            .filter(|&idx| idx as u64 != endcap_idx)
            .collect::<Vec<_>>();

        for idx in elements_to_check {
            self.check_block(idx as u64, issues)?;
        }

        if self.block_records.len() >= 2
            && self
                .check_block(self.block_records.len() - 1, &mut Vec::new())
                .is_err()
        {
            issues.push("last block is corrupted: removing".to_owned());
            let txs_next_start_idx = self.block_records.last().unwrap().unwrap().txs_start_idx;
            let _ = std::mem::replace(
                &mut *self.block_records.last_mut().unwrap().unwrap(),
                BlockRecord::endcap(txs_next_start_idx),
            );
            self.block_records.truncate(self.block_records.len() - 1)?;
        }

        Ok(())
    }

    fn check_block(&self, idx: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
        match self.get_block(idx) {
            Ok(_) => (),
            Err(e) => {
                issues.push(format!("block {} is corrupted: {}", idx, e));
            }
        }

        Ok(())
    }

    fn get_block(&self, idx: u64) -> eyre::Result<Block> {
        let record = self
            .block_records
            .get(idx)
            .wrap_err("could not read block record")?;

        if record.is_endcap() {
            return Err(eyre!("cannot get endcap"));
        }

        let block_txs_count = self
            .block_records
            .get(idx + 1)
            .wrap_err("failed to get account data size (from next record)")?
            .txs_start_idx
            - record.txs_start_idx;

        let txs = {
            let mut txs = Vec::with_capacity(block_txs_count as usize);

            for tx_idx in record.txs_start_idx..(record.txs_start_idx + block_txs_count) {
                let tx = self.txs.get(tx_idx)?.clone();
                txs.push(tx);
            }

            txs
        };

        let block = Block {
            slot: record.slot,
            height: record.height,
            ts: record.ts,
            txs,
        };
        eyre::ensure!(checksum(&block) == record.checksum, "checksum mismatch");

        Ok(block)
    }

    pub fn blocks(&self) -> BlockIter<'_, BCS, TXCS> {
        BlockIter::new(self)
    }
}

pub struct BlockIter<'db, const BCS: usize, const TXCS: usize> {
    db: &'db MonotonousBlockDb<BCS, TXCS>,
    idx: u64,
    idx_back: u64,
}
impl<'db, const BCS: usize, const TXCS: usize> BlockIter<'db, BCS, TXCS> {
    fn new(db: &'db MonotonousBlockDb<BCS, TXCS>) -> Self {
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
            Some(self.db.get_block(self.idx_back))
        }
    }
}

impl<'db, const BCS: usize, const TXCS: usize> ExactSizeIterator for BlockIter<'db, BCS, TXCS> {
    fn len(&self) -> usize {
        self.db.block_records.len().saturating_sub(1) as usize
    }
}

impl<'db, const BCS: usize, const TXCS: usize> FusedIterator for BlockIter<'db, BCS, TXCS> {}
