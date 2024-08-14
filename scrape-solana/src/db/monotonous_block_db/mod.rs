use eyre::{eyre, WrapErr};

use super::{chunk_sz, HugeVec, MB};
use crate::crc_checksum_serde::checksum;
use crate::model::{Block, Tx};
use crate::select_random_elements;

mod model;
use model::BlockRecord;

const MAX_AUTO_TX_LOSS: u64 = 16_384;

pub struct MonotonousBlockDb {
    pub(super) block_records: HugeVec<BlockRecord, { chunk_sz::<BlockRecord>(32 * MB) }>,
    pub(super) txs: HugeVec<Tx, 2048>, // up to ~2.5MB per chunk ≃ up to 2.5GB mem usage
}

impl MonotonousBlockDb {
    pub fn initialize(&mut self) -> eyre::Result<()> {
        eyre::ensure!(self.block_records.is_empty());
        eyre::ensure!(self.txs.is_empty());

        // insert endcap
        self.block_records.push(BlockRecord::endcap(0))?;

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
        let txs_start_idx = self.txs.len().saturating_sub(1);

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

        let endcap_idx = self.block_records.len() - 1;
        let elements_to_check = select_random_elements(&self.block_records, n_samples)
            .map(|(idx, _)| idx)
            .filter(|&idx| idx as u64 != endcap_idx)
            .collect::<Vec<_>>();

        for idx in elements_to_check {
            self.check_block(idx as u64, issues)?;
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
        let block_record = self
            .block_records
            .get(idx)
            .wrap_err("could not read block record")?;

        if block_record.is_endcap() {
            return Err(eyre!("cannot get endcap"));
        }

        let tx_idx_range = {
            let next_block = self
                .block_records
                .get(idx + 1)
                .wrap_err("could not read next block record")?;
            let start = block_record.txs_start_idx;
            let end = next_block.txs_start_idx;
            start..end
        };

        let txs: Vec<Tx> = tx_idx_range
            .map(|idx| self.txs.get(idx).map(|tx_ref| tx_ref.to_owned()))
            .collect::<eyre::Result<_, _>>()?;

        let block = Block {
            slot: block_record.slot,
            height: block_record.height,
            ts: block_record.ts,
            txs,
        };

        if checksum(&block) != block_record.checksum {
            return Err(eyre!("checksum mismatch"));
        }

        Ok(block)
    }
}