use std::collections::HashMap;
use std::sync::mpsc::sync_channel;

use eyre::{WrapErr, eyre};
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use super::HugeVec;
use crate::crc_checksum_serde::checksum;
use crate::model::{Block, Tx};
use crate::select_random_elements;

mod model;
use model::BlockRecord;

mod block_iter;
pub use block_iter::BlockIter;

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

    const fn max_auto_tx_loss() -> u64 {
        let two_block_chunks_of_txs = BCS as u64 * 2 * 4000;
        if two_block_chunks_of_txs > TXCS as u64 {
            two_block_chunks_of_txs + TXCS as u64
        } else {
            TXCS as u64
        }
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

    pub fn assume_max_size_for_heal(&mut self) -> eyre::Result<()> {
        self.block_records
            .assume_max_size_for_heal()
            .wrap_err("failed to assume max size for heal")?;
        self.txs
            .assume_max_size_for_heal()
            .wrap_err("failed to assume max size for heal")?;
        Ok(())
    }

    pub fn quick_heal(&mut self, n_samples: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
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
            .map(|b| !b.is_endcap() || b.txs_start_idx != self.txs.len())
            .unwrap()
        {
            let (n_bad_blocks, n_bad_txs) = {
                let new_endcap_idx = self
                    .block_records
                    .iter()
                    .enumerate()
                    .rev()
                    .filter_map(|(idx, maybe_br)| maybe_br.ok().map(|br| (idx, br)))
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

            if n_bad_blocks >= Self::max_auto_tx_loss() || n_bad_txs >= Self::max_auto_tx_loss() {
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
        if n_samples == u64::MAX {
            // check ALL blocks
            for idx in 0..endcap_idx {
                self.heal_check_block(idx, issues)?;
            }
        } else {
            let elements_to_check = select_random_elements(&self.block_records, n_samples)
                .map(|(idx, _)| idx)
                .filter(|&idx| idx != endcap_idx)
                .collect::<Vec<_>>();

            for idx in elements_to_check {
                self.heal_check_block(idx, issues)?;
            }
        }

        if self.block_records.len() >= 2
            && self
                .heal_check_block(self.block_records.len() - 1, &mut Vec::new())
                .is_err()
        {
            issues.push("last block is corrupted: removing".to_owned());
            let txs_next_start_idx = self.block_records.last().unwrap().unwrap().txs_start_idx;
            let _ = std::mem::replace(
                &mut *self.block_records.last_mut().unwrap().unwrap(),
                BlockRecord::endcap(txs_next_start_idx),
            );
            self.block_records.truncate(self.block_records.len() - 1)?;
            self.txs.truncate(txs_next_start_idx)?;
        }

        Ok(())
    }

    fn heal_check_block(&self, idx: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
        match self
            .get_block_unchecked(idx)
            .and_then(|(block_rec, txs)| check_rebuild_block(block_rec, txs))
        {
            Ok(_) => (),
            Err(e) => {
                issues.push(format!("block {} is corrupted: {}", idx, e));
            }
        }

        Ok(())
    }

    fn get_block_unchecked(&self, idx: u64) -> eyre::Result<(BlockRecord, Vec<Tx>)> {
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
            .wrap_err("failed to get block tx count (from next record)")?
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

        Ok((record.to_owned(), txs))
    }

    pub fn discard_after_corrupted(&mut self) -> eyre::Result<u64> {
        for idx in 0..self.block_records.len().saturating_sub(1) {
            if self
                .get_block_unchecked(idx)
                .and_then(|(block_rec, txs)| check_rebuild_block(block_rec, txs))
                .is_err()
            {
                let prev_len = self.block_records.len().saturating_sub(1);
                self.truncate(idx)?;
                return Ok(prev_len.saturating_sub(idx.saturating_sub(1)));
            }
        }

        Ok(0)
    }

    fn truncate(&mut self, new_len: u64) -> eyre::Result<()> {
        if new_len + 1 >= self.block_records.len() {
            return Ok(());
        }

        self.block_records.truncate(new_len + 1)?;

        // replace endcap
        let mut last_block = self.block_records.get_mut(new_len)?;
        let new_endcap = BlockRecord::endcap(last_block.txs_start_idx);
        *last_block = new_endcap;
        let new_endcap = last_block;

        // truncate txs
        self.txs.truncate(new_endcap.txs_start_idx)?;

        Ok(())
    }

    pub fn blocks(&self) -> BlockIter<'_, BCS, TXCS> {
        BlockIter::new(self)
    }

    pub fn block_range(&self, start_ts: i64, end_ts: i64) -> BlockIter<'_, BCS, TXCS> {
        if self.block_records.len() == 1 {
            return self.blocks();
        }

        let start_idx = self
            .block_records
            .partition_point(|br| br.is_endcap() || br.ts.unwrap() < start_ts)
            .min(self.block_records.len().saturating_sub(2));
        let end_idx = self
            .block_records
            .partition_point(|br| !br.is_endcap() && br.ts.unwrap() <= end_ts)
            .min(self.block_records.len().saturating_sub(1));

        BlockIter::new_range(self, start_idx, end_idx)
    }

    fn guess_shard_config(&self, problems: &mut Vec<String>) -> Option<(u64, u64)> {
        let mut slot_diffs = HashMap::new();

        let mut last_slot = self
            .block_records
            .iter()
            .rev()
            .skip(1)
            .rev()
            .filter_map(|maybe_b| maybe_b.ok().map(|b| b.slot))
            .next()
            .unwrap_or(0);
        for block_record in self
            .block_records
            .iter()
            .skip(1)
            .filter_map(|maybe_br| maybe_br.ok().take_if(|br| !br.is_endcap()))
        {
            let counter = slot_diffs
                .entry((block_record.slot as i128 - last_slot as i128).unsigned_abs() as u64)
                .or_insert(0);
            *counter += 1;
            last_slot = block_record.slot;
        }

        if let Some(n) = slot_diffs
            .iter()
            .max_by_key(|(_, count)| **count)
            .map(|(n, _)| n)
            .copied()
        {
            for (other_n, count) in slot_diffs {
                if other_n % n != 0 {
                    problems.push(format!("inconsistent slot difference: {other_n} (present {count} times) is not a multiple of {n}"));
                }
            }

            let mut residues = HashMap::new();
            for block_record in self
                .block_records
                .iter()
                .skip(1)
                .filter_map(|maybe_br| maybe_br.ok().take_if(|br| !br.is_endcap()))
            {
                let residue = block_record.slot % n;
                let (counter, examples) = residues
                    .entry(residue)
                    .or_insert((0, Vec::with_capacity(5)));
                *counter += 1;

                if examples.len() < examples.capacity() {
                    examples.push(block_record.slot);
                }
            }

            let i = residues
                .iter()
                .max_by_key(|(_, (count, _))| *count)
                .map(|(i, _)| *i)
                .unwrap();
            if residues.len() > 1 {
                for (other_i, (count, examples)) in residues {
                    if other_i != i {
                        problems.push(format!("inconsistent shard id: {other_i} (present {count} times) is not equal to {i} (examples: {examples:?})"));
                    }
                }
            }

            Some((n, i))
        } else {
            None
        }
    }

    pub fn stats(&self) -> BlockDbStats {
        let mut problems = Vec::new();

        let shard_config = self.guess_shard_config(&mut problems);

        let (checksum_res_tx, checksum_res_rx) =
            sync_channel::<bool>(4 * rayon::current_num_threads());
        let checksum_agg_handle = std::thread::Builder::new()
            .spawn(move || {
                let mut bad_count = 0u64;
                while let Ok(res) = checksum_res_rx.recv() {
                    if !res {
                        bad_count += 1;
                    }
                }
                bad_count
            })
            .expect("failed to spawn checksum result aggregator");

        let mut n_txs_corrupted = 0;
        let mut n_rec_corrupted = 0;
        let mut n_missing = 0;
        let mut last_slot = self
            .block_records
            .iter()
            .filter_map(|maybe_br| maybe_br.ok().map(|br| br.slot))
            .next()
            .unwrap_or(0);
        let expected_slot_diff = shard_config.map(|s| s.0).unwrap_or(1);
        for idx in 0..self.block_records.len() - 1 {
            match self.get_block_unchecked(idx) {
                Ok((block_record, txs)) => {
                    if (block_record.slot as i128 - last_slot as i128).unsigned_abs() as u64
                        > expected_slot_diff
                    {
                        n_missing += 1;
                    }
                    last_slot = block_record.slot;

                    // compute checksum
                    let checksum_res_tx = checksum_res_tx.clone();
                    rayon::spawn(move || {
                        let res = check_rebuild_block(block_record, txs).is_ok();
                        checksum_res_tx
                            .send(res)
                            .expect("checksum aggregator panicked");
                    })
                }
                Err(_) => {
                    // no need to call get_block
                    n_rec_corrupted += 1;
                }
            }
        }
        std::mem::drop(checksum_res_tx);
        n_txs_corrupted += checksum_agg_handle
            .join()
            .expect("checksum aggregator panicked");

        BlockDbStats {
            shard_config,
            n_blocks: self.block_records.len(),
            n_txs: self.txs.len(),
            n_rec_corrupted,
            n_txs_corrupted,
            n_missing,
            problems,
        }
    }

    pub fn checksum(&mut self) -> u64 {
        const CRC: crc::Crc<u64, crc::Table<1>> =
            crc::Crc::<u64, crc::Table<1>>::new(&crc::CRC_64_GO_ISO);
        let mut hasher = CRC.digest();

        hasher.update(&self.block_records.len().to_le_bytes());

        let chunked_blocks = self.blocks().chunks(rayon::current_num_threads() * 4);
        let blocks_csum = chunked_blocks
            .into_iter()
            .map(|chunk| {
                let chunk = chunk.filter_map(|mb| mb.ok()).collect_vec();
                chunk
                    .into_par_iter()
                    .map(|b| checksum(&b))
                    .reduce(|| 0u64, |a, b| a.wrapping_add(b))
            })
            .fold(0u64, |a, b| a.wrapping_add(b));

        hasher.update(&blocks_csum.to_le_bytes());

        hasher.finalize()
    }
}

fn check_rebuild_block(block_rec: BlockRecord, txs: Vec<Tx>) -> eyre::Result<Block> {
    let block = Block {
        slot: block_rec.slot,
        height: block_rec.height,
        ts: block_rec.ts,
        txs,
    };

    eyre::ensure!(checksum(&block) == block_rec.checksum, "checksum mismatch");

    Ok(block)
}

pub struct BlockDbStats {
    pub shard_config: Option<(u64, u64)>,
    pub n_blocks: u64,
    pub n_txs: u64,
    pub n_rec_corrupted: u64,
    pub n_txs_corrupted: u64,
    pub n_missing: u64,
    pub problems: Vec<String>,
}
