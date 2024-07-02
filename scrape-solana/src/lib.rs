use std::{fmt::{Debug, Display}, io, path::PathBuf};
use eyre::{Context, OptionExt};
use mmap_vec::MmapVec;

use solana_sdk::transaction::Legacy;
use solana_transaction_status::UiConfirmedBlock;

mod mmap_map;
use mmap_map::MmapMap;

pub struct Db {
    block_records: MmapVec<BlockRecord>,
    tx_records: MmapVec<TxRecord>,
    tx_data: MmapVec<u8>,
    account_records: MmapMap<AccountID, AccountRecord>,
    account_data: MmapVec<u8>,
}

impl Db {
    pub unsafe fn open(root_path: PathBuf, mut out: impl io::Write) -> eyre::Result<Self> {
        std::fs::create_dir_all(&root_path).wrap_err("Failed to create DB dir")?;

        macro_rules! open_vec_table {
            ($name:ident) => {
                let $name = unsafe { MmapVec::with_name(root_path.join(stringify!($name))) }
                    .wrap_err(concat!("Failed to open table: ", stringify!($name)))?;
            }
        }

        open_vec_table!(block_records);
        open_vec_table!(tx_records);
        open_vec_table!(tx_data);
        open_vec_table!(account_data);

        // build account records map from underlying vec
        let account_records = unsafe { MmapMap::with_name(root_path.join("account_records")) }
            .wrap_err("Failed to open table: account_records")?;

        let mut db = Db { block_records, tx_records, tx_data, account_records, account_data };

        writeln!(out, "Auto-healing DB...")?;
        db.heal(&mut out)
            .wrap_err("Failed to auto-heal DB")?;
        writeln!(out, "DB healed...")?;

        Ok(db)
    }

    fn heal(&mut self, mut out: impl io::Write) -> eyre::Result<()> {
        let r1 = self.heal_tx_records(&mut out);
        let r2 = self.heal_tx_data(out);
        self.sync()?;

        // handle errors *after* sync
        r1?;
        r2?;

        Ok(())
    }

    fn heal_tx_records(&mut self, mut out: impl io::Write) -> eyre::Result<()> {
        // HACK: assumes blocks and tx records are ordered in decreasing block nums

        // find expected length of tx_records
        let mut expected_len = self.tx_records.len();
        for b in self.block_records.iter().rev() {
            if b.tx_count != u64::MAX {
                expected_len = (b.tx_start_idx + b.tx_count) as usize;
                break;
            }
        }

        // prune possibly partially fetched tx_records
        if expected_len != self.tx_records.len() {
            let r1 = writeln!(
                out,
                "dropping {} txs (possible partial fetch)",
                self.tx_records.len() - expected_len,
            );
            self.tx_records.truncate(expected_len);
            r1?; // error not critical
        }

        Ok(())
    }

    fn heal_tx_data(&mut self, mut out: impl io::Write) -> eyre::Result<()> {
        // HACK: assumes tx records and data are contiguous and in same order

        // find expected length of tx_records
        let mut expected_len = self.tx_data.len();
        for tx  in self.tx_records.iter().rev() {
            if tx.data_sz != u32::MAX {
                expected_len = (tx.data_start_idx + tx.data_sz as u64) as usize;
                break;
            }
        }

        // prune possibly partially fetched tx_records
        if expected_len != self.tx_data.len() {
            let r2 = writeln!(
                out,
                "dropping {} bytes from tx data (possible partial fetch)",
                self.tx_data.len() - expected_len,
            );
            self.tx_data.truncate(expected_len);
            r2?; // error not critical
        }

        Ok(())
    }

    pub fn store_block(&mut self, block: UiConfirmedBlock) -> eyre::Result<()> {
        let block_num = block.block_height.ok_or_eyre("could not get block height from block")?;
        if block_num >= self.block_records.last().map(|b| b.num).unwrap_or(u64::MAX) {
            // HACK: DO NOT REMOVE UNLESS YOU CHANGE HEALING LOGIC
            return Err(eyre::eyre!("block is already in store or higher than those in store"));
        }

        let tx_start_idx = self.tx_records.len() as u64;

        for tx in block.transactions.as_ref().unwrap_or(&Vec::new()) {
            let tx_data = todo!();

            let tx_rec = TxRecord {
                data_start_idx: todo!(),
                data_sz: todo!(),
                version: tx.version.map(|v| v.into()).unwrap_or(TxVersion(u32::MAX)),
                fee: tx.meta.as_ref().map(|m| m.fee).unwrap_or(u64::MAX),
                compute_units: tx
                    .meta
                    .as_ref()
                    .map(|m| m.compute_units_consumed.clone().unwrap_or(u64::MAX))
                    .unwrap_or(u64::MAX),
            };

            // TODO: push tx data
            // TODO: find and store accounts as needed

            self.tx_records.push(tx_rec).wrap_err("could not save tx")?;
        }

        self.block_records.push(BlockRecord {
            num: block_num,
            ts: block.block_time.unwrap_or(i64::MAX),
            tx_start_idx,
            tx_count: block.transactions.as_ref().map(|v| v.len() as u64).unwrap_or(u64::MAX),
        }).wrap_err("error storing block")?;

        Ok(())
    }

    pub fn has_account(&self, id: &AccountID) -> bool {
        self.account_records.contains_key(id)
    }

    pub fn store_new_account(&mut self, account: ()) {
        todo!()
    }

    pub fn last_block_num(&self) -> Option<u64> {
        self.block_records.last().map(|b| b.num)
    }

    pub fn sync(&mut self) -> eyre::Result<()> {
        macro_rules! sync_table {
            ($name:ident) => {
                self.$name.sync()
                    .wrap_err(concat!("could not sync table: ", stringify!($name)))?;
            }
        }

        sync_table!(account_data);
        sync_table!(account_records);

        sync_table!(tx_data);
        sync_table!(tx_records);

        sync_table!(block_records);

        Ok(())
    }
}

#[repr(C, packed)]
#[derive(Default, Debug)]
pub struct BlockRecord {
    pub num: u64,
    pub ts: i64,
    pub tx_start_idx: u64,
    pub tx_count: u64,
}

#[repr(C, packed)]
#[derive(Default, Debug)]
pub struct TxRecord {
    pub data_start_idx: u64,
    pub data_sz: u32,
    pub version: TxVersion,
    pub fee: u64,
    pub compute_units: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct TxVersion(u32);
impl From<solana_sdk::transaction::TransactionVersion> for TxVersion {
    fn from(value: solana_sdk::transaction::TransactionVersion) -> Self {
        use solana_sdk::transaction::TransactionVersion::*;
        match value {
            Legacy(_) => Self(u32::MAX),
            Number(n) => Self(n as u32),
        }
    }
}
impl From<Option<solana_sdk::transaction::TransactionVersion>> for TxVersion {
    fn from(value: Option<solana_sdk::transaction::TransactionVersion>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Self(u32::MAX), // legacy
        }
    }
}
impl Default for TxVersion {
    fn default() -> Self {
        Self(u32::MAX)
    }
}
impl PartialOrd for TxVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let a = self.0.wrapping_add(1);
        let b = other.0.wrapping_add(1);
        a.partial_cmp(&b)
    }
}
impl Debug for TxVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut f = f.debug_tuple("TxVersion");
        if self.0 == u32::MAX {
            f.field(&"LEGACY");
        } else {
            f.field(&self.0);
        }
        f.finish()
    }
}


#[repr(C)]
#[derive(Default, Debug)]
pub struct AccountRecord {
    pub owner: AccountID,
    pub blocknum: u64,
    pub start_idx: u64,
    pub sz: u32,
    pub is_executable: bool,
}

#[repr(transparent)]
#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccountID([u8; 32]);

impl Display for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::Engine;
        f.write_str(&base64::prelude::BASE64_STANDARD.encode(&self.0))
    }
}
impl Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::Engine;
        f.debug_tuple("AccountID")
            .field(&base64::prelude::BASE64_STANDARD.encode(&self.0))
            .finish()
    }
}

impl Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("blocks", &self.block_records)
            .field("tx_records", &self.tx_records)
            .field("tx_data", &self.tx_data)
            .field("accounts", &"?")
            .field("account_data", &self.account_data)
            .finish()
    }
}
