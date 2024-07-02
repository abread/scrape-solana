use base64::{prelude::BASE64_STANDARD, Engine};
use eyre::{eyre, Context, OptionExt};
use mmap_vec::MmapVec;
use nonmax::NonMaxU64;
use std::{
    collections::{BTreeSet, HashSet}, fmt::{Debug, Display}, io, path::PathBuf
};

use serde::{Deserialize, Serialize};
use solana_transaction_status::{
    UiCompiledInstruction, UiConfirmedBlock, UiInstruction, UiParsedInstruction,
};

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
            };
        }

        open_vec_table!(block_records);
        open_vec_table!(tx_records);
        open_vec_table!(tx_data);
        open_vec_table!(account_data);

        // build account records map from underlying vec
        let account_records = unsafe { MmapMap::with_name(root_path.join("account_records")) }
            .wrap_err("Failed to open table: account_records")?;

        let mut db = Db {
            block_records,
            tx_records,
            tx_data,
            account_records,
            account_data,
        };

        writeln!(out, "Auto-healing DB...")?;
        db.heal(&mut out).wrap_err("Failed to auto-heal DB")?;
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
        for tx in self.tx_records.iter().rev() {
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

    pub fn store_block(&mut self, block: UiConfirmedBlock, mut account_fetcher: impl FnMut(&AccountID) -> eyre::Result<solana_sdk::account::Account>) -> eyre::Result<()> {
        let block_num = block
            .block_height
            .ok_or_eyre("could not get block height from block")?;
        if block_num >= self.block_records.last().map(|b| b.num).unwrap_or(u64::MAX) {
            // HACK: DO NOT REMOVE UNLESS YOU CHANGE HEALING LOGIC
            return Err(eyre::eyre!(
                "block is already in store or higher than those in store"
            ));
        }

        let tx_start_idx = self.tx_records.len() as u64;

        for tx in block.transactions.as_ref().unwrap_or(&Vec::new()) {
            let tx_data: TxPayload = tx
                .transaction
                .clone()
                .try_into()
                .wrap_err("could not parse tx data")?;

            for account_id in tx_data.instrs.iter().map(|i| tx_data.account_table[i.program_account_idx as usize].clone()).collect::<BTreeSet<_>>() {
                if !self.has_account(&account_id) {
                    let account = account_fetcher(&account_id)?;
                    self.store_new_account(account);
                }
            }

            let tx_data = bincode::serialize(&tx_data).wrap_err("could not serialize tx data")?;

            let data_start_idx = self.tx_data.len();
            let data_sz = tx_data.len();
            self.tx_data
                .reserve(data_sz)
                .wrap_err("failed to allocate space for tx data")?;
            for byte in tx_data.into_iter() {
                self.tx_data
                    .push_within_capacity(byte)
                    .map_err(|_| eyre::eyre!("could not push tx data to pre-allocated space"))?;
            }

            let version = tx
                .version
                .as_ref()
                .cloned()
                .map(|v| v.into())
                .unwrap_or_default();

            let tx_rec = TxRecord {
                data_start_idx: data_start_idx as u64,
                data_sz: data_sz as u32,
                version,
                fee: tx
                    .meta
                    .as_ref()
                    .map(|m| m.fee.try_into())
                    .transpose()
                    .wrap_err("failed to parse tx fee")?,
                compute_units: tx
                    .meta
                    .as_ref()
                    .map(|m| m.compute_units_consumed.clone().unwrap_or(u64::MAX))
                    .unwrap_or(u64::MAX),
            };

            self.tx_records.push(tx_rec).wrap_err("could not save tx")?;
        }

        self.block_records
            .push(BlockRecord {
                num: block_num,
                ts: block.block_time.unwrap_or(i64::MAX),
                tx_start_idx,
                tx_count: block
                    .transactions
                    .as_ref()
                    .map(|v| v.len() as u64)
                    .unwrap_or(u64::MAX),
            })
            .wrap_err("error storing block")?;

        Ok(())
    }

    pub fn has_account(&self, id: &AccountID) -> bool {
        self.account_records.contains_key(id)
    }

    pub fn store_new_account(&mut self, account: solana_sdk::account::Account) {
        todo!()
    }

    pub fn last_block_num(&self) -> Option<u64> {
        self.block_records.last().map(|b| b.num)
    }

    pub fn sync(&mut self) -> eyre::Result<()> {
        macro_rules! sync_table {
            ($name:ident) => {
                self.$name
                    .sync()
                    .wrap_err(concat!("could not sync table: ", stringify!($name)))?;
            };
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
    pub fee: Option<NonMaxU64>,
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

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TxPayload {
    account_table: Vec<AccountID>,
    instrs: Vec<TxInstruction>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TxInstruction {
    program_account_idx: u8,
    account_idxs: Vec<u8>,
    data: Vec<u8>,
    stack_height: Option<u32>,
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

const ACCOUNT_ID_LEN: usize = 32;

#[repr(transparent)]
#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountID([u8; ACCOUNT_ID_LEN]);

impl Display for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::Engine;
        f.write_str(&BASE64_STANDARD.encode(&self.0))
    }
}
impl Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use base64::Engine;
        f.debug_tuple("AccountID")
            .field(&BASE64_STANDARD.encode(&self.0))
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

impl Debug for TxInstruction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxInstruction")
            .field("program_account_idx", &self.program_account_idx)
            .field("account_idxs", &self.account_idxs)
            .field("data", &BASE64_STANDARD.encode(&self.data))
            .field("stack_height", &self.stack_height)
            .finish()
    }
}

impl TryFrom<solana_transaction_status::UiMessage> for TxPayload {
    type Error = eyre::Report;

    fn try_from(value: solana_transaction_status::UiMessage) -> Result<Self, Self::Error> {
        match value {
            solana_transaction_status::UiMessage::Parsed(p) => Self::try_from(p),
            solana_transaction_status::UiMessage::Raw(r) => Self::try_from(r),
        }
    }
}

impl TryFrom<solana_transaction_status::UiParsedMessage> for TxPayload {
    type Error = eyre::Report;

    fn try_from(value: solana_transaction_status::UiParsedMessage) -> Result<Self, Self::Error> {
        let account_table = value
            .account_keys
            .into_iter()
            .map(TryInto::<AccountID>::try_into)
            .collect::<Result<_, _>>()?;

        let instrs = value
            .instructions
            .into_iter()
            .map(TryInto::<TxInstruction>::try_into)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            account_table,
            instrs,
        })
    }
}

impl TryFrom<solana_transaction_status::UiRawMessage> for TxPayload {
    type Error = eyre::Report;

    fn try_from(value: solana_transaction_status::UiRawMessage) -> eyre::Result<Self> {
        let account_table = value
            .account_keys
            .into_iter()
            .map(TryInto::<AccountID>::try_into)
            .collect::<Result<_, _>>()?;

        let instrs = value
            .instructions
            .into_iter()
            .map(TryInto::<TxInstruction>::try_into)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            account_table,
            instrs,
        })
    }
}

impl TryFrom<solana_transaction_status::parse_accounts::ParsedAccount> for AccountID {
    type Error = eyre::Report;

    fn try_from(
        value: solana_transaction_status::parse_accounts::ParsedAccount,
    ) -> Result<Self, Self::Error> {
        value.pubkey.try_into()
    }
}

impl TryFrom<String> for AccountID {
    type Error = eyre::Report;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let bytes = BASE64_STANDARD
            .decode(value)
            .wrap_err("error decoding account id")?;
        let bytes = bytes
            .try_into()
            .map_err(|_| eyre!("account id has wrong size"))?;
        Ok(AccountID(bytes))
    }
}

impl TryFrom<UiCompiledInstruction> for TxInstruction {
    type Error = eyre::Report;

    fn try_from(value: UiCompiledInstruction) -> Result<Self, Self::Error> {
        Ok(Self {
            program_account_idx: value.program_id_index,
            account_idxs: value.accounts,
            data: BASE64_STANDARD
                .decode(value.data)
                .wrap_err("error decoding tx data")?,
            stack_height: value.stack_height,
        })
    }
}

impl TryFrom<UiInstruction> for TxInstruction {
    type Error = eyre::Report;

    fn try_from(value: UiInstruction) -> Result<Self, Self::Error> {
        match value {
            UiInstruction::Compiled(c) => c.try_into(),
            UiInstruction::Parsed(_) => Err(eyre::eyre!(
                "unreachable code path: txs should not be parsed yet :("
            )),
        }
    }
}

impl TryFrom<solana_transaction_status::EncodedTransaction> for TxPayload {
    type Error = eyre::Report;

    fn try_from(value: solana_transaction_status::EncodedTransaction) -> Result<Self, Self::Error> {
        match value {
            solana_transaction_status::EncodedTransaction::LegacyBinary(_)
            | solana_transaction_status::EncodedTransaction::Binary(_, _)
            | solana_transaction_status::EncodedTransaction::Accounts(_) => Err(eyre::eyre!(
                "unreachable code path: txs should be json-formatted :("
            )),
            solana_transaction_status::EncodedTransaction::Json(tx) => tx.message.try_into(),
        }
    }
}
