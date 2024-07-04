use eyre::{eyre, Context};
use mmap_vec::MmapVec;
use nonmax::NonMaxU64;
use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
    io,
    num::NonZeroI64,
    ops::Range,
    path::PathBuf,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
use solana_transaction_status::{UiCompiledInstruction, UiConfirmedBlock, UiInstruction};

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

        write!(
            out,
            "loaded {} blocks, {} txs, {} accounts, {}B of tx data and {}B of account data",
            db.block_records.len(),
            db.tx_records.len(),
            db.account_records.len(),
            db.tx_data.len(),
            db.account_data.len()
        )?;

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
            self.tx_records
                .truncate(expected_len)
                .wrap_err("failed to prune tx records from partially processed block")?;
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

        // prune possibly partially fetched tx_data
        if expected_len != self.tx_data.len() {
            let r2 = writeln!(
                out,
                "dropping {} bytes from tx data (possible partial fetch)",
                self.tx_data.len() - expected_len,
            );
            self.tx_data
                .truncate(expected_len)
                .wrap_err("failed to prune tx data from partially processed block")?;
            r2?; // error not critical
        }

        Ok(())
    }

    pub fn store_block(
        &mut self,
        slot: u64,
        block: UiConfirmedBlock,
        mut account_fetcher: impl FnMut(
            &[AccountID],
        ) -> eyre::Result<(
            Vec<Option<solana_sdk::account::Account>>,
            Range<u64>,
        )>,
    ) -> eyre::Result<()> {
        if slot
            >= self
                .block_records
                .last()
                .map(|b| b.slot)
                .unwrap_or(u64::MAX)
        {
            // HACK: DO NOT REMOVE UNLESS YOU CHANGE HEALING LOGIC
            return Err(eyre::eyre!(
                "block is already in store or higher than those in store"
            ));
        }

        let tx_start_idx = self.tx_records.len() as u64;
        let mut accounts_to_fetch = BTreeSet::new();

        for tx in block.transactions.as_ref().unwrap_or(&Vec::new()) {
            let tx_data: TxPayload = tx
                .transaction
                .clone()
                .try_into()
                .wrap_err("could not parse tx data")?;

            accounts_to_fetch.extend(
                tx_data
                    .instrs
                    .iter()
                    .map(|i| tx_data.account_table[i.program_account_idx as usize].clone()),
            );

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

        match self
            .block_records
            .push(BlockRecord {
                slot,
                ts: block.block_time.and_then(NonZeroI64::new),
                tx_start_idx,
                tx_count: block
                    .transactions
                    .as_ref()
                    .map(|v| v.len() as u64)
                    .unwrap_or(u64::MAX),
            })
            .wrap_err("error storing block")
        {
            Ok(_) => (),
            Err(e) => {
                // roll back tx storage
                self.heal(io::stderr()).wrap_err(
                    "failed to revert tx storage operations after failing to store block",
                )?;

                return Err(e);
            }
        }

        let accounts_to_fetch = accounts_to_fetch
            .into_iter()
            .filter(|id| !self.has_account(id))
            .collect::<Vec<_>>();

        if accounts_to_fetch.len() > 0 {
            let (accounts, block_range) = account_fetcher(&accounts_to_fetch)?;
            for (id, maybe_account) in accounts_to_fetch.into_iter().zip(accounts) {
                match maybe_account {
                    Some(account) => self.store_new_account(id, account, block_range.clone())?,
                    None => {
                        eprintln!("could not fetch account {id}");
                    }
                }
            }
        }

        Ok(())
    }

    pub fn has_account(&self, id: &AccountID) -> bool {
        self.account_records.contains_key(id)
    }

    pub fn store_new_account(
        &mut self,
        id: AccountID,
        account: solana_sdk::account::Account,
        block_range: Range<u64>,
    ) -> eyre::Result<()> {
        let start_idx = self.account_data.len() as u64;
        let sz = account.data.len() as u32;
        self.account_data
            .reserve(account.data.len())
            .wrap_err("failed to allocate space for account data")?;
        for b in account.data {
            self.account_data
                .push_within_capacity(b)
                .map_err(|_| eyre!("failed to push account data to preallocated space"))?;
        }

        let account_record = AccountRecord {
            owner: AccountID(account.owner.to_bytes()),
            min_height: block_range.start,
            max_height: block_range.end,
            start_idx,
            sz,
            is_executable: account.executable,
        };

        if self.account_records.insert(id, account_record).is_some() {
            // roll back account data storage: we don't error out on account storage
            if let Err(e) = self.account_data.truncate(start_idx as usize) {
                eprintln!("error rolling back store_new_account: {:?}", e);
            }

            Err(eyre::eyre!("account already exists"))
        } else {
            Ok(())
        }
    }

    pub fn last_block_slot(&self) -> Option<u64> {
        self.block_records.last().map(|b| b.slot)
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
    pub slot: u64,
    pub ts: Option<NonZeroI64>,
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
        if self.0 == u32::MAX {
            write!(f, "TxVersion(LEGACY)")
        } else {
            write!(f, "TxVersion({})", self.0)
        }
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

impl Debug for TxInstruction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxInstruction")
            .field("program_account_idx", &self.program_account_idx)
            .field("account_idxs", &self.account_idxs)
            .field("data", &bs58::encode(&self.data).into_string())
            .field("stack_height", &self.stack_height)
            .finish()
    }
}

#[repr(C)]
#[derive(Default, Debug)]
pub struct AccountRecord {
    pub owner: AccountID,
    pub min_height: u64,
    pub max_height: u64,
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
        Display::fmt(
            &solana_sdk::pubkey::Pubkey::new_from_array(self.0.clone()),
            f,
        )
    }
}
impl Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AccountID({})",
            solana_sdk::pubkey::Pubkey::new_from_array(self.0.clone())
        )
    }
}
impl From<AccountID> for [u8; ACCOUNT_ID_LEN] {
    fn from(value: AccountID) -> Self {
        value.0
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
        solana_sdk::pubkey::Pubkey::from_str(&value)
            .map_err(|e| e.into())
            .map(|p| p.into())
    }
}

impl From<solana_sdk::pubkey::Pubkey> for AccountID {
    fn from(value: solana_sdk::pubkey::Pubkey) -> Self {
        Self(value.to_bytes())
    }
}

impl TryFrom<UiCompiledInstruction> for TxInstruction {
    type Error = eyre::Report;

    fn try_from(value: UiCompiledInstruction) -> Result<Self, Self::Error> {
        Ok(Self {
            program_account_idx: value.program_id_index,
            account_idxs: value.accounts,
            data: bs58::decode(value.data)
                .into_vec()
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
