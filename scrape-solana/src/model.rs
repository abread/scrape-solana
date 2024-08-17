use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
};

use eyre::{OptionExt, WrapErr};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Block {
    pub slot: u64,
    pub height: u64,
    pub ts: Option<i64>,
    pub txs: Vec<Tx>,
}

impl Block {
    pub fn from_solana_sdk(
        slot: u64,
        block: solana_transaction_status::UiConfirmedBlock,
    ) -> eyre::Result<Self> {
        let txs = block
            .transactions
            .unwrap_or_default()
            .into_iter()
            .map(Tx::try_from)
            .collect::<Result<_, _>>()?;

        Ok(Self {
            slot,
            height: block.block_height.ok_or_eyre("missing block height")?,
            ts: block.block_time,
            txs,
        })
    }

    pub fn referenced_programs(&self) -> BTreeSet<AccountID> {
        self.txs
            .iter()
            .flat_map(|tx| tx.referenced_programs())
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Tx {
    pub version: TxVersion,
    pub fee: u64,
    pub compute_units: u64,
    pub payload: TxPayload,
}

impl Tx {
    pub fn referenced_programs(&self) -> impl Iterator<Item = AccountID> + '_ {
        self.payload
            .instrs
            .iter()
            .map(|i| self.payload.account_table[i.program_account_idx as usize].clone())
    }
}

impl TryFrom<solana_transaction_status::EncodedTransactionWithStatusMeta> for Tx {
    type Error = eyre::Report;

    fn try_from(
        tx: solana_transaction_status::EncodedTransactionWithStatusMeta,
    ) -> Result<Self, Self::Error> {
        let meta = tx.meta.ok_or_eyre("missing tx meta")?;
        let payload = tx.transaction.try_into()?;

        Ok(Self {
            version: tx.version.into(),
            fee: meta.fee,
            compute_units: meta.compute_units_consumed.unwrap_or(0),
            payload,
        })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Account {
    pub id: AccountID,
    pub owner: AccountID,
    pub min_height: u64,
    pub max_height: u64,
    pub is_executable: bool,
    pub data: Vec<u8>, // prolly elf
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
        Self(u32::MAX) // legacy
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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct TxPayload {
    pub account_table: Vec<AccountID>,
    pub instrs: Vec<TxInstruction>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq)]
pub struct TxInstruction {
    pub program_account_idx: u8,
    pub account_idxs: Vec<u8>,
    pub data: Vec<u8>,
    pub stack_height: Option<u32>,
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

const ACCOUNT_ID_LEN: usize = 32;

#[repr(transparent)]
#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountID([u8; ACCOUNT_ID_LEN]);

impl Display for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&solana_sdk::pubkey::Pubkey::new_from_array(self.0), f)
    }
}
impl Debug for AccountID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AccountID({})",
            solana_sdk::pubkey::Pubkey::new_from_array(self.0)
        )
    }
}
impl From<AccountID> for [u8; ACCOUNT_ID_LEN] {
    fn from(value: AccountID) -> Self {
        value.0
    }
}
impl AsRef<[u8]> for AccountID {
    fn as_ref(&self) -> &[u8] {
        &self.0
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
        use std::str::FromStr;

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

impl TryFrom<solana_transaction_status::UiCompiledInstruction> for TxInstruction {
    type Error = eyre::Report;

    fn try_from(
        value: solana_transaction_status::UiCompiledInstruction,
    ) -> Result<Self, Self::Error> {
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

impl TryFrom<solana_transaction_status::UiInstruction> for TxInstruction {
    type Error = eyre::Report;

    fn try_from(value: solana_transaction_status::UiInstruction) -> Result<Self, Self::Error> {
        use solana_transaction_status::UiInstruction::*;

        match value {
            Compiled(c) => c.try_into(),
            Parsed(_) => Err(eyre::eyre!(
                "unreachable code path: txs should not be parsed yet :("
            )),
        }
    }
}

impl TryFrom<solana_transaction_status::EncodedTransaction> for TxPayload {
    type Error = eyre::Report;

    fn try_from(value: solana_transaction_status::EncodedTransaction) -> Result<Self, Self::Error> {
        use solana_transaction_status::EncodedTransaction::*;
        match value {
            LegacyBinary(_) | Binary(_, _) | Accounts(_) => Err(eyre::eyre!(
                "unreachable code path: txs should be json-formatted :("
            )),
            Json(tx) => tx.message.try_into(),
        }
    }
}
