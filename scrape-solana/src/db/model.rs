use crate::{
    crc_checksum_serde::checksum,
    model::{Account, AccountID},
};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub(crate) struct AccountRecord {
    pub id: AccountID,
    pub owner: AccountID,
    pub min_height: u64,
    pub max_height: u64,
    pub data_start_idx: u64,
    pub is_executable: bool,
    pub checksum: u64,
}

impl AccountRecord {
    pub fn new(account: &Account, data_start_idx: u64) -> Self {
        Self {
            id: account.id.clone(),
            owner: account.owner.clone(),
            min_height: account.min_height,
            max_height: account.max_height,
            data_start_idx,
            is_executable: account.is_executable,
            checksum: checksum(account),
        }
    }

    pub fn endcap(data_next_start_idx: u64) -> Self {
        let mut endcap = AccountRecord {
            id: AccountID::default(),
            owner: AccountID::default(),
            min_height: 1,
            max_height: 0,
            data_start_idx: data_next_start_idx,
            is_executable: false,
            checksum: checksum(Account {
                id: AccountID::default(),
                owner: AccountID::default(),
                min_height: 1,
                max_height: 0,
                is_executable: false,
                data: vec![],
            }),
        };
        endcap.checksum ^= u64::MAX; // invert checksum to distinguish it from regular records
        endcap
    }

    pub fn is_endcap(&self) -> bool {
        self.min_height == 1
            && self.max_height == 0
            && self.id == AccountID::default()
            && self.owner == AccountID::default()
            && !self.is_executable
            && self.checksum == Self::endcap(self.data_start_idx).checksum
    }
}
