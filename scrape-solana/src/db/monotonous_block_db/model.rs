use std::sync::OnceLock;

use crate::{crc_checksum_serde::checksum, model::Block};
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct BlockRecord {
    pub slot: u64,
    pub height: u64,
    pub ts: Option<i64>,
    pub txs_start_idx: u64,
    pub checksum: u64,
}

impl BlockRecord {
    pub fn new(block: &Block, txs_start_idx: u64) -> Self {
        Self {
            slot: block.slot,
            height: block.height,
            ts: block.ts,
            txs_start_idx,
            checksum: checksum(block),
        }
    }

    pub fn endcap(txs_next_start_idx: u64) -> Self {
        BlockRecord {
            slot: 0,
            height: u64::MAX,
            ts: None,
            txs_start_idx: txs_next_start_idx,
            checksum: Self::endcap_checksum(),
        }
    }

    pub fn is_endcap(&self) -> bool {
        self.slot == 0
            && self.height == u64::MAX
            && self.ts.is_none()
            && self.checksum == Self::endcap_checksum()
    }

    fn endcap_checksum() -> u64 {
        static CACHE: OnceLock<u64> = OnceLock::new();

        *CACHE.get_or_init(|| {
            checksum(Block {
                slot: 0,
                height: u64::MAX,
                ts: None,
                txs: vec![],
            }) ^ u64::MAX
        })
    }
}
