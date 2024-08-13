use std::{
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::JoinHandle,
};

use eyre::eyre;

use crate::{db::DbSlotLimits, model::Block, solana_api::SolanaApi};

use super::db::DbOperation;

pub enum BlockFetcherOperation {
    Stop,
}

pub fn spawn_block_fetcher(
    forward_chance: f64,
    step: u64,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
) -> (
    SyncSender<BlockFetcherOperation>,
    JoinHandle<eyre::Result<()>>,
) {
    let (tx, rx) = sync_channel(1);
    let handle = std::thread::spawn(move || {
        block_fetcher_worker(forward_chance, step, rx, api, block_handler_tx, db_tx)
    });
    (tx, handle)
}

fn block_fetcher_worker(
    forward_chance: f64,
    step: u64,
    rx: Receiver<BlockFetcherOperation>,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let limits = read_limits(&db_tx)?;

    let mut left_slot = limits.left_slot.unwrap_or(limits.middle_slot);
    let mut right_slot = limits
        .right_slot
        .unwrap_or(limits.middle_slot.saturating_add(limits.middle_slot));

    loop {
        if let Ok(BlockFetcherOperation::Stop) = rx.try_recv() {
            break;
        }

        let (next_slot, slot, side_str) = if rand::random::<f64>() < forward_chance {
            (right_slot.saturating_add(step), &mut right_slot, "right")
        } else {
            (left_slot.saturating_sub(step), &mut left_slot, "left")
        };

        let block = api.fetch_block(*slot)?;
        block_handler_tx
            .send(block)
            .map_err(|_| eyre!("block handler closed"))?;
        println!("fetched block at slot {slot} ({side_str} side)");
        *slot = next_slot;
    }

    Ok(())
}

fn read_limits(db_tx: &SyncSender<DbOperation>) -> eyre::Result<DbSlotLimits> {
    let (tx, rx) = sync_channel(1);
    db_tx
        .send(DbOperation::ReadLimits { reply: tx })
        .map_err(|_| eyre!("db closed"))?;
    rx.recv().map_err(|_| eyre!("db closed"))
}
