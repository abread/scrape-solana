use std::{
    path::PathBuf,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::JoinHandle,
};

use eyre::{eyre, WrapErr};
use itertools::Itertools;

use crate::{
    db,
    model::Block,
    solana_api::{self, SolanaApi},
};

use super::db::DbOperation;

pub enum DBFullHealerOperation {
    Cancel,
}

pub fn spawn_db_full_healer(
    orig_db_path: PathBuf,
    step: u64,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
) -> (
    SyncSender<DBFullHealerOperation>,
    JoinHandle<eyre::Result<()>>,
) {
    let (tx, rx) = sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("db_full_healer".to_owned())
        .spawn(move || {
            match db_full_healer_actor(orig_db_path, step, rx, api, block_handler_tx, db_tx) {
                Ok(x) => Ok(x),
                Err(e) => {
                    eprintln!("block fetcher actor failed: {e}");
                    Err(e)
                }
            }
        })
        .expect("failed to spawn block fetcher actor thread");
    (tx, handle)
}

fn db_full_healer_actor(
    orig_db_path: PathBuf,
    step: u64,
    rx: Receiver<DBFullHealerOperation>,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let old_db = db::open(orig_db_path.clone(), std::io::stdout())?;

    // salvage accounts
    for accounts_chunk in old_db
        .accounts()
        .filter_map(|maybe_acc| maybe_acc.ok())
        .chunks(db::DB_PARAMS.account_rec_cs)
        .into_iter()
        .map(|chunk| chunk.collect_vec())
    {
        if let Ok(DBFullHealerOperation::Cancel) = rx.try_recv() {
            return Err(eyre!("heal cancelled"));
        }

        db_tx
            .send(DbOperation::StoreNewAccounts(accounts_chunk))
            .map_err(|_| eyre!("new db closed"))?;
    }

    // salvage blocks, filling in gaps
    let (left, middle_slot, right) = old_db.split();

    let (left_cancel_tx, left_cancel_rx) = sync_channel(1);
    let (right_cancel_tx, right_cancel_rx) = sync_channel(1);

    let left_thread_handle = {
        let start = left
            .blocks()
            .last()
            .and_then(|b| b.ok())
            .map(|b| b.slot)
            .unwrap_or(middle_slot);
        let end = middle_slot;
        let range = (start..=end).rev().step_by(step as usize);
        let block_handler_tx = block_handler_tx.clone();
        let api = Arc::clone(&api);
        let db_tx = db_tx.clone();

        std::thread::Builder::new()
            .name("db-heal-l".to_owned())
            .spawn(move || {
                match block_db_full_healer_actor(
                    left,
                    range,
                    left_cancel_rx,
                    api,
                    block_handler_tx,
                    db_tx,
                ) {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        eprintln!("block fetcher actor failed: {e}");
                        Err(e)
                    }
                }
            })
            .expect("failed to spawn block fetcher actor thread")
    };

    let right_thread_handle = {
        let start = middle_slot + step;
        let end = right
            .blocks()
            .last()
            .and_then(|b| b.ok())
            .map(|b| b.slot)
            .unwrap_or(middle_slot);
        let range = (start..=end).step_by(step as usize);
        let block_handler_tx = block_handler_tx;
        let api = Arc::clone(&api);
        let db_tx = db_tx.clone();

        std::thread::Builder::new()
            .name("db-heal-r".to_owned())
            .spawn(move || {
                match block_db_full_healer_actor(
                    right,
                    range,
                    right_cancel_rx,
                    api,
                    block_handler_tx,
                    db_tx,
                ) {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        eprintln!("block fetcher actor failed: {e}");
                        Err(e)
                    }
                }
            })
            .expect("failed to spawn block fetcher actor thread")
    };

    let cancel_handle = std::thread::Builder::new()
        .name("db-heal-cancel".to_owned())
        .spawn(move || {
            if let Ok(DBFullHealerOperation::Cancel) = rx.recv() {
                let _ = left_cancel_tx.send(DBFullHealerOperation::Cancel);
                let _ = right_cancel_tx.send(DBFullHealerOperation::Cancel);
            }
        })
        .expect("failed to spawn db-heal-cancel thread");

    right_thread_handle
        .join()
        .expect("right db healer thread panicked")?;
    left_thread_handle
        .join()
        .expect("left db healer thread panicked")?;
    cancel_handle.join().expect("healer cancel thread panicked");
    Ok(())
}

fn block_db_full_healer_actor<const BCS: usize, const TXCS: usize>(
    db: db::MonotonousBlockDb<BCS, TXCS>,
    slots: impl Iterator<Item = u64>,
    cancel_rx: Receiver<DBFullHealerOperation>,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let mut stored_blocks = db.blocks().filter_map(|b| b.ok()).peekable();

    for slot_chunk in slots.chunks(db::DB_PARAMS.block_rec_cs).into_iter() {
        'filler_loop: for slot in slot_chunk {
            if let Ok(DBFullHealerOperation::Cancel) = cancel_rx.try_recv() {
                return Err(eyre!("Heal cancelled"));
            }

            let block = if stored_blocks.peek().map(|b| b.slot) == Some(slot) {
                stored_blocks.next().unwrap()
            } else {
                eprintln!("refetching {slot}");
                loop {
                    if let Ok(DBFullHealerOperation::Cancel) = cancel_rx.try_recv() {
                        return Err(eyre!("Heal cancelled"));
                    }

                    match api.fetch_block(slot) {
                        Ok(Some(block)) => {
                            break Block::from_solana_sdk(slot, block)
                                .map_err(|e| eyre!("failed to convert block: {e}"))?;
                        }
                        Ok(None) => {
                            continue 'filler_loop; // skip this slot, still (forever(?)) missing
                        }
                        Err(solana_api::Error::Timeout(e)) => {
                            eprintln!("timeout fetching block at slot {slot}: {e}");
                            continue;
                        }
                        Err(solana_api::Error::PostTimeoutCooldown) => {
                            // not really a timeout, just continuing the previous timeout
                            continue;
                        }
                        Err(solana_api::Error::SolanaClient(e)) => {
                            return Err(e).wrap_err("failed to fetch block");
                        }
                    }
                }
            };

            block_handler_tx
                .send(block)
                .map_err(|_| eyre!("block handler closed"))?;
        }

        db_tx
            .send(DbOperation::Sync)
            .map_err(|_| eyre!("db closed"))?;
    }

    Ok(())
}
