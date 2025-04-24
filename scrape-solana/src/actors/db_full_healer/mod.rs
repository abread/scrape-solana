use std::{
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, SyncSender, TryRecvError, sync_channel},
    },
    thread::JoinHandle,
};

mod sparse_block_db;
use sparse_block_db::SparseBlockDb;
pub use sparse_block_db::recover_blocks_from_db;

use eyre::{WrapErr, eyre};
use itertools::Itertools;

use crate::{
    db::{self, DbSlotLimits},
    model::Block,
    solana_api::{self, SolanaApi},
};

use super::db::DbOperation;

pub enum DBFullHealerOperation {}

pub fn spawn_db_full_healer(
    recovered_blocks: SparseBlockDb,
    step: u64,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
    cancel_rx: Receiver<DBFullHealerOperation>,
) -> JoinHandle<eyre::Result<()>> {
    std::thread::Builder::new()
        .name("db_full_healer".to_owned())
        .spawn(move || {
            match db_full_healer_actor(
                recovered_blocks,
                step,
                api,
                block_handler_tx,
                db_tx,
                cancel_rx,
            ) {
                Ok(x) => Ok(x),
                Err(e) => {
                    eprintln!("db full healer actor failed: {e}");
                    Err(e)
                }
            }
        })
        .expect("failed to spawn db full healer  actor thread")
}

fn db_full_healer_actor(
    recovered_blocks: SparseBlockDb,
    step: u64,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
    cancel_rx: Receiver<DBFullHealerOperation>,
) -> eyre::Result<()> {
    // salvage blocks, filling in gaps
    let (start, middle, end) = recovered_blocks
        .limits()
        .expect("no blocks were recovered. nothing to save");

    let newdb_limits = read_limits(&db_tx)?;

    assert_eq!(
        newdb_limits.middle_slot, middle,
        "middle slot mismatch between new db and recovered blocks median"
    );

    let cancel_rx = Mutex::new(cancel_rx);
    std::thread::scope(|scope| {
        let lhandle = std::thread::Builder::new()
            .name("db-heal-l".to_owned())
            .spawn_scoped(scope, {
                let block_handler_tx = block_handler_tx.clone();
                let db_tx = db_tx.clone();

                || match block_db_full_healer_actor(
                    &recovered_blocks,
                    (start..=middle).rev(),
                    &cancel_rx,
                    &api,
                    block_handler_tx,
                    db_tx,
                ) {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        eprintln!("left block healer actor failed: {e}");
                        Err(e)
                    }
                }
            })
            .expect("failed to spawn left heal thread");

        let rhandle = std::thread::Builder::new()
            .name("db-heal-r".to_owned())
            .spawn_scoped(scope, {
                let block_handler_tx = block_handler_tx.clone();
                let db_tx = db_tx.clone();

                || match block_db_full_healer_actor(
                    &recovered_blocks,
                    (middle + step)..=end,
                    &cancel_rx,
                    &api,
                    block_handler_tx,
                    db_tx,
                ) {
                    Ok(x) => Ok(x),
                    Err(e) => {
                        eprintln!("right block healer actor failed: {e}");
                        Err(e)
                    }
                }
            })
            .expect("failed to spawn right heal thread");

        lhandle.join().expect("left db healer thread panicked")?;
        rhandle.join().expect("right db healer thread panicked")?;
        Ok::<(), eyre::Report>(())
    })?;

    Ok(())
}

fn block_db_full_healer_actor(
    recovered_blocks: &SparseBlockDb,
    slots: impl Iterator<Item = u64>,
    cancel_rx: &Mutex<Receiver<DBFullHealerOperation>>,
    api: &SolanaApi,
    block_handler_tx: SyncSender<Block>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let healed_limits = read_limits(&db_tx)?;
    let already_healed = |slot: u64| -> bool {
        healed_limits
            .left_slot
            .map(|ls| ls <= slot && slot <= healed_limits.middle_slot)
            .unwrap_or(false)
            || healed_limits
                .right_slot
                .map(|rs| healed_limits.middle_slot < slot && slot <= rs)
                .unwrap_or(false)
    };

    let slots = slots.skip_while(|&s| already_healed(s));

    for slot_chunk in slots.chunks(db::DB_PARAMS.block_rec_cs).into_iter() {
        'filler_loop: for slot in slot_chunk {
            if let Err(TryRecvError::Disconnected) = cancel_rx.lock().unwrap().try_recv() {
                return Err(eyre!("Heal cancelled"));
            }

            let block = match recovered_blocks.get(slot) {
                Some(block) => block,
                None => {
                    eprintln!("refetching {slot}");
                    loop {
                        if let Err(TryRecvError::Disconnected) =
                            cancel_rx.lock().unwrap().try_recv()
                        {
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

fn read_limits(db_tx: &SyncSender<DbOperation>) -> eyre::Result<DbSlotLimits> {
    let (tx, rx) = sync_channel(1);
    db_tx
        .send(DbOperation::ReadLimits { reply: tx })
        .map_err(|_| eyre!("db closed"))?;
    rx.recv().map_err(|_| eyre!("db closed"))
}
