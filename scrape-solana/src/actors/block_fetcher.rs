use std::{
    fmt::Display,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use eyre::{eyre, WrapErr};
use solana_transaction_status::UiConfirmedBlock;

use crate::{
    db::DbSlotLimits,
    solana_api::{self, SolanaApi},
};

use super::db::DbOperation;

pub enum BlockFetcherOperation {
    Stop,
}

pub fn spawn_block_fetcher(
    fallback_forward_chance: f64,
    step: u64,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<(u64, UiConfirmedBlock)>,
    db_tx: SyncSender<DbOperation>,
) -> (
    SyncSender<BlockFetcherOperation>,
    JoinHandle<eyre::Result<()>>,
) {
    let (tx, rx) = sync_channel(1);
    let handle = std::thread::Builder::new()
        .name("block_fetcher".to_owned())
        .spawn(move || {
            match block_fetcher_actor(
                fallback_forward_chance,
                step,
                rx,
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
        .expect("failed to spawn block fetcher actor thread");
    (tx, handle)
}

#[derive(PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

impl Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Side::Left => write!(f, "left"),
            Side::Right => write!(f, "right"),
        }
    }
}

const FETCH_BACK_THRESH: Duration = Duration::from_secs(60 * 60); // 1h
const FETCH_FORWARD_THRESH: Duration = Duration::from_secs(60 * 60 * 24); // 1d

fn block_fetcher_actor(
    fallback_forward_chance: f64,
    step: u64,
    rx: Receiver<BlockFetcherOperation>,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<(u64, UiConfirmedBlock)>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let limits = read_limits(&db_tx)?;

    let mut left_slot = limits
        .left_slot
        .map(|s| s.saturating_sub(step))
        .unwrap_or(limits.middle_slot);
    let mut right_slot = limits
        .right_slot
        .map(|s| s + step)
        .unwrap_or(limits.middle_slot + step);

    // will be updated to keep data 1h~1day behind network tip
    let mut forward_chance = fallback_forward_chance;
    let mut last_right_block_ts = chrono::Utc::now(); // assume latest block is from now

    loop {
        if let Ok(BlockFetcherOperation::Stop) = rx.try_recv() {
            break;
        }

        let side = if rand::random::<f64>() < forward_chance {
            Side::Right
        } else {
            Side::Left
        };

        let slot = match side {
            Side::Left => left_slot,
            Side::Right => right_slot,
        };

        match api.fetch_block(slot) {
            Ok(Some(block)) => {
                if side == Side::Right {
                    // grab reference block time
                    last_right_block_ts = block
                        .block_time
                        .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
                        .unwrap_or(last_right_block_ts);
                }

                if block_handler_tx.send((slot, block)).is_err() {
                    println!("block fetcher: block handler closed. terminating");
                    break;
                }

                println!("fetched block at slot {slot} ({side} side)");
            }
            Ok(None) => {}
            Err(solana_api::Error::Timeout(e)) => {
                eprintln!("timeout fetching block at slot {slot} ({side} side): {e}");
                continue;
            }
            Err(solana_api::Error::PostTimeoutCooldown) => {
                // not really a timeout, just continuing the previous timeout
                // but it's so large that we might as well sync the db while we wait
                db_tx
                    .send(DbOperation::Sync)
                    .map_err(|_| eyre!("db closed"))?;
                continue;
            }
            Err(solana_api::Error::SolanaClient(e)) => {
                return Err(e).wrap_err("failed to fetch block");
            }
        }

        // update slot
        match side {
            Side::Left => right_slot = right_slot.saturating_add(step),
            Side::Right => left_slot = left_slot.saturating_sub(step),
        }

        forward_chance = {
            let now = chrono::Utc::now();
            if last_right_block_ts > now - FETCH_BACK_THRESH {
                0.0
            } else if last_right_block_ts > now - FETCH_FORWARD_THRESH {
                0.5
            } else {
                1.0
            }
        };
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
