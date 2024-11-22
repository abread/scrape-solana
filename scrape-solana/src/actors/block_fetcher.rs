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
    forward_fetch_chance: Option<f64>,
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
            match block_fetcher_actor(forward_fetch_chance, step, rx, api, block_handler_tx, db_tx)
            {
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
    forward_fetch_chance: Option<f64>,
    step: u64,
    rx: Receiver<BlockFetcherOperation>,
    api: Arc<SolanaApi>,
    block_handler_tx: SyncSender<(u64, UiConfirmedBlock)>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let limits = match read_limits(&db_tx) {
        Ok(limits) => limits,
        Err(e) => {
            eprintln!("block fetcher: failed to get db limits: {e}, exiting");
            return Ok(());
        }
    };

    let mut left_slot = limits
        .left_slot
        .map(|s| s.saturating_sub(step))
        .unwrap_or(limits.middle_slot);
    let mut right_slot = limits
        .right_slot
        .map(|s| s + step)
        .unwrap_or(limits.middle_slot + step);

    let mut left_attempts = 0;
    let mut right_attempts = 0;

    // will be updated to keep data 1h~1day behind network tip
    let mut forward_chance = forward_fetch_chance.unwrap_or(0.5);
    let mut last_right_block_ts = limits
        .right_ts
        .and_then(|ts| chrono::DateTime::from_timestamp(ts, 0))
        .unwrap_or(chrono::Utc::now());

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

        // register fetch attempt
        match side {
            Side::Left => left_attempts += 1,
            Side::Right => right_attempts += 1,
        }

        // if we get stuck, try waiting a long long time, then bail if still stuck
        {
            let attempts = match side {
                Side::Left => left_attempts,
                Side::Right => right_attempts,
            };

            if attempts > 10 && attempts < 10 + 60 * 60 / 30 {
                std::thread::sleep(Duration::from_secs(30));
                continue;
            } else if attempts > 10 + 60 * 60 / 30 + 3 {
                return Err(eyre!(
                    "block fetcher: stuck at slot {slot} ({side} side), bailing"
                ));
            }
        }

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

                // don't count it as a fetch attempt
                match side {
                    Side::Left => left_attempts -= 1,
                    Side::Right => right_attempts -= 1,
                }
                continue;
            }
            Err(solana_api::Error::SolanaClient(e)) => {
                return Err(e).wrap_err("failed to fetch block");
            }
        }

        // update slot
        match side {
            Side::Left => left_slot = left_slot.saturating_sub(step),
            Side::Right => right_slot = right_slot.saturating_add(step),
        }

        // zero attempt counter
        match side {
            Side::Left => left_attempts = 0,
            Side::Right => right_attempts = 0,
        }

        forward_chance = forward_fetch_chance.unwrap_or({
            let now = chrono::Utc::now();
            if last_right_block_ts > now - FETCH_BACK_THRESH {
                0.0
            } else if last_right_block_ts > now - FETCH_FORWARD_THRESH {
                0.5
            } else {
                1.0
            }
        });
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
