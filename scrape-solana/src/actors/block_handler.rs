use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, SyncSender},
        Arc,
    },
    time::{Duration, Instant},
};

use eyre::WrapErr;
use solana_transaction_status::UiConfirmedBlock;

use crate::{
    model::{Account, AccountID, Block},
    solana_api::{self, SolanaApi},
};

use super::db::DbOperation;

type BlockHandlerSpawnReturn = (
    SyncSender<(u64, UiConfirmedBlock)>,
    SyncSender<Block>,
    std::thread::JoinHandle<eyre::Result<()>>,
    std::thread::JoinHandle<eyre::Result<()>>,
);

pub fn spawn_block_handler(
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> BlockHandlerSpawnReturn {
    let (tx, rx) = std::sync::mpsc::sync_channel(128);
    let handler_handle = std::thread::Builder::new()
        .name("block handler".to_owned())
        .spawn(move || block_handler_actor(rx, api, db_tx))
        .expect("failed to spawn block handler thread");

    let (sdk_tx, sdk_rx) = std::sync::mpsc::sync_channel(128);
    let converter_tx = tx.clone();
    let converter_handle = std::thread::Builder::new()
        .name("block converter".to_owned())
        .spawn(move || block_converter_actor(sdk_rx, converter_tx))
        .expect("failed to spawn block converter thread");

    (sdk_tx, tx, handler_handle, converter_handle)
}

fn block_converter_actor(
    rx: Receiver<(u64, UiConfirmedBlock)>,
    tx: SyncSender<Block>,
) -> eyre::Result<()> {
    println!("block converter ready");
    while let Ok((slot, block)) = rx.recv() {
        let block = Block::from_solana_sdk(slot, block)?;

        if tx.send(block).is_err() {
            println!("block converter: handler stopped. terminating");
            break;
        }
    }

    Ok(())
}

fn block_handler_actor(
    rx: Receiver<Block>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let (account_fetcher_tx, account_fetcher_rx) = std::sync::mpsc::sync_channel(4096);

    let should_stop = Arc::new(AtomicBool::new(false));
    let account_fetcher_handle = {
        let should_stop = Arc::clone(&should_stop);
        let db_tx = db_tx.clone();
        std::thread::Builder::new()
            .name("account fetcher".to_owned())
            .spawn(move || account_fetcher_actor(account_fetcher_rx, api, db_tx, should_stop))
            .wrap_err("failed to spawn account fetcher thread")?
    };

    let mut last_sync = Instant::now();

    println!("block handler ready");
    while let Ok(block) = rx.recv() {
        let account_ids = block
            .txs
            .iter()
            .flat_map(|tx| {
                tx.payload.instrs.iter().map(move |instr| {
                    tx.payload.account_table[instr.program_account_idx as usize].clone()
                })
            })
            .collect();

        if db_tx.send(DbOperation::StoreBlock(block)).is_err() {
            println!("block handler: db closed. terminating");
            break;
        }

        if account_fetcher_tx.send(account_ids).is_err() {
            println!("block handler: db closed. terminating");
            break;
        }

        if last_sync.elapsed() > Duration::from_secs(60) {
            if db_tx.send(DbOperation::Sync).is_err() {
                println!("block handler: db closed. terminating");
                break;
            }
            last_sync = Instant::now();
        }
    }

    // allow account fetcher to exit
    should_stop.store(true, Ordering::Relaxed);
    std::mem::drop(account_fetcher_tx);

    // sync db before exit to avoid losing data (when exiting faster than other components)
    if db_tx.send(DbOperation::Sync).is_err() {
        eprintln!("WARN: block handler: db closed before final sync");
    }

    // wait for account fetcher to exit passing its status to the main thread
    account_fetcher_handle
        .join()
        .expect("account fetcher panicked")?;

    Ok(())
}

fn account_fetcher_actor(
    rx: Receiver<BTreeSet<AccountID>>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
    should_stop: Arc<AtomicBool>,
) -> eyre::Result<()> {
    println!("block handler[account fetcher] ready");

    let mut last_block_height = match get_block_height(&api, &should_stop) {
        Some(res) => res?,
        None => return Ok(()), // stop signal
    };

    let (filter_tx, filter_rx) = std::sync::mpsc::sync_channel(1);

    loop {
        // collect all pending account ids
        let Ok(mut account_ids) = rx.recv() else {
            break;
        };

        // sleep a bit to allow account ids to accumulate
        std::thread::sleep(Duration::from_secs(1));

        while let Ok(mut more_account_ids) = rx.try_recv() {
            account_ids.append(&mut more_account_ids);
        }

        // filter out duplicates
        if db_tx
            .send(DbOperation::FilterNewAccountSet {
                account_ids,
                reply: filter_tx.clone(),
            })
            .is_err()
        {
            println!("account fetcher: db closed. terminating");
            break;
        }
        let Ok(account_ids) = filter_rx.recv() else {
            println!("account fetcher: db panicked. terminating");
            break;
        };

        if account_ids.is_empty() {
            continue; // wait for more
        }

        // keep min height
        let mut min_height = last_block_height;

        // fetch accounts
        let all_account_ids = account_ids.iter().cloned().collect::<Vec<_>>();
        for account_ids in all_account_ids.chunks(100) {
            let raw_accounts = match fetch_raw_accounts(&api, account_ids, &should_stop) {
                Some(res) => res?,
                None => break, // stop signal
            };

            // fetch max height
            let max_height = match get_block_height(&api, &should_stop) {
                Some(res) => res?,
                None => break, // stop signal
            };
            last_block_height = max_height;

            let accounts =
                parse_accounts(account_ids.to_vec(), raw_accounts, min_height, max_height);

            // store accounts
            if db_tx.send(DbOperation::StoreNewAccounts(accounts)).is_err() {
                println!("block handler[account fetcher]: db closed. terminating");
                break;
            }

            min_height = max_height;
        }
    }

    // sync db before exit to avoid losing data (when exiting faster than other components)
    if db_tx.send(DbOperation::Sync).is_err() {
        eprintln!("WARN: block handler[account fetcher]: db closed before final sync");
    }

    Ok(())
}

fn fetch_raw_accounts(
    api: &SolanaApi,
    account_ids: &[AccountID],
    should_stop: &AtomicBool,
) -> Option<eyre::Result<Vec<Option<solana_sdk::account::Account>>>> {
    let account_ids = account_ids
        .iter()
        .map(|id| solana_sdk::pubkey::Pubkey::new_from_array(id.to_owned().into()))
        .collect::<Vec<_>>();

    loop {
        match api.fetch_accounts(&account_ids) {
            Ok(accounts) => break Some(Ok(accounts)),
            Err(solana_api::Error::Timeout(e)) => {
                eprintln!("timeout fetching accounts: {e}");
                if should_stop.load(Ordering::Relaxed) {
                    break None;
                }
            }
            Err(solana_api::Error::PostTimeoutCooldown) => {
                if should_stop.load(Ordering::Relaxed) {
                    break None;
                }
            }
            Err(e) => break Some(Err(e.into())),
        }
    }
}

fn get_block_height(api: &SolanaApi, should_stop: &AtomicBool) -> Option<eyre::Result<u64>> {
    loop {
        match api.get_block_height() {
            Ok(height) => break Some(Ok(height)),
            Err(solana_api::Error::Timeout(e)) => {
                eprintln!("timeout fetching block height: {e}");
                if should_stop.load(Ordering::Relaxed) {
                    return None;
                }
            }
            Err(solana_api::Error::PostTimeoutCooldown) => {
                if should_stop.load(Ordering::Relaxed) {
                    return None;
                }
            }
            Err(e) => break Some(Err(e.into())),
        }
    }
}

fn parse_accounts(
    account_ids_vec: Vec<AccountID>,
    raw_accounts: Vec<Option<solana_sdk::account::Account>>,
    min_height: u64,
    max_height: u64,
) -> Vec<Account> {
    account_ids_vec
        .into_iter()
        .zip(raw_accounts)
        .inspect(|(id, maybe_account)| {
            if maybe_account.is_none() {
                eprintln!("missing account: {id}");
            }
        })
        .filter_map(|(id, maybe_account)| maybe_account.map(|account| (id, account)))
        .map(|(id, account)| Account {
            id,
            owner: account.owner.into(),
            data: account.data,
            is_executable: account.executable,
            min_height,
            max_height,
        })
        .collect()
}
