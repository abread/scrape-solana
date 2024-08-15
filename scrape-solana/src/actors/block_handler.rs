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

pub fn spawn_block_handler(
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> (
    SyncSender<(u64, UiConfirmedBlock)>,
    std::thread::JoinHandle<eyre::Result<()>>,
) {
    let (tx, rx) = std::sync::mpsc::sync_channel(128);
    let handle = std::thread::Builder::new()
        .name("block handler".to_owned())
        .spawn(move || block_handler_actor(rx, api, db_tx))
        .expect("failed to spawn block handler thread");
    (tx, handle)
}

fn block_handler_actor(
    rx: Receiver<(u64, UiConfirmedBlock)>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let (new_acc_tx, new_acc_rx) = std::sync::mpsc::sync_channel(256);

    let should_stop = Arc::new(AtomicBool::new(false));
    let filtered_handler_handle = {
        let should_stop = Arc::clone(&should_stop);
        let db_tx = db_tx.clone();
        std::thread::Builder::new()
            .name("filtered account_ids handler".to_owned())
            .spawn(move || filtered_account_ids_handler_actor(new_acc_rx, api, db_tx, should_stop))
            .wrap_err("failed to spawn filtered account ids handler thread")?
    };

    let mut last_sync = Instant::now();

    println!("block handler ready");
    while let Ok((slot, block)) = rx.recv() {
        let block = Block::from_solana_sdk(slot, block)?;

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

        if db_tx
            .send(DbOperation::FilterNewAccountSet {
                account_ids,
                reply: new_acc_tx.clone(),
            })
            .is_err()
        {
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

    // allow filtered account_ids handler to exit
    should_stop.store(true, Ordering::Relaxed);
    std::mem::drop(new_acc_tx);

    filtered_handler_handle
        .join()
        .expect("filtered new account handler panicked")?;

    Ok(())
}

fn filtered_account_ids_handler_actor(
    rx: Receiver<BTreeSet<AccountID>>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
    should_stop: Arc<AtomicBool>,
) -> eyre::Result<()> {
    println!("block handler[filtered account_ids handler] ready");

    // between filtering and storing some account ids can get duplicated
    // so we need a last dedupe step
    let mut last_account_ids = BTreeSet::new();

    let mut last_block_height = match get_block_height(&api, &should_stop) {
        Some(res) => res?,
        None => return Ok(()), // stop signal
    };

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
        account_ids.retain(|el| !last_account_ids.contains(el));
        if account_ids.is_empty() {
            continue; // wait for more
        }

        // keep min height
        let min_height = last_block_height;

        // fetch accounts
        let account_ids_vec = account_ids.iter().cloned().collect::<Vec<_>>();
        let raw_accounts = match fetch_raw_accounts(&api, &account_ids_vec, &should_stop) {
            Some(res) => res?,
            None => break, // stop signal
        };

        // fetch max height
        let max_height = match get_block_height(&api, &should_stop) {
            Some(res) => res?,
            None => break, // stop signal
        };
        last_block_height = max_height;

        // parse accounts
        let accounts = account_ids_vec
            .into_iter()
            .zip(raw_accounts)
            .inspect(|(id, maybe_account)| {
                if maybe_account.is_none() {
                    eprintln!("missing account: {id}");
                }
            })
            .filter_map(|(id, maybe_account)| maybe_account.map(|account| (id, account)))
            .map(|(id, account)| Account {
                id: id.into(),
                owner: account.owner.into(),
                data: account.data,
                is_executable: account.executable,
                min_height,
                max_height,
            })
            .collect();

        // store accounts
        if db_tx.send(DbOperation::StoreNewAccounts(accounts)).is_err() {
            println!("block handler[filtered account_ids handler]: db closed. terminating");
            break;
        }

        last_account_ids = account_ids;
    }

    Ok(())
}

fn fetch_raw_accounts(
    api: &SolanaApi,
    account_ids: &[AccountID],
    should_stop: &AtomicBool,
) -> Option<eyre::Result<Vec<Option<solana_sdk::account::Account>>>> {
    let account_ids = account_ids
        .into_iter()
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
