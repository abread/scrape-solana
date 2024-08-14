use std::{
    collections::BTreeSet,
    sync::{
        mpsc::{Receiver, SyncSender},
        Arc,
    },
    time::{Duration, Instant},
};

use eyre::WrapErr;
use solana_transaction_status::UiConfirmedBlock;

use crate::{
    model::{AccountID, Block},
    solana_api::SolanaApi,
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
    let filtered_handler_handle = {
        let db_tx = db_tx.clone();
        std::thread::Builder::new()
            .name("filtered account_ids handler".to_owned())
            .spawn(move || filtered_account_ids_handler_actor(new_acc_rx, api, db_tx))
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

    std::mem::drop(new_acc_tx); // allow filtered account_ids handler to exit
    filtered_handler_handle
        .join()
        .expect("filtered new account handler panicked")?;

    Ok(())
}

fn filtered_account_ids_handler_actor(
    rx: Receiver<BTreeSet<AccountID>>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    println!("block handler[filtered account_ids handler] ready");

    // between filtering and storing some account ids can get duplicated
    // so we need a last dedupe step
    let mut last_account_ids = BTreeSet::new();

    loop {
        // collect all pending account ids
        let Ok(mut account_ids) = rx.recv() else {
            break;
        };
        std::thread::sleep(Duration::from_secs(1)); // sleep a bit to allow account ids to
                                                    // accumulate
        while let Ok(mut more_account_ids) = rx.try_recv() {
            account_ids.append(&mut more_account_ids);
        }
        account_ids.retain(|el| !last_account_ids.contains(el));
        if account_ids.is_empty() {
            continue; // wait for more
        }

        let accounts = api.fetch_accounts(account_ids.clone())?;
        if db_tx.send(DbOperation::StoreNewAccounts(accounts)).is_err() {
            println!("block handler[filtered account_ids handler]: db closed. terminating");
            break;
        }

        last_account_ids = account_ids;
    }

    Ok(())
}
