use std::{
    collections::BTreeSet,
    sync::{
        mpsc::{Receiver, SyncSender},
        Arc,
    },
    time::{Duration, Instant},
};

use eyre::eyre;

use crate::{
    model::{AccountID, Block},
    solana_api::SolanaApi,
};

use super::db::DbOperation;

pub fn spawn_block_handler(
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> (SyncSender<Block>, std::thread::JoinHandle<eyre::Result<()>>) {
    let (tx, rx) = std::sync::mpsc::sync_channel(128);
    let handle = std::thread::spawn(move || block_handler_worker(rx, api, db_tx));
    (tx, handle)
}

fn block_handler_worker(
    rx: Receiver<Block>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    let (new_acc_tx, new_acc_rx) = std::sync::mpsc::sync_channel(256);
    let filtered_handler_handle = {
        let db_tx = db_tx.clone();
        std::thread::spawn(move || filtered_account_ids_handler_worker(new_acc_rx, api, db_tx))
    };

    let mut last_sync = Instant::now();

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

        db_tx
            .send(DbOperation::StoreBlock(block))
            .map_err(|_| eyre!("db closed"))?;

        db_tx
            .send(DbOperation::FilterNewAccountSet {
                set: account_ids,
                reply: new_acc_tx.clone(),
            })
            .map_err(|_| eyre::eyre!("db channel closed"))?;

        if last_sync.elapsed() > Duration::from_secs(60) {
            db_tx
                .send(DbOperation::Sync)
                .map_err(|_| eyre!("db closed"))?;
            last_sync = Instant::now();
        }
    }

    filtered_handler_handle
        .join()
        .expect("filtered new account handler panicked")?;

    Ok(())
}

fn filtered_account_ids_handler_worker(
    rx: Receiver<BTreeSet<AccountID>>,
    api: Arc<SolanaApi>,
    db_tx: SyncSender<DbOperation>,
) -> eyre::Result<()> {
    loop {
        // collect all pending account ids
        let Ok(mut account_ids) = rx.recv() else {
            break;
        };
        while let Ok(mut more_account_ids) = rx.try_recv() {
            account_ids.append(&mut more_account_ids);
        }

        let accounts = api.fetch_accounts(account_ids.clone())?;
        db_tx
            .send(DbOperation::StoreNewAccounts(accounts))
            .map_err(|_| eyre!("db closed"))?;
    }

    Ok(())
}
