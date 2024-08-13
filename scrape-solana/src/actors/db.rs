use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::JoinHandle,
};

use crate::{
    db::{Db, DbSlotLimits},
    model::{Account, AccountID, Block},
};

pub enum DbOperation {
    StoreBlock(Block),
    FilterNewAccountSet {
        set: BTreeSet<AccountID>,
        reply: SyncSender<BTreeSet<AccountID>>,
    },
    StoreNewAccounts(Vec<Account>),
    ReadLimits {
        reply: SyncSender<DbSlotLimits>,
    },
    Sync,
}

pub fn spawn_db_actor(
    root_path: PathBuf,
    default_middle_slot_getter: impl FnOnce() -> u64 + Send + 'static,
) -> (SyncSender<DbOperation>, JoinHandle<eyre::Result<()>>) {
    let (tx, rx) = sync_channel(256);
    let handle = std::thread::spawn(move || {
        let db = Db::open(root_path, default_middle_slot_getter, std::io::stdout())?;
        actor(db, rx)
    });
    (tx, handle)
}

fn actor(mut db: Db, rx: Receiver<DbOperation>) -> eyre::Result<()> {
    use DbOperation::*;

    for op in rx {
        match op {
            StoreBlock(block) => {
                let slot = block.slot;
                db.push_block(block)?;
                println!("saved block at slot {slot}");
            }
            FilterNewAccountSet {
                set: mut account_ids,
                reply,
            } => {
                account_ids.retain(|id| !db.has_account(id));
                if reply.send(account_ids).is_err() {
                    let _ = db.sync();
                    return Err(eyre::eyre!("failed to send FilterNewAccountSet reply"));
                }
            }
            StoreNewAccounts(new_accounts) => {
                let new_accounts_count = new_accounts.len();
                for account in new_accounts {
                    db.store_new_account(account)?;
                }
                println!("stored {new_accounts_count} new accounts");
            }
            ReadLimits { reply } => {
                let limits = db.slot_limits()?;
                if reply.send(limits).is_err() {
                    let _ = db.sync();
                    return Err(eyre::eyre!("failed to send ReadLimits reply"));
                }
            }
            Sync => {
                db.sync()?;
                println!("db synced");
            }
        }
    }

    db.sync()?;
    Ok(())
}
