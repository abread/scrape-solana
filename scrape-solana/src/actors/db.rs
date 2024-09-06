use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::mpsc::{sync_channel, Receiver, SyncSender},
    thread::JoinHandle,
};

use crate::{
    db::{self, Db, DbSlotLimits},
    model::{Account, AccountID, Block},
};

pub enum DbOperation {
    StoreBlock(Block),
    FilterNewAccountSet {
        account_ids: BTreeSet<AccountID>,
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
    let (tx, rx) = sync_channel(4096);
    let handle = std::thread::Builder::new()
        .name("db".to_owned())
        .spawn(move || {
            let db = db::open_or_create(root_path, default_middle_slot_getter, std::io::stdout())?;
            actor(db, rx)
        })
        .expect("failed to spawn db thread");
    (tx, handle)
}

fn actor(mut db: Db, rx: Receiver<DbOperation>) -> eyre::Result<()> {
    use DbOperation::*;

    println!("db ready");
    for op in rx {
        match op {
            StoreBlock(block) => {
                let slot = block.slot;
                db.push_block(block)?;
                println!("saved block at slot {slot}");
            }
            FilterNewAccountSet {
                mut account_ids,
                reply,
            } => {
                account_ids.retain(|id| !db.has_account(id));
                if reply.send(account_ids).is_err() {
                    println!("db: failed to send FilterNewAccountSet reply");
                    break;
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
                    println!("db: failed to send ReadLimits reply");
                    break;
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
