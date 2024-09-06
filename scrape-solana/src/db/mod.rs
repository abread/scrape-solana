use crate::model::{AccountID, Block};
use crate::{
    crc_checksum_serde::checksum,
    huge_vec::{FsStore, ZstdTransformer},
    model::Account,
    select_random_elements,
};
use eyre::{eyre, WrapErr};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
};

mod model;
use model::AccountRecord;

//mod huge_map;
//use huge_map::MapFsStore;

mod monotonous_block_db;
pub use monotonous_block_db::MonotonousBlockDb;

use self::monotonous_block_db::BlockIter;

pub(crate) type HugeVec<T, const CHUNK_SZ: usize> = crate::huge_vec::HugeVec<
    T,
    FsStore<crate::huge_vec::Chunk<T, CHUNK_SZ>, ZstdTransformer>,
    CHUNK_SZ,
>;
/*pub(crate) type HugeMap<K, V, const CHUNK_SZ: usize> =
huge_map::HugeMap<K, V, MapFsStore<ZstdTransformer>, CHUNK_SZ>;*/

#[derive(Clone, Copy)]
// copy is a hack here
pub struct DbParams {
    pub block_rec_cs: usize,
    pub tx_cs: usize,
    pub account_rec_cs: usize,
    pub account_data_cs: usize,
}
const DB_PARAMS_BY_VERSION: [DbParams; 3] = [
    DbParams {
        block_rec_cs: 699,
        tx_cs: 2048,
        account_rec_cs: 1290,
        account_data_cs: 268435,
    },
    DbParams {
        block_rec_cs: 699,
        tx_cs: 2048,
        account_rec_cs: 1290,
        account_data_cs: 268435,
    },
    DbParams {
        block_rec_cs: 2048,
        tx_cs: 16384,
        account_rec_cs: 2048,
        account_data_cs: 8 * 1024 * 1024, // 8MB
    },
];

macro_rules! db_version {
    ($name:ident, $v:literal) => {
        pub type $name = DbGeneric<
            $v,
            { DB_PARAMS_BY_VERSION[$v].block_rec_cs },
            { DB_PARAMS_BY_VERSION[$v].tx_cs },
            { DB_PARAMS_BY_VERSION[$v].account_rec_cs },
            { DB_PARAMS_BY_VERSION[$v].account_data_cs },
        >;
    };
}
db_version!(DbV1, 1);
db_version!(DbV2, 2);

pub const LATEST_VERSION: u64 = DB_PARAMS_BY_VERSION.len() as u64 - 1;
pub const DB_PARAMS: DbParams = DB_PARAMS_BY_VERSION[LATEST_VERSION as usize];
pub type Db = DbV2;

pub fn open(root_path: PathBuf, out: impl io::Write) -> eyre::Result<Db> {
    let version: u64 = match std::fs::read_to_string(root_path.join("version")) {
        Ok(v) => Ok(v),
        Err(e) => Err(e),
    }?
    .parse()?;

    open_existing(root_path, version, out)
}

pub fn open_or_create(
    root_path: PathBuf,
    default_middle_slot_getter: impl FnOnce() -> u64,
    out: impl io::Write,
) -> eyre::Result<Db> {
    let version: u64 = match std::fs::read_to_string(root_path.join("version")) {
        Ok(v) => Ok(v),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Db::create(root_path, default_middle_slot_getter(), out)
        }
        Err(e) => Err(e),
    }?
    .parse()?;

    open_existing(root_path, version, out)
}

fn open_existing(root_path: PathBuf, version: u64, mut out: impl io::Write) -> eyre::Result<Db> {
    let mut db = match version {
        1 => {
            let old_db = DbV1::open_existing(root_path.clone())?;
            upgrade_db(root_path, version, old_db, &mut out)
        }
        2 => Db::open_existing(root_path),
        _ => Err(eyre!("unsupported version: {version}")),
    }?;

    let _ = writeln!(out, "Auto-healing DB...");
    let (issues, res) = db.heal(128);
    for issue in issues {
        let _ = writeln!(out, " > {issue}");
    }
    db.sync()?;
    res.wrap_err("Failed to auto-heal DB")?;
    let _ = writeln!(out, "DB healed");

    let _ = writeln!(
        out,
        "loaded {}+{} blocks, {}+{} txs, {} accounts and {}B of account data",
        db.left.block_records.len() - 1,
        db.right.block_records.len() - 1,
        db.left.txs.len(),
        db.right.txs.len(),
        db.account_records.len() - 1,
        db.account_data.len()
    );

    Ok(db)
}

fn upgrade_db<
    const OLD_VERSION: u64,
    const OLD_BCS: usize,
    const OLD_TXCS: usize,
    const OLD_ARCS: usize,
    const OLD_ADCS: usize,
>(
    root_path: PathBuf,
    old_version: u64,
    mut old_db: DbGeneric<OLD_VERSION, OLD_BCS, OLD_TXCS, OLD_ARCS, OLD_ADCS>,
    mut out: impl io::Write,
) -> eyre::Result<Db> {
    let _ = writeln!(out, "upgrading db from version {old_version}");
    assert!(
        OLD_VERSION != LATEST_VERSION,
        "LOGIC BUG: db is already at latest version"
    );

    let _ = writeln!(out, "running full heal on old db");
    let (issues, res) = old_db.heal(u64::MAX);
    for issue in issues {
        let _ = writeln!(out, " > {issue}");
    }
    old_db.sync()?;
    res?;
    let _ = writeln!(out, "full heal on old db complete");

    let old_db = old_db; // make old db immutable

    let _ = writeln!(
        out,
        "loaded {}+{} blocks, {}+{} txs, {} accounts and {}B of account data",
        old_db.left.block_records.len() - 1,
        old_db.right.block_records.len() - 1,
        old_db.left.txs.len(),
        old_db.right.txs.len(),
        old_db.account_records.len() - 1,
        old_db.account_data.len()
    );

    let _ = writeln!(out, "creating new db in temporary directory");
    let new_path = tempfile::tempdir_in(root_path.parent().unwrap_or(Path::new("..")))?;
    {
        let mut new_db = Db::create(new_path.path().to_owned(), old_db.middle_slot, &mut out)?;
        new_db.sync()?;
    }

    let _ = writeln!(out, "copying data from old db");
    let (b_tx, b_rx) = std::sync::mpsc::sync_channel::<eyre::Result<Block>>(1024);
    let (a_tx, a_rx) = std::sync::mpsc::sync_channel::<eyre::Result<Account>>(1024);

    let putter_thread_handle = {
        let new_path = new_path.path().to_owned();
        std::thread::spawn(move || {
            let mut new_db = Db::open_existing(new_path)?;

            for block in b_rx {
                let block = block.wrap_err("upgrade failed: failed to fetch block from old db")?;
                new_db
                    .push_block(block)
                    .wrap_err("upgrade failed: failed to push block to new db")?;
            }

            for account in a_rx {
                let account =
                    account.wrap_err("upgrade failed: failed to fetch account from old db")?;
                new_db
                    .store_new_account(account)
                    .wrap_err("upgrade failed: failed to store account in new db")?;
            }

            new_db.sync()?;
            Ok::<_, eyre::Report>(())
        })
    };

    // copy blocks
    // iterate left blocks in reverse to match storage order (from middle slot to 0)
    for block in old_db.left_blocks().rev().chain(old_db.right_blocks()) {
        let _ = b_tx.send(block);
    }
    std::mem::drop(b_tx);

    for account_idx in 0..old_db.account_records.len().saturating_sub(1) {
        if old_db
            .account_records
            .get(account_idx)
            .map(|r| r.is_endcap())
            .unwrap_or(false)
        {
            continue;
        }
        let account = old_db.get_account_by_idx(account_idx);
        let _ = a_tx.send(account);
    }
    std::mem::drop(a_tx);

    putter_thread_handle
        .join()
        .expect("upgrade failed: putter thread panicked")?;

    let new_checksum_handle = {
        let new_path = new_path.path().to_owned();
        std::thread::spawn(move || {
            let new_db = Db::open_existing(new_path)?;
            new_db.checksum()
        })
    };
    let old_checksum = old_db.checksum()?;
    let new_checksum = new_checksum_handle
        .join()
        .expect("checksum thread panicked")?;
    eyre::ensure!(
        old_checksum == new_checksum,
        "checksum mismatch after upgrade: old {old_checksum:#X} != new {new_checksum:#X}"
    );
    let _ = writeln!(out, "checksum ok after migration!");

    let _ = writeln!(
        out,
        "renaming old db to temporary path and new db to final path"
    );
    let old_path = root_path.with_extension(old_version.to_string());
    std::fs::rename(&root_path, &old_path).wrap_err("failed to rename old DB to temporary path")?;
    std::fs::rename(&new_path, &root_path).wrap_err("failed to rename new DB to final path")?;
    std::mem::forget(new_path); // do not remove!

    let _ = writeln!(out, "db moved to final path, removing old db");
    std::fs::remove_dir_all(old_path).wrap_err("failed to remove old DB")?;

    let _ = writeln!(out, "migration complete");
    Db::open_existing(root_path)
}

pub struct DbGeneric<
    const VERSION: u64,
    const BCS: usize,
    const TXCS: usize,
    const ARCS: usize,
    const ADCS: usize,
> {
    middle_slot: u64,

    // blocks up to middle_slot
    left: MonotonousBlockDb<BCS, TXCS>,

    // blocks ahead of middle_slot
    right: MonotonousBlockDb<BCS, TXCS>,

    account_records: HugeVec<AccountRecord, ARCS>,
    account_data: HugeVec<u8, ADCS>,
    /*account_index: HugeMap<
        AccountID,
        u64,
        //{ chunk_sz::<vector_trees::btree::BVecTreeNode<AccountID, u64>>(256 * MB) },
        1, // limitation in current slicing implementation
    >,*/
}

pub struct DbSlotLimits {
    pub middle_slot: u64,
    pub left_slot: Option<u64>,
    pub right_slot: Option<u64>,
}

impl<
        const VERSION: u64,
        const BCS: usize,
        const TXCS: usize,
        const ARCS: usize,
        const ADCS: usize,
    > DbGeneric<VERSION, BCS, TXCS, ARCS, ADCS>
{
    const fn max_auto_account_data_loss() -> u64 {
        let three_blocks = ADCS as u64 * 3;
        let max_account_data = 10 * 1024 * 1024;

        if three_blocks > max_account_data + ADCS as u64 {
            three_blocks
        } else {
            max_account_data + ADCS as u64
        }
    }

    fn create(root_path: PathBuf, middle_slot: u64, mut out: impl io::Write) -> eyre::Result<Self> {
        // create root_path
        std::fs::create_dir_all(&root_path).wrap_err("failed to create root directory")?;

        // write middle_slot
        let mut middle_slot_file = tempfile::NamedTempFile::new_in(&root_path)?;
        write!(middle_slot_file, "{}", middle_slot).wrap_err("failed to write middle_slot file")?;
        middle_slot_file.persist(root_path.join("middle_slot"))?;

        // write version file
        let mut version_file = tempfile::NamedTempFile::new_in(&root_path)?;
        write!(version_file, "{}", VERSION).wrap_err("failed to write version file")?;
        version_file.persist(root_path.join("version"))?;

        // create tables
        let mut db = Self::open_existing(&root_path)?;
        db.left.initialize()?;
        db.right.initialize()?;

        // insert accounts end cap
        if db.account_records.is_empty() {
            db.account_records.push(AccountRecord::endcap(0))?;
        }

        if cfg!(debug_assertions) {
            let (issues, res) = db.heal(u64::MAX);
            assert!(issues.is_empty());
            res.unwrap();
        }
        db.sync()?;

        let _ = writeln!(out, "DB created at {:?}", root_path);
        Ok(db)
    }

    fn open_existing(root_path: impl AsRef<Path>) -> eyre::Result<Self> {
        let root_path = root_path.as_ref();

        // open tables
        macro_rules! open_vec_table {
            ($name:ident) => {
                let store = FsStore::open(
                    root_path.join(stringify!($name)),
                    ZstdTransformer::default(),
                )
                .wrap_err(concat!("Failed to open vec store: ", stringify!($name)))?;
                let $name = HugeVec::new(store)
                    .wrap_err(concat!("Failed to open vec: ", stringify!($name)))?;
            };
        }

        open_vec_table!(block_records_left);
        open_vec_table!(txs_left);
        open_vec_table!(block_records_right);
        open_vec_table!(txs_right);
        open_vec_table!(account_data);
        open_vec_table!(account_records);

        // build account records map from underlying vec
        /*let store = MapFsStore::new(root_path.join("account_index"), ZstdTransformer::default());
        let account_index =
            HugeMap::open(store).wrap_err("Failed to create table: account_index")?;*/

        // create left and right monotonous dbs
        let left = MonotonousBlockDb {
            block_records: block_records_left,
            txs: txs_left,
        };
        let right = MonotonousBlockDb {
            block_records: block_records_right,
            txs: txs_right,
        };

        // read middle slot
        let middle_slot = std::fs::read_to_string(root_path.join("middle_slot"))
            .wrap_err("failed to read middle slot")?
            .parse()
            .wrap_err("failed to parse middle slot")?;

        // check version
        let version: u64 = std::fs::read_to_string(root_path.join("version"))
            .wrap_err("failed to read version")?
            .parse()
            .wrap_err("failed to parse version")?;
        eyre::ensure!(
            version == VERSION,
            "version mismatch: got {version} instead of {VERSION}"
        );

        let db = DbGeneric {
            middle_slot,
            left,
            right,
            account_records,
            account_data,
            //account_index,
        };

        Ok(db)
    }

    fn heal(&mut self, n_samples: u64) -> (Vec<String>, eyre::Result<()>) {
        let mut issues = Vec::new();

        let r1 = self.left.heal(n_samples, &mut issues);
        let r2 = self.right.heal(n_samples, &mut issues);
        let r3 = self.heal_accounts(n_samples, &mut issues);
        let r4 = Ok(()); //let r4 = self.heal_account_index(n_samples, &mut issues);

        (issues, self.sync().and(r1).and(r2).and(r3).and(r4))
    }

    /*fn heal_account_index(&mut self, n_samples: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
        if !self.is_account_index_healthy(n_samples, issues) {
            issues.push("rebuilding account index".to_owned());

            self.account_index.clear();
            for (idx, record) in self.account_records.iter().enumerate() {
                if let Some(idx_dup) = self.account_index.insert(record.id.clone(), idx as u64) {
                    issues.push(format!(
                        "found duplicate account record: indices {idx} and {idx_dup} both have account {}",
                        record.id,
                    ));
                }
            }
        }

        Ok(())
    }

    fn is_account_index_healthy(&self, n_samples: u64, issues: &mut Vec<String>) -> bool {
        // HACK: only checks a few elements
        if self.account_index.len() != self.account_records.len().saturating_sub(1) {
            issues.push(format!(
                "account index and record sizes do not match: {} != {} (+ endcap)",
                self.account_index.len(),
                self.account_records.len().saturating_sub(1)
            ));
            return false;
        }

        for (idx, record) in select_random_elements(&self.account_records, n_samples) {
            let id = &record.id;
            if self.account_index.get(id).map(|r| *r) != Some(idx as u64) {
                issues.push(format!(
                    "found account storage inconsistency for id={id:?},idx={idx}"
                ));
                return false;
            }
        }

        true
    }*/

    fn heal_accounts(&mut self, n_samples: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
        if self.account_records.is_empty() {
            issues.push("account records empty, missing endcap: reinserting".to_owned());
            self.account_records.push(AccountRecord::endcap(0))?;
            return Ok(());
        }

        while self.account_records.len() >= 2
            && self
                .account_records
                .get(self.account_records.len() - 2)?
                .is_endcap()
        {
            issues.push("account records have >1 trailing endcap: removing".to_owned());
            self.account_records
                .truncate(self.account_records.len() - 1)?;
        }

        if self
            .account_records
            .last()?
            .map(|b| !b.is_endcap() || b.data_start_idx != self.account_data.len())
            .unwrap()
        {
            let (n_bad_accounts, n_bad_account_data_bytes) = {
                let new_endcap_idx = self
                    .account_records
                    .iter()
                    .enumerate()
                    .rev()
                    .find(|(_, account_rec)| account_rec.data_start_idx <= self.account_data.len());

                if let Some((idx, account_rec)) = new_endcap_idx {
                    let n_bad_account_data_bytes =
                        self.account_data.len() - account_rec.data_start_idx;
                    let n_bad_accounts = self.account_records.len() - (idx as u64 + 1);
                    (n_bad_accounts, n_bad_account_data_bytes)
                } else {
                    let n_bad_account_data_bytes = self.account_data.len();
                    let n_bad_accounts = self.account_records.len();
                    (n_bad_accounts, n_bad_account_data_bytes)
                }
            };

            if n_bad_accounts >= Self::max_auto_account_data_loss()
                || n_bad_account_data_bytes >= Self::max_auto_account_data_loss()
            {
                issues.push(format!("account records bad endcap: would drop {} accounts and {} bytes of data to autofix (out of {} accounts and {} account data bytes). aborting", n_bad_accounts, n_bad_account_data_bytes, self.account_records.len(), self.account_data.len()));
                return Err(eyre!(
                    "account records missing endcap. too many dropped account_data/accounts to autofix"
                ));
            } else {
                issues.push(format!("account records bad endcap: dropped {} accounts and {} bytes of data to autofix (out of {} accounts and {} account data bytes)", n_bad_accounts, n_bad_account_data_bytes, self.account_records.len(), self.account_data.len()));
                self.account_records
                    .truncate(self.account_records.len() - n_bad_accounts)?;
                self.account_data
                    .truncate(self.account_data.len() - n_bad_account_data_bytes)?;
            }
        }

        let endcap_idx = self.account_records.len() - 1;

        if n_samples == u64::MAX {
            for idx in 0..endcap_idx {
                self.check_account(idx, issues)?;
            }
        } else {
            let elements_to_check = select_random_elements(&self.account_records, n_samples)
                .map(|(idx, _)| idx)
                .filter(|&idx| idx as u64 != endcap_idx)
                .collect::<Vec<_>>();

            for idx in elements_to_check {
                self.check_account(idx as u64, issues)?;
            }
        }

        if self.account_records.len() >= 2
            && self
                .check_account(self.account_records.len() - 1, &mut Vec::new())
                .is_err()
        {
            issues.push("last account is corrupted: removing".to_owned());
            let data_next_start_idx = self.account_records.last().unwrap().unwrap().data_start_idx;
            let _ = std::mem::replace(
                &mut *self.account_records.last_mut().unwrap().unwrap(),
                AccountRecord::endcap(data_next_start_idx),
            );
            self.account_records
                .truncate(self.account_records.len() - 1)?;
        }

        Ok(())
    }

    fn check_account(&mut self, idx: u64, issues: &mut Vec<String>) -> eyre::Result<()> {
        match self.get_account_by_idx(idx) {
            Ok(_) => (),
            Err(e) => {
                issues.push(format!("account with idx {} is corrupted: {}", idx, e));
            }
        }

        Ok(())
    }

    fn get_account_by_idx(&self, idx: u64) -> eyre::Result<Account> {
        let record = self
            .account_records
            .get(idx)
            .wrap_err("failed to get account record")?;

        eyre::ensure!(!record.is_endcap(), "cannot get endcap record");

        let account_data_len = self
            .account_records
            .get(idx + 1)
            .wrap_err("failed to get account data size (from next record)")?
            .data_start_idx
            - record.data_start_idx;

        // fetch account data
        let data = {
            let mut data = Vec::with_capacity(account_data_len as usize);

            for b_idx in record.data_start_idx..(record.data_start_idx + account_data_len) {
                let b = *self.account_data.get(b_idx)?;
                data.push(b);
            }

            data
        };

        let account = Account {
            id: record.id.clone(),
            owner: record.owner.clone(),
            min_height: record.min_height,
            max_height: record.max_height,
            is_executable: record.is_executable,
            data,
        };
        eyre::ensure!(checksum(&account) == record.checksum, "checksum mismatch");

        Ok(account)
    }

    pub fn push_block(&mut self, block: Block) -> eyre::Result<()> {
        let side = if block.slot > self.middle_slot {
            &mut self.right
        } else {
            &mut self.left
        };

        side.push(&block)?;

        Ok(())
    }

    pub fn has_account(&self, id: &AccountID) -> bool {
        self.account_records
            .iter()
            .any(|r| r.id == *id && !r.is_endcap())
    }

    pub fn store_new_account(&mut self, account: Account) -> eyre::Result<()> {
        if self.has_account(&account.id) {
            eprintln!("account {} already present in index", account.id);
            return Ok(()); // ignore problem, but register it
        }

        // create account record
        let start_idx = self.account_data.len();
        let account_rec = AccountRecord::new(&account, start_idx);

        // store account data
        for b in account.data {
            self.account_data
                .push(b)
                .wrap_err("failed to push account data")?;
        }

        // store account record
        // push a new endcap, replace the old one with the new record
        {
            self.account_records
                .push(AccountRecord::endcap(self.account_data.len()))?;
            let mut old_endcap = self
                .account_records
                .get_mut(self.account_records.len() - 2)?;
            let _ = std::mem::replace(&mut *old_endcap, account_rec);
        }

        /*if self
            .account_index
            .insert(account.id.clone(), self.account_records.len() - 1)
            .is_some()
        {
            self.sync()?;
            unreachable!("non-present account became present");
        }*/

        Ok(())
    }

    pub fn accounts(&self) -> AccountIter<'_, VERSION, BCS, TXCS, ARCS, ADCS> {
        AccountIter::new(self)
    }

    pub fn slot_limits(&self) -> eyre::Result<DbSlotLimits> {
        Ok(DbSlotLimits {
            middle_slot: self.middle_slot,
            left_slot: self.left.block_records.second_last()?.map(|r| r.slot),
            right_slot: self.right.block_records.second_last()?.map(|r| r.slot),
        })
    }

    pub fn checksum(&self) -> eyre::Result<u64> {
        let (b_tx, b_rx) = std::sync::mpsc::sync_channel(128);
        let (a_tx, a_rx) = std::sync::mpsc::sync_channel(128);
        let worker = std::thread::spawn(|| {
            const CRC: crc::Crc<u64, crc::Table<1>> =
                crc::Crc::<u64, crc::Table<1>>::new(&crc::CRC_64_GO_ISO);
            let mut hasher = CRC.digest();

            for block in b_rx {
                let block = block?;
                hasher.update(&checksum(&block).to_le_bytes());
            }

            for account in a_rx {
                let account = account?;
                hasher.update(&checksum(&account).to_le_bytes());
            }

            Ok(hasher.finalize())
        });

        for block in self.left_blocks().chain(self.right_blocks()) {
            b_tx.send(block).expect("checksum worker panicked?");
        }
        std::mem::drop(b_tx);

        for account_idx in 0..self.account_records.len().saturating_sub(1) {
            if self
                .account_records
                .get(account_idx)
                .map(|r| r.is_endcap())
                .unwrap_or(false)
            {
                continue;
            }
            a_tx.send(self.get_account_by_idx(account_idx))
                .expect("checksum worker panicked?");
        }
        std::mem::drop(a_tx);

        worker.join().expect("checksum worker panicked")
    }

    pub fn sync(&mut self) -> eyre::Result<()> {
        self.left.sync()?;
        self.right.sync()?;

        macro_rules! sync_table {
            ($name:ident) => {
                self.$name
                    .sync()
                    .wrap_err(concat!("could not sync table: ", stringify!($name)))?;
            };
        }

        sync_table!(account_data);
        sync_table!(account_records);
        //sync_table!(account_index);
        Ok(())
    }

    pub fn left_blocks(&self) -> std::iter::Rev<BlockIter<'_, BCS, TXCS>> {
        self.left.blocks().rev()
    }

    pub fn right_blocks(&self) -> BlockIter<'_, BCS, TXCS> {
        self.right.blocks()
    }

    pub fn blocks(
        &self,
    ) -> std::iter::Chain<std::iter::Rev<BlockIter<'_, BCS, TXCS>>, BlockIter<'_, BCS, TXCS>> {
        self.left_blocks().chain(self.right_blocks())
    }

    pub fn stats(&self) -> DbStats {
        let left_stats = self.left.stats();
        let mut right_stats = self.right.stats();

        let mut problems = left_stats.problems;
        problems.append(&mut right_stats.problems);

        let (guessed_shard_n, guessed_shard_i) =
            match (left_stats.shard_config, right_stats.shard_config) {
                (Some(l), Some(r)) if l == r => (Some(l.0), Some(l.1)),
                (Some(l), None) => (Some(l.0), Some(l.1)),
                (None, Some(r)) => (Some(r.0), Some(r.1)),
                (None, None) => (None, None),
                (Some(l), Some(r)) => {
                    problems.push(format!(
                    "left and right block dbs have different guessed shard configs: {l:?} != {r:?}"
                ));
                    (Some(l.0), Some(l.1))
                }
            };

        let ts_range = {
            let all_blocks = || {
                self.left
                    .block_records
                    .iter()
                    .rev()
                    .chain(self.right.block_records.iter())
            };
            let first_ts = all_blocks()
                .find_map(|b| b.ts)
                .map(|ts| chrono::DateTime::from_timestamp(ts, 0).expect("invalid ts"));
            let last_ts = all_blocks()
                .rev()
                .find_map(|b| b.ts)
                .map(|ts| chrono::DateTime::from_timestamp(ts, 0).expect("invalid ts"));

            first_ts.zip(last_ts)
        };

        DbStats {
            version: VERSION,
            bcs: BCS,
            txcs: TXCS,
            arcs: ARCS,
            adcs: ADCS,

            middle_height: self.middle_slot,
            guessed_shard_n,
            guessed_shard_i,

            accounts_count: self.account_records.len(),
            account_data_bytes: self.account_data.len(),

            ts_range,

            left_blocks_count: left_stats.n_blocks,
            left_txs_count: left_stats.n_txs,
            left_corrupted_block_recs: left_stats.n_rec_corrupted,
            left_corrupted_txs: left_stats.n_txs_corrupted,
            left_missing_blocks: left_stats.n_missing,

            right_blocks_count: right_stats.n_blocks,
            right_txs_count: right_stats.n_txs,
            right_corrupted_block_recs: right_stats.n_rec_corrupted,
            right_corrupted_txs: right_stats.n_txs_corrupted,
            right_missing_blocks: right_stats.n_missing,

            problems,
        }
    }

    pub fn split(
        self,
    ) -> (
        MonotonousBlockDb<BCS, TXCS>,
        u64,
        MonotonousBlockDb<BCS, TXCS>,
    ) {
        (self.left, self.middle_slot, self.right)
    }
}

#[derive(Debug)]
pub struct DbStats {
    pub version: u64,
    pub bcs: usize,
    pub txcs: usize,
    pub arcs: usize,
    pub adcs: usize,

    pub middle_height: u64,
    pub guessed_shard_n: Option<u64>,
    pub guessed_shard_i: Option<u64>,

    pub ts_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,

    pub accounts_count: u64,
    pub account_data_bytes: u64,

    pub left_blocks_count: u64,
    pub left_txs_count: u64,
    pub left_corrupted_block_recs: u64,
    pub left_corrupted_txs: u64,
    pub left_missing_blocks: u64,

    pub right_blocks_count: u64,
    pub right_txs_count: u64,
    pub right_corrupted_block_recs: u64,
    pub right_corrupted_txs: u64,
    pub right_missing_blocks: u64,

    pub problems: Vec<String>,
}

pub struct AccountIter<
    'db,
    const VERSION: u64,
    const BCS: usize,
    const TXCS: usize,
    const ARCS: usize,
    const ADCS: usize,
> {
    db: &'db DbGeneric<VERSION, BCS, TXCS, ARCS, ADCS>,
    idx: u64,
    idx_back: u64,
}
impl<
        'db,
        const VERSION: u64,
        const BCS: usize,
        const TXCS: usize,
        const ARCS: usize,
        const ADCS: usize,
    > AccountIter<'db, VERSION, BCS, TXCS, ARCS, ADCS>
{
    fn new(db: &'db DbGeneric<VERSION, BCS, TXCS, ARCS, ADCS>) -> Self {
        Self {
            db,
            idx: 0,
            idx_back: db.account_records.len().saturating_sub(1),
        }
    }
}

impl<
        'db,
        const VERSION: u64,
        const BCS: usize,
        const TXCS: usize,
        const ARCS: usize,
        const ADCS: usize,
    > Iterator for AccountIter<'db, VERSION, BCS, TXCS, ARCS, ADCS>
{
    type Item = eyre::Result<Account>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.db.account_records.len().saturating_sub(1) {
            None
        } else {
            while self.idx < self.db.account_records.len().saturating_sub(1)
                && self
                    .db
                    .account_records
                    .get(self.idx)
                    .map(|r| r.is_endcap())
                    .unwrap_or(false)
            {
                self.idx += 1;
            }

            let account = self.db.get_account_by_idx(self.idx);
            self.idx += 1;
            Some(account)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.db.account_records.len().saturating_sub(2) as usize;
        (len, Some(len))
    }
}

impl<
        'db,
        const VERSION: u64,
        const BCS: usize,
        const TXCS: usize,
        const ARCS: usize,
        const ADCS: usize,
    > DoubleEndedIterator for AccountIter<'db, VERSION, BCS, TXCS, ARCS, ADCS>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.idx_back == 0 {
            None
        } else {
            self.idx_back -= 1;

            while self.idx > 0
                && self
                    .db
                    .account_records
                    .get(self.idx)
                    .map(|r| r.is_endcap())
                    .unwrap_or(false)
            {
                self.idx -= 1;
            }
            if self
                .db
                .account_records
                .get(self.idx)
                .map(|r| r.is_endcap())
                .unwrap_or(false)
            {
                None
            } else {
                Some(self.db.get_account_by_idx(self.idx_back))
            }
        }
    }
}

impl<
        'db,
        const VERSION: u64,
        const BCS: usize,
        const TXCS: usize,
        const ARCS: usize,
        const ADCS: usize,
    > ExactSizeIterator for AccountIter<'db, VERSION, BCS, TXCS, ARCS, ADCS>
{
    fn len(&self) -> usize {
        self.db.account_records.len().saturating_sub(1) as usize
    }
}

impl<
        'db,
        const VERSION: u64,
        const BCS: usize,
        const TXCS: usize,
        const ARCS: usize,
        const ADCS: usize,
    > std::iter::FusedIterator for AccountIter<'db, VERSION, BCS, TXCS, ARCS, ADCS>
{
}
