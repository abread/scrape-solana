use crate::model::{AccountID, Block};
use crate::{
    crc_checksum_serde::checksum,
    huge_vec::{FsStore, ZstdTransformer},
    model::Account,
    select_random_elements,
};
use eyre::{eyre, Context};
use std::{
    io,
    path::{Path, PathBuf},
};

mod model;
use model::AccountRecord;

//mod huge_map;
//use huge_map::MapFsStore;

mod monotonous_block_db;
use monotonous_block_db::MonotonousBlockDb;

const MAX_AUTO_ACCOUNT_DATA_LOSS: u64 = 2 * 1024 * 1024; // 2MiB

pub(crate) type HugeVec<T, const CHUNK_SZ: usize> = crate::huge_vec::HugeVec<
    T,
    FsStore<crate::huge_vec::Chunk<T, CHUNK_SZ>, ZstdTransformer>,
    CHUNK_SZ,
>;
/*pub(crate) type HugeMap<K, V, const CHUNK_SZ: usize> =
huge_map::HugeMap<K, V, MapFsStore<ZstdTransformer>, CHUNK_SZ>;*/

pub(crate) const fn chunk_sz<T>(target_mem_usage_bytes: usize) -> usize {
    let target_chunk_sz_bytes =
        target_mem_usage_bytes / crate::huge_vec::CHUNK_CACHE_RECLAMATION_INTERVAL;

    target_chunk_sz_bytes / std::mem::size_of::<T>()
}
pub(crate) const MB: usize = 1024 * 1024;

pub struct Db {
    middle_slot: u64,

    // blocks up to middle_slot
    left: MonotonousBlockDb,

    // blocks ahead of middle_slot
    right: MonotonousBlockDb,

    account_records: HugeVec<AccountRecord, { chunk_sz::<AccountRecord>(128 * MB) }>,
    account_data: HugeVec<u8, { chunk_sz::<u8>(256 * MB) }>,
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

impl Db {
    pub fn open(
        root_path: PathBuf,
        default_middle_slot_getter: impl FnOnce() -> u64,
        out: impl io::Write,
    ) -> eyre::Result<Self> {
        // create if not exists
        match std::fs::read_dir(&root_path) {
            Ok(_) => Self::open_existing(root_path, out),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                Self::initialize(root_path, default_middle_slot_getter(), out)
            }
            Err(_) => Err(eyre::eyre!(
                "failed to open DB: {root_path:?} is not a directory"
            )),
        }
    }

    fn initialize(
        root_path: PathBuf,
        middle_slot: u64,
        mut out: impl io::Write,
    ) -> eyre::Result<Self> {
        // create root_path
        std::fs::create_dir_all(&root_path).wrap_err("failed to create root directory")?;

        // write version file
        let version_file = root_path.join("version");
        std::fs::write(version_file, "1").wrap_err("failed to write version file")?;

        // write middle_slot
        let middle_slot_file = root_path.join("middle_slot");
        std::fs::write(middle_slot_file, middle_slot.to_string())
            .wrap_err("failed to write middle_slot file")?;

        // create tables
        let mut db = Self::open_or_create_raw(&root_path)?;
        db.left.initialize()?;
        db.right.initialize()?;

        // insert end cap
        db.account_records.push(AccountRecord::endcap(0))?;

        if cfg!(debug) {
            let (issues, res) = db.heal(u64::MAX);
            assert!(issues.is_empty());
            res.unwrap();
        }
        db.sync()?;

        let _ = writeln!(out, "DB created at {:?}", root_path);
        Ok(db)
    }

    fn open_existing(root_path: PathBuf, mut out: impl io::Write) -> eyre::Result<Self> {
        let mut db = Self::open_or_create_raw(root_path)?;

        let _ = writeln!(out, "Auto-healing DB...");
        let (issues, res) = db.heal(128);
        for issue in issues {
            let _ = writeln!(out, " > {issue}");
        }
        res.wrap_err("Failed to auto-heal DB")?;
        let _ = writeln!(out, "DB healed");

        let _ = writeln!(
            out,
            "loaded {}+{} blocks, {}+{} txs, {} accounts and {}B of account data",
            db.left.block_records.len(),
            db.right.block_records.len(),
            db.left.txs.len(),
            db.right.txs.len(),
            db.account_records.len(),
            db.account_data.len()
        );

        Ok(db)
    }

    fn open_or_create_raw(root_path: impl AsRef<Path>) -> eyre::Result<Self> {
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
        let middle_slot =
            std::fs::read(root_path.join("middle_slot")).wrap_err("failed to read middle slot")?;
        let middle_slot =
            String::from_utf8(middle_slot).wrap_err("middle slot is not valid utf8")?;
        let middle_slot = middle_slot
            .parse()
            .wrap_err("failed to parse middle slot")?;

        let db = Db {
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
            .map(|b| !b.is_endcap() || b.data_start_idx > self.account_data.len())
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

            if n_bad_accounts >= MAX_AUTO_ACCOUNT_DATA_LOSS
                || n_bad_account_data_bytes >= MAX_AUTO_ACCOUNT_DATA_LOSS
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
        let elements_to_check = select_random_elements(&self.account_records, n_samples)
            .map(|(idx, _)| idx)
            .filter(|&idx| idx as u64 != endcap_idx)
            .collect::<Vec<_>>();

        for idx in elements_to_check {
            self.check_account(idx as u64, issues)?;
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
            return Err(eyre!("account {} already present in index", account.id));
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

    pub fn slot_limits(&self) -> eyre::Result<DbSlotLimits> {
        Ok(DbSlotLimits {
            middle_slot: self.middle_slot,
            left_slot: self.left.block_records.second_last()?.map(|r| r.slot),
            right_slot: self.right.block_records.second_last()?.map(|r| r.slot),
        })
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
}
