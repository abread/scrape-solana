use std::{path::{Path, PathBuf}, sync::{Arc, Mutex}, time::Duration};
use eyre::{Context, Result};

use clap::Parser;
use mmap_vec::MmapVec;
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

const ENDPOINT: &'static str = "https://api.devnet.solana.com";
//const ENDPOINT: &'static str = "https://api.mainnet-beta.solana.com";

#[derive(clap::Parser)]
#[command(name = "scape-solana", version, about, long_about = None)]
struct App {
    /// Database file path
    #[arg(short, long, default_value = "solana_data_db")]
    db_root_path: PathBuf,
}

#[repr(C, packed)]
#[derive(Default)]
struct TxRecord {
    block_id: u64,
    fee: u64,
    compute_units: u64,
}

#[repr(C, packed)]
#[derive(Default)]
struct BlockRecord {
    block_id: u64,
    ts: i64,
    tx_count: u64,
}

struct Db {
    blocks: MmapVec<BlockRecord>,
    txs: MmapVec<TxRecord>,
}

fn main() -> Result<()> {
    let args = App::parse();
    let mut db = open_db(args.db_root_path).wrap_err("failed to open db")?;

    let client = RpcClient::new_with_commitment(ENDPOINT, CommitmentConfig::finalized());
    let next_block = if let Some(b) = db.blocks.last() {
        b.block_id - 1
    } else {
        client.get_block_height().wrap_err("failed to get latest blocknum")?
    };

    // cleanup semi-fetched txs
    let mut trunc_idx = db.txs.len();
    for i in (0..db.txs.len()).rev() {
        if db.txs[i].block_id == next_block {
            trunc_idx = i;
        } else {
            break
        }
    }
    if trunc_idx != db.txs.len() {
        db.txs.truncate(trunc_idx);
    }

    let db = Arc::new(Mutex::new(db));

    let db2 = Arc::clone(&db);
    ctrlc::set_handler(move || {
        db2.lock().unwrap().sync().unwrap();
        std::process::exit(0);
    }).wrap_err("could not set Ctrl+C handler")?;

    let db2 = Arc::clone(&db);
    std::thread::spawn(move || {
        db2.lock().unwrap().sync().unwrap();
        std::thread::sleep(Duration::from_secs(5))
    });

    std::thread::sleep(Duration::from_millis(500)); // ensure we respect rate limits
    for block_num in (0..next_block).rev() {
        let block = client.get_block(block_num).wrap_err("failed to fetch next block")?;

        {
            let mut d = db.lock().unwrap();
            let block_rec = BlockRecord {
                block_id: block_num,
                ts: block.block_time.unwrap_or(i64::MAX),
                tx_count: block.transactions.len() as u64,
            };

            for tx in block.transactions {
                let tx_rec = TxRecord {
                    block_id: block_num,
                    fee: tx.meta.as_ref().map(|m| m.fee).unwrap_or(u64::MAX),
                    compute_units: tx.meta.as_ref().map(|m| m.compute_units_consumed.clone().unwrap_or(u64::MAX)).unwrap_or(u64::MAX),
                };

                d.txs.push(tx_rec).wrap_err("could not save tx")?;
            }
            d.blocks.push(block_rec).wrap_err("could not save block")?;
        }

        println!("fetched block {block_num}");
        std::thread::sleep(Duration::from_millis(500));
    }

    Ok(())
}


impl Db {
    fn sync(&mut self) -> Result<()> {
        self.txs.sync().wrap_err("could not sync txs table")?;
        self.blocks.sync().wrap_err("could not sync blocks table")?;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.sync().unwrap();
    }
}

fn open_db(db_root_path: PathBuf) -> Result<Db> {
    std::fs::create_dir_all(&db_root_path).wrap_err("Failed to create DB dir")?;

    let blocks = unsafe { MmapVec::with_name(db_root_path.join("blocks")) }
        .wrap_err("Failed to open blocks table")?;

    let txs = unsafe { MmapVec::with_name(db_root_path.join("txs")) }
        .wrap_err("Failed to open txs table")?;

    Ok(Db {blocks, txs})
}
